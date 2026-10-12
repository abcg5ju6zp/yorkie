/*
 * Copyright 2026 The Yorkie Authors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package documents_test

import (
	"context"
	stderrors "errors"
	"log"
	"os"
	"testing"
	gotime "time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yorkie-team/yorkie/api/types"
	"github.com/yorkie-team/yorkie/pkg/document"
	"github.com/yorkie-team/yorkie/pkg/document/change"
	"github.com/yorkie-team/yorkie/pkg/document/json"
	"github.com/yorkie-team/yorkie/pkg/document/presence"
	"github.com/yorkie-team/yorkie/pkg/key"
	"github.com/yorkie-team/yorkie/server/backend"
	"github.com/yorkie-team/yorkie/server/backend/database"
	"github.com/yorkie-team/yorkie/server/clients"
	"github.com/yorkie-team/yorkie/server/documents"
	"github.com/yorkie-team/yorkie/server/packs"
	"github.com/yorkie-team/yorkie/server/profiling/prometheus"
	"github.com/yorkie-team/yorkie/test/helper"
)

var testBackend *backend.Backend

func TestMain(m *testing.M) {
	met, err := prometheus.NewMetrics()
	if err != nil {
		log.Fatal(err)
	}

	// nil Mongo config selects the in-memory database, so these tests run
	// without any external service.
	testBackend, err = backend.New(
		helper.TestBackendConfig(),
		nil,
		helper.TestMembershipConfig(),
		helper.TestHousekeepingConfig(),
		met, nil, nil,
	)
	if err != nil {
		log.Fatal(err)
	}

	code := m.Run()

	// Shutdown removes this process from the cluster-nodes table; with the
	// in-memory backend and no started membership that row never existed, so
	// its "document not found" cleanup error is expected and must not fail
	// the package after every test passed.
	if err := testBackend.Shutdown(); err != nil {
		log.Printf("backend shutdown: %v", err)
	}
	os.Exit(code)
}

type retirementFixture struct {
	t       *testing.T
	ctx     context.Context
	project *types.Project
	be      *backend.Backend
}

func newFixture(t *testing.T) *retirementFixture {
	t.Helper()
	ctx := context.Background()

	// Each test gets its own project so global retirement finalization
	// (which scans every project) cannot perturb its counts.
	bootstrapKey := helper.TestKey(t).String() + "-bootstrap"
	bootstrap, err := testBackend.DB.ActivateClient(ctx, database.DefaultProjectID, bootstrapKey, nil)
	require.NoError(t, err)
	projInfo, err := testBackend.DB.CreateProjectInfo(ctx, t.Name(), bootstrap.ID)
	require.NoError(t, err)
	project := projInfo.ToProject()
	// Force the change-pull path regardless of global defaults.
	project.SnapshotThreshold = 1 << 20

	return &retirementFixture{
		t:       t,
		ctx:     ctx,
		project: project,
		be:      testBackend,
	}
}

func (f *retirementFixture) activate(name key.Key) *database.ClientInfo {
	f.t.Helper()
	clientInfo, err := f.be.DB.ActivateClient(f.ctx, f.project.ID, name.String(), nil)
	require.NoError(f.t, err)
	return clientInfo
}

func (f *retirementFixture) createDoc(clientInfo *database.ClientInfo, docKey key.Key) *database.DocInfo {
	f.t.Helper()
	docInfo, err := f.be.DB.FindOrCreateDocInfo(f.ctx, clientInfo.RefKey(), docKey, false)
	require.NoError(f.t, err)
	return docInfo
}

// attach runs the same client-row transition an AttachDocument RPC does.
func (f *retirementFixture) attach(clientInfo *database.ClientInfo, docInfo *database.DocInfo) *database.ClientInfo {
	f.t.Helper()
	attached, err := clients.AttachDocument(
		f.ctx, f.be, clientInfo, docInfo, false, 0, change.InitialCheckpoint,
	)
	require.NoError(f.t, err)
	require.NoError(f.t, f.be.DB.UpdateClientInfoAfterPushPull(f.ctx, attached, docInfo))
	return attached
}

func (f *retirementFixture) reload(clientInfo *database.ClientInfo) *database.ClientInfo {
	f.t.Helper()
	info, err := clients.FindActiveClientInfo(f.ctx, f.be, clientInfo.RefKey())
	require.NoError(f.t, err)
	return info
}

// editPack builds a PushPull pack carrying one local string-set change.
func (f *retirementFixture) editPack(
	clientInfo *database.ClientInfo,
	docKey key.Key,
	field, value string,
) *change.Pack {
	f.t.Helper()
	actorID, err := clientInfo.ID.ToActorID()
	require.NoError(f.t, err)

	doc := document.New(docKey)
	doc.SetActor(actorID)
	require.NoError(f.t, doc.Update(func(r *json.Object, _ *presence.Presence) error {
		r.SetString(field, value)
		return nil
	}))
	return doc.CreateChangePack()
}

func (f *retirementFixture) pushPull(
	clientInfo *database.ClientInfo,
	docInfo *database.DocInfo,
	pack *change.Pack,
	status document.StatusType,
) *packs.ServerPack {
	f.t.Helper()
	res, err := packs.PushPull(f.ctx, f.be, f.project, clientInfo, docInfo.RefKey(), pack, packs.PushPullOptions{
		Mode:   types.SyncModePushPull,
		Status: status,
	})
	require.NoError(f.t, err)
	return res
}

// TestRetirementGenerationFlow covers the lifecycle of one removal fixed as a
// queryable retirement generation: claim, wait policy, force convergence,
// idempotent retry, blocked re-attach and Watches, retained tombstone, and the
// deferred purge freeing the key for a new generation only at the end.
func TestRetirementGenerationFlow(t *testing.T) {
	f := newFixture(t)
	docKey := helper.TestKey(t)

	owner := f.activate(helper.TestKey(t, 1))
	peer := f.activate(helper.TestKey(t, 2))
	doc := f.createDoc(owner, docKey)
	owner = f.attach(owner, doc)
	peer = f.attach(peer, doc)

	// Give the generation a non-empty history the recovery window must keep.
	f.pushPull(owner, doc, f.editPack(owner, docKey, "k", "v1"), document.StatusAttached)
	doc, err := f.be.DB.FindDocInfoByRefKey(f.ctx, doc.RefKey())
	require.NoError(t, err)
	require.Equal(t, int64(1), doc.ServerSeq)

	// Subscribe a Watch so Watch convergence can be observed.
	ownerActor, err := owner.ID.ToActorID()
	require.NoError(t, err)
	sub, clientIDs, err := f.be.PubSub.Subscribe(f.ctx, ownerActor, doc.RefKey(), 0)
	require.NoError(t, err)
	require.Len(t, clientIDs, 1)

	// 01. Normal removal keeps its wait policy while a peer is attached.
	_, err = documents.RetireDocument(f.ctx, f.be, doc.RefKey(), false)
	assert.ErrorIs(t, err, packs.ErrDocumentAttached)
	alive, err := f.be.DB.FindDocInfoByRefKey(f.ctx, doc.RefKey())
	require.NoError(t, err)
	assert.False(t, alive.IsRemoved(), "a refused removal must not claim the generation")

	// 02. Force removal claims the generation and closes its Watch. Client
	// rows converge lazily (on their next contact or at finalize) so a
	// concurrent detach or remove from another client is not raced.
	retired, err := documents.RetireDocument(f.ctx, f.be, doc.RefKey(), true)
	require.NoError(t, err)
	assert.True(t, retired.IsRemoved())
	removedAt := retired.RemovedAt

	peerAttached, err := f.be.DB.IsDocumentAttachedOrAttaching(f.ctx, doc.RefKey(), "")
	require.NoError(t, err)
	assert.True(t, peerAttached, "attachments are left to converge on contact or at finalize")
	assert.True(t, sub.IsDead(), "retirement closes the generation's Watch")

	// 03. The generation stays queryable and a retry continues the same
	// generation instead of re-stamping it.
	found, err := documents.FindRetiredDocInfoByKey(f.ctx, f.be, f.project.ID, docKey)
	require.NoError(t, err)
	assert.Equal(t, doc.ID, found.ID)
	assert.Equal(t, removedAt, found.RemovedAt)

	for _, force := range []bool{false, true} {
		retry, err := documents.RetireDocument(f.ctx, f.be, doc.RefKey(), force)
		require.NoError(t, err)
		assert.Equal(t, removedAt, retry.RemovedAt, "retry must keep the original generation")
	}

	// 04. New attaches and writes by key are refused while the generation is
	// retained; a key lookup hides it but an ID lookup still answers.
	_, err = f.be.DB.FindOrCreateDocInfo(f.ctx, owner.RefKey(), docKey, false)
	assert.ErrorIs(t, err, database.ErrDocumentRetired)

	_, err = documents.FindDocInfoByKey(f.ctx, f.be, f.project, docKey)
	assert.ErrorIs(t, err, database.ErrDocumentNotFound)
	byID, err := f.be.DB.FindDocInfoByRefKey(f.ctx, doc.RefKey())
	require.NoError(t, err)
	assert.True(t, byID.IsRemoved())

	// 05. The tombstone is retained within the recovery window: the last
	// changes remain readable and no purge candidate is offered early.
	changes, err := f.be.DB.FindChangesBetweenServerSeqs(f.ctx, doc.RefKey(), 1, byID.ServerSeq)
	require.NoError(t, err)
	assert.NotEmpty(t, changes, "last server sequence is retained for offline clients")

	candidates, _, err := f.be.DB.FindPurgeCandidates(f.ctx, 100, gotime.Unix(0, 0), database.ZeroID)
	require.NoError(t, err)
	for _, c := range candidates {
		assert.NotEqual(t, doc.ID, c.ID, "a recent retirement is not purgeable yet")
	}

	// 06. After the window, finalize purges the generation and frees the key.
	_, n, purged, err := documents.FinalizeRetiredDocuments(
		f.ctx, f.be, 100, gotime.Now(), database.ZeroID,
	)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, n, 1)
	assert.GreaterOrEqual(t, purged, 1)

	_, err = f.be.DB.FindDocInfoByRefKey(f.ctx, doc.RefKey())
	assert.ErrorIs(t, err, database.ErrDocumentNotFound)
	changes, err = f.be.DB.FindChangesBetweenServerSeqs(f.ctx, doc.RefKey(), 1, byID.ServerSeq)
	require.NoError(t, err)
	assert.Empty(t, changes, "history is released when the generation is purged")

	// Finalize also force-converged the attachments that never synced, so no
	// client row keeps an attachment to a purged generation.
	for _, info := range []*database.ClientInfo{f.reload(owner), f.reload(peer)} {
		assert.Equal(t, database.DocumentRemoved, info.Documents[doc.ID].Status)
	}

	// 07. The name is reusable only now, and produces a distinct generation.
	sameName := f.createDoc(owner, docKey)
	assert.NotEqual(t, doc.ID, sameName.ID)
	assert.False(t, sameName.IsRemoved())
}

// TestLatePushPullOnRetiredGeneration verifies that a client reaching a
// retired generation with a late PushPull converges to removed without
// extending it: its changes are dropped, server_seq is frozen, the response
// carries the removed flag, and a repeat is idempotent.
func TestLatePushPullOnRetiredGeneration(t *testing.T) {
	f := newFixture(t)
	docKey := helper.TestKey(t)

	owner := f.activate(helper.TestKey(t, 1))
	peer := f.activate(helper.TestKey(t, 2))
	doc := f.createDoc(owner, docKey)
	owner = f.attach(owner, doc)

	// The owner pushes one real change so the frozen server_seq is non-zero.
	firstPack := f.editPack(owner, docKey, "k", "v1")
	res := f.pushPull(owner, doc, firstPack, document.StatusAttached)
	assert.False(t, res.IsRemoved)
	frozen, err := f.be.DB.FindDocInfoByRefKey(f.ctx, doc.RefKey())
	require.NoError(t, err)
	require.Equal(t, int64(1), frozen.ServerSeq)

	// The peer attaches and syncs to the frozen sequence.
	peer = f.attach(peer, doc)
	peer = f.reload(peer)
	syncPack := change.NewPack(docKey, peer.Checkpoint(doc.ID), nil, nil, nil)
	res = f.pushPull(peer, doc, syncPack, document.StatusAttached)
	assert.False(t, res.IsRemoved)
	peer = f.reload(peer)
	require.Equal(t, int64(1), peer.Checkpoint(doc.ID).ServerSeq)

	// A different client (force) retires the document while the peer is
	// offline. Rows are not flipped yet: the peer stays attached and
	// converges itself when it next contacts the server.
	_, err = documents.RetireDocument(f.ctx, f.be, doc.RefKey(), true)
	require.NoError(t, err)
	peer = f.reload(peer)
	assert.Equal(t, database.DocumentAttached, peer.Documents[doc.ID].Status)

	// The peer comes back with a local edit it made while offline.
	latePack := f.editPack(peer, docKey, "k", "late")
	res = f.pushPull(peer, doc, latePack, document.StatusAttached)
	assert.True(t, res.IsRemoved, "the late client receives the removed result")

	stored, err := f.be.DB.FindDocInfoByRefKey(f.ctx, doc.RefKey())
	require.NoError(t, err)
	assert.Equal(t, int64(1), stored.ServerSeq, "late changes must not extend the retired generation")
	lateChanges, err := f.be.DB.FindChangeInfosBetweenServerSeqs(f.ctx, doc.RefKey(), 2, 2)
	require.NoError(t, err)
	assert.Empty(t, lateChanges)

	peer = f.reload(peer)
	assert.Equal(t, database.DocumentRemoved, peer.Documents[doc.ID].Status)

	// A repeat late sync (and a late detach) keeps converging: still removed,
	// still frozen, no error.
	res = f.pushPull(peer, doc, change.NewPack(docKey, peer.Checkpoint(doc.ID), nil, nil, nil), document.StatusAttached)
	assert.True(t, res.IsRemoved)
	res = f.pushPull(peer, doc, change.NewPack(docKey, peer.Checkpoint(doc.ID), nil, nil, nil), document.StatusDetached)
	assert.True(t, res.IsRemoved)

	stored, err = f.be.DB.FindDocInfoByRefKey(f.ctx, doc.RefKey())
	require.NoError(t, err)
	assert.Equal(t, int64(1), stored.ServerSeq)
	assert.True(t, stored.IsRemoved())
}

// TestRetirementCompatibility covers the cases that must stay compatible:
// removing a non-existent, an unattached, and an already-removed document.
func TestRetirementCompatibility(t *testing.T) {
	f := newFixture(t)
	docKey := helper.TestKey(t)
	owner := f.activate(helper.TestKey(t))
	doc := f.createDoc(owner, docKey)

	// The document was created but never attached: normal removal works.
	retired, err := documents.RetireDocument(f.ctx, f.be, doc.RefKey(), false)
	require.NoError(t, err)
	assert.True(t, retired.IsRemoved())

	// Already removed: normal and force both continue the same generation.
	again, err := documents.RetireDocument(f.ctx, f.be, doc.RefKey(), false)
	require.NoError(t, err)
	assert.Equal(t, retired.RemovedAt, again.RemovedAt)
	again, err = documents.RetireDocument(f.ctx, f.be, doc.RefKey(), true)
	require.NoError(t, err)
	assert.Equal(t, retired.RemovedAt, again.RemovedAt)

	// RemoveDocument keeps its old signature.
	err = documents.RemoveDocument(f.ctx, f.be, doc.RefKey(), false)
	assert.NoError(t, err)

	// Non-existent document: a generation cannot be claimed.
	missing := types.DocRefKey{ProjectID: f.project.ID, DocID: database.ZeroID}
	_, err = documents.RetireDocument(f.ctx, f.be, missing, true)
	assert.ErrorIs(t, err, database.ErrDocumentNotFound)

	// Admin-style lookup by key finds the retired generation for idempotent
	// retries, while a live lookup hides it.
	_, err = documents.FindDocInfoByKey(f.ctx, f.be, f.project, docKey)
	assert.True(t, stderrors.Is(err, database.ErrDocumentNotFound))
	r2, err := documents.FindRetiredDocInfoByKey(f.ctx, f.be, f.project.ID, docKey)
	require.NoError(t, err)
	assert.Equal(t, doc.ID, r2.ID)

	// Converging an already-retired, already-drained generation is a safe
	// no-op even after every client row converged.
	require.NoError(t, documents.ConvergeRetiredDocument(f.ctx, f.be, r2))
}

// TestRetiredGenerationSkipsCompaction verifies housekeeping never compacts
// a retired generation (which would wipe the tombstone history).
func TestRetiredGenerationSkipsCompaction(t *testing.T) {
	f := newFixture(t)
	docKey := helper.TestKey(t)
	owner := f.activate(helper.TestKey(t))
	doc := f.createDoc(owner, docKey)
	owner = f.attach(owner, doc)
	f.pushPull(owner, doc, f.editPack(owner, docKey, "k", "v1"), document.StatusAttached)

	_, err := documents.RetireDocument(f.ctx, f.be, doc.RefKey(), true)
	require.NoError(t, err)
	retired, err := f.be.DB.FindDocInfoByRefKey(f.ctx, doc.RefKey())
	require.NoError(t, err)

	err = f.be.DB.CompactChangeInfos(f.ctx, retired, retired.ServerSeq, nil)
	assert.ErrorIs(t, err, database.ErrDocumentRetired)

	// Compaction candidate queries must not select the retired generation.
	candidates, _, _, err := f.be.DB.FindCompactionCandidates(f.ctx, 100, 1, 0, database.ZeroID)
	require.NoError(t, err)
	for _, c := range candidates {
		assert.NotEqual(t, doc.ID, c.ID)
	}
}
