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

package packs_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yorkie-team/yorkie/api/converter"
	"github.com/yorkie-team/yorkie/api/types"
	"github.com/yorkie-team/yorkie/pkg/document"
	"github.com/yorkie-team/yorkie/pkg/document/change"
	"github.com/yorkie-team/yorkie/pkg/document/json"
	"github.com/yorkie-team/yorkie/pkg/document/presence"
	"github.com/yorkie-team/yorkie/pkg/document/time"
	"github.com/yorkie-team/yorkie/pkg/document/yson"
	"github.com/yorkie-team/yorkie/pkg/key"
	"github.com/yorkie-team/yorkie/server/backend"
	"github.com/yorkie-team/yorkie/server/backend/database"
	"github.com/yorkie-team/yorkie/server/packs"
	"github.com/yorkie-team/yorkie/server/profiling/prometheus"
	"github.com/yorkie-team/yorkie/test/helper"
)

// newMemoryBackend builds a backend on the in-memory database without
// starting membership or housekeeping, so the compaction orchestration can
// be driven directly.
func newMemoryBackend(t *testing.T) *backend.Backend {
	t.Helper()

	metrics, err := prometheus.NewMetrics()
	require.NoError(t, err)

	be, err := backend.New(
		helper.TestBackendConfig(),
		nil,
		helper.TestMembershipConfig(),
		helper.TestHousekeepingConfig(),
		metrics, nil, nil,
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = be.Shutdown() })
	return be
}

// compactionFixture activates a client, attaches a document and pushes five
// integer changes. It returns the project, client info, document key and the
// actor.
func compactionFixture(
	t *testing.T,
	ctx context.Context,
	be *backend.Backend,
) (*types.Project, *database.ClientInfo, key.Key, time.ActorID) {
	t.Helper()

	projectInfo, err := be.DB.FindProjectInfoByID(ctx, database.DefaultProjectID)
	require.NoError(t, err)

	clientInfo, err := be.DB.ActivateClient(ctx, projectInfo.ID, t.Name(), nil)
	require.NoError(t, err)

	docKey := helper.TestKey(t)
	docInfo, err := be.DB.FindOrCreateDocInfo(ctx, clientInfo.RefKey(), docKey, false)
	require.NoError(t, err)
	require.NoError(t, clientInfo.AttachDocument(docInfo.ID, false, docInfo.Epoch, 0, change.InitialCheckpoint))
	require.NoError(t, be.DB.UpdateClientInfoAfterPushPull(ctx, clientInfo, docInfo))

	bytesID, err := clientInfo.ID.Bytes()
	require.NoError(t, err)
	actorID, err := time.ActorIDFromBytes(bytesID)
	require.NoError(t, err)

	doc := document.New(docKey)
	doc.SetActor(actorID)
	for idx := range 5 {
		require.NoError(t, doc.Update(func(root *json.Object, _ *presence.Presence) error {
			root.SetInteger("k", idx)
			return nil
		}))
	}

	project := projectInfo.ToProject()
	serverPack, err := packs.PushPull(
		ctx, be, project, clientInfo, docInfo.RefKey(), doc.CreateChangePack(), packs.PushPullOptions{
			Status: document.StatusAttached,
		},
	)
	require.NoError(t, err)
	require.Empty(t, serverPack.SnapshotLen())

	return project, clientInfo, docKey, actorID
}

// detachClient marks the fixture client detached, mirroring the state
// housekeeping compaction requires.
func detachClient(
	t *testing.T,
	ctx context.Context,
	be *backend.Backend,
	clientInfo *database.ClientInfo,
	docInfo *database.DocInfo,
) {
	t.Helper()
	require.NoError(t, clientInfo.DetachDocument(docInfo.ID))
	require.NoError(t, be.DB.UpdateClientInfoAfterPushPull(ctx, clientInfo, docInfo))
}

func TestCompactPublishesSnapshotAndPrunesCoveredHistory(t *testing.T) {
	ctx := context.Background()
	be := newMemoryBackend(t)

	project, clientInfo, docKey, _ := compactionFixture(t, ctx, be)
	docInfo, err := be.DB.FindDocInfoByKey(ctx, project.ID, docKey)
	require.NoError(t, err)
	detachClient(t, ctx, be, clientInfo, docInfo)

	// 1. Compaction pins the current head, publishes at seq 5 and prunes 1..5.
	require.NoError(t, packs.Compact(ctx, be, project.ID, docInfo, false))

	reloaded, err := be.DB.FindDocInfoByRefKey(ctx, docInfo.RefKey())
	require.NoError(t, err)
	assert.Equal(t, int64(5), reloaded.ServerSeq, "server seq stays monotonic")
	assert.Equal(t, int64(0), reloaded.Epoch)
	assert.Equal(t, int64(5), reloaded.SnapshotServerSeq)

	active, err := be.DB.FindActiveCompactionInfo(ctx, docInfo.RefKey())
	require.NoError(t, err)
	assert.Nil(t, active)

	remaining, err := be.DB.FindChangeInfosBetweenServerSeqs(ctx, docInfo.RefKey(), 1, 5)
	require.NoError(t, err)
	assert.Empty(t, remaining)

	// 2. The covered state rebuilds from the published snapshot alone.
	built, err := packs.BuildInternalDocForServerSeq(ctx, be, reloaded, 5)
	require.NoError(t, err)
	builtRoot, err := yson.FromCRDT(built.RootObject())
	require.NoError(t, err)
	builtMarshal, err := builtRoot.(yson.Object).Marshal()
	require.NoError(t, err)
	assert.Equal(t, `{"k":Int(4)}`, builtMarshal)
}

func TestCompactResumesInterruptedGeneration(t *testing.T) {
	ctx := context.Background()
	be := newMemoryBackend(t)

	project, clientInfo, docKey, _ := compactionFixture(t, ctx, be)
	docInfo, err := be.DB.FindDocInfoByKey(ctx, project.ID, docKey)
	require.NoError(t, err)
	detachClient(t, ctx, be, clientInfo, docInfo)

	// A generation claimed by a process that died before saving its
	// candidate: recovery rebuilds, publishes and prunes it.
	anchor, err := be.DB.FindLatestChangeInfo(ctx, docInfo.RefKey(), docInfo.ServerSeq)
	require.NoError(t, err)
	require.NotNil(t, anchor)
	gen := &database.CompactionInfo{
		ID:            types.NewID(),
		ProjectID:     project.ID,
		DocID:         docInfo.ID,
		ServerSeq:     docInfo.ServerSeq,
		Lamport:       anchor.Lamport,
		VersionVector: anchor.VersionVector.DeepCopy(),
	}
	require.NoError(t, be.DB.CreateCompactionInfo(ctx, gen))

	active, err := be.DB.FindActiveCompactionInfo(ctx, docInfo.RefKey())
	require.NoError(t, err)
	require.NotNil(t, active)

	recovered, err := packs.RecoverActiveCompactions(ctx, be)
	require.NoError(t, err)
	assert.Equal(t, 1, recovered)

	active, err = be.DB.FindActiveCompactionInfo(ctx, docInfo.RefKey())
	require.NoError(t, err)
	assert.Nil(t, active)

	reloaded, err := be.DB.FindDocInfoByRefKey(ctx, docInfo.RefKey())
	require.NoError(t, err)
	assert.Equal(t, int64(5), reloaded.SnapshotServerSeq)
	remaining, err := be.DB.FindChangeInfosBetweenServerSeqs(ctx, docInfo.RefKey(), 1, 5)
	require.NoError(t, err)
	assert.Empty(t, remaining)

	// Recovery is a no-op once every generation is finished.
	recovered, err = packs.RecoverActiveCompactions(ctx, be)
	require.NoError(t, err)
	assert.Equal(t, 0, recovered)
}

func TestCompactResumesGenerationWithSavedCandidate(t *testing.T) {
	ctx := context.Background()
	be := newMemoryBackend(t)

	project, clientInfo, docKey, _ := compactionFixture(t, ctx, be)
	docInfo, err := be.DB.FindDocInfoByKey(ctx, project.ID, docKey)
	require.NoError(t, err)
	detachClient(t, ctx, be, clientInfo, docInfo)

	// Simulate a process that built and stored the candidate snapshot and
	// died before publishing: recovery must not rebuild; it publishes the
	// stored candidate, advances the floor and prunes.
	anchor, err := be.DB.FindLatestChangeInfo(ctx, docInfo.RefKey(), docInfo.ServerSeq)
	require.NoError(t, err)
	require.NotNil(t, anchor)
	gen := &database.CompactionInfo{
		ID:            types.NewID(),
		ProjectID:     project.ID,
		DocID:         docInfo.ID,
		ServerSeq:     docInfo.ServerSeq,
		Lamport:       anchor.Lamport,
		VersionVector: anchor.VersionVector.DeepCopy(),
	}
	require.NoError(t, be.DB.CreateCompactionInfo(ctx, gen))

	candidate, err := packs.BuildInternalDocForServerSeq(ctx, be, docInfo, docInfo.ServerSeq)
	require.NoError(t, err)
	require.NoError(t, be.DB.SaveCompactionSnapshot(ctx, gen, candidate))

	has, err := be.DB.HasCompactionSnapshot(ctx, gen)
	require.NoError(t, err)
	assert.True(t, has)

	recovered, err := packs.RecoverActiveCompactions(ctx, be)
	require.NoError(t, err)
	assert.Equal(t, 1, recovered)

	reloaded, err := be.DB.FindDocInfoByRefKey(ctx, docInfo.RefKey())
	require.NoError(t, err)
	assert.Equal(t, int64(5), reloaded.SnapshotServerSeq)
	floor, err := be.DB.FindClosestCompactedSnapshotInfo(ctx, docInfo.RefKey(), 5)
	require.NoError(t, err)
	assert.Equal(t, int64(5), floor.ServerSeq)
	remaining, err := be.DB.FindChangeInfosBetweenServerSeqs(ctx, docInfo.RefKey(), 1, 5)
	require.NoError(t, err)
	assert.Empty(t, remaining)
}

func TestPushPullBelowFloorReceivesSnapshot(t *testing.T) {
	ctx := context.Background()
	be := newMemoryBackend(t)

	project, clientInfo, docKey, _ := compactionFixture(t, ctx, be)
	docInfo, err := be.DB.FindDocInfoByKey(ctx, project.ID, docKey)
	require.NoError(t, err)
	detachClient(t, ctx, be, clientInfo, docInfo)

	require.NoError(t, packs.Compact(ctx, be, project.ID, docInfo, false))

	// A brand-new client at checkpoint 0 is below the floor, so instead of a
	// change range with a hole it receives the published snapshot and
	// converges.
	fresh, err := be.DB.ActivateClient(ctx, project.ID, "fresh-client", nil)
	require.NoError(t, err)
	require.NoError(t, fresh.AttachDocument(docInfo.ID, false, docInfo.Epoch, 0, change.InitialCheckpoint))
	require.NoError(t, be.DB.UpdateClientInfoAfterPushPull(ctx, fresh, docInfo))

	emptyPack := change.NewPack(docKey, change.InitialCheckpoint, nil, nil, nil)
	snapshotPack, err := packs.PushPull(
		ctx, be, project, fresh, docInfo.RefKey(), emptyPack, packs.PushPullOptions{
			Status: document.StatusAttached,
		},
	)
	require.NoError(t, err)
	require.NotEmpty(t, snapshotPack.SnapshotLen())
	obj, _, err := converter.BytesToSnapshot(snapshotPack.Snapshot)
	require.NoError(t, err)
	root, err := yson.FromCRDT(obj)
	require.NoError(t, err)
	rootMarshal, err := root.(yson.Object).Marshal()
	require.NoError(t, err)
	assert.Equal(t, `{"k":Int(4)}`, rootMarshal)
}

// TestCompactionIsIdempotentAcrossTriggers drives the compaction twice, as a
// duplicate housekeeping/admin trigger on the same node would.
func TestCompactionIsIdempotentAcrossTriggers(t *testing.T) {
	ctx := context.Background()
	be := newMemoryBackend(t)

	project, clientInfo, docKey, _ := compactionFixture(t, ctx, be)
	docInfo, err := be.DB.FindDocInfoByKey(ctx, project.ID, docKey)
	require.NoError(t, err)
	detachClient(t, ctx, be, clientInfo, docInfo)

	require.NoError(t, packs.Compact(ctx, be, project.ID, docInfo, true))
	// A second trigger finds nothing left to do and does not change the
	// floor or the stored state.
	require.NoError(t, packs.Compact(ctx, be, project.ID, docInfo, true))

	reloaded, err := be.DB.FindDocInfoByRefKey(ctx, docInfo.RefKey())
	require.NoError(t, err)
	assert.Equal(t, int64(5), reloaded.ServerSeq)
	assert.Equal(t, int64(5), reloaded.SnapshotServerSeq)

	active, err := be.DB.FindActiveCompactionInfo(ctx, docInfo.RefKey())
	require.NoError(t, err)
	assert.Nil(t, active)
}
