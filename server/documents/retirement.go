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

package documents

import (
	"context"
	stderrors "errors"
	"fmt"
	gotime "time"

	"github.com/yorkie-team/yorkie/api/types"
	"github.com/yorkie-team/yorkie/server/backend"
	"github.com/yorkie-team/yorkie/server/backend/database"
	"github.com/yorkie-team/yorkie/server/logging"
	"github.com/yorkie-team/yorkie/server/packs"
)

const (
	retirementFinalizeKey = "housekeeping/retirement"
)

// RetireDocument fixes one removal of the document identified by refKey as a
// single queryable retirement generation and converges attachments and
// Watches onto it.
//
// The generation is claimed by setting removed_at exactly once. force=false
// keeps the ordinary wait policy: the call fails while any other client is
// attached or attaching, so callers retry after the clients detach.
// force=true is the administrator policy: it does not wait and converges
// every attachment onto the generation immediately.
//
// Everything after the claim is idempotent and keyed by the document's
// DocID, which is the generation identity: a retry after a lost response, a
// node restart, or a long-offline caller continues the same generation
// instead of starting a new one. The generation stays queryable until the
// deferred purge runs (see FinalizeRetiredDocuments), so its last server
// sequence, snapshot and version vector remain available for offline clients
// to recognize the deletion.
func RetireDocument(
	ctx context.Context,
	be *backend.Backend,
	refKey types.DocRefKey,
	force bool,
) (*database.DocInfo, error) {
	// FindDocInfoByRefKey reads by ID and therefore also matches an already
	// removed, not yet purged generation.
	docInfo, err := be.DB.FindDocInfoByRefKey(ctx, refKey)
	if err != nil {
		return nil, err
	}

	// 01. Claim the generation. A repeated call on an already removed
	// document falls through to convergence with the original removed_at.
	if !docInfo.IsRemoved() {
		if !force {
			isAttached, err := be.DB.IsDocumentAttachedOrAttaching(ctx, refKey, "")
			if err != nil {
				return nil, err
			}
			if isAttached {
				return nil, fmt.Errorf("remove document %s: %w", refKey, packs.ErrDocumentAttached)
			}
		}

		if err := be.DB.UpdateDocInfoStatusToRemoved(ctx, refKey); err != nil {
			return nil, err
		}

		docInfo, err = be.DB.FindDocInfoByRefKey(ctx, refKey)
		if err != nil {
			return nil, err
		}
	}

	// 02. Converge attachments and Watches onto the generation. The claim is
	// already durable, so a convergence failure must not read as a failed
	// removal: the generation stays queryably retired and any retry resumes
	// here. Failures are logged and housekeeping re-runs convergence before
	// purging.
	if err := ConvergeRetiredDocument(ctx, be, docInfo); err != nil {
		logging.From(ctx).Warnf("converge retired document %s: %v", refKey, err)
	}

	return docInfo, nil
}

// ConvergeRetiredDocument drives the Watch side of an already claimed
// retirement generation: a final DocChanged is delivered to every Watch and
// the streams are closed on every node, so a missed broadcast or a
// disconnected Watch cannot keep serving the old copy, and reconnects resolve
// against the retired generation.
//
// Client rows are not flipped here. Attachments converge themselves on their
// next PushPull, Detach or Remove against the frozen generation, which always
// carries the removed flag; attachments that never come back are force
// converged by FinalizeRetiredDocuments right before the purge. Keeping the
// rows live lets a concurrent detach or remove from another client succeed
// instead of racing a server-side row transition.
//
// It is safe to call repeatedly. Watch fan-out is best-effort and logged,
// matching the rest of the invalidation paths.
func ConvergeRetiredDocument(
	ctx context.Context,
	be *backend.Backend,
	docInfo *database.DocInfo,
) error {
	refKey := docInfo.RefKey()

	// Close the local Watches (delivering a final DocChanged first), so this
	// node converges even when the cluster fan-out fails or is unavailable
	// (single-server or embedded mode).
	be.PubSub.CloseDocument(ctx, refKey)

	// Then ask every peer node to do the same and drop its caches. The
	// broadcast is keyed by DocID, so it can never reach a same-key document
	// a later generation creates.
	if err := be.BroadcastDocumentRetirement(ctx, refKey); err != nil {
		logging.From(ctx).Warnf("broadcast retirement of %s: %v", refKey, err)
	}

	return nil
}

// FinalizeRetiredDocuments purges generations whose recovery window has
// elapsed. It is the housekeeping entry point: it lists retired documents
// claimed at or before retiredBefore and purges each one after re-running
// convergence. The deferred ordering is what leaves the last server
// sequence, snapshot and version vector in place long enough for offline
// clients to recognize the deletion.
//
// The cursor is the document ID. When the list is exhausted the returned
// cursor is ZeroID, with which the caller restarts the next term.
func FinalizeRetiredDocuments(
	ctx context.Context,
	be *backend.Backend,
	candidatesLimit int,
	retiredBefore gotime.Time,
	lastDocID types.ID,
) (types.ID, int, int, error) {
	locker, ok := be.Lockers.LockerWithTryLock(retirementFinalizeKey)
	if !ok {
		return lastDocID, 0, 0, nil
	}
	defer locker.Unlock()

	candidates, nextID, err := be.DB.FindPurgeCandidates(ctx, candidatesLimit, retiredBefore, lastDocID)
	if err != nil {
		return lastDocID, 0, 0, err
	}

	purged := 0
	for _, docInfo := range candidates {
		if err := finalizeRetiredDocument(ctx, be, docInfo, retiredBefore); err != nil {
			logging.From(ctx).Warnf("finalize retired document %s: %v", docInfo.RefKey(), err)
			continue
		}
		purged++
	}

	return nextID, len(candidates), purged, nil
}

// finalizeRetiredDocument purges a single retired generation. All actions are
// fenced by the generation's DocID and removed_at: the document is reloaded by
// ID, must still be the same removed generation, and must still be older than
// the cutoff. A document created later with the same key has a different DocID
// and is therefore unreachable from here even under retries.
func finalizeRetiredDocument(
	ctx context.Context,
	be *backend.Backend,
	docInfo *database.DocInfo,
	retiredBefore gotime.Time,
) error {
	refKey := docInfo.RefKey()

	locker := be.Lockers.Locker(packs.DocKey(docInfo.ProjectID, docInfo.Key))
	defer locker.Unlock()

	current, err := be.DB.FindDocInfoByRefKey(ctx, refKey)
	if stderrors.Is(err, database.ErrDocumentNotFound) {
		// A previous cycle (or a manual purge) already finished this
		// generation: the retry continues with the same result.
		return nil
	}
	if err != nil {
		return err
	}

	// Generation fence: only purge the exact generation the candidate
	// described. removed_at can never move backwards, and a same-key new
	// document carries a different DocID.
	if !current.IsRemoved() || current.RemovedAt != docInfo.RemovedAt {
		return nil
	}
	if current.RemovedAt.After(retiredBefore) {
		return nil
	}

	// Close Watches, including any that missed the removal-time broadcast.
	if err := ConvergeRetiredDocument(ctx, be, current); err != nil {
		return err
	}

	// Force-converge attachments that never contacted the server during the
	// recovery window. New attaches have been refused since the generation was
	// claimed, so after this the attachment gates must read empty.
	if _, err := be.DB.MarkDocumentClientsRemoved(ctx, refKey); err != nil {
		return err
	}
	attached, err := be.DB.IsDocumentAttachedOrAttaching(ctx, refKey, "")
	if err != nil {
		return err
	}
	if attached {
		// Defensive: the write converged the rows but a gate still reports an
		// attachment (e.g. a concurrent attach slipping past on another
		// node). Wait for a later cycle rather than purging history an
		// attachment can read.
		logging.From(ctx).Warnf("defer purge of %s: attachment still reported", refKey)
		return nil
	}

	// Finally release the generation: changes, snapshots, snapshot bodies and
	// version vectors, then the document row.
	counts, err := be.DB.PurgeDocument(ctx, refKey)
	if err != nil {
		return err
	}

	logging.From(ctx).Infow(
		fmt.Sprintf("purged retired document [project_id=%s doc_id=%s]", refKey.ProjectID, refKey.DocID),
		"changes", counts["changes"],
		"snapshots", counts["snapshots"],
		"versionvectors", counts["versionvectors"],
	)

	return nil
}
