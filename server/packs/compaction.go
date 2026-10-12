/*
 * Copyright 2025 The Yorkie Authors. All rights reserved.
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

// Package packs implements PushPullPack which is used to sync the document
// between the client and the server.
package packs

import (
	"context"
	stderrors "errors"
	"fmt"

	"github.com/yorkie-team/yorkie/api/types"
	"github.com/yorkie-team/yorkie/pkg/document"
	"github.com/yorkie-team/yorkie/pkg/document/json"
	"github.com/yorkie-team/yorkie/pkg/document/presence"
	"github.com/yorkie-team/yorkie/pkg/document/yson"
	"github.com/yorkie-team/yorkie/pkg/errors"
	"github.com/yorkie-team/yorkie/server/backend"
	"github.com/yorkie-team/yorkie/server/backend/database"
	"github.com/yorkie-team/yorkie/server/logging"
)

var (
	// ErrDocumentNotRemoved is returned when the document is not removed yet.
	ErrDocumentNotRemoved = errors.FailedPrecond("document is not removed yet")

	// ErrDocumentAttached is returned when the document is attached when
	// deleting the document.
	ErrDocumentAttached = errors.FailedPrecond("document is attached").WithCode("ErrDocumentAttached")
)

// recoverCompactionBatchSize bounds how many interrupted generations one
// recovery pass walks in a single query.
const recoverCompactionBatchSize = 100

// Compact compacts the given document by publishing a snapshot at the current
// server sequence and pruning the history it covers.
//
// The run is fixed to a single compaction generation: the server sequence,
// lamport and version vector captured at the claim never change, the rebuilt
// snapshot is first held as an invisible candidate, and the history is only
// pruned after that candidate has atomically become the published snapshot.
// Changes a concurrent PushPull appends after the claim therefore keep their
// sequences and are never pruned. Every stage is idempotent, so an uncertain
// database response, another trigger (another node, an admin force,
// housekeeping) or a restart simply resumes the same generation from database
// facts; at most one generation is active per document.
func Compact(
	ctx context.Context,
	be *backend.Backend,
	projectID types.ID,
	docInfo *database.DocInfo,
	force bool,
) error {
	refKey := docInfo.RefKey()

	// 1. Skip a document already found too large to compact at this server
	// seq. Rebuilding it would only fail the same way, and doing so every
	// housekeeping cycle turns one oversized document into a recurring
	// full rebuild. A forced compaction tries again.
	if seq, ok := be.Cache.OversizedCompaction.Get(refKey); ok && seq == docInfo.ServerSeq && !force {
		return fmt.Errorf(
			"compact document %s: skipped at server seq %d: %w",
			docInfo.ID, seq, database.ErrChangeTooLarge,
		)
	}

	// 2. Pin against fresh database facts. The passed docInfo is read under
	// the document lock before this call, but re-reading here is what makes a
	// resumed generation converge on one point.
	fresh, err := be.DB.FindDocInfoByRefKey(ctx, refKey)
	if err != nil {
		return err
	}

	// 3. Resume the active generation instead of starting another one. This
	// is the same path a restart, an uncertain response or a trigger on
	// another node takes; the attachment gate does not apply to finishing a
	// compaction that already claimed the document.
	active, err := be.DB.FindActiveCompactionInfo(ctx, refKey)
	if err != nil {
		return err
	}
	if active != nil {
		return driveCompaction(ctx, be, fresh, active)
	}

	// 4. Check if the document is attached.
	if !force {
		isAttached, err := be.DB.IsDocumentAttachedOrAttaching(ctx, refKey, "")
		if err != nil {
			return err
		}
		if isAttached {
			return fmt.Errorf("compact document %s: %w", docInfo.ID, ErrDocumentAttached)
		}
	} else {
		logging.DefaultLogger().Infof("force compacting document %s (skipping attachment check)", docInfo.ID)
	}

	// 5. Pin the immutable point: the latest stored change at the current
	// server sequence supplies its lamport and version vector. Presence-only
	// sequences carry no stored operation on MongoDB, so when no stored
	// change exists (or nothing lies beyond the current floor) there is no
	// history to compact.
	anchor, err := be.DB.FindLatestChangeInfo(ctx, refKey, fresh.ServerSeq)
	if err != nil {
		return err
	}
	if anchor == nil || anchor.ServerSeq <= fresh.SnapshotServerSeq {
		return nil
	}

	// 6. Claim the unique active generation. A duplicate claim means another
	// node or trigger won the race; drive that generation rather than ours.
	gen := &database.CompactionInfo{
		ID:            types.NewID(),
		ProjectID:     projectID,
		DocID:         fresh.ID,
		ServerSeq:     fresh.ServerSeq,
		Lamport:       anchor.Lamport,
		VersionVector: anchor.VersionVector.DeepCopy(),
	}
	if err := be.DB.CreateCompactionInfo(ctx, gen); err != nil {
		if !stderrors.Is(err, database.ErrCompactionInProgress) {
			return err
		}
		active, findErr := be.DB.FindActiveCompactionInfo(ctx, refKey)
		if findErr != nil {
			return findErr
		}
		if active != nil {
			return driveCompaction(ctx, be, fresh, active)
		}
		return err
	}

	return driveCompaction(ctx, be, fresh, gen)
}

// driveCompaction advances one claimed generation through its remaining
// stages. It is safe to run concurrently or repeatedly: every stage first
// asks the database what already happened and only writes what is still
// missing, so a response that never comes back or a process restart leaves
// the generation resumable from the same pinned point.
func driveCompaction(
	ctx context.Context,
	be *backend.Backend,
	docInfo *database.DocInfo,
	gen *database.CompactionInfo,
) error {
	// failGeneration ends a generation that cannot produce a candidate. It
	// never runs after publication: the snapshot is only published once it
	// is fully stored, so failure here cannot replace the last readable
	// snapshot.
	failGeneration := func(cause error) error {
		logging.DefaultLogger().Errorf(
			"[CD] Document %s compaction generation %s failed at seq %d: %v",
			docInfo.ID, gen.ID, gen.ServerSeq, cause,
		)
		if err := be.DB.DeleteCompactionInfo(ctx, gen); err != nil {
			logging.DefaultLogger().Errorf(
				"[CD] Document %s failed to discard generation %s: %v",
				docInfo.ID, gen.ID, err,
			)
		}
		return cause
	}

	// Stage 1: build and store the isolated candidate snapshot, replaying
	// only changes within the pinned point. If the candidate is already
	// stored (an interrupted run got this far), replay is skipped.
	hasCandidate, err := be.DB.HasCompactionSnapshot(ctx, gen)
	if err != nil {
		return err
	}
	if !hasCandidate {
		candidate, err := buildCandidateDocument(ctx, be, docInfo, gen.ServerSeq)
		if err != nil {
			return failGeneration(err)
		}

		if err := be.DB.SaveCompactionSnapshot(ctx, gen, candidate); err != nil {
			if stderrors.Is(err, database.ErrChangeTooLarge) {
				be.Cache.OversizedCompaction.Add(docInfo.RefKey(), gen.ServerSeq)
			}
			return failGeneration(err)
		}
	}

	// Stage 2: publish the snapshot index unless database facts already show
	// it published. The upsert is the reader-visible commit; the changes it
	// covers are still all present until stage 3.
	facts, err := readCompactionFacts(ctx, be, gen)
	if err != nil {
		return err
	}
	if !facts.published {
		if err := be.DB.PublishCompactionSnapshot(ctx, gen); err != nil {
			return err
		}
	}
	// Force every reader of this node to rebuild from the published snapshot
	// rather than a cache entry below the new floor.
	be.Cache.Snapshot.Remove(docInfo.RefKey())

	// Stage 3: prune only the covered history. The delete targets server
	// sequences at or below the pinned point, which the published snapshot
	// now covers; changes appended after the claim are above it and stay.
	// The delete is idempotent, so an uncertain prune response just retries.
	if _, err := be.DB.PruneCompactionChanges(ctx, gen); err != nil {
		return err
	}

	// Stage 4: end the generation and drop its (now copied) candidate bytes.
	if err := be.DB.DeleteCompactionInfo(ctx, gen); err != nil {
		return err
	}
	be.Cache.OversizedCompaction.Remove(docInfo.RefKey())

	logging.DefaultLogger().Infof(
		"[CD] Document %s compacted at server seq %d (generation %s)",
		docInfo.ID, gen.ServerSeq, gen.ID,
	)
	return nil
}

// compactionFacts is the database state a resumed generation continues from.
type compactionFacts struct {
	// published is true when the published snapshot at the pinned sequence
	// and the document floor at or beyond it both exist.
	published bool
}

// compactionFacts reads the database to decide which stages remain. Because
// every answer comes from stored facts rather than from the response of the
// write that produced them, an uncertain write leaves the generation
// resumable.
func readCompactionFacts(
	ctx context.Context,
	be *backend.Backend,
	gen *database.CompactionInfo,
) (compactionFacts, error) {
	snapshot, err := be.DB.FindSnapshotInfo(ctx, gen.RefKey(), gen.ServerSeq)
	if err != nil && !stderrors.Is(err, database.ErrSnapshotNotFound) {
		return compactionFacts{}, err
	}
	snapshotExists := snapshot != nil && snapshot.ID != ""

	docInfo, err := be.DB.FindDocInfoByRefKey(ctx, gen.RefKey())
	if err != nil {
		return compactionFacts{}, err
	}

	return compactionFacts{
		published: snapshotExists && docInfo.SnapshotServerSeq >= gen.ServerSeq,
	}, nil
}

// buildCandidateDocument replays the document up to the pinned server
// sequence in an isolated instance and validates that the rebuilt content
// round-trips. The replay never reads past serverSeq, so changes a
// concurrent PushPull appends after the claim cannot enter the candidate.
func buildCandidateDocument(
	ctx context.Context,
	be *backend.Backend,
	docInfo *database.DocInfo,
	serverSeq int64,
) (*document.InternalDocument, error) {
	doc, err := BuildInternalDocForServerSeq(ctx, be, docInfo, serverSeq)
	if err != nil {
		logging.DefaultLogger().Errorf("[CD] Document %s failed to apply changes: %v\n", docInfo.ID, err)
		return nil, err
	}

	if docInfo.DisablePresence {
		doc.ResetPresences()
	}

	// Rebuild a fresh root from the YSON of the rebuilt document and require
	// byte-identical content, so a replay that drops or invents data never
	// reaches the candidate snapshot.
	root, err := yson.FromCRDT(doc.RootObject())
	if err != nil {
		return nil, err
	}

	newDoc := document.New(docInfo.Key)
	if err = newDoc.Update(func(r *json.Object, _ *presence.Presence) error {
		r.SetYSON(root)
		return nil
	}); err != nil {
		return nil, err
	}

	newRoot, err := yson.FromCRDT(newDoc.RootObject())
	if err != nil {
		return nil, err
	}

	prevMarshalled, err := root.(yson.Object).Marshal()
	if err != nil {
		return nil, err
	}
	newMarshalled, err := newRoot.(yson.Object).Marshal()
	if err != nil {
		return nil, err
	}
	if prevMarshalled != newMarshalled {
		return nil, fmt.Errorf("content mismatch after rebuild: %s", docInfo.ID)
	}

	return doc, nil
}

// RecoverActiveCompactions resumes compaction generations left active by a
// crash or an interrupted response. It is run on housekeeping cycles (only
// on the leader); each resumed generation goes through the same idempotent
// stages as a fresh compaction. A document locked by a running request is
// skipped and picked up on the next pass.
func RecoverActiveCompactions(ctx context.Context, be *backend.Backend) (int, error) {
	recovered := 0
	var afterID types.ID

	for {
		infos, err := be.DB.FindRecoverableCompactionInfos(ctx, recoverCompactionBatchSize, afterID)
		if err != nil {
			return recovered, err
		}
		if len(infos) == 0 {
			return recovered, nil
		}

		for _, gen := range infos {
			afterID = gen.ID
			docInfo, err := be.DB.FindDocInfoByRefKey(ctx, gen.RefKey())
			if err != nil {
				if stderrors.Is(err, database.ErrDocumentNotFound) {
					// The document was purged while the generation lingered;
					// its candidate rows are all that is left to reclaim.
					if delErr := be.DB.DeleteCompactionInfo(ctx, gen); delErr != nil {
						logging.DefaultLogger().Errorf(
							"[CD] reclaim orphaned generation %s: %v", gen.ID, delErr,
						)
					}
					continue
				}
				return recovered, err
			}

			// Do not block on a document a running admin/RPC/housekeeping
			// request already holds; that request finishes or resumes the
			// generation itself.
			locker, ok := be.Lockers.LockerWithTryLock(DocKey(gen.ProjectID, docInfo.Key))
			if !ok {
				continue
			}

			err = driveCompaction(ctx, be, docInfo, gen)
			locker.Unlock()
			if err != nil {
				logging.DefaultLogger().Errorf(
					"[CD] resume generation %s of document %s: %v",
					gen.ID, docInfo.ID, err,
				)
				continue
			}
			recovered++
		}

		if len(infos) < recoverCompactionBatchSize {
			return recovered, nil
		}
	}
}

// Purge removes the document from the database permanently.
func Purge(
	ctx context.Context,
	be *backend.Backend,
	projectID types.ID,
	docInfo *database.DocInfo,
) error {
	// 0. Invalidate snapshot cache.
	be.Cache.Snapshot.Remove(docInfo.RefKey())

	// 1. Check if the document is removed.
	if !docInfo.IsRemoved() {
		return fmt.Errorf("purge document %s: %w", docInfo.ID, ErrDocumentNotRemoved)
	}

	// 2. Purge the document from the database.
	counts, err := be.DB.PurgeDocument(ctx, docInfo.RefKey())
	if err != nil {
		return err
	}

	logging.From(ctx).Infow(fmt.Sprintf(
		"purged document internals [project_id=%s doc_id=%s]",
		projectID, docInfo.ID,
	), "changes", counts["changes"], "snapshots", counts["snapshots"], "versionvectors", counts["versionvectors"])

	return nil
}
