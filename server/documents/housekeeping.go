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

package documents

import (
	"context"
	"time"

	"github.com/yorkie-team/yorkie/api/types"
	"github.com/yorkie-team/yorkie/server/backend"
	"github.com/yorkie-team/yorkie/server/backend/database"
	"github.com/yorkie-team/yorkie/server/logging"
)

const (
	compactionKey = "housekeeping/compaction"

	// compactionUnitTimeout bounds the compaction of a single document after
	// the cycle context is cancelled. A document already in hand either
	// completes (claim, compacted change and purge all land) or keeps its last
	// readable snapshot and untrimmed changes (the claim is rolled back by the
	// database layer); the timeout keeps that detached work bounded.
	compactionUnitTimeout = 30 * time.Second
)

// CandidatePairDoc represents a pair of Project and Document.
type CandidatePairDoc struct {
	Project  *types.Project
	Document *database.DocInfo
}

// CompactDocuments compacts documents by removing old changes and creating
// a new initial change using direct document iteration.
func CompactDocuments(
	ctx context.Context,
	be *backend.Backend,
	candidatesLimit int,
	compactionMinChanges int,
	lastServerSeq int64,
	lastDocID types.ID,
) (int64, types.ID, int, int, error) {
	// Shutdown has already begun: do not claim the cycle lock, query for
	// candidates or touch any resource.
	if ctx.Err() != nil {
		return lastServerSeq, lastDocID, 0, 0, nil
	}

	locker, ok := be.Lockers.LockerWithTryLock(compactionKey)
	if !ok {
		return lastServerSeq, lastDocID, 0, 0, nil
	}
	defer locker.Unlock()

	resultServerSeq, resultDocID, candidates, err := FindCompactionCandidates(
		ctx,
		be,
		candidatesLimit,
		compactionMinChanges,
		lastServerSeq,
		lastDocID,
	)
	if err != nil {
		return lastServerSeq, lastDocID, 0, 0, err
	}

	// If no candidates found, reset to beginning for next cycle.
	if len(candidates) == 0 {
		return 0, database.ZeroID, 0, 0, nil
	}

	seq, id, attempted, compactedCount := compactCandidates(
		ctx,
		lastServerSeq,
		lastDocID,
		candidates,
		func(unitCtx context.Context, pair CandidatePairDoc) (bool, error) {
			return CompactDocument(unitCtx, be, pair.Project, pair.Document, false)
		},
	)

	// When the cycle ran to the end, the cursor is the tail of the fetched
	// batch, as before. An interrupted cycle returns the position
	// compactCandidates held at, which never passes an unsettled document.
	if ctx.Err() == nil {
		seq, id = resultServerSeq, resultDocID
	}

	return seq, id, attempted, compactedCount, nil
}

// compactCandidates walks the candidates one document at a time under the
// cycle context and compacts each on a detached, bounded unit context.
//
// Safe boundaries on shutdown:
//   - Before a document is claimed, a cancelled cycle context stops the loop,
//     so that document keeps its last readable snapshot and untrimmed changes.
//   - A document already claimed runs to completion detached from the signal;
//     the claim/settle logic in the database layer rolls back an interrupted
//     write, so the outcome is either a complete compaction or the untouched
//     document.
//
// The returned cursor only advances past settled documents: on interruption
// it stops at the last settled one, leaving the in-hand document (when its
// result is uncertain) for the next cycle. The conditional server_seq claim
// makes retrying an already-finished document harmless.
func compactCandidates(
	ctx context.Context,
	lastServerSeq int64,
	lastDocID types.ID,
	candidates []CandidatePairDoc,
	compactOne func(context.Context, CandidatePairDoc) (bool, error),
) (int64, types.ID, int, int) {
	committedServerSeq := lastServerSeq
	committedDocID := lastDocID
	attempted := 0
	compactedCount := 0

	for _, pair := range candidates {
		// Stop claiming new documents as soon as shutdown begins.
		if ctx.Err() != nil {
			break
		}
		attempted++

		unitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), compactionUnitTimeout)
		compacted, err := compactOne(unitCtx, pair)
		cancel()

		if err != nil {
			// The signal raced the in-hand document: its claim may still be
			// settling, so hold the cursor before it rather than report it as
			// done. Outside shutdown this is an ordinary per-document failure:
			// log and move on, as before.
			if ctx.Err() != nil {
				logging.From(ctx).Warnf(
					"interrupted while compacting document %s: %v", pair.Document.ID, err)
				break
			}
			logging.From(ctx).Warnf("failed to compact document %s: %v", pair.Document.ID, err)
		}

		if compacted {
			compactedCount++
		}
		committedServerSeq = pair.Document.ServerSeq
		committedDocID = pair.Document.ID

		if ctx.Err() != nil {
			break
		}
	}

	return committedServerSeq, committedDocID, attempted, compactedCount
}

// FindCompactionCandidates finds candidates to compact by directly querying documents.
func FindCompactionCandidates(
	ctx context.Context,
	be *backend.Backend,
	candidatesLimit int,
	compactionMinChanges int,
	lastServerSeq int64,
	lastDocID types.ID,
) (int64, types.ID, []CandidatePairDoc, error) {
	candidates, resultServerSeq, resultDocID, err := be.DB.FindCompactionCandidates(
		ctx, candidatesLimit, compactionMinChanges, lastServerSeq, lastDocID)
	if err != nil {
		return 0, database.ZeroID, nil, err
	}

	var pairs []CandidatePairDoc
	for _, candidate := range candidates {
		info, err := be.DB.FindProjectInfoByID(ctx, candidate.ProjectID)
		if err != nil {
			return 0, database.ZeroID, nil, err
		}

		pairs = append(pairs, CandidatePairDoc{
			Project:  info.ToProject(),
			Document: candidate,
		})
	}

	return resultServerSeq, resultDocID, pairs, nil
}
