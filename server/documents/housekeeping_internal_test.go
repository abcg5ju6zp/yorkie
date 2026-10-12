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
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/yorkie-team/yorkie/api/types"
	"github.com/yorkie-team/yorkie/server/backend/database"
	"github.com/yorkie-team/yorkie/server/logging"
)

// testCtx returns a context with the default logger bound; compactCandidates
// logs from failure paths.
func testCtx() context.Context {
	return logging.With(context.Background(), logging.DefaultLogger())
}

func docCandidates(n int) []CandidatePairDoc {
	pairs := make([]CandidatePairDoc, n)
	for i := range n {
		pairs[i] = CandidatePairDoc{
			Document: &database.DocInfo{
				ID:        types.ID(fmt.Sprintf("%024d", i+1)),
				ServerSeq: int64(100 + i),
			},
		}
	}
	return pairs
}

// TestCompactCandidates_NormalRunAdvancesToTail preserves the normal-cycle
// behavior: every candidate is attempted and the cursor ends at the batch
// tail.
func TestCompactCandidates_NormalRunAdvancesToTail(t *testing.T) {
	candidates := docCandidates(3)
	var attempted int

	seq, id, attemptedN, compacted := compactCandidates(
		context.Background(), 0, database.ZeroID, candidates,
		func(_ context.Context, _ CandidatePairDoc) (bool, error) {
			attempted++
			return true, nil
		},
	)

	assert.Equal(t, 3, attempted)
	assert.Equal(t, 3, attemptedN)
	assert.Equal(t, 3, compacted)
	assert.Equal(t, candidates[2].Document.ServerSeq, seq)
	assert.Equal(t, candidates[2].Document.ID, id)
}

// TestCompactCandidates_PreCancelledTouchesNothing: a signal present before the
// loop starts leaves every document with its snapshot and untrimmed changes;
// the cursor stays at its input position.
func TestCompactCandidates_PreCancelledTouchesNothing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	candidates := docCandidates(3)
	inputSeq, inputID := int64(50), types.ID("aaaaaaaaaaaaaaaaaaaaaaaa")

	seq, id, attempted, compacted := compactCandidates(
		ctx, inputSeq, inputID, candidates,
		func(context.Context, CandidatePairDoc) (bool, error) {
			t.Fatal("no document must be claimed after cancellation")
			return false, nil
		},
	)

	assert.Equal(t, 0, attempted)
	assert.Equal(t, 0, compacted)
	assert.Equal(t, inputSeq, seq)
	assert.Equal(t, inputID, id)
}

// TestCompactCandidates_StopsBeforeNextDocument: cancellation observed between
// documents finishes no new claim; the settled first document advances the
// cursor, the rest are left for the next cycle.
func TestCompactCandidates_StopsBeforeNextDocument(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	candidates := docCandidates(3)

	seq, id, attempted, compacted := compactCandidates(
		ctx, 0, database.ZeroID, candidates,
		func(_ context.Context, pair CandidatePairDoc) (bool, error) {
			if pair.Document.ID == candidates[0].Document.ID {
				cancel()
				return true, nil
			}
			t.Fatal("only the first document may be claimed")
			return false, nil
		},
	)

	assert.Equal(t, 1, attempted)
	assert.Equal(t, 1, compacted)
	assert.Equal(t, candidates[0].Document.ServerSeq, seq)
	assert.Equal(t, candidates[0].Document.ID, id)
}

// TestCompactCandidates_InFlightCompletesAfterSignal is the "finish the current
// document" branch: the in-hand compaction runs on a detached unit context and
// may complete successfully after the cycle is cancelled; the cursor advances
// past exactly that document.
func TestCompactCandidates_InFlightCompletesAfterSignal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	candidates := docCandidates(3)

	seq, id, attempted, compacted := compactCandidates(
		ctx, 0, database.ZeroID, candidates,
		func(unitCtx context.Context, pair CandidatePairDoc) (bool, error) {
			if pair.Document.ID == candidates[0].Document.ID {
				cancel()
				// The unit context must stay alive despite the cycle signal.
				assert.NoError(t, unitCtx.Err())
				<-ctx.Done()
				assert.NoError(t, unitCtx.Err())
				return true, nil
			}
			t.Fatal("only the in-hand document may be processed")
			return false, nil
		},
	)

	assert.Equal(t, 1, attempted)
	assert.Equal(t, 1, compacted)
	assert.Equal(t, candidates[0].Document.ServerSeq, seq)
	assert.Equal(t, candidates[0].Document.ID, id)
}

// TestCompactCandidates_InFlightFailureUnderSignalHoldsCursor: when the in-hand
// document's outcome is uncertain at shutdown, the cursor must not advance
// past it so the next cycle re-examines it rather than treating it as done.
func TestCompactCandidates_InFlightFailureUnderSignalHoldsCursor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	candidates := docCandidates(3)
	inputSeq, inputID := int64(50), types.ID("aaaaaaaaaaaaaaaaaaaaaaaa")

	seq, id, attempted, compacted := compactCandidates(
		ctx, inputSeq, inputID, candidates,
		func(context.Context, CandidatePairDoc) (bool, error) {
			cancel()
			return false, errors.New("write interrupted")
		},
	)

	assert.Equal(t, 1, attempted)
	assert.Equal(t, 0, compacted)
	assert.Equal(t, inputSeq, seq)
	assert.Equal(t, inputID, id)
}

// TestCompactCandidates_OrdinaryFailureAdvances preserves existing behavior:
// outside shutdown a failed document is logged and skipped and the sweep
// continues through the batch, ending at the tail.
func TestCompactCandidates_OrdinaryFailureAdvances(t *testing.T) {
	candidates := docCandidates(3)
	var attempted int

	seq, id, attemptedN, compacted := compactCandidates(
		context.Background(), 0, database.ZeroID, candidates,
		func(_ context.Context, pair CandidatePairDoc) (bool, error) {
			attempted++
			if pair.Document.ID == candidates[1].Document.ID {
				return false, errors.New("ordinary failure")
			}
			return true, nil
		},
	)

	assert.Equal(t, 3, attempted)
	assert.Equal(t, 3, attemptedN)
	assert.Equal(t, 2, compacted)
	assert.Equal(t, candidates[2].Document.ServerSeq, seq)
	assert.Equal(t, candidates[2].Document.ID, id)
}
