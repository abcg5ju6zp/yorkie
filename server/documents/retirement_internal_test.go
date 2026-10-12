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
	"testing"
	gotime "time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yorkie-team/yorkie/api/types"
	"github.com/yorkie-team/yorkie/pkg/key"
	"github.com/yorkie-team/yorkie/server/backend"
	"github.com/yorkie-team/yorkie/server/backend/database"
	"github.com/yorkie-team/yorkie/server/backend/database/memory"
	"github.com/yorkie-team/yorkie/server/backend/pubsub"
	yorkiesync "github.com/yorkie-team/yorkie/server/backend/sync"
)

func newLightBackend(t *testing.T) (*backend.Backend, *types.Project) {
	t.Helper()

	db, err := memory.New()
	require.NoError(t, err)
	proj, err := db.CreateProjectInfo(context.Background(), t.Name(), types.ID("000000000000000000000000"))
	require.NoError(t, err)

	be := &backend.Backend{
		DB:      db,
		Lockers: yorkiesync.New(),
		PubSub:  pubsub.New(),
	}
	return be, proj.ToProject()
}

// TestFinalizeFencedByGeneration proves a late finalize aimed at an already
// purged generation can never touch the same-key document created afterwards:
// the generation identity is the DocID.
func TestFinalizeFencedByGeneration(t *testing.T) {
	ctx := context.Background()
	be, project := newLightBackend(t)

	clientInfo, err := be.DB.ActivateClient(ctx, project.ID, "client-a", nil)
	require.NoError(t, err)

	docKey := key.Key("tests$generation-fence")
	docA, err := be.DB.FindOrCreateDocInfo(ctx, clientInfo.RefKey(), docKey, false)
	require.NoError(t, err)
	require.False(t, docA.IsRemoved())

	_, err = RetireDocument(ctx, be, docA.RefKey(), true)
	require.NoError(t, err)
	retiredA, err := be.DB.FindDocInfoByRefKey(ctx, docA.RefKey())
	require.NoError(t, err)
	require.True(t, retiredA.IsRemoved())

	// First finalize purges generation A.
	require.NoError(t, finalizeRetiredDocument(ctx, be, retiredA, gotime.Now()))
	_, err = be.DB.FindDocInfoByRefKey(ctx, docA.RefKey())
	require.ErrorIs(t, err, database.ErrDocumentNotFound)

	// A new same-key document is a new generation with a new DocID.
	docB, err := be.DB.FindOrCreateDocInfo(ctx, clientInfo.RefKey(), docKey, false)
	require.NoError(t, err)
	require.NotEqual(t, docA.ID, docB.ID)
	require.False(t, docB.IsRemoved())

	// A late, stale finalize carrying generation A's candidate reloads A by ID,
	// finds it gone, and no-ops. Generation B must be untouched.
	require.NoError(t, finalizeRetiredDocument(ctx, be, retiredA, gotime.Now()))

	docBAfter, err := be.DB.FindDocInfoByRefKey(ctx, docB.RefKey())
	require.NoError(t, err)
	assert.Equal(t, docB.ID, docBAfter.ID)
	assert.False(t, docBAfter.IsRemoved(), "a same-key new generation must not be purged by a stale finalize")
}

// TestFinalizeRespectsRecoveryWindow proves finalize does not release a
// generation whose retention window has not elapsed, even when handed the
// candidate directly.
func TestFinalizeRespectsRecoveryWindow(t *testing.T) {
	ctx := context.Background()
	be, project := newLightBackend(t)

	clientInfo, err := be.DB.ActivateClient(ctx, project.ID, "client-a", nil)
	require.NoError(t, err)

	docKey := key.Key("tests$retention-window")
	doc, err := be.DB.FindOrCreateDocInfo(ctx, clientInfo.RefKey(), docKey, false)
	require.NoError(t, err)

	retired, err := RetireDocument(ctx, be, doc.RefKey(), true)
	require.NoError(t, err)

	// Cutoff before the generation was claimed: nothing is released.
	require.NoError(t, finalizeRetiredDocument(ctx, be, retired, gotime.Unix(0, 0)))
	stillThere, err := be.DB.FindDocInfoByRefKey(ctx, doc.RefKey())
	require.NoError(t, err)
	assert.True(t, stillThere.IsRemoved())

	// Sanity: the same generation finalizes once the window elapses.
	require.NoError(t, finalizeRetiredDocument(ctx, be, retired, gotime.Now()))
	_, err = be.DB.FindDocInfoByRefKey(ctx, doc.RefKey())
	assert.ErrorIs(t, err, database.ErrDocumentNotFound)

	// And a repeat after purge is a harmless no-op (retry continues the result).
	require.NoError(t, finalizeRetiredDocument(ctx, be, retired, gotime.Now()))
}

// TestFindPurgeCandidatesCursor verifies the ID-cursor pagination used by the
// housekeeping task.
func TestFindPurgeCandidatesCursor(t *testing.T) {
	ctx := context.Background()
	be, project := newLightBackend(t)

	clientInfo, err := be.DB.ActivateClient(ctx, project.ID, "client-a", nil)
	require.NoError(t, err)

	const total = 3
	for i := 0; i < total; i++ {
		doc, err := be.DB.FindOrCreateDocInfo(
			ctx, clientInfo.RefKey(), key.Key("tests$purge-cursor-"+string(rune('a'+i))), false,
		)
		require.NoError(t, err)
		_, err = RetireDocument(ctx, be, doc.RefKey(), true)
		require.NoError(t, err)
	}

	cutoff := gotime.Now()
	var seen []types.ID
	var cursor = database.ZeroID
	for {
		batch, next, err := be.DB.FindPurgeCandidates(ctx, 2, cutoff, cursor)
		require.NoError(t, err)
		if len(batch) == 0 {
			break
		}
		for _, info := range batch {
			seen = append(seen, info.ID)
		}
		require.NotEqual(t, database.ZeroID, next)
		cursor = next
	}
	assert.Len(t, seen, total)
}
