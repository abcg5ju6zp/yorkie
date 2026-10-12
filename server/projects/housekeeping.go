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

package projects

import (
	"context"
	"fmt"
	"time"

	"github.com/yorkie-team/yorkie/api/types"
	"github.com/yorkie-team/yorkie/server/backend"
	"github.com/yorkie-team/yorkie/server/backend/database"
	"github.com/yorkie-team/yorkie/server/logging"
)

const (
	statsRefreshKey = "housekeeping/project-stats-refresh"

	// statsUnitTimeout bounds refreshing a single project after the cycle
	// context is cancelled. A project already in hand finishes both counts
	// and the stats write as one complete publication; the timeout keeps the
	// detached work bounded.
	statsUnitTimeout = 15 * time.Second
)

// RefreshStats walks projects in cursor order, counts activated clients and
// non-removed documents per project against the MongoDB secondary, and writes
// the values back as cached stats on the project document.
//
// Returns the new cursor position and the number of projects processed.
//
// Per-project failures are logged and the project is skipped, so a single bad
// project cannot stall subsequent refreshes in the same cycle.
//
// On a shutdown signal the cycle stops claiming new projects, the in-hand
// project still publishes its complete count pair, and the cursor is held at
// the last fully refreshed project rather than advanced to the batch tail;
// unprocessed projects are left for the next cycle. Partial counts are never
// written because the stats update follows both counts.
func RefreshStats(
	ctx context.Context,
	be *backend.Backend,
	limit int,
	lastProjectID types.ID,
) (types.ID, int, error) {
	// Shutdown has already begun: do not claim the cycle lock, page projects
	// or touch any resource.
	if ctx.Err() != nil {
		return lastProjectID, 0, nil
	}

	locker, ok := be.Lockers.LockerWithTryLock(statsRefreshKey)
	if !ok {
		return lastProjectID, 0, nil
	}
	defer locker.Unlock()

	projects, tailID, err := be.DB.FindProjectInfosForRefresh(ctx, limit, lastProjectID)
	if err != nil {
		return lastProjectID, 0, err
	}

	if len(projects) == 0 {
		return database.ZeroID, 0, nil
	}

	processed := 0
	committedCursor := lastProjectID

	for _, p := range projects {
		// Stop claiming new projects as soon as shutdown begins.
		if ctx.Err() != nil {
			return committedCursor, processed, nil
		}

		// Detach the in-hand project from the cycle signal so its two counts
		// and update converge into one complete publication instead of
		// leaving partial counts behind.
		unitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), statsUnitTimeout)
		err := refreshOneProject(unitCtx, be, p.ID)
		cancel()

		if err != nil {
			logging.From(ctx).Warnf("refresh project stats %s: %v", p.ID, err)
		} else {
			processed++
			committedCursor = p.ID
		}

		// After the in-hand project settles, honour a signal observed while it
		// was running: hold the cursor at the last completed project.
		if ctx.Err() != nil {
			return committedCursor, processed, nil
		}
	}

	// Full batch drained: advance to the batch tail as before.
	return tailID, processed, nil
}

// refreshOneProject counts the project's clients and documents and publishes
// both values in one stats update. It returns an error if either count fails,
// in which case nothing is written, so a project can never publish partial
// counts.
func refreshOneProject(ctx context.Context, be *backend.Backend, projectID types.ID) error {
	cc, err := be.DB.CountActivatedClients(ctx, projectID)
	if err != nil {
		return fmt.Errorf("count activated clients %s: %w", projectID, err)
	}
	dc, err := be.DB.CountAliveDocuments(ctx, projectID)
	if err != nil {
		return fmt.Errorf("count alive documents %s: %w", projectID, err)
	}
	if err := be.DB.UpdateProjectStats(ctx, projectID, cc, dc, time.Now()); err != nil {
		return fmt.Errorf("update project stats %s: %w", projectID, err)
	}
	return nil
}
