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

package housekeeping

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/yorkie-team/yorkie/server/backend/membership"
)

func newTestHousekeeping(t *testing.T) *Housekeeping {
	t.Helper()

	// The manager is never started in these tests: only its IsLeader method
	// value is used (and tests override h.isLeader).
	manager := membership.New(nil, "127.0.0.1:0", &membership.Config{
		LeaseDuration:   "1h",
		RenewalInterval: "1h",
	})
	h, err := New(&Config{}, manager)
	assert.NoError(t, err)
	h.shutdownTimeout = 5 * time.Second
	t.Cleanup(func() { _ = h.Stop() })
	return h
}

// registerCycle registers a task (the gocron job never fires in these tests)
// and returns its tracker, the unit Stop actually waits on.
func registerCycle(t *testing.T, h *Housekeeping, name string) *taskCycle {
	t.Helper()
	assert.NoError(t, h.RegisterTask(name, time.Hour, func(context.Context) error {
		t.Fatal("the registered job body must not run without a started scheduler")
		return nil
	}))
	return h.cycles[len(h.cycles)-1]
}

// TestStop_PropagatesSignalAndWaits verifies the core shutdown contract:
// Stop shares one cancellation signal with the in-flight cycle and returns
// only after that cycle reaches its safe boundary.
func TestStop_PropagatesSignalAndWaits(t *testing.T) {
	h := newTestHousekeeping(t)
	h.isLeader = func() bool { return true }

	started := make(chan struct{})
	cycle := registerCycle(t, h, "deactivation")

	go h.runTask(cycle, func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		return nil
	})

	<-started
	done := make(chan error, 1)
	go func() { done <- h.Stop() }()

	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not wait for / return after the cycle observed cancellation")
	}
}

// TestStop_TimeoutIsJudgeable verifies a cycle that ignores the signal makes
// Stop return an errors.Is-matchable timeout naming the stuck task type.
func TestStop_TimeoutIsJudgeable(t *testing.T) {
	h := newTestHousekeeping(t)
	h.isLeader = func() bool { return true }
	h.shutdownTimeout = 50 * time.Millisecond

	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	cycle := registerCycle(t, h, "compaction")
	go h.runTask(cycle, func(_ context.Context) error {
		<-release
		return nil
	})

	// Let the cycle enter its body before Stop.
	time.Sleep(20 * time.Millisecond)

	err := h.Stop()
	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrStopTimedOut), "want ErrStopTimedOut in error chain, got %v", err)
	assert.Contains(t, err.Error(), "compaction")

	// Repeated Stop is idempotent: it reports the same judgement instead of
	// racing the scheduler or pretending the shutdown was clean.
	assert.Equal(t, err, h.Stop())
}

// TestStop_WithoutTasksIsClean verifies no-candidate / disabled-task setups
// (no registered cycles) and repeated Stop keep their no-op behavior.
func TestStop_WithoutTasksIsClean(t *testing.T) {
	h := newTestHousekeeping(t)
	assert.NoError(t, h.Stop())
	assert.NoError(t, h.Stop())
}

// TestStop_DrainsAllThreeTaskTypes verifies deactivation, compaction and
// project-stats cycles are all awaited under one shared deadline.
func TestStop_DrainsAllThreeTaskTypes(t *testing.T) {
	h := newTestHousekeeping(t)
	h.isLeader = func() bool { return true }

	for _, name := range []string{"deactivation", "compaction", "project-stats"} {
		cycle := registerCycle(t, h, name)
		go h.runTask(cycle, func(ctx context.Context) error {
			<-ctx.Done()
			return nil
		})
	}

	assert.NoError(t, h.Stop())
}

// TestStop_LiveSchedulerSmoke drives the real gocron scheduler: cycles run on
// a short interval, Stop cancels and drains them cleanly, and after Stop the
// task body is no longer entered.
func TestStop_LiveSchedulerSmoke(t *testing.T) {
	h := newTestHousekeeping(t)
	h.isLeader = func() bool { return true }

	var runs atomic.Int32
	assert.NoError(t, h.RegisterTask("deactivation", 5*time.Millisecond, func(ctx context.Context) error {
		runs.Add(1)
		return nil
	}))

	assert.NoError(t, h.Start(context.Background()))
	time.Sleep(80 * time.Millisecond)
	assert.Positive(t, runs.Load(), "scheduler should have run cycles")

	assert.NoError(t, h.Stop())

	after := runs.Load()
	time.Sleep(40 * time.Millisecond)
	assert.Equal(t, after, runs.Load(), "no new cycle may start after Stop")
}

// TestRunTask_NeverStartsAfterStop verifies no new cycle starts once shutdown
// has begun even if the scheduler dispatches the wrapper.
func TestRunTask_NeverStartsAfterStop(t *testing.T) {
	h := newTestHousekeeping(t)
	h.isLeader = func() bool { return true }
	assert.NoError(t, h.Stop())

	var entered atomic.Bool
	cycle := &taskCycle{name: "deactivation"}
	h.runTask(cycle, func(context.Context) error {
		entered.Store(true)
		return nil
	})
	assert.False(t, entered.Load(), "task body must not run after Stop")
}

// TestRunTask_NonLeaderSkips preserves the non-leader behavior: leadership
// check fails, the task body never runs and the cycle settles immediately.
func TestRunTask_NonLeaderSkips(t *testing.T) {
	h := newTestHousekeeping(t)
	h.isLeader = func() bool { return false }

	var entered atomic.Bool
	cycle := &taskCycle{name: "compaction"}
	h.runTask(cycle, func(context.Context) error {
		entered.Store(true)
		return nil
	})
	assert.False(t, entered.Load())
}

// TestRunTask_AlreadyCancelledSkips covers the race where the signal landed
// after dispatch admission but before the leader-gated body ran.
func TestRunTask_AlreadyCancelledSkips(t *testing.T) {
	h := newTestHousekeeping(t)
	h.isLeader = func() bool { return true }
	h.cancel()

	var entered atomic.Bool
	cycle := &taskCycle{name: "project-stats"}
	h.runTask(cycle, func(context.Context) error {
		entered.Store(true)
		return nil
	})
	assert.False(t, entered.Load())
}

// TestRunTask_TaskErrorIsSwallowed preserves current behavior: a task error is
// logged by the wrapper, not propagated (Stop only tracks completion).
func TestRunTask_TaskErrorIsSwallowed(t *testing.T) {
	h := newTestHousekeeping(t)
	h.isLeader = func() bool { return true }

	cycle := &taskCycle{name: "deactivation"}
	assert.NotPanics(t, func() {
		h.runTask(cycle, func(context.Context) error {
			return errors.New("boom")
		})
	})
}
