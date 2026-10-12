/*
 * Copyright 2021 The Yorkie Authors. All rights reserved.
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
	"fmt"
	"sync"
	"time"

	"github.com/go-co-op/gocron/v2"

	"github.com/yorkie-team/yorkie/server/backend/membership"
	"github.com/yorkie-team/yorkie/server/logging"
)

// defaultShutdownTimeout bounds how long Stop waits for in-flight housekeeping
// cycles to reach their safe boundaries. It matches the process-level graceful
// shutdown budget, so a healthy node still drains the common fast path while a
// stuck cycle cannot delay resource closure indefinitely.
const defaultShutdownTimeout = 10 * time.Second

// ErrStopTimedOut is returned (wrapped per task type) by Stop when one or more
// in-flight housekeeping cycles do not reach their safe boundaries within the
// shutdown timeout. Callers can judge the result with errors.Is.
var ErrStopTimedOut = errors.New("housekeeping stop timed out before task reached a safe boundary")

// taskCycle tracks in-flight cycles of one registered task type so Stop can
// wait for each of the three task types independently.
type taskCycle struct {
	name string
	wg   sync.WaitGroup
}

// Housekeeping is the housekeeping service. It periodically runs housekeeping
// tasks.
type Housekeeping struct {
	Config *Config

	scheduler  gocron.Scheduler
	membership *membership.Manager

	// isLeader reports whether this node leads housekeeping. It is a field so
	// tests can exercise the cycle wrapper without driving membership.
	isLeader func() bool

	// ctx is the single shutdown signal shared by every cycle. Cancelling it
	// both stops new cycles from starting and asks running cycles to wind up
	// at their next safe boundary.
	ctx    context.Context
	cancel context.CancelFunc

	// launchMu makes the stopped flag and the per-cycle WaitGroup counters
	// safe against Stop racing a scheduler dispatch: runTask holds RLock while
	// checking stopped and calling Add, Stop holds Lock before cancelling and
	// waiting. Once Stop holds Lock, no Add can still be in flight.
	launchMu sync.RWMutex
	stopped  bool

	cyclesMu sync.Mutex
	cycles   []*taskCycle

	shutdownTimeout time.Duration

	stopOnce   sync.Once
	stopResult error
}

// New creates a new housekeeping instance.
func New(conf *Config, manager *membership.Manager) (*Housekeeping, error) {
	shutdownTimeout := defaultShutdownTimeout
	scheduler, err := gocron.NewScheduler(
		// Our own wait drains the job funcs before gocron is asked to stop, so
		// on the clean path this budget never binds. Keep it short so a cycle
		// that missed our deadline fails gocron's wait quickly instead of
		// stretching the shutdown for another ten seconds.
		gocron.WithStopTimeout(2 * time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("new scheduler: %w", err)
	}

	// Bind the default logger so cycles log even before the server bootstrap
	// sets one (and so every cycle shares the same logger with its signal).
	ctx, cancel := context.WithCancel(logging.With(context.Background(), logging.DefaultLogger()))

	return &Housekeeping{
		Config:     conf,
		scheduler:  scheduler,
		membership: manager,
		isLeader:   manager.IsLeader,

		ctx:    ctx,
		cancel: cancel,

		shutdownTimeout: shutdownTimeout,
	}, nil
}

// RegisterTask registers task the housekeeping service.
func (h *Housekeeping) RegisterTask(
	name string,
	interval time.Duration,
	task func(ctx context.Context) error,
) error {
	cycle := &taskCycle{name: name}
	h.cyclesMu.Lock()
	h.cycles = append(h.cycles, cycle)
	h.cyclesMu.Unlock()

	if _, err := h.scheduler.NewJob(
		gocron.DurationJob(interval),
		gocron.NewTask(func() {
			h.runTask(cycle, task)
		}),
	); err != nil {
		return fmt.Errorf("scheduler new job: %w", err)
	}

	return nil
}

// runTask is the per-cycle wrapper. It refuses to start once shutdown has
// begun, passes the shared shutdown context into the task body, and reports
// in-flight status to Stop via the cycle's WaitGroup.
func (h *Housekeeping) runTask(cycle *taskCycle, task func(ctx context.Context) error) {
	h.launchMu.RLock()
	if h.stopped {
		h.launchMu.RUnlock()
		return
	}
	cycle.wg.Add(1)
	h.launchMu.RUnlock()
	defer cycle.wg.Done()

	if !h.isLeader() {
		logging.From(h.ctx).Debug("skipping task - not leader")
		return
	}
	if h.ctx.Err() != nil {
		return
	}

	logging.From(h.ctx).Debugf("running housekeeping task %q", cycle.name)
	if err := task(h.ctx); err != nil {
		// A cycle unwinding on the shutdown signal is expected, not an error:
		// the task is responsible for leaving its cursors at safe positions.
		if h.ctx.Err() != nil {
			logging.From(h.ctx).Debugf("housekeeping task %q interrupted by shutdown: %v", cycle.name, err)
			return
		}
		logging.From(h.ctx).Error(err)
	}
}

// Start starts the housekeeping service.
func (h *Housekeeping) Start(ctx context.Context) error {
	h.scheduler.Start()
	return nil
}

// Stop signals shutdown: no new cycle starts, every running cycle receives the
// same cancellation signal, and Stop waits for each task type to reach its safe
// boundary. It is idempotent: repeated calls return the first call's result.
// A timeout is reported per stuck task type wrapped with ErrStopTimedOut so the
// caller can tell a clean drain from an unfinished one before databases and
// RPC pools are closed.
func (h *Housekeeping) Stop() error {
	h.stopOnce.Do(func() {
		h.stopResult = h.shutdown()
	})
	return h.stopResult
}

func (h *Housekeeping) shutdown() error {
	// Block new dispatches first, then share the signal with running cycles.
	h.launchMu.Lock()
	h.stopped = true
	h.cancel()
	h.launchMu.Unlock()

	var errs []error

	// Wait for every task type concurrently under one shared deadline: the
	// three cycles are independent, and the deadline must cover the slowest,
	// not the sum of their drain times.
	deadline := time.Now().Add(h.shutdownTimeout)
	var wg sync.WaitGroup
	var errMu sync.Mutex

	h.cyclesMu.Lock()
	cycles := append([]*taskCycle(nil), h.cycles...)
	h.cyclesMu.Unlock()

	for _, cycle := range cycles {
		wg.Add(1)
		go func(cycle *taskCycle) {
			defer wg.Done()

			done := make(chan struct{})
			go func() {
				cycle.wg.Wait()
				close(done)
			}()

			timeout := time.Until(deadline)
			if timeout <= 0 {
				timeout = time.Millisecond
			}
			select {
			case <-done:
			case <-time.After(timeout):
				errMu.Lock()
				errs = append(errs, fmt.Errorf("housekeeping task %q: %w", cycle.name, ErrStopTimedOut))
				errMu.Unlock()
			}
		}(cycle)
	}
	wg.Wait()

	// All registered job funcs have returned (or the deadline expired), so the
	// scheduler will not launch or be waiting on housekeeping work anymore.
	if err := h.scheduler.StopJobs(); err != nil {
		errs = append(errs, fmt.Errorf("scheduler stop jobs: %w", err))
	}
	if err := h.scheduler.Shutdown(); err != nil {
		errs = append(errs, fmt.Errorf("scheduler shutdown: %w", err))
	}

	return errors.Join(errs...)
}
