/*
 * Copyright 2023 The Yorkie Authors. All rights reserved.
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

// Package housekeeping is the package for housekeeping service. It cleans up
// the resources and data that is no longer needed.
package housekeeping

import (
	"fmt"
	"time"
)

// Config is the configuration for the housekeeping service.
type Config struct {
	// Interval is the time between housekeeping runs.
	Interval string `yaml:"Interval"`

	// CandidatesLimit is the maximum number of candidates to be returned in a single query.
	CandidatesLimit int `yaml:"CandidatesLimit"`

	// DeactivateConcurrency is the maximum number of concurrent client
	// deactivations within a single housekeeping cycle. Values <= 1 fall
	// back to sequential execution (legacy behavior).
	DeactivateConcurrency int `yaml:"DeactivateConcurrency"`

	// CompactionMinChanges is the minimum number of changes to compact a document.
	CompactionMinChanges int `yaml:"CompactionMinChanges"`

	// ProjectStatsRefreshInterval is the interval between project-stats refresh cycles.
	// If empty or "0s", the project-stats refresh task is not registered.
	ProjectStatsRefreshInterval string `yaml:"ProjectStatsRefreshInterval"`

	// RetirementRetentionInterval is how long a removed document's
	// retirement generation is kept after removal before its changes,
	// snapshots, version vectors and document row are purged. During the
	// window the generation is queryable and retains the last server
	// sequence, snapshot and version vector, so offline clients can still
	// recognize the deletion. If empty or "0s", the deferred purge task is
	// not registered and generations are retained until a manual purge.
	RetirementRetentionInterval string `yaml:"RetirementRetentionInterval"`
}

// Validate validates the configuration.
func (c *Config) Validate() error {
	if _, err := time.ParseDuration(c.Interval); err != nil {
		return fmt.Errorf(
			`invalid argument %s for "--housekeeping-interval" flag: %w`,
			c.Interval,
			err,
		)
	}

	if c.CandidatesLimit <= 0 {
		return fmt.Errorf(
			`invalid argument %d for "--housekeeping-candidates-limit" flag`,
			c.CandidatesLimit,
		)
	}

	if c.DeactivateConcurrency < 0 {
		return fmt.Errorf(
			`invalid argument %d for "--housekeeping-deactivate-concurrency" flag: must be >= 0`,
			c.DeactivateConcurrency,
		)
	}

	if c.CompactionMinChanges <= 0 {
		return fmt.Errorf(
			`invalid argument %d for "--housekeeping-compaction-min-changes" flag`,
			c.CompactionMinChanges,
		)
	}

	if c.ProjectStatsRefreshInterval != "" {
		d, err := time.ParseDuration(c.ProjectStatsRefreshInterval)
		if err != nil {
			return fmt.Errorf(
				`invalid argument %s for "--housekeeping-project-stats-refresh-interval" flag: %w`,
				c.ProjectStatsRefreshInterval,
				err,
			)
		}
		if d < 0 {
			return fmt.Errorf(
				`invalid argument %s for "--housekeeping-project-stats-refresh-interval" flag: must be >= 0`,
				c.ProjectStatsRefreshInterval,
			)
		}
	}

	if c.RetirementRetentionInterval != "" {
		d, err := time.ParseDuration(c.RetirementRetentionInterval)
		if err != nil {
			return fmt.Errorf(
				`invalid argument %s for "--housekeeping-retirement-retention-interval" flag: %w`,
				c.RetirementRetentionInterval,
				err,
			)
		}
		if d < 0 {
			return fmt.Errorf(
				`invalid argument %s for "--housekeeping-retirement-retention-interval" flag: must be >= 0`,
				c.RetirementRetentionInterval,
			)
		}
	}

	return nil
}

// ParseInterval parses the interval.
func (c *Config) ParseInterval() (time.Duration, error) {
	interval, err := time.ParseDuration(c.Interval)
	if err != nil {
		return 0, fmt.Errorf("parse interval %s: %w", c.Interval, err)
	}

	return interval, nil
}

// ParseProjectStatsRefreshInterval parses the configured interval. Returns 0 if empty.
func (c *Config) ParseProjectStatsRefreshInterval() (time.Duration, error) {
	if c.ProjectStatsRefreshInterval == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(c.ProjectStatsRefreshInterval)
	if err != nil {
		return 0, fmt.Errorf(
			`invalid argument %s for "--housekeeping-project-stats-refresh-interval" flag: %w`,
			c.ProjectStatsRefreshInterval,
			err,
		)
	}
	if d < 0 {
		return 0, fmt.Errorf(
			`invalid argument %s for "--housekeeping-project-stats-refresh-interval" flag: must be >= 0`,
			c.ProjectStatsRefreshInterval,
		)
	}
	return d, nil
}

// ParseRetirementRetentionInterval parses the configured retention interval
// of a retired document generation. Returns 0 when unset, which disables the
// deferred purge task; an explicit "0s" also disables it.
func (c *Config) ParseRetirementRetentionInterval() (time.Duration, error) {
	if c.RetirementRetentionInterval == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(c.RetirementRetentionInterval)
	if err != nil {
		return 0, fmt.Errorf(
			`invalid argument %s for "--housekeeping-retirement-retention-interval" flag: %w`,
			c.RetirementRetentionInterval,
			err,
		)
	}
	return d, nil
}
