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

package housekeeping_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/yorkie-team/yorkie/server/backend/housekeeping"
)

func TestConfig(t *testing.T) {
	t.Run("validate test", func(t *testing.T) {
		validConf := housekeeping.Config{
			Interval:             "1m",
			CandidatesLimit:      100,
			CompactionMinChanges: 100,
		}
		assert.NoError(t, validConf.Validate())

		conf1 := validConf
		conf1.Interval = "hour"
		assert.Error(t, conf1.Validate())

		conf2 := validConf
		conf2.CandidatesLimit = 0
		assert.Error(t, conf2.Validate())

		conf3 := validConf
		conf3.CompactionMinChanges = 0
		assert.Error(t, conf3.Validate())

		// A negative DeactivateConcurrency must be rejected. This guards the
		// check against being masked by an ensure-default coercion upstream.
		conf4 := validConf
		conf4.DeactivateConcurrency = -1
		assert.Error(t, conf4.Validate())

		// Zero is a valid sequential opt-in and must pass validation.
		conf5 := validConf
		conf5.DeactivateConcurrency = 0
		assert.NoError(t, conf5.Validate())
	})

	t.Run("parse test", func(t *testing.T) {
		validConf := housekeeping.Config{
			Interval: "50s",
		}
		duration, err := validConf.ParseInterval()
		assert.NoError(t, err)
		assert.Equal(t, "50s", duration.String())

		conf1 := validConf
		conf1.Interval = "1 hour"
		_, err = conf1.ParseInterval()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "parse interval")
	})

	t.Run("retirement retention interval test", func(t *testing.T) {
		base := housekeeping.Config{Interval: "1m", CandidatesLimit: 100, CompactionMinChanges: 100}

		conf := base
		conf.RetirementRetentionInterval = "24h"
		assert.NoError(t, conf.Validate())
		d, err := conf.ParseRetirementRetentionInterval()
		assert.NoError(t, err)
		assert.Equal(t, "24h0m0s", d.String())

		// Empty and explicit zero disable the deferred purge task.
		empty := base
		assert.NoError(t, empty.Validate())
		d, err = empty.ParseRetirementRetentionInterval()
		assert.NoError(t, err)
		assert.Zero(t, d)

		zero := base
		zero.RetirementRetentionInterval = "0s"
		assert.NoError(t, zero.Validate())
		d, err = zero.ParseRetirementRetentionInterval()
		assert.NoError(t, err)
		assert.Zero(t, d)

		bad := base
		bad.RetirementRetentionInterval = "soon"
		assert.Error(t, bad.Validate())
	})
}
