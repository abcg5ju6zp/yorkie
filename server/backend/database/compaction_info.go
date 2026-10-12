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

package database

import (
	gotime "time"

	"github.com/yorkie-team/yorkie/api/types"
	"github.com/yorkie-team/yorkie/pkg/document/time"
)

// CompactionStatus is the phase of a compaction generation.
type CompactionStatus string

const (
	// CompactionStatusCandidate means the generation has claimed its pinned
	// point and is building (or has built) the candidate snapshot. The
	// candidate is invisible to snapshot readers.
	CompactionStatusCandidate CompactionStatus = "candidate"

	// CompactionStatusPublished means the candidate snapshot has been
	// atomically published as the new readable snapshot index. Only the
	// history prune and the generation cleanup remain.
	CompactionStatusPublished CompactionStatus = "published"
)

// CompactionInfo is a single, at-most-one-per-document generation of a
// compaction. It pins the immutable server sequence (and its lamport and
// version vector) the whole generation works against, and carries the
// isolated candidate snapshot until it is published.
//
// A row exists only while the generation is active: it is created when the
// generation is claimed and deleted once the covered history is pruned. A
// crashed or interrupted run therefore leaves a row behind, which is how a
// restart finds the generation and resumes it from database facts.
type CompactionInfo struct {
	// ID is the unique ID of the generation.
	ID types.ID `bson:"_id"`

	// ProjectID is the ID of the project that the document belongs to.
	ProjectID types.ID `bson:"project_id"`

	// DocID is the ID of the document being compacted.
	DocID types.ID `bson:"doc_id"`

	// ServerSeq is the immutable server sequence the generation is pinned
	// to. Every replay, the candidate snapshot and the prune point use this
	// value; changes that arrive after the claim are never part of it.
	ServerSeq int64 `bson:"server_seq"`

	// Lamport is the lamport timestamp of the last stored change at
	// ServerSeq when the generation was claimed.
	Lamport int64 `bson:"lamport"`

	// VersionVector is the version vector of the last stored change at
	// ServerSeq when the generation was claimed.
	VersionVector time.VersionVector `bson:"version_vector"`

	// Status is the current phase of the generation.
	Status CompactionStatus `bson:"status"`

	// Snapshot holds the compressed candidate snapshot when it fits the
	// record. It is nil when the body is stored externally.
	Snapshot []byte `bson:"snapshot,omitempty"`

	// HasExternalBody means the candidate snapshot is stored in the
	// companion bodies collection rather than inline.
	HasExternalBody bool `bson:"has_external_body"`

	// LiveSize is the live size of the rebuilt document, in bytes.
	LiveSize int64 `bson:"live_size"`

	// CreatedAt is when the generation was claimed.
	CreatedAt gotime.Time `bson:"created_at"`

	// UpdatedAt is when the generation last moved to a new status.
	UpdatedAt gotime.Time `bson:"updated_at"`
}

// RefKey returns the refKey of the document this generation compacts.
func (i *CompactionInfo) RefKey() types.DocRefKey {
	return types.DocRefKey{
		ProjectID: i.ProjectID,
		DocID:     i.DocID,
	}
}

// DeepCopy returns a deep copy of the CompactionInfo.
func (i *CompactionInfo) DeepCopy() *CompactionInfo {
	if i == nil {
		return nil
	}

	return &CompactionInfo{
		ID:              i.ID,
		ProjectID:       i.ProjectID,
		DocID:           i.DocID,
		ServerSeq:       i.ServerSeq,
		Lamport:         i.Lamport,
		VersionVector:   i.VersionVector.DeepCopy(),
		Status:          i.Status,
		Snapshot:        append([]byte(nil), i.Snapshot...),
		HasExternalBody: i.HasExternalBody,
		LiveSize:        i.LiveSize,
		CreatedAt:       i.CreatedAt,
		UpdatedAt:       i.UpdatedAt,
	}
}
