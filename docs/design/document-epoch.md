---
title: document-epoch
target-version: 0.7.3
---
# Document Epoch

> **Update (atomic snapshot compaction).** Compaction no longer resets
> `serverSeq` or bumps the epoch. It pins the current server sequence,
> publishes a snapshot at that point and prunes only the covered history;
> changes appended concurrently keep their sequences and clients behind the
> new snapshot floor receive the snapshot instead of a change range. The
> epoch fields and checks below remain in place for compatibility but are
> no longer triggered by compaction. See
> [Atomic snapshot compaction](#atomic-snapshot-compaction-current).

## Summary

When an administrator force-compacts a document through the Dashboard, the
server resets its `serverSeq` (to 0 or 1, depending on whether a compacted
change is stored) and purges all prior changes and snapshots.
Clients that were connected before the compaction still hold checkpoints
referencing the old `serverSeq` (e.g., 17459). On the next PushPull these
clients receive an `ErrInvalidServerSeq` error and become permanently stuck —
they cannot sync and there is no way to distinguish a compaction-induced reset
from a genuine protocol violation.

This document proposes adding an **epoch** field to `DocInfo` and
`ClientDocInfo`. The epoch is a monotonically increasing integer that
increments on every compaction. By comparing epochs at the start of PushPull,
the server can detect stale clients and return a dedicated error that tells the
client to reattach.

### Goals

- Detect, during PushPull, whether a client's checkpoint belongs to a
  pre-compaction epoch.
- Return a dedicated `ErrEpochMismatch` error so clients can distinguish
  compaction resets from protocol violations.
- Allow clients to self-heal by performing an automatic detach → re-attach
  cycle upon receiving the error.
- Maintain backward compatibility with existing documents that have never been
  compacted.

### Non-Goals

- Fixing CRDT tree integrity issues that may occur during compaction itself
  (e.g., "node not found" errors).
- Resolving revision label collisions caused by `serverSeq` resets.
- Automatically recovering unsent local changes on the client side.

## Proposal Details

### What is an Epoch?

An epoch is a monotonically increasing integer that represents a document's
"generation." Every time a document is compacted, all of its changes and
snapshots are purged, `serverSeq` is reset (to 0 or 1), and the epoch increments by 1.

Think of it like a book's edition number. Page 153 in the 1st edition
(epoch 0) and page 153 in the 2nd edition (epoch 1) contain entirely different
content. If a reader asks the bookstore for "everything after page 153 of the
1st edition," the bookstore must respond: "a new edition has been published —
please start over from the beginning."

The same applies here. A `serverSeq` is only meaningful within its epoch. Once
the epoch changes, all prior checkpoints are invalidated and clients must
reattach to receive the compacted document.

### Schema Changes

Add an `Epoch` field to both `DocInfo` and `ClientDocInfo`:

```go
type DocInfo struct {
    // ...existing fields...
    Epoch int64 `bson:"epoch"`
}

type ClientDocInfo struct {
    // ...existing fields...
    Epoch int64 `bson:"epoch"`
}
```

The default value of `Epoch` is 0, which means existing documents and clients
require no migration.

### Flow

**Attach:** When a client attaches to a document, the server records the
document's current epoch in the client's `ClientDocInfo.Epoch`.

```text
Client → AttachDocument → Server
Server: ClientDocInfo.Epoch = DocInfo.Epoch
```

**PushPull (normal):** The server compares epochs before processing changes.
If they match, the existing logic runs unchanged.

```text
Client(epoch=1) → PushPullChanges → Server(doc.epoch=1)
Server: client.Epoch == doc.Epoch → proceed as usual
```

**Compaction:** When the admin compacts a document, the server increments the
document's epoch alongside the `serverSeq` reset.

```text
Admin → CompactDocumentByAdmin → Server
Server: doc.Epoch++ (1 → 2), doc.ServerSeq → 0/1
```

**PushPull (after compaction):** A client from the previous epoch attempts to
sync. The server detects the epoch mismatch and returns a dedicated error.

```text
Client(epoch=1) → PushPullChanges → Server(doc.epoch=2)
Server: client.Epoch(1) != doc.Epoch(2) → return ErrEpochMismatch
Client: detach → re-attach (receives new epoch)
```

### Epoch Check Placement

The epoch comparison must happen in **two places**: `pushPack` (before
changes are stored) and `preparePack` (before pulling changes).

#### pushPack (push-side guard)

`PushPull` executes `pushPack` before `preparePack`. If the epoch check
only exists in `preparePack`, stale-epoch changes are already inserted into
the in-memory `changeCache` by the time the mismatch is detected. These
cached changes reference pre-compaction CRDT node IDs that no longer exist
in the post-compaction snapshot, causing `ErrNotApplicableDataType` on
subsequent attach or compaction attempts.

To prevent this, `pushPack` must check the epoch **before** calling
`CreateChangeInfos`:

```go
func pushPack(...) {
    // ...filter already-pushed changes...

    if len(pushables) > 0 {
        docInfo, err := be.DB.FindDocInfoByRefKey(ctx, docKey)
        if err != nil {
            return ..., err
        }
        if clientDocInfo := clientInfo.Documents[docKey.DocID];
            clientDocInfo != nil && clientDocInfo.Epoch != docInfo.Epoch {
            // Discard stale-epoch changes silently. preparePack will
            // return ErrEpochMismatch (or allow detach) downstream.
            pushables = nil
        }
    }

    docInfo, cpAfterPush, err := be.DB.CreateChangeInfos(...)
    // ...
}
```

When the epoch mismatches, `pushPack` discards the pushable changes and
proceeds with an empty push. This allows `PushPull` to continue into
`pullPack`, where `preparePack` handles the epoch mismatch appropriately:
returning `ErrEpochMismatch` for normal syncs, or allowing detach to
proceed.

#### preparePack (pull-side guard)

The existing epoch check in `preparePack` remains unchanged. It compares
epochs before the `serverSeq` validation and returns `ErrEpochMismatch`
to the client:

```go
func preparePack(...) {
    if clientEpoch != docEpoch {
        return ErrEpochMismatch
    }

    if initialServerSeq < reqPack.Checkpoint.ServerSeq {
        return ErrInvalidServerSeq
    }
    // ...
}
```

### Code Change Points

1. **`DocInfo`** (`server/backend/database/doc_info.go`): Add `Epoch` field.
2. **`ClientDocInfo`** (`server/backend/database/client_info.go`): Add `Epoch`
   field.
3. **`CompactChangeInfos`** (`server/backend/database/mongo/client.go`):
   Increment `doc.Epoch` alongside the `serverSeq` reset.
4. **`AttachDocument`**: Set `ClientDocInfo.Epoch = DocInfo.Epoch` on attach.
5. **`pushPack`** (`server/packs/pushpull.go`): Add epoch comparison before
   `CreateChangeInfos` to prevent stale-epoch changes from polluting the
   in-memory cache.
6. **`preparePack`** (`server/packs/pushpull.go`): Add epoch comparison before
   the `serverSeq` check.
7. **Error definition**: Add `ErrEpochMismatch` with an appropriate gRPC
   status code mapping.
8. **SDKs** (JS, iOS, Android): Handle `ErrEpochMismatch` by performing
   detach → re-attach.

### Backward Compatibility

- `Epoch` defaults to 0 for both `DocInfo` and `ClientDocInfo`.
- Documents that have never been compacted have `DocInfo.Epoch = 0`.
- Existing clients have `ClientDocInfo.Epoch = 0`.
- Since 0 == 0, the epoch check passes and existing behavior is unaffected.

### Risks and Mitigation

| Risk | Mitigation |
|------|------------|
| Client has unsent local changes when it receives `ErrEpochMismatch`; reattach discards them | Expose `ErrEpochMismatch` as an SDK event so the application can notify the user before reattaching |
| Older SDK versions do not recognize `ErrEpochMismatch` | They fall through to the existing `ErrInvalidServerSeq` behavior — no worse than today |
| Epoch counter overflows | `int64` supports over 9 × 10¹⁸ compactions — practically unlimited |
| Stale-epoch push pollutes in-memory cache before `preparePack` detects mismatch | `pushPack` checks epoch before `CreateChangeInfos` and discards stale changes (see "Epoch Check Placement") |

## Atomic snapshot compaction (current)

### Why the epoch reset was replaced

Resetting `serverSeq` and forcing every connected client through
`ErrEpochMismatch` → detach/re-attach was disruptive, and the underlying
implementation wrote a snapshot and pruned history in two separate steps.
A snapshot write that succeeded while the prune failed (or a PushPull that
appended between the two) left clients reading a snapshot whose sequence no
longer matched their history.

Compaction now publishes a snapshot at the current head and prunes the
covered history in an ordered, recoverable sequence, without touching
`serverSeq` or the epoch.

### Generation state machine

A **compaction generation** is a persisted row (the `compactions`
collection / memory table), one active row per document at most. A unique
index on `(project_id, doc_id)` is the cross-node guarantee that only one
generation runs at a time: a housekeeping cycle, an admin force and a
compaction RPC on another node all converge on the same generation instead
of starting a second one.

```text
claim (pin serverSeq + lamport + version vector)
  -> candidate (replay changes up to the pinned seq, validate, store invisibly)
  -> published (atomic upsert of the snapshot row + document floor advance)
  -> prune (delete changes <= pinned seq and older snapshots)
  -> delete generation
```

- **Pin:** the claim reads the document head and the last stored change at
  that sequence. The generation's `serverSeq`, lamport and version vector
  never change afterwards; a PushPull that appends after the claim is not
  part of the generation.
- **Isolated candidate:** the candidate snapshot is rebuilt by replaying
  only the changes within the pinned point and stored in a separate
  collection. Published-snapshot readers (`FindClosestSnapshotInfo`) cannot
  see it. Content is validated by a YSON rebuild/round-trip before saving.
  A build or size failure deletes the generation and the candidate; the
  last readable snapshot is untouched.
- **Atomic publish:** the snapshot row upsert is the reader-visible commit.
  The document's `snapshot_server_seq` floor advances to the pinned seq
  without constraining `server_seq`, so concurrent pushes keep their
  sequences.
- **Prune:** only changes at or below the pinned seq (covered by the
  published snapshot) and older snapshots are deleted; appended changes are
  never touched.
- **Idempotency and recovery:** each stage re-checks database facts
  (candidate stored? snapshot row + floor present? covered changes gone?)
  before writing, so an uncertain save/publish/prune response simply
  resumes the same generation. Rows left by a crashed process are reclaimed
  by a recovery pass at the start of every housekeeping cycle.

### Read compatibility

- `DocInfo.SnapshotServerSeq` is the history floor.
- A pull whose client checkpoint is at or below the floor receives the
  published snapshot (the floor lookup reads the database directly, so it is
  visible on every cluster node immediately).
- Document rebuilds (`BuildInternalDocForServerSeq`) ignore a cached
  document below the floor and rebuild from the closest published snapshot.
- Presence-only sequences keep their semantics: they are not pruned as
  operations, and a document whose stored history contains no operation
  change compacts as a no-op.
- `--housekeeping-disable-compaction` turns automatic compaction off; an
  interrupted generation is still recovered, and admin-forced compaction
  remains available.
