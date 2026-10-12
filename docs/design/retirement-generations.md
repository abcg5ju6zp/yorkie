# Document retirement generations

## Background

Removing a document used to be a single `removed_at` flip with three gaps:

1. Attachments and Watches of offline or disconnected clients were not
   converged. A failed `DocChanged` broadcast, a dropped deactivation fan-out
   or a failed purge left clients and server-side state attached to a document
   whose `removed_at` was already set.
2. The deferred work was not tied to any durable state, so a lost response, a
   node restart or a retried request could not tell whether the removal had
   happened and could not continue it.
3. The same key could be re-attached immediately, starting a new document
   row while the old one was still around. Late pushes, detaches and purges
   keyed by key could then address the wrong generation.

## Decision

One removal fixes one **retirement generation** identified by the document's
`DocID`. The generation moves through three phases:

```text
alive ──RetireDocument──▶ retired (recovery window) ──housekeeping──▶ purged
```

### Claim

`RetireDocument(refKey, force)` claims the generation by setting `removed_at`
exactly once (`UpdateDocInfoStatusToRemoved` is conditional and idempotent).

- `force=false` keeps the wait policy: it fails with `ErrDocumentAttached`
  while another client is attached or attaching.
- `force=true` (administrators) does not wait.
- A repeated call on an already removed document succeeds and keeps the
  original `removed_at`, so retries continue the same generation.

### Block and converge

Once claimed:

- `FindOrCreateDocInfo` refuses the key with `ErrDocumentRetired` for the
  whole recovery window. New attaches and therefore new writes cannot enter
  the generation; a same-key document becomes possible only after purge.
- `pushPack` discards every late change into the frozen generation (and the
  database refuses any change appended to a removed row), so a late push can
  neither extend nor resurrect it. The pull still runs and returns the
  removed flag.
- `ConvergeRetiredDocument` moves every client row still attaching or
  attached onto the generation (`MarkDocumentClientsRemoved`), invalidates
  their cached rows, and closes the document's Watches on every node with a
  final `DocChanged` (`PubSub.CloseDocument`, propagated through the existing
  document cache-invalidation RPC keyed by the `DocRefKey`).
- Watches re-resolve against the retired generation and are rejected.

### Retain and purge

While the generation is retired its row, changes (last server sequence),
snapshot and version vectors stay readable. An offline client pulling from
the `DocID` receives the removed flag; its row converges idempotently, as do
late `DetachDocument` fan-outs from deactivation.

`FinalizeRetiredDocuments` (leader housekeeping, interval from
`Housekeeping.RetirementRetentionInterval`, default 24h; `0s` disables it)
lists rows with `removed_at <= now-retention` and per generation:

1. takes the document lock and re-loads the row by `DocID` and `removed_at`;
2. re-runs convergence and waits until no attachment is reported;
3. closes Watches and releases changes, snapshots, snapshot bodies and
   version vectors;
4. deletes the document row.

A same-key document created afterwards has a new `DocID`, so the purge of the
old candidate cannot reach it.

## Client RPC compatibility

- The client `RemoveDocument` and `DetachDocument` methods keep their existing
  preconditions (`ErrDocumentNotAttached` on detached/removed rows).
- `PushPullChanges` against a retired generation is served (not rejected) so a
  watcher syncing after a force removal learns the result; client-seq
  continuity is not enforced against a generation whose checkpoint the server
  reset during convergence.
- Server-driven convergence (force removal, RemoveOnDetach detach,
  deactivation fan-out) is idempotent.

## See also

- `server/documents/retirement.go`
- `server/packs/pushpull.go` (retirement fence in `pushPack`/`pullPack`)
- `server/backend/pubsub/pubsub.go` (`CloseDocument`)
