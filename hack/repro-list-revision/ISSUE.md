# List can return a revision above an uncommitted write; informers then miss it permanently (reproduced on PostgreSQL, single node)

## Summary

A range read with `revision=0` reports `MAX(id)` as its header revision. Rows commit out of id order, so that header can sit above a write that is not yet visible to the query. The response then claims to be the snapshot at revision R while missing a write with revision < R.

A Kubernetes reflector lists at R and then watches from R+1, so it never receives that write. Its cache stays wrong until the next relist. The watch catch-up path has the same shape: `After()` takes `MAX(id)` from its own query and filters the broadcast stream at that revision.

This is the scenario described in #676, which was closed without comment. That report was theoretical and MySQL-only. This one adds a black-box reproduction on PostgreSQL with a single kine server and no HA, using only the etcd v3 API. Related but distinct: #731, which is about progress notifications.

## Environment

- kine at `b2598a7` (master as of 2026-10-07), single process.
- PostgreSQL 16.15 on localhost, default settings.
- `--endpoint postgres://…`, everything else default.

## Reproduction

The harness is [`hack/repro-list-revision/main.go`](https://github.com/nmarasoiu/kine/blob/claude/elegant-franklin-a1wbrd/hack/repro-list-revision/main.go), a self-contained Go program that uses `go.etcd.io/etcd/client/v3` only:

- N writers `Put` write-once keys concurrently and record each acknowledged revision.
- A checker repeatedly lists the prefix. For every list at header revision R, every acknowledged write with revision ≤ R must be present. This is etcd's snapshot guarantee.
- 20 "informers" each list at R, watch from R+1, and apply events to a local map. After writes stop and 3 s pass, each map must equal a fresh list.

```
go run ./hack/repro-list-revision -endpoint 127.0.0.1:2379 -writers 16 -duration 15s
```

## Results

Every run on unmodified master failed:

| writers | runs | snapshot violations | permanently stale informer keys |
|---|---|---|---|
| 64 | 3 | 43, 58, 32 | 25, 41, 3 |
| 16 | 2 | 29, 37 | 4, 12 |
| 4 | 2 | 1, 3 | 2, 3 |

Example output:

```
SNAPSHOT VIOLATION: list at rev 24271 shows /repro/…/w015/k20 at mod_revision 0, but a write at rev 24265 (<= 24271) was acknowledged
INFORMER 13 (listed at rev 42189) is permanently stale: /repro/…/w001/k312 cached at mod_revision 0, truth 42188
```

The row exists in the table with the acknowledged id. It was just not yet committed when the list's statement took its snapshot.

## Mechanism

1. Insert A gets id 101, and insert B gets id 102. B commits first. Sequence ids are allocated at insert time and become visible at commit.
2. A List runs. In its statement snapshot, `MAX(id) = 102` and row 101 is invisible, so it returns header revision 102 without A.
3. A commits. The poll loop's gap handling delivers 101 correctly to the broadcaster. A watcher started from 103, or one that filters at 102 (`EventBatch.After(rev)` in `LogStructured.Watch`), drops it.

The poll loop already maintains the correct boundary: the highest revision it has passed contiguously, with gaps waited out or filled. List, `After()`, and `CurrentRevision()` don't use it.

## Prototype fix (for discussion, not a PR)

Commit [on our fork](https://github.com/nmarasoiu/kine/tree/claude/elegant-franklin-a1wbrd), confined to `pkg/logstructured/sqllog/sql.go`:

- The poll loop publishes its contiguous high-water mark (`polledRev`).
- `CurrentRevision()` reads `MAX(id)`, waits until the poll loop has passed it, then returns it.
- `List`/`ListStream` with `revision=0` read at that settled revision instead of "current", and report it.
- `After()` truncates its events to the settled revision and reports that revision, so later events come from the poll stream.

With this change the same harness reports **0 violations and 0 stale informers** across 64, 16, and 4 writers, and `go test ./pkg/...` passes.

Costs and caveats:

- Reads with `revision=0` now wait up to one poll round-trip. At 64 writers, throughput in this harness dropped noticeably. A real fix would probably wake the waiters from the poll loop instead of sleeping in 1 ms steps.
- The `key == ""` (all keys) list path is left on the old query in the prototype.
- The poll loop's "skip after 1 s" fallback still breaks the invariant, as it already does today.

## Questions for maintainers

1. Is waiting on the poll watermark for `revision=0` reads acceptable, or would you rather compute a "safe" revision in SQL, for example from `pg_snapshot_xmin` on Postgres?
2. Should this fold into the #731 rework, or be fixed separately in sqllog?

Side note, unrelated: `pkg/drivers/pgsql/pgsql.go` sets `FillRetryDuration = time.Millisecond + 5`, which is 1 ms plus 5 ns. It looks like `5 * time.Millisecond` was intended.
