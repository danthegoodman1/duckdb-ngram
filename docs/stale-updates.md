# Rowid safety, updates, and fail-closed recovery

**Status: implemented in metadata format 3 (2026-08-11).** Updates, deletes,
checkpoint vacuum, WAL replay, and reopened databases cannot silently make an
ngram query omit a matching row. When the extension cannot prove that indexed
rowids are still safe, it scans or refuses before returning.

This note describes the v1.5.5-specific mechanism, its proof, and its costs.
The implementation is deliberately smaller than a dirty-rowgroup overlay: one
zero-posting native guard plus one permanent uncertainty bit per ngram index.

## The invariant

Postings cover rowids through the metadata high-water mark. Rows after that
mark, including transaction-local inserts, are read by a live tail scan. Every
posting candidate is fetched under the caller's transaction and rechecked
against the real predicate.

That is exhaustive if both conditions hold:

1. a live rowid at or below the mark never changes to a new value in place or
   moves to another rowid; and
2. an append never reuses a rowid at or below a range the guard has already
   observed.

Each ngram index therefore owns a DuckDB `NGRAM_ROWID_GUARD` index. It stores no
keys or postings. Its physical column dependencies make DuckDB rewrite updates
of covered columns as delete plus insert, and its non-ART type permanently
disables v1.5.5's ART-only moving-vacuum path. The guard persists four facts:

- the greatest append rowid it has observed;
- a permanent `unsafe_reuse` latch;
- a random incarnation token, also recorded by ngram metadata; and
- a checkpoint-iteration seal and compatibility bit.

The live predicate recheck remains authoritative. The guard only proves that
postings are a candidate superset; it never makes postings a source of truth.

## Atomic creation

The hard case is a transaction whose write plan predates guard creation. A
native index created later cannot retroactively change that plan. The first
ngram index on an unprotected column uses this barrier:

1. Mark the creating DuckDB transaction as `CREATE_INDEX`, acquire the external
   checkpoint lock exclusively, and reject active or recently committed
   writers. An explicit transaction that already performed global update,
   delete, or catalog work is rejected early; transaction-local inserts remain
   supported.
2. Add and immediately drop a UUID-named nullable column. DuckDB replaces the
   base `DataTable`, marking every pre-existing storage snapshot `ALTERED`.
   Writers blocked behind the exclusive lock can no longer commit against that
   old storage.
3. Create the scan-free rowid guard while still holding the exclusive lock. Its
   build state holds the table append lock from the allocated-row baseline read
   through `PhysicalCreateIndex::AddIndex`, closing the insert gap between
   baseline and physical installation. The guard mints its own token.
4. `__ngram_creation_finish` validates the exact fresh guard and replacement
   storage, copies the token, and releases the exclusive lock. The potentially
   long postings build then runs with only the ordinary Phase 10 vacuum fence.

Reads continue during the build. Writers whose snapshots overlap the barrier
may fail with DuckDB's altered-table transaction conflict and must retry after
the create commits. A staged writer whose rollback restores the old storage
makes the creator's commit fail; guard and shadows are not published, and a
fresh retry is safe.

The ADD/DROP barrier invalidates DuckDB's reservoir sample. It also cannot run
through every pre-existing index dependency. Two narrowly proven protectors
avoid it:

- an exact format-3 ngram guard may protect another column only if that guard's
  persisted physical dependency set contains the new target; the new guard
  copies no broader coverage than that proven set;
- an explicit, non-internal native ART on the target may protect it only when
  its committed catalog timestamp is strictly older than every active
  transaction. That fallback creates a target-only guard.

The normal barrier-created guard depends on every physical `VARCHAR` column
that exists at creation time. This lets later ngram indexes on those columns
reuse the protection proof without another ADD/DROP barrier.

## Updates, deletes, and vacuum

- Updating any guard-covered column, including `NULL` and equal-value cases,
  becomes delete plus insert. The old posting is a harmless false positive that
  recheck discards; the new rowid is in the live tail until refresh.
- Updating an uncovered column may remain in place. That is safe because it
  cannot change the indexed value. A normal broad guard covers all `VARCHAR`
  columns that existed when it was created; a native-ART fallback may cover
  only the indexed target.
- Delete alone is safe. Visibility and recheck remove deleted candidates.
- The non-ART guard makes `CanRebuildExistingIndexesAfterVacuum` false even
  when `vacuum_rebuild_indexes` is enabled, so vacuum cannot move a surviving
  live rowid.
- DuckDB may still discard fully deleted *trailing* row groups. That moves no
  live row. If a later committed append reuses any rowid at or below the
  guard's maximum, `unsafe_reuse` latches before the append becomes visible.

Guard mutation is not rollback-aware, so the guard tells a reused range from a
retried one by the checkpoint that reuse requires. Each time an append advances
the maximum, the guard records the database header's checkpoint iteration. An
append that starts at or below the maximum latches `unsafe_reuse` only when
that iteration has changed since the advance, or when either value is unknown:
an in-memory database (its checkpoints vacuum but leave no header counter), a
checkpoint in progress, or a guard bound from disk or WAL. A commit that a later
UNIQUE index rejects has advanced the maximum before the host rolls its rows
back; the next append starts at the same rowid, finds the same iteration, and
leaves the guard usable. A vacuum that reclaims trailing rowids needs the
exclusive vacuum lock, which every inserting transaction holds shared from its
first insert through its commit, so it always falls strictly between two
appends and always writes a new header iteration.

## Query and maintenance behavior

Guard uncertainty includes a missing or replaced name/token, wrong type or
column dependency, incompatible source/version, unbound buffered WAL replay,
checkpoint-seal mismatch, guard maximum behind the ngram high-water mark, and
the unsafe latch.

| Entry point | On guard uncertainty |
| --- | --- |
| `ngram_search` | one exhaustive live-table scan with the original matching semantics |
| transparent `NGRAM_INDEX_SCAN` | one ordinary sequential-scan fallback with the original predicate |
| `ngram_candidates` | every visible rowid at or below the recorded high-water mark; its caller still owns the disjoint tail |
| `ngram_refresh` / `ngram_compact` | refuse with a rebuild-required error |
| `ngram_index_stats` | report the reason in `stale_reason` |

There is no list of dirty groups or repeated overlay work. One boolean selects
one scan, so 1, 10, 100, or 1,000 uncertain events have the same query-plan
shape and bounded state. Proven table/meta identity staleness also makes both
full-result paths scan; the candidate-only and maintenance APIs raise because
they have no equivalent exhaustive fallback. A present malformed shadow object
is corruption and always propagates rather than being treated as unavailable.

Recovery is explicit:

```sql
PRAGMA drop_ngram_index('logs', 'message');
PRAGMA create_ngram_index('logs', 'message');
```

Public drop validates metadata layout and catalog OID at execution time. For a
format-3 index it drops the recorded guard only when name, type, table, column,
and token prove the incarnation; a genuinely missing guard is recoverable.
Format-2 metadata is drop-only and is accepted only when it truly has no guard
columns and no guard covering the target column on the base table.

## WAL, checkpoints, and reopen

`SerializeToWAL` records the guard's live state at creator commit. That includes
creator-local and external rows committed before publication; later WAL
transactions replay through DuckDB's buffered unbound-index appends. Disk
checkpoint serialization records the current state plus the database header's
next checkpoint iteration.

An extension-free or context-free checkpoint can copy an unbound index without
applying its buffered replays. Binding carries the persisted seal as pending;
the next query validation compares it while holding a shared checkpoint lock,
and a bound checkpoint compares it under the checkpoint's exclusive lock before
writing a new seal. Either path treats a mismatch as unsafe; a bound guard
latches it permanently. This prevents a crash, extension-free reopen, and
shutdown checkpoint from laundering a reused-rowid event back to “clean,”
without racing DuckDB's non-atomic header iteration counter.

To avoid needless fallback on ordinary lifecycle paths, compatible guards are
bound best-effort when the extension loads before any user connection and when
the last connection closes without an active transaction. Contextful explicit,
forced, and automatic checkpoints also bind indexes themselves. A `DETACH`
that checkpoints an untouched unbound guard has no safe callback; it may
conservatively require rebuild on the next attach.

Malformed non-identity persisted guard options (source, version, seal, or
state) create a bound quarantine guard with
`protection_compatible=false` and `unsafe_reuse=true` instead of leaving
DuckDB's v1.5.5 bind state stuck at `BINDING`. Queries scan, future protection
proofs reject it, ordinary extension-loaded writes remain usable, and public
drop can recover it while the recorded token remains readable and matches.
A missing or wrong-typed identity token must make exact drop refuse. Every use
of checkpoint internals is gated by the exact v1.5.5 version/source pin.

## API and operational costs

- **Load the extension before writing guarded tables.** Stock DuckDB can read
  them, but guarded-table DML and guard-touching ALTER are unsupported without
  `ngram`. In the pinned host, INSERT and UPDATE fail on the unknown custom
  type, DELETE can busy-spin in index binding and must be terminated, and
  guarded-column DROP/rename is refused. An unrelated ADD COLUMN works.
- A normal broad guard makes updates to every then-existing `VARCHAR` column
  full delete-plus-insert rewrites. Bulk INSERT callback work is O(1) per
  vector: the guard stores no key data and handles the sequence rowid vector
  without flattening or per-row iteration.
- DELETE and rollback cleanup on a wide guarded table may fetch every
  guard-covered `VARCHAR`: v1.5.5 unions index dependencies before calling the
  value-independent `TryDelete`. Multiple broad guards deduplicate that union.
- ADD COLUMN is safe for existing ngram indexes, but a newly added `VARCHAR` is
  outside their dependency sets. Indexing that new column requires an old
  guard that already covers it, an old-enough explicit ART on the target, or
  dropping/rebuilding the table's ngram indexes.
- DuckDB index dependencies refuse table/column rename and many DROP/ALTER
  operations while a guard exists. Drop the ngram indexes first.
- Creation invalidates the reservoir sample and can make overlapping writers
  retry. Quiescing writes around a first build avoids that API cost.
- The native guard queries the host's built-in `pragma_version()` at load and
  accepts only DuckDB v1.5.5 source `d8cdaa33` (local build form) or
  `d8cdaa33fd` (official binary form). Query and maintenance paths fail closed
  on mismatch; the generic drop validator stays available when the extension
  is loadable so an exact incompatible guard can be removed.

A small release characterization (`threads=1`, `/usr/bin/time`) makes the
shape concrete. Starting from 200,000 rows, appending 500,000 rows took 0.21 s
unguarded, 0.22 s with one broad guard, and 0.22 s with two; peak RSS was
60.1/60.2/60.5 MB. Updating 100,000 covered rows took 0.04/4.73/4.76 s and
40.0/47.9/48.0 MB respectively. The material cost is DuckDB's delete-plus-
insert rewrite; the sequence-vector fast path makes another zero-data guard
negligible.

For the first-build barrier, a cold 57,946,112-byte table with 200,000 rows and
16 varied `VARCHAR` columns was compared with the same file copied byte-for-
byte. Cold open plus `count(*)` and ADD UUID column + DROP it + create a
16-column guard both rounded to 0.01 s; peak RSS was 26,372 versus 26,468 KiB,
and the checkpointed file size stayed exactly 57,946,112 bytes. These are
single-run mechanism measurements, not portable throughput claims. “Cold” here
means a fresh DuckDB process; the OS page cache was not flushed. Run from the
repository root, this compact recipe reproduces the data shapes and measured
statements (the wide-table number measures only the barrier and zero-data
guard, not the postings build):

```sh
build/release/duckdb /tmp/ng-base.db -c "SET threads=1; CREATE TABLE t AS SELECT i, md5(i::VARCHAR) s, md5((i+1)::VARCHAR) note FROM range(200000) r(i); CHECKPOINT"
cp /tmp/ng-base.db /tmp/ng-u-a.db; cp /tmp/ng-base.db /tmp/ng-u-u.db; cp /tmp/ng-base.db /tmp/ng-g1-base.db; cp /tmp/ng-base.db /tmp/ng-g2-base.db
build/release/duckdb /tmp/ng-g1-base.db -c "LOAD ngram; CREATE INDEX g1 ON t USING NGRAM_ROWID_GUARD(s,note)"; build/release/duckdb /tmp/ng-g2-base.db -c "LOAD ngram; CREATE INDEX g1 ON t USING NGRAM_ROWID_GUARD(s,note); CREATE INDEX g2 ON t USING NGRAM_ROWID_GUARD(s,note)"
cp /tmp/ng-g1-base.db /tmp/ng-g1-a.db; cp /tmp/ng-g1-base.db /tmp/ng-g1-u.db; cp /tmp/ng-g2-base.db /tmp/ng-g2-a.db; cp /tmp/ng-g2-base.db /tmp/ng-g2-u.db
/usr/bin/time -f 'wall=%e user=%U sys=%S maxrss_kb=%M' build/release/duckdb /tmp/ng-u-a.db  -c "SET threads=1; INSERT INTO t SELECT i, md5(i::VARCHAR), md5((i+1)::VARCHAR) FROM range(200000,700000) r(i)"
/usr/bin/time -f 'wall=%e user=%U sys=%S maxrss_kb=%M' build/release/duckdb /tmp/ng-g1-a.db -c "LOAD ngram; SET threads=1; INSERT INTO t SELECT i, md5(i::VARCHAR), md5((i+1)::VARCHAR) FROM range(200000,700000) r(i)"
/usr/bin/time -f 'wall=%e user=%U sys=%S maxrss_kb=%M' build/release/duckdb /tmp/ng-g2-a.db -c "LOAD ngram; SET threads=1; INSERT INTO t SELECT i, md5(i::VARCHAR), md5((i+1)::VARCHAR) FROM range(200000,700000) r(i)"
/usr/bin/time -f 'wall=%e user=%U sys=%S maxrss_kb=%M' build/release/duckdb /tmp/ng-u-u.db  -c "SET threads=1; UPDATE t SET note=note||'x' WHERE i<100000"
/usr/bin/time -f 'wall=%e user=%U sys=%S maxrss_kb=%M' build/release/duckdb /tmp/ng-g1-u.db -c "LOAD ngram; SET threads=1; UPDATE t SET note=note||'x' WHERE i<100000"
/usr/bin/time -f 'wall=%e user=%U sys=%S maxrss_kb=%M' build/release/duckdb /tmp/ng-g2-u.db -c "LOAD ngram; SET threads=1; UPDATE t SET note=note||'x' WHERE i<100000"
build/release/duckdb /tmp/ng-wide.db -c "SET threads=1; CREATE TABLE w AS SELECT i, md5((i+0)::VARCHAR)c00, md5((i+1)::VARCHAR)c01, md5((i+2)::VARCHAR)c02, md5((i+3)::VARCHAR)c03, md5((i+4)::VARCHAR)c04, md5((i+5)::VARCHAR)c05, md5((i+6)::VARCHAR)c06, md5((i+7)::VARCHAR)c07, md5((i+8)::VARCHAR)c08, md5((i+9)::VARCHAR)c09, md5((i+10)::VARCHAR)c10, md5((i+11)::VARCHAR)c11, md5((i+12)::VARCHAR)c12, md5((i+13)::VARCHAR)c13, md5((i+14)::VARCHAR)c14, md5((i+15)::VARCHAR)c15 FROM range(200000) r(i); CHECKPOINT"; cp /tmp/ng-wide.db /tmp/ng-wide-guard.db
/usr/bin/time -f 'wall=%e user=%U sys=%S maxrss_kb=%M' build/release/duckdb /tmp/ng-wide.db -c "SET threads=1; SELECT count(*) FROM w"
/usr/bin/time -f 'wall=%e user=%U sys=%S maxrss_kb=%M' build/release/duckdb /tmp/ng-wide-guard.db -c "LOAD ngram; SET threads=1; ALTER TABLE w ADD COLUMN __ngram_barrier_bench BOOLEAN; ALTER TABLE w DROP COLUMN __ngram_barrier_bench; CREATE INDEX gw ON w USING NGRAM_ROWID_GUARD(c00,c01,c02,c03,c04,c05,c06,c07,c08,c09,c10,c11,c12,c13,c14,c15); CHECKPOINT"
stat -c '%s bytes, %b blocks' /tmp/ng-wide.db /tmp/ng-wide-guard.db
```

These costs replace the former probabilistic row witnesses and blanket
user-managed ART requirement. Format 3 stores no witnesses and maintenance
performs no random row fetches: the guard/barrier proof is deterministic, and
any uncertainty has an exhaustive fallback. An explicit ART remains an
optional creation protector for the added-column/dependency cases above.
