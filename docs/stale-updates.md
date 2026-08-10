# Rows changed by in-place `UPDATE`

**Status: deferred (2026-08-10).** The shipped behaviour is documented in the
README under "Not detected — misses only, never wrong rows" and "When to
rebuild": an `UPDATE` to an indexed column requires a rebuild, and so does a
checkpoint that vacuums deleted rows. This note records what the gap is, what
DuckDB v1.5.5 offers for closing it, which design would close it, and what
should trigger revisiting the decision.

## The gap

DuckDB v1.5.5 updates rows in place: the rowid survives, the content changes.
The index holds postings built from the old value, and the row sits below the
high-water mark, so refresh — which scans the tail — walks straight past it. A
query whose needle matches the new value but shares no trigram with the old one
never gets that rowid as a candidate. The row is missing from the answer.

Two properties bound the damage:

- **Recheck makes false positives impossible.** Trigram intersection produces
  candidates; the real predicate is applied to each against the live table. A
  stale index can return a short answer, never a wrong row.
- **A rebuild is a total repair.** `drop_ngram_index` + `create_ngram_index`
  re-reads the table as it stands, so nothing is permanently damaged. Neither
  `ngram_refresh` (tail only) nor `ngram_compact` (folds generations without
  re-reading the base table) helps.

What makes this the weak point of an otherwise fail-loud design is detection.
Every other provable stale state — a vacuumed table, a re-created table, a
renamed column — makes `ngram_search` raise a rebuild-required error. An
in-place `UPDATE` is caught only when it happens to touch one of the 32 recorded
row witnesses, which for a small update on a large table is unlikely. The rest
of the time the index answers, slightly short, indefinitely.

`ngram_auto_accelerate` is off by default because of this: a plain `LIKE`
rewritten into a path that can miss rows is not an acceptable default. An
explicit `ngram_search` call is where that trade-off belongs.

The companion case is a checkpoint that vacuums deleted rows. `DELETE` alone is
safe — transaction visibility hides the rows and recheck discards their
postings — but vacuuming relocates surviving rows to new rowids, which
invalidates the mapping the index recorded.

## Signals v1.5.5 exposes

Two mechanisms in the vendored source would support automatic detection. Both
are reachable in-process from C++ far more cheaply than through their SQL
surface (`PRAGMA storage_info` materialises a row per column segment with
strings and stats; walking the row-group collection directly does not).

**`ColumnData::HasChanges(start_row, end_row)`**
(`duckdb/src/storage/table/column_data.cpp:59`), backed by
`UpdateSegment::HasUpdates`. Reports pending updates over a rowid range at
vector granularity (2,048 rows). It reflects the in-memory update chain, so a
checkpoint merges the updates into base data and clears it — precise, but
transient. Surfaced to SQL as the `has_updates` column of `PRAGMA storage_info`.

**Per-row-group `block_id`** (also in `PRAGMA storage_info`). `RowGroup::WriteToDisk`
reuses a row group's existing metadata when `can_reuse_metadata && !HasChanges()`
(`duckdb/src/storage/table/row_group.cpp:1322`), so a persistent row group that
did not change keeps its blocks across checkpoints. An unchanged `block_id`
therefore *proves* unchanged content. Row-group granularity (~122,880 rows),
survives restarts — the durable counterpart to the signal above.

The two are complementary: `HasChanges` covers updates that have not been
checkpointed yet, `block_id` covers everything after.

## Options considered

| | Approach | Result |
| --- | --- | --- |
| A | Document the requirement; rebuild is the remedy | Shipped. Silent misses remain possible when the requirement is not followed. |
| B | Detect changed rows and make `ngram_search` refuse | Contract stays "rebuild after update", but the system enforces it. |
| C | Detect changed rows and scan them: candidates = index ∪ tail scan ∪ dirty-range scan | Queries stay correct *and* complete with no rebuild. |

## If this is revisited, build C

Both B and C need the same detection machinery, which is the bulk of the work.
They differ in how they tolerate imprecision, and that difference is decisive:

- Under C a false positive costs scan time — a row group is rescanned that did
  not need it.
- Under B a false positive takes the index **offline** and demands a rebuild of
  a table nobody changed.

A refuse-policy therefore needs detection that is precise in both directions;
an overlay only needs it to be conservative, which is far easier to guarantee.
The concrete case that separates them: row groups still transient when the
index was built get new block ids at the first checkpoint with no content
change at all — the common shape of bulk-load-then-build. Under C that is one
wasted rescan. Under B a brand-new index errors on every query until it is
rebuilt, and the rebuild inherits the same problem.

So the detection lands once and feeds the overlay, and the policy becomes a
setting on top of a verdict already proven conservative — `scan` (stay
complete, pay the scan) or `error` (refuse, demand a rebuild).

Repair is surgical rather than a full rebuild. Because `segment_no = rowid >>
SEGMENT_SHIFT`, re-indexing a dirty range is the Phase 8 bounded-refresh path
pointed at a rowid range instead of the tail: delete that range's segment rows,
re-pack, drop the range from the dirty set.

## What it would cost

**Performance** scales with dirtied data volume, not update count, and the
persistent signal's granularity is a whole row group. Estimated against measured
Phase 7/8 figures at a ~90 bytes/row shape: roughly 10 ms to rescan one dirty
row group, so ~10 dirty row groups doubles the 0.133 s warm p95 and ~100 makes
it several times worse. Scattered single-row updates are the pathological case —
1,000 of them across 1,000 distinct row groups dirties ~123 M rows, which is a
full scan. Clustered updates are cheap; prompt surgical repair keeps the dirty
set a queue rather than a permanent state. The worst case is the speed of no
index at all, and even then everything outside the dirty set stays accelerated.

**Coupling** is the durable cost. The extension currently reads rowids and
column values — stable public surface. This design depends on what `block_id`
means and on checkpoint's rewrite rules, and a DuckDB version bump could change
either with the failure mode being missed rows. That needs the churn harness
aimed directly at it plus a test that fails loudly when the internals shift.

**Scope** is a full phase on the scale of Phase 5 or 8: a new shadow table and a
`format_version` bump, per-query baseline caching whose invalidation must not
err stale, and adversarial testing of the detection itself.

## What should trigger revisiting

Any one of these:

1. The workload starts updating indexed column values, rather than appending
   and deleting.
2. `ngram_auto_accelerate` should default to on, so plain `LIKE` is accelerated
   transparently — which requires that a rewritten query cannot miss rows.
3. A miss is observed in production.
