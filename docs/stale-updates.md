# Rows changed by in-place `UPDATE`

**Status: deferred (2026-08-10).** The shipped behaviour is documented in the
README under "Not detected — misses only, never wrong rows" and "When to
rebuild": an `UPDATE` to an indexed column requires a rebuild, and so does a
checkpoint that vacuums deleted rows. This note records what the gap is, what
DuckDB v1.5.5 offers for closing it, which design would close it, and what
should trigger revisiting the decision.

A deployment that cannot live with the gap has a remedy that needs no code
here — an ART index on the same column, which makes DuckDB route updates
through the append path this extension already handles exactly. It is
[Option D](#option-d-let-an-art-index-convert-the-updates) below, measured and
verified, and it costs more to build and store than the trigram index it
protects.

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
| D | Carry an ART index on the same column, so DuckDB routes updates through the append path | Closes both halves of the gap today, with no extension code. Costs more to build and store than the trigram index it protects. |

## Option D: let an ART index convert the updates

Available now, needs no change to this extension, and closes the vacuum case as
well as the `UPDATE` case. One declaration per indexed column:

```sql
CREATE INDEX logs_message_idx ON logs(message);
```

**Why it closes the `UPDATE` case.** `TableCatalogEntry::BindUpdateConstraints`
(`duckdb/src/catalog/catalog_entry/table_catalog_entry.cpp:327-336`) converts an
`UPDATE` into a delete plus an insert when the updated column appears in some
index's column set. The rewritten row therefore leaves its old rowid and is
appended at a fresh one above the high-water mark, which puts it in the tail —
correct on the next query, before any refresh runs — and `ngram_refresh` then
indexes it exactly. The column sets have to intersect: a `PRIMARY KEY (id)` does
nothing for `UPDATE logs SET message = ...`.

**Why it closes the vacuum case.** `RowGroupCollection::InitializeVacuumState`
sets `can_change_row_ids = !has_indexes || can_rebuild_indexes`
(`duckdb/src/storage/table/row_group_collection.cpp:1359-1369`), where
`has_indexes` counts DuckDB's own ART indexes. Shadow tables are invisible to
it, so a table carrying only an ngram index is the case where DuckDB believes
nothing depends on its rowids and relocates them freely. Any ART index flips
that, and vacuum falls back to trimming trailing deletions (`:1410-1422`), which
never move a live row. Here the column does not matter — a primary key on an
unrelated column is enough.

**Measured** on 500,000 rows, one row group deleted, full checkpoint, 200,000
rows appended, and every 500th row updated:

| | Bare table | `PRIMARY KEY (id)` | `INDEX ON (message)` |
| --- | --- | --- | --- |
| Lowest surviving rowid after vacuum | 122,880 → **0** | unchanged | unchanged |
| Rowid of an updated row | unchanged | unchanged | 100 → **200,000** |
| Selective needles resolved after churn (of 5) | **0** | — | **5** |

The five needles covered a row that vacuum moved, a row that was updated, and a
row appended afterwards. The bare table missed all of them while
`ngram_index_stats` reported the witness verdict; the ART-indexed table matched
a full scan on every one, both before and after `ngram_refresh`.

That third case is the sharpest reason to care. Once a vacuum has left the table
shorter than the mark, later appends land in the reclaimed rowid space *below*
it, where the index claims coverage it does not have and the tail scan does not
reach. They are missed from the moment they are written, and go on being missed
as long as the index stands.

**What it costs.** Measured over 10.9 M rows / 0.98 GB of enwik9 text, 24
threads, each index built alone on its own copy:

| | Build | On disk |
| --- | --- | --- |
| ngram index | 9.2 s | +1.00 GB (1.02× the text) |
| ART index on the same column | 27.7 s | +1.71 GB (1.74× the text) |

Three times the build time and 1.7× the storage of the index it exists to
protect, for a structure no query ever reads. At 100 GB that is roughly 171 GB
of ART on top of ~100 GB of trigrams. Two further consequences worth planning
for: every `UPDATE` becomes a full row rewrite rather than an in-place edit, and
each one consumes a fresh rowid, so a heavy update workload inflates the rowid
space and with it the index's segment count — `ngram_compact` is the answer, and
`generations` in `ngram_index_stats` is the signal.

Untested here: ART behaviour on very long values, and whether the storage ratio
holds for text shaped unlike prose. Both are worth checking against a sample of
real data before committing to this.

**When D beats a rebuild:** the table is large enough that
`drop_ngram_index` + `create_ngram_index` is disruptive, updates are frequent
enough that scheduling rebuilds around them is awkward, and the disk is
available. When updates are rare or batched, a rebuild is cheaper in every
dimension.

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
