# ngram — exhaustive substring search for DuckDB

`ngram` gives DuckDB a durable trigram index that accelerates substring search
over large text columns:

```sql
INSTALL ngram FROM community;
LOAD ngram;

PRAGMA create_ngram_index('logs', 'message');

SELECT * FROM ngram_search('logs', 'connection reset');
```

Every query returns **every** matching row. The index proposes candidates; the
original predicate is then re-evaluated against the real string, so an index
that is imprecise, lagging, or simply unusable can cost you time — never a row,
and never a wrong row.

Postings live in ordinary DuckDB tables: they are WAL-recovered and
buffer-managed like any other data. Each index also installs a tiny native
rowid guard on its base table so updates and vacuum cannot silently invalidate
those postings. Load `ngram` before writing a guarded table; stock DuckDB can
read it, but guarded-table DML is unsupported without the custom index type.

---

## Contents

- [Installing](#installing)
- [Quick start](#quick-start)
- [The correctness contract](#the-correctness-contract)
- [Staleness and fail-closed recovery](#staleness-and-fail-closed-recovery)
- [Maintenance](#maintenance)
- [API reference](#api-reference)
- [Settings](#settings)
- [Performance expectations](#performance-expectations)
- [Limitations](#limitations)
- [Platform support](#platform-support)
- [Building from source](#building-from-source)

---

## Installing

```sql
INSTALL ngram FROM community;
LOAD ngram;
```

The extension is built against DuckDB **v1.5.5** and links its internal C++
API, so it must be loaded into a matching DuckDB version.

> The community-extensions submission is prepared but not yet merged (see
> [`packaging/SUBMISSION.md`](packaging/SUBMISSION.md)). Until it is, load the
> binary directly:
>
> ```sh
> duckdb -unsigned -c "LOAD '/path/to/ngram.duckdb_extension';"
> ```

---

## Quick start

```sql
LOAD ngram;

CREATE TABLE logs AS SELECT * FROM read_csv('logs/*.csv');

-- Build the index. This is a normal DuckDB query internally: parallel,
-- spillable, and it can take a while on a large table.
PRAGMA create_ngram_index('logs', 'message');

-- Explicit search: exact, complete, rechecked.
SELECT * FROM ngram_search('logs', 'connection reset');

-- Or let plain SQL use the index (opt-in, see Settings).
SET ngram_auto_accelerate = true;
SELECT * FROM logs WHERE message LIKE '%connection reset%';

-- After appending rows, fold them into the index.
INSERT INTO logs SELECT * FROM read_csv('logs/2026-08-*.csv');
PRAGMA ngram_refresh('logs');

-- Check on the index.
PRAGMA ngram_index_stats('logs');
```

> **One statement at a time.** DuckDB expands a pragma before the surrounding
> batch executes, so `PRAGMA create_ngram_index('t', 'c')` cannot appear in the
> same multi-statement batch as the `CREATE TABLE t` it refers to. Run it as its
> own statement.

---

## The correctness contract

A row that contains `needle` necessarily contains every trigram of `needle`.
Intersecting the posting lists of those trigrams therefore yields a **superset**
of the true matches. Everything the index does to go faster — probing only the
rarest few trigrams, skipping segments, letting the index lag behind the table —
can only make that superset larger, never smaller.

The answer is then narrowed to exactly the truth by four mechanisms:

1. **Recheck.** Every candidate row is fetched and the original predicate is
   evaluated against the real string. For the transparent path this is the
   query's own pushed-down filter, evaluated exactly, so query semantics never
   depend on how the index normalizes text. This is why **false positives are
   impossible in every state the index can be in**.
2. **The tail scan.** Rows appended since the last `create`/`refresh` are past
   the index's high-water mark. Every query brute-force scans that tail and
   unions the matches in. A rowid zone-map filter means the scan skips the
   indexed row groups entirely, so it costs about what the unindexed tail costs.
3. **The transaction-local phase.** Rows your own open transaction has inserted
   but not committed have no permanent rowids yet and are never indexed. They
   are covered by the same tail scan, so a search inside a writing transaction
   sees your own uncommitted rows.
4. **The rowid guard.** A zero-posting non-ART index makes covered-column
   updates delete-plus-insert and prevents checkpoint vacuum from moving live
   rowids. It detects reuse of a discarded trailing range. If its identity or
   durable state is uncertain, explicit and transparent search perform one
   full scan instead of trusting postings.

Short needles (fewer characters than the gram size, 3 by default) cannot be
probed at all. Those queries fall back to a full scan — slower, still exact.

### Case sensitivity

An index is case-insensitive by default (`case_insensitive = true`), folding
text with a simple per-codepoint lowercase that index build and needle
decomposition share.

- `ngram_search` matches with the index's own semantics: case-insensitively
  over a case-insensitive index, byte-exact over a case-sensitive one.
- The transparent path always applies **your** predicate. `LIKE` and
  `contains()` stay case-sensitive even over a case-insensitive index (the
  folded index just proposes a few extra candidates that recheck discards);
  `ILIKE` only ever probes a case-insensitive index, because a case-sensitive
  one could not propose the case-variant matches it must find.

---

## Staleness and fail-closed recovery

Format-3 indexes make exhaustiveness unconditional. Covered-column updates,
including changes below the high-water mark, become delete-plus-insert; their
new rowids are found in the tail. Deletes leave harmless false-positive
postings. The non-ART guard prevents moving vacuum even when DuckDB's
`vacuum_rebuild_indexes` setting is enabled. Vacuum may discard fully deleted
trailing row groups, but the first committed append into their reused rowids
permanently marks the guard uncertain.

Every query validates the exact guard name, type, column dependency, random
incarnation token, source/version, durable checkpoint seal, high-water mark,
and reuse latch. Behavior is fail closed:

| State | Explicit search | Transparent predicate | `ngram_candidates` | Maintenance |
| --- | --- | --- | --- | --- |
| Guard proves the indexed prefix safe | postings + live recheck + disjoint tail | `NGRAM_INDEX_SCAN` | posting candidates in the prefix | refresh/compact allowed |
| Guard is missing, replaced, incompatible, unbound with replay, or cannot exclude rowid reuse | one full live-table scan | one sequential-scan fallback | every visible rowid through the recorded mark | refuse; rebuild required |
| Valid meta proves the base-table identity/fingerprint stale | one full live-table scan | sequential-scan fallback | raise | refuse; rebuild required |

A malformed, wrong-kind, or colliding meta/shadow object is corruption rather
than staleness and raises on every path.

`ngram_index_stats.stale_reason` explains the latter two states. The guard
records the checkpoint iteration each time its maximum advances and latches
unsafe only when an append at or below the maximum finds a different iteration,
which is the only way a vacuumed rowid can come back. A commit that a later
UNIQUE index rejects leaves no row and leaves the guard usable when the next
append finds the same iteration; a checkpoint between the two appends, an
in-memory database, a checkpoint in progress, or a freshly reopened file makes
that check conservative. The unsafe state costs performance, never correctness.

Creation itself closes stale-snapshot races. The first build briefly takes an
exclusive checkpoint lock, adds and drops a UUID dummy column to invalidate old
table storage, installs the scan-free guard atomically with its rowid baseline,
then releases the exclusive before the long postings scan. Reads continue;
overlapping writers may receive a transaction conflict and should retry.
Creation also invalidates DuckDB's reservoir sample.

The normal guard depends on every existing physical `VARCHAR`, so updates to
those columns become full row rewrites. A `VARCHAR` added later does not make an
existing ngram index incorrect, but it is outside that guard's coverage; to
index the new column, first add an explicit ART on it or drop/rebuild the
table's ngram indexes. DuckDB also refuses column/table rename and dependent
DROP/ALTER operations while the native guard exists.

Load the extension before writing a guarded table. Stock v1.5.5 can SELECT it,
but INSERT/UPDATE fail on the unknown index type, DELETE may busy-spin in the
host binder, and guard-touching ALTER is refused. An unrelated ADD COLUMN is
safe. An extension-free or context-free checkpoint is detected by the durable
seal and may conservatively require rebuild; a `DETACH` of an untouched unbound
guard is the main ordinary example.

The implementation and recovery proof are in
[docs/stale-updates.md](docs/stale-updates.md). There are no probabilistic row
witnesses or undetected update/vacuum miss cases in format 3.

---

## Maintenance

### When to refresh

`PRAGMA ngram_refresh('logs')` indexes the rows appended since the last build or
refresh. Its cost is proportional to the size of that tail, not to the size of
the table, so refreshing often is cheap. Run it after a batch of `INSERT`s.

Skipping it is safe: unindexed rows are found by the tail scan. That scan is a
brute-force read of the tail, so queries get gradually slower as the tail grows.
Refresh when the tail is a meaningful fraction of the table.

### Catching up on a large tail, one bounded step at a time

A refresh is one transaction. After a very large ingest that means one very long
transaction, and a crash partway through it rolls the whole catch-up back to
where it started. Pass a row bound to break the catch-up into committed steps:

```sql
-- indexes at most ~1,000,000 rows of tail, commits, and reports progress
PRAGMA ngram_refresh('logs', 1000000);
-- ┌─────────────┬──────────────┬───────────┬────────────────┐
-- │ column_name │ rows_indexed │ hwm_rowid │ remaining_tail │
-- ├─────────────┼──────────────┼───────────┼────────────────┤
-- │ message     │      1000000 │   1000099 │        2100000 │
-- └─────────────┴──────────────┴───────────┴────────────────┘
```

Each call commits durably before returning, so a crash costs the increment in
flight and nothing already committed — the next call picks up from the last
committed high-water mark. Queries stay exact at every step in between: rows
past the mark are simply still tail.

The loop lives in your code, and it ends when `remaining_tail` reaches 0:

```python
while True:
    row = con.execute("PRAGMA ngram_refresh('logs', 1000000)").fetchone()
    print(f"indexed {row[1]} rows, mark now {row[2]}, {row[3]} to go")
    if row[3] == 0:
        break
```

One row comes back per indexed column of the table — one row for the usual
single-index table, and `col = 'message'` narrows it to one index as always.
Calling it again on a caught-up index is a cheap no-op that reports
`remaining_tail = 0`, so the loop is safe to run one extra time. A bound wider
than the tail does exactly what the unbounded form does, in one call, and still
reports progress — which is the way to get a summary out of a full catch-up.

The bound is approximate in one direction only: it is spent as a span of rowids,
so deletes and gaps mean a call may index **fewer** rows than asked, never more.
A bound of 100,000 rows on a tail whose rowids are half deleted indexes ~50,000
rows and advances the mark 100,000 rowids. Bounds large enough to cross one of
the index's 2²⁰-rowid segment boundaries are snapped back to it, because an
increment that ends on a boundary writes exactly the segment rows one unbounded
refresh would have written; smaller bounds are honoured as given and leave one
segment split across two generations, which the next `ngram_compact` merges.

**Pick a bound of a whole number of 2²⁰-rowid segments if you can** — 1048576,
2097152, and so on. Because the snap rounds down, a bound just short of a
segment boundary alternates between a full increment and a tiny one: at
`max_rows = 1048575` the loop reports 1,048,575 rows, then 1 row, then
1,048,575, then 1. Every call is correct and the loop still terminates in the
same number of rows, but half of them are near-empty transactions. A mark that
does not start on a boundary has the same effect once, on the first call.

A long run of deleted rowids costs one no-op call per bound: crossing a gap of
1,000 deleted rowids at `max_rows = 100` takes ten calls that each report
`rows_indexed = 0` before the loop reaches live rows again. They are cheap —
there is nothing to index, so the call is a mark update — but if a catch-up
crawls, a wider bound walks the gap in fewer steps.

Without a bound, `ngram_refresh` behaves exactly as it always has and returns no
rows.

### When to compact

`PRAGMA ngram_compact('logs')` merges the posting-list rows that share a key.
Each refresh appends a new generation of rows rather than rewriting existing
blobs, so a table that has been refreshed many times reads more rows per probe
than it needs to. The ordinary form is shadow-only: it performs zero base-table
row reads and deliberately retains postings for deleted rows, which fetch and
recheck already remove from exact results.
`PRAGMA ngram_compact('logs', purge = true)` additionally drops those dead
postings.

**On an append-only table, compaction has little to merge.** Each refresh
generation lands in a fresh range of rowids, so successive generations barely
share `(gram, segment_no)` keys. Refresh performs a validated, byte-sorted
stats-only fold itself, so stats history is no longer a reason to compact.

An older format-3 index remains exact, but its unsorted stats may prune poorly
until the next refresh (including a no-op refresh), compact, or rebuild rewrites
them. The fold is transactional and filtered lookup skips its deleted MVCC row
groups, although their file space can remain allocated until a checkpoint.

Compaction is mainly for **delete-heavy or interleaved workloads**. Check
`fragmented_keys` and `generations` in `ngram_index_stats` before running it,
and budget disk for the rewrite (the file grows before it shrinks). Reserve
`purge = true` for after a large `DELETE`: it rewrites every key and makes one
base-table pass per selected indexed column, retaining relevant live rowids at
or below the indexed HWM.
That one-BIGINT temp and the selected encoded source are spillable, and can use
substantial temporary disk alongside the packed output and MVCC-old rows.
Purging is consequently much more expensive than a merge-only compaction.

Corruption checks follow the data each path reads: a probe validates requested
gram stats, merge-only compact validates selected segment rows, purge validates
all segment rows, and the refresh stats fold validates every historical stats
row it rewrites. Compact rebuilds stats from the resulting segment metadata;
it does not separately validate superseded stats rows.

### When to rebuild

Rebuild when `ngram_index_stats.stale_reason` is non-`NULL` or maintenance says
the rowid guard cannot prove the indexed prefix safe. Common causes are actual
reuse of a vacuumed trailing range, an extension-free/context-free checkpoint,
a missing or re-created guard, incompatible persisted guard state, or an
append at or below the guard's maximum in an in-memory database.

Ordinary UPDATE, DELETE, checkpoint, append, and reopen in isolation do not
require a rebuild. A trailing DELETE + checkpoint + later rowid reuse is the
combined exception described above. Queries remain exact even in an uncertain
state; they simply scan.

```sql
PRAGMA drop_ngram_index('logs', 'message');
PRAGMA create_ngram_index('logs', 'message');
```

An explicit native ART is not needed for normal ngram correctness. Its one
remaining lifecycle use is as a creation protector when an existing DuckDB
index dependency prevents the ADD/DROP barrier, or when indexing a `VARCHAR`
added after the existing broad guards were created.

### Reading `ngram_index_stats`

```sql
PRAGMA ngram_index_stats('logs');
```

| Column | Meaning |
| --- | --- |
| `column_name`, `gram_size`, `case_insensitive` | the options the index was built with |
| `hwm_rowid` | the highest rowid the index covers |
| `table_max_rowid` | the table's current highest rowid — the gap is what the tail scan reads on every query |
| `remaining_tail` | committed rows past `hwm_rowid`: what the tail scan actually reads, and what a refresh would index (the rowid gap counts deleted rows, this does not) |
| `distinct_grams` | size of the index's gram dictionary |
| `segments` | posting-list rows |
| `fragmented_keys` | keys stored as more than one row; the compaction target |
| `generations` | build plus refresh generations present |
| `posting_entries`, `postings_bytes` | total postings and their encoded size |
| `stale_reason` | `NULL` when the guard proves the indexed prefix safe; otherwise the identity, compatibility, or rowid-reuse uncertainty forcing scan/rebuild |

---

## API reference

### Index lifecycle

```sql
PRAGMA create_ngram_index('table', 'column');
PRAGMA create_ngram_index('table', 'column', gram = 3, case_insensitive = true);
PRAGMA drop_ngram_index('table', 'column');

PRAGMA ngram_indexes;
PRAGMA ngram_index_status('database_name', 'index_ref');
PRAGMA drop_ngram_index_by_id('database_name', 'index_ref');
```

`gram` is the number of characters per gram (default 3). Larger grams are more
selective but cannot answer needles shorter than themselves; smaller grams
accept shorter needles but propose far more candidates.

Indexes are supported on `VARCHAR` columns of DuckDB base tables. Views,
temporary tables, tables in foreign catalogs (SQLite, Postgres, …), tables with
generated columns, and tables with a user column named `rowid` are rejected.

Each new index receives a canonical UUIDv4 `index_ref` and its own opaque
storage schema. `PRAGMA ngram_indexes` lists registered indexes and
pre-registry legacy allocations across attached DuckDB catalogs. Use the
catalog-qualified status/drop forms whenever the base table or indexed column
has disappeared; copied attached databases may legitimately contain the same
UUID, so the catalog name is part of the public identity.

Lifecycle status is deliberately conservative:

| Status | Meaning |
| --- | --- |
| `READY` | Exact storage, owner, and rowid guard validate; indexed reads may accelerate. |
| `SCAN_ONLY` | The allocation is owned but identity/guard safety is uncertain; exhaustive queries scan. |
| `ORPHAN` | The recorded base table or column is absent. Stable-ID drop remains available. |
| `REPLACED` | Same-session identity proves a different table now occupies the recorded name. Stable-ID drop never touches it. |
| `LEGACY_REBUILD` | A pre-registry v2 allocation is visible and drop-only. |
| `UNREGISTERED` | Structurally valid opaque v3 storage lost its registry row and can be removed by its derived UUID. |
| `MISSING_STORAGE` / `MALFORMED` | Required proof is missing or catalog objects are corrupt. Drop fails closed with manual-repair guidance. |

DuckDB v1.5.5 refuses table and indexed-column rename while the physical guard
exists, including case-only rename. Moving a table between schemas and renaming
a schema are host-not-implemented. The supported workflow is therefore:

```sql
PRAGMA ngram_indexes;  -- save database_name + index_ref
PRAGMA drop_ngram_index_by_id('database_name', 'index_ref');
ALTER TABLE old_name RENAME TO new_name;
PRAGMA create_ngram_index('new_name', 'column');
```

The registry and opaque schemas are extension-owned ordinary catalog objects.
Do not edit them directly. Identifiable corruption remains observable, but
there is intentionally no force-drop or rebind API: without an exact meta/guard
proof, automated cleanup could delete a replacement object or strand a live
guard.

### Maintenance

```sql
PRAGMA ngram_refresh('table');                        -- index the whole tail, returns nothing
PRAGMA ngram_refresh('table', 1000000);               -- at most ~1e6 rows, returns a progress row
PRAGMA ngram_refresh('table', max_rows = 1000000);    -- same, named
PRAGMA ngram_refresh('table', 1000000, col = 'c');    -- one index of a multi-index table
PRAGMA ngram_compact('table');
PRAGMA ngram_compact('table', col = 'c', purge = true);
PRAGMA ngram_index_stats('table');
```

Pragma named parameters take `=`, not `:=`. Each call is one transaction,
whether or not it is bounded; `max_rows` must be at least 1.

The bounded form returns one row per index it advanced:

| Column | Meaning |
| --- | --- |
| `column_name` | the indexed column this row is about |
| `rows_indexed` | committed rows this call brought under the mark (rows whose value is `NULL` included: they are covered, they just hold no grams) |
| `hwm_rowid` | the high-water mark the call committed |
| `remaining_tail` | committed rows still past it — loop until this is 0 |

### Querying

```sql
SELECT * FROM ngram_search('table', 'needle');
SELECT * FROM ngram_search('table', 'needle', col := 'column');
SELECT rowid FROM ngram_candidates('table', 'column', 'needle');
```

`ngram_search` returns rows exactly and completely. DuckDB pushes its parent
projection into the function: only requested fields plus the indexed `VARCHAR`
needed for recheck are fetched (`count(*)` fetches just that recheck column).
`col` is only needed when a table has indexes on more than one column. (The
parameter is `col`, not `column`, because `column` is a reserved word.)

`ngram_candidates` is the raw, lossy candidate set: a superset of the true
matches **among indexed rows only**. It does not recheck and it does not cover
the tail. It exists for inspection and for building your own pipelines; if you
use it, you own the recheck and the tail scan. Candidates stream one rowid
segment at a time. If the pre-decode work or memory admission check fails, this
candidate-only API returns a resource-limit error instead of allocating an
unbounded posting list.

Both exact query paths inspect posting counts before opening a postings blob.
They reserve a bounded manifest and per-worker segment workspace against
DuckDB's memory limit, decode/intersect/fetch one segment at a time, and scan
instead if the estimated work, memory, or candidate density is too high. The
probe budget for one query is the smaller of one quarter of `memory_limit` and
256 MiB; `EXPLAIN ANALYZE` reports the selected mode and decoded rowid count.

### Transparent acceleration

With `SET ngram_auto_accelerate = true`, an optimizer pass rewrites qualifying
scans into `NGRAM_INDEX_SCAN`. It fires for `contains(col, 'lit')`,
`col LIKE '%lit%'`, `col ILIKE '%lit%'` (case-insensitive indexes only),
`regexp_matches(col, 'literal')`, multi-segment patterns like
`col LIKE '%a%b%'`, and those combined with other filters via `AND`.

It declines — leaving an ordinary sequential scan — for `_` wildcards, `ESCAPE`
clauses, anchored/prefix/suffix patterns, `NOT LIKE`, `OR`-ed predicates,
expressions over the column (`lower(col) LIKE …`), needles shorter than the gram
size, tables with no index on that column, and any state where identity checks
or the rowid guard cannot prove the indexed prefix safe.

`EXPLAIN` shows which happened:

```sql
EXPLAIN SELECT * FROM logs WHERE message LIKE '%reset%';
-- ... NGRAM_INDEX_SCAN  Table: logs  Ngram Column: message  Ngram Needles: reset

EXPLAIN ANALYZE SELECT * FROM logs WHERE message LIKE '%reset%';
-- ... Ngram Mode: index (<= 1423 candidates, 9012 decoded rowids)
-- or   Ngram Mode: full scan fallback: candidate fraction exceeded
```

Two kill switches: `SET ngram_auto_accelerate = false`, and DuckDB's own
`SET disabled_optimizers = 'extension'`.

### Helper functions

```sql
SELECT trigrams('hello');                     -- ['hel', 'ell', 'llo']
SELECT trigrams('Hello', 4, false);           -- gram size 4, case-sensitive
SELECT ngram_encode_postings([1, 2, 5]);      -- posting blob codec
SELECT ngram_decode_postings(blob);
```

---

## Settings

| Setting | Default | What it does |
| --- | --- | --- |
| `ngram_auto_accelerate` | `false` | Whether plain `LIKE`/`contains`/`ILIKE` may be rewritten to use the index. Rewrites are exhaustive and resource-bounded, including guard-, work-, memory-, and density-driven full-scan fallback. It remains opt-in so enabling the extension does not silently change query plans. |
| `ngram_max_candidate_fraction` | `0.01` | A full-result ngram query whose candidate upper bound exceeds this fraction of the table scans instead. This bounds fetch-and-recheck work after the probe; the raw candidate API does not use this fetch-vs-scan policy. |
| `ngram_max_grams_per_query` | `3` | How many of the needle's rarest grams to probe. Each extra gram costs another posting-list decode but can narrow the candidate set; the default balances those costs for natural-language text. |
| `ngram_max_probe_rowids` | `100000000` | Hard upper bound on posting rowids decoded by one query. Exact query paths scan instead when the estimate exceeds it; `ngram_candidates` returns a resource-limit error. |
| `ngram_build_partitions` | `0` | How many rowid-range partitions `create_ngram_index`, `ngram_refresh` and `ngram_compact` split their packing pass into. Build and refresh size zero from `memory_limit` using a sample; compact instead uses fine segment-aligned ranges without sampling the base. Because range width is rounded down to whole segments, auto can emit up to nearly twice its 4096-range request. An explicit value overrides either policy. Raise it if a build runs out of memory on unusually long rows, or lower it to pack in fewer passes. The index it produces is identical whatever you set. |

All five are session settings; set them per connection.

**For exact/full-result queries, raising or lowering these never changes the
rows returned**, only the strategy and time taken. Probing fewer grams can only
widen the candidate set, every candidate is rechecked, and resource gates
choose between two exhaustive strategies. For the deliberately lossy
`ngram_candidates`, `ngram_max_grams_per_query` can change the (still safe)
superset and a resource gate changes rows into a pre-decode error.

Worth tuning by hand if your data is unusual:

- **A small alphabet** (hex dumps, base64, DNA) makes every gram dense, so the
  intersection needs more grams to bite. Try `ngram_max_grams_per_query = 4`
  or higher, and consider `gram = 4` at build time.
- **Long, highly distinctive needles** get nothing from extra grams — the first
  few may already isolate the answer, so raising the probe budget can add work
  without narrowing the candidate set.
- **A table that is mostly matches** for your typical needle should not use the
  index at all; that is what the gate is for, and lowering
  `ngram_max_candidate_fraction` further makes it give up sooner.

---

## Performance expectations

<!-- BEGIN NGRAM RELEASE EVIDENCE -->
### Checked bounded benchmark

Same-machine observations; queries are warm-cache for a **non-default case-sensitive trigram
index** over nonempty line-per-row `enwik9`. These are not cold-cache, large-scale, or
shipped-default claims. Raw evidence: [`benchmarks/artifacts/enwik9-current-v1.json`](benchmarks/artifacts/enwik9-current-v1.json).

- Engine commit: `b6a388c8c39f`; build commit: `b6a388c8c39f`; DuckDB v1.5.5 / source d8cdaa33;
  static-extension release CLI. The numbers describe the engine commit's `src/**` and are
  re-collected on release; later commits keep this block until the next collection.
- Corpus: 10,920,423 rows, 0.919 GiB of UTF-8 text; three fresh load/build pairs.
- Timed load—fresh CLI and absent DB through create, hex decode, insert, CHECKPOINT—was
  2.299 s median (2.170–2.401 s). Timed index build—fresh CLI through create-index and
  CHECKPOINT—was 7.834 s median (7.781–7.941 s), 120.14 MiB/s of source text.
- Paired whole-database size increase: 0.996 GiB apparent, 0.996 GiB allocated
  (median); 1.083× source bytes. This whole-DB effect includes allocator/checkpoint effects.
- Build-process max RSS: 11.752 GiB median. Sampled peak temp apparent file bytes: 0.000 GiB
  median, polled every 100 ms; zero means none observed, not proof that no brief spill occurred.
  Acquisition, normalization, relation/stat checks, EXPLAIN, and parity are untimed. Loads may
  read cached transport pages; builds follow relation identity and may read cached source pages.

| needle class | ngram_search mode | exact matches | candidates | ngram_search p50 / p95 / range | scan p50 / p95 / range | scan ÷ search p50 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| rare | index | 1 | 12 | 7 / 7 ms / 6–7 ms | 43 / 45 ms / 42–47 ms | 6.14× |
| moderate | index | 26,068 | 26,381 | 15 / 16 ms / 14–17 ms | 29 / 30 ms / 27–31 ms | 1.93× |
| dense | full-scan-fallback | 1,963,067 | 1,963,067 | 44 / 45 ms / 43–46 ms | 39 / 40 ms / 37–40 ms | 0.89× |

The timed campaign adds one warmup per variant after untimed parity/EXPLAIN executions, then
twenty-one measured observations per variant using a fixed-seed interleaving on one connection.
Timer resolution is one millisecond. Exact ngram_search and scan counts match every observation;
candidate counts are a separately measured lossy superset.
`check` verifies the artifact, the pinned gitlinks, commit ancestry, and this rendered block on
any commit; `--current-source` also requires `src/**` and the tool to hash to the artifact's
records, which holds at the engine commit and on release tags. `validate` applies the strict
check to any artifact path:

```sh
python3 benchmarks/release_evidence.py check
python3 benchmarks/release_evidence.py check --current-source
python3 benchmarks/release_evidence.py validate --artifact benchmarks/artifacts/enwik9-current-v1.json
```

Optional `--binary` validation requires the exact CLI/SHA recorded for the artifact build commit;
an arbitrary later rebuild is rejected.

For a fresh independent run:

```sh
python3 benchmarks/release_evidence.py collect --output-root ./release-evidence-work \
  --artifact ./release-evidence-rerun.json
python3 benchmarks/release_evidence.py validate --artifact ./release-evidence-rerun.json \
  --binary build/release/duckdb
```

`enwik9` is the first billion bytes of the March 2006 English Wikipedia dump, obtained from
[Matt Mahoney's canonical benchmark page](https://www.mattmahoney.net/dc/textdata.html). It is attributable to English Wikipedia
contributors. SHA-256 values were freshly observed; raw MD5/SHA-1 are published there. The
corpus is not redistributed or licensed by this repository's MIT license; do not assume
public-domain status. Review [Wikimedia reuse guidance](https://dumps.wikimedia.org/legal.html) and the
[Wikimedia Terms of Use](https://foundation.wikimedia.org/wiki/Policy:Terms_of_Use).
<!-- END NGRAM RELEASE EVIDENCE -->

---

## Limitations

- Needles shorter than the gram size cannot use the index (they fall back to a
  full scan, still exact).
- No fuzzy or similarity ranking, and no general regular-expression support —
  only regexes that are a plain literal reduce to an indexable substring.
- Guarded tables require `ngram` to be loaded for supported DML. Without it,
  treat the base table as read-only; v1.5.5 DELETE may busy-spin while trying
  to bind the unknown custom index type.
- The rowid guard is pinned to host-reported DuckDB v1.5.5 source
  `d8cdaa33` (local build) or `d8cdaa33fd` (official binary). Other hosts load
  only for fail-closed inspection and cleanup; create/query/maintenance refuse
  to trust the custom index internals.
- A first build invalidates DuckDB's reservoir sample and may make overlapping
  writers retry. Guard dependencies restrict column/table rename and dependent
  DROP/ALTER operations until the ngram index is dropped. On v1.5.5, table
  schema moves and schema rename are not implemented; use stable-ID drop,
  rename, then rebuild.
- One index per (table, column). Multi-column indexes do not exist; build one
  index per column you search.
- Temporary tables and non-DuckDB catalogs are not supported.
- Inside an explicit transaction, DuckDB's pragma preprocessor sets
  `current_transaction_invalidation_policy` for the duration of a multi-statement
  pragma expansion and restores DuckDB's default afterwards, not whatever you had
  configured. If you set that policy yourself, re-set it after running an ngram
  pragma inside a transaction.

---

## Platform support

The last full distribution-matrix run was Phase 9, before the format-3 rowid
guard: DuckDB v1.5.5 built on Linux (x86_64, arm64), macOS (x86_64, arm64),
Windows (x86_64 MSVC, x86_64 MinGW, arm64), and Wasm (mvp, eh, threads).
Linux x86_64, macOS arm64, and all three Windows targets each passed the same
2,559 assertions in 21 test cases; the remaining targets built and linked.
The two opt-in musl targets were not built. The final format-3 submission
commit must rerun that matrix; local Phase 11 results are recorded separately
in `ngram_index_plan.md` and do not establish cross-platform coverage for the
new custom index type and extension callbacks.

---

## Building from source

```sh
git clone --recurse-submodules <repo>
cd duckdb-ngram
make                # release build; ./build/release/duckdb has the extension linked in
make test           # sqllogictest suite
GEN=ninja make debug        # DEBUG + AddressSanitizer build
```

The loadable binary is
`build/release/extension/ngram/ngram.duckdb_extension`; load it into a stock
DuckDB v1.5.5 with:

```sh
duckdb -unsigned -c "LOAD '/path/to/ngram.duckdb_extension';"
```

Property-based and long-running harnesses live in `scripts/`:

```sh
python3 scripts/differential_search.py --trials 8 --seed 12345
python3 scripts/differential_search.py --transparent --trials 8 --seed 12345
python3 scripts/churn_maintenance.py --rounds 40 --seed 12345
python3 scripts/crash_maintenance.py --seed 12345
```

Benchmarks and corpus generation live in `benchmarks/`; see
[`benchmarks/RESULTS.md`](benchmarks/RESULTS.md).

## License

MIT. See [LICENSE](LICENSE).
