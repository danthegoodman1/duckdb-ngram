# Development Plan: DuckDB `ngram` Extension — Exhaustive Substring Search

## Overarching Goal

Build a C++ DuckDB extension providing a disk-persisted trigram index that accelerates
substring search (`LIKE '%needle%'` / `contains(col, 'needle')`) over large text columns,
returning **every** matching row (exact search, 100% recall). Postings live in ordinary
DuckDB shadow tables (the duckdb-fts model), while a zero-posting native rowid guard keeps
the base-table mapping safe or forces one exhaustive scan. The index is durable,
buffer-managed, and WAL-recovered. It remains readable without the extension, but guarded
base-table DML requires the custom type to be loaded. Query acceleration is a transparent
optimizer rewrite plus explicit table functions; every result is rechecked against the
real string, so index imprecision or guard uncertainty can only cost performance, never
correctness.

Performance target: hundreds of milliseconds per selective substring query over TB-scale
corpora. Interactive (sub-ms) latency is a non-goal, which is why the shadow-table
architecture is chosen over putting postings in a native `BoundIndex`: DuckDB's custom-index
path (as of Aug 2026 main) pins index buffers permanently after first touch and writes the
whole index image to the WAL at `CREATE INDEX`. Phase 11 uses only a zero-data native guard
for rowid safety. Non-goals: similarity/fuzzy ranking,
regex literal extraction (stretch, later), needles shorter than the gram size (fall back
to seq scan, still exhaustive).

## Key Research Facts (self-contained reference)

Verified against the pinned build target duckdb v1.5.5 (`d8cdaa33`). The original research
commit (main `e500d778`, exactly one squashed dev cycle ahead of v1.5.5) is kept as
submodule tag `research-e500d778` for the main-only references flagged below.

- **Correctness invariant**: any row containing the needle contains every trigram of the
  needle, so intersecting posting lists yields a superset of true matches; recheck with
  the original predicate filters to the exact answer. Dropping constraints (rarest-K
  trigram selection, hashing, skipped hot trigrams, index lag) only grows candidates.
- **Rewrite recognition**: the EXPRESSION_REWRITER pass normalizes `col LIKE '%x%'` to
  `contains(col, 'x')` (`src/optimizer/rule/like_optimizations.cpp:135-137`) and
  FILTER_PUSHDOWN moves it into `LogicalGet.table_filters` as an `ExpressionFilter`
  (`src/optimizer/filter_combiner.cpp:378-413`), deleting the `LogicalFilter` — both
  before post-optimize extension hooks run (`src/optimizer/optimizer.cpp:331-351`).
  `ILIKE` is NOT normalized to contains but IS pushed into `table_filters` as
  `(col ~~* '%x%')` (expression pushdown accepts any single-column non-volatile
  expression, `table_scan.cpp:891-893`), so both cases are read from the same place.
  `regexp_matches` with a pure literal also becomes `contains`
  (`regex_optimizations.cpp:180-199`). On v1.5.5 `contains()` is a stable terminal form —
  the post-v1.5.5 IN-list/prefix rewrites do not exist yet; recheck when bumping the pin.
  Pushed expressions have column refs rebound to `BoundReferenceExpression(..., 0)` over a
  one-column chunk.
- **No planner hook for custom indexes**: `TableScanInitGlobal` binds and scans ART only
  (`src/function/table/table_scan.cpp:735-740`); `IndexType` has no scan hook. The blessed
  pattern is an `OptimizerExtension` (post-optimize) swapping `get.function`/`get.bind_data`
  for a custom table function (both public, `logical_get.hpp:32-44`) — duckdb-vss's
  `hnsw_index_scan` shape. Register via `OptimizerExtension::Register(config, ext)`;
  extension passes run as `OptimizerType::EXTENSION`, so `SET
  disabled_optimizers='extension'` is a free kill switch for differential testing. No
  in-tree v1.5.5 demo of the rowid + `ExpressionFilter` + recheck shape exists (main's
  `RowIdOptimizerExtension` is post-v1.5.5; readable at
  `research-e500d778:test/extension/loadable_extension_demo.cpp:848` for reference only).
- **Rowid fetch**: `DataTable::Fetch` is public (`data_table.hpp:104-106`) and already
  handles mixed committed + transaction-local rowids with original-order restoration
  (`data_table.cpp:485-557`), so no separate local-fetch phase is needed for rows the
  index knows. The batched fetch driver template is `table_scan.cpp:102-265` (storage
  phase `:196-222`; the LOCAL_STORAGE scan phase `:223-237` covers rows an index has
  never seen — for ngram that role belongs to the HWM tail scan). Hold
  `DuckTransactionManager::SharedVacuumLock()` (`duck_transaction_manager.hpp:84`)
  unconditionally from probe through fetch — v1.5.5's built-in acquisition gate
  (`table_scan.cpp:724-733`) is narrower than main's, and unconditional is the safe
  superset. Async caveat: when the storage phase yields an empty chunk, set the
  `HAVE_MORE_OUTPUT` async result instead of looping (`table_scan.cpp:212-216`).
- **The fetched-storage path does not re-apply filters** (no TableFilter parameter
  anywhere in `Fetch` → `RowGroupCollection::Fetch` → `RowGroup::FetchRow`); recheck is
  the extension's responsibility, which the design requires anyway. Asymmetry: the
  LOCAL_STORAGE scan phase DOES apply pushed filters (`table_scan.cpp:148`).
- **Plan verification is DEBUG-only on v1.5.5** (`column_binding_resolver.cpp:238-245`):
  in release builds a broken rewrite surfaces as `InternalException` at physical planning
  (`physical_plan_generator.cpp:37-41`), not at optimizer Verify. Run rewrite tests
  against a DEBUG duckdb build to get per-pass verification.
- **Interaction risk**: `LATE_MATERIALIZATION` runs before extension hooks
  (`optimizer.cpp:282-287`) and can restructure a `LogicalGet` into a rowid-join shape
  for `ORDER BY`/`LIMIT` plans — plan-shape tests must cover
  `LIKE '%x%' ORDER BY ... LIMIT n`.
- **Pragma mechanics** (`statement_preprocessor.cpp`, byte-identical to main): a pragma's
  `query` callback runs during whole-batch preprocessing inside a transaction context.
  Multi-statement expansions are wrapped in BEGIN/COMMIT only when >1 statement and not
  already inside a transaction; single-statement expansions are NOT wrapped; expansions
  ending in a SELECT are not wrapped; inside a user transaction the wrapper instead sets
  `current_transaction_invalidation_policy` globally and restores the hardcoded default
  (a user's custom policy is silently reset — document).
- **Shadow postings** persist/recover as ordinary tables. Phase 11 additionally installs a
  zero-posting non-ART guard: stock DuckDB can read the database, but guarded-table DML is
  unsupported without the extension (`index_binder.cpp:27`; v1.5.5 DELETE may busy-spin
  after a failed bind). The guard deliberately makes rowid-moving vacuum ineligible
  (`row_group_collection.cpp:28-39`, `:1359-1369`) without paying the fixed-buffer/WAL-image
  costs that made native posting storage unattractive.
- **No triggers, no change feed, no persistent table identity** (all verified on v1.5.5
  during Phase 5): `CREATE TRIGGER` is a parser error; catalog ownership dependencies
  (`ALTER SEQUENCE ... OWNED BY tbl`, which does cascade a DROP TABLE onto the sequence)
  are in-memory only and are gone after a reopen; `DataTableInfo` carries no persisted
  table id; catalog oids (`duckdb_tables().table_oid`) are stable across ADD/DROP/RENAME
  COLUMN, ALTER TYPE, RENAME TABLE and CREATE INDEX, change on DROP+CREATE and CREATE OR
  REPLACE, and are renumbered on reopen. They come from one process-wide counter
  (`DatabaseManager::NextOid`) and ATTACH mints fresh ones for everything it loads, so a
  DETACH + re-ATTACH renumbers a healthy table exactly as a DROP + re-CREATE does — an oid
  is proof of identity only within the instance *and* the attach incarnation that handed
  it out, which is why the index records the attached database's oid alongside the
  table's.
- **A vacuum's row motion can be perfectly masked by later appends**: with 3 row groups of
  2048 rows, deleting the middle group and checkpointing merges to 2 groups and moves
  surviving rowids (the row with id 4096 lands at rowid 2048); re-inserting 2048 rows
  restores the row-group layout, the row count and the max rowid to their pre-delete
  values while the moved rows stay moved. Row counts, `pragma_storage_info` layout and
  the column's block layout are therefore all unsound as "no motion" oracles; only the
  *contents* of recorded rowids distinguish the two states. Phase 5 sampled those contents;
  Phase 11 supersedes that probabilistic detector with a non-ART guard that prevents live
  motion and durably latches later reuse, then removes the witnesses. Related: row-group
  merges also happen with no deletes at all (adjacent partial groups), and those preserve
  rowids — so a changed layout is not evidence of motion either.
- **`pragma_storage_info`'s `start` is relative to the row group**, not an absolute rowid;
  the absolute (start, count) layout comes from `DataTable::GetPartitionStats`, and
  `count` there includes rows deleted but not yet vacuumed. Cost is metadata-only: 2 ms
  over a 20M-row / 163-row-group table.
- **The sqllogictest runner sets `checkpoint_wal_size = 0`** (`test/helpers/test_helpers.cpp`
  `GetTestConfig`), so every statement checkpoints and any DELETE is vacuumed
  immediately. Tests that need an un-vacuumed delete must raise `checkpoint_threshold`
  themselves.
- **Ecosystem** (rechecked 2026-08-08, Phase 6): still no trigram/ngram/substring index
  among the 306 community extensions, and the name `ngram` is unclaimed (no core
  extension uses it either). The nearest neighbours are scalar matchers or prefix
  structures, not substring indexes: `fuzzycomplete`, `rapidfuzz`, `splink_udfs`
  (an `ngrams()` list function), `marisa` (prefix trie), `lsh`. duckdb/duckdb discussion
  #16071 (trigram tokenizer) is still open and unanswered. duckdb-fts merged its trigram
  work to main on 2026-08-04 (PR #52, "Add indexed wildcard and regex search") and has not
  released it — the repo has no tags and no releases, and its last version bump predates
  the merge, so `INSTALL fts` on v1.5.5 does not include it. What it does is
  dictionary-level: trigrams over the deduplicated *term* dictionary (`term_grams`,
  `raw_term_grams` keyed by termid), used to expand a whole-token wildcard/regex query
  term before BM25 scoring; its own README says the sidecars "grow with the term
  dictionary rather than with the document corpus" and that patterns are matched "as one
  whole-token pattern ... against the normalized raw-term dictionary". That is a different
  structure and a different guarantee from a character-trigram index over raw strings that
  matches across token boundaries and scales with the corpus. No scope collision.

## Public API (target surface)

```sql
INSTALL ngram FROM community; LOAD ngram;

PRAGMA create_ngram_index('logs', 'message',
    gram := 3, case_insensitive := true, maintenance := 'manual');
PRAGMA drop_ngram_index('logs', 'message');

-- transparent (optimizer rewrite, recheck always applied)
SELECT * FROM logs WHERE message LIKE '%connection reset%';

-- explicit
SELECT * FROM ngram_search('logs', 'connection reset');           -- rows, rechecked
SELECT rowid FROM ngram_candidates('logs', 'message', 'needle');  -- lossy candidates

-- maintenance / observability
PRAGMA ngram_refresh('logs');            -- index the tail past the high-water mark
PRAGMA ngram_refresh('logs', col := 'message');
PRAGMA ngram_compact('logs');            -- merge fragmented segments, drop dead postings
PRAGMA ngram_compact('logs', purge := true);
PRAGMA ngram_index_stats('logs');        -- incl. fragmentation and a staleness verdict

SET ngram_auto_accelerate = true;
SET ngram_max_candidate_fraction = 0.05;
SET ngram_max_grams_per_query = 8;   -- probe only the K rarest trigrams of the needle
```

## Implementation Principles

- Recall is structurally protected: candidate generation may only ever over-approximate;
  every optimization must be provably superset-preserving; recheck always runs.
- An accelerated path may answer only when exactness is established for its snapshot. If
  that cannot be established, scan or refuse; availability does not outrank completeness.
- The index is an optimization, never a correctness dependency: rows past the refresh
  high-water mark are covered by a brute-force tail scan unioned into results;
  transaction-local rows are scanned separately.
- Acquire rowid-stability protection before validating or consuming rowid-derived state,
  and keep it through the operation that publishes or reads that state.
- Admit probe work against explicit memory and work budgets before decoding postings;
  query limits that run after materialization are not resource limits.
- Build and maintenance run through DuckDB's own SQL engine (parallel, out-of-core,
  compressed) rather than custom pipelines; the extension orchestrates queries.
- Normalization (case folding, unicode) is defined in exactly one place and shared by
  index build and needle decomposition — a mismatch is the one bug class that drops rows.
- The public surface (pragmas, functions, settings) hides the storage backend so a future
  native custom-index backend can be swapped in without breaking users.
- Performance claims are generated from versioned benchmark artifacts tied to a commit,
  corpus, settings, and implementation; prose is not the source of truth.
- Pin the DuckDB version per release; the C++ extension ABI couples to internals
  (`OptimizerExtension`, `DataTable::Fetch`, `ExpressionFilter`).

## Testing Strategy

- Differential testing as the backbone: every accelerated query path is compared against
  brute-force `LIKE`/`contains` on randomized corpora (unicode, NULLs, empty strings,
  needles spanning gram boundaries) — results must be identical, always.
- sqllogictest suites for API surface, EXPLAIN plan shape, fallback behavior, and
  persistence across close/reopen (with and without the extension loaded).
- Concurrency tests: index probes racing inserts/deletes/updates and checkpoints;
  maintenance racing rowid-moving checkpoints; uncommitted-row visibility inside a
  writing transaction. Correctness races use deterministic barriers, not timing alone.
- Resource tests record peak resident memory, decoded rowids, rows/bytes scanned, spill,
  and wall time; latency alone cannot prove bounded execution.
- Linux release, DEBUG + AddressSanitizer/UBSan, fixed-seed differential/churn, and focused
  concurrency tests form the pre-release correctness lane; the distribution matrix remains
  the portability lane.
- Scale benchmark tracked per release: index build time, index size ratio, and query
  latency percentiles on a ≥100 GB corpus, plus a measured scaling curve (1/10/100 GB)
  supporting an explicit TB extrapolation. True-TB validation needs hardware this project
  does not have (see the Phase 6 gate) and is deferred until it does.

## Phase 1: Scaffold and Trigram Primitives

Goal:
A building, testable extension exposing correct trigram extraction and needle
decomposition, pinned to a known DuckDB version.

Scope:
- Extension scaffold from `extension-template` (C++), CI build, pinned duckdb submodule.
- `trigrams(text, gram := 3, case_insensitive := true)` scalar function: normalization,
  case folding, unicode-aware extraction.
- Needle decomposition helper (shared code with extraction) incl. short-needle detection.

Out of scope:
- Any storage, pragmas, or planner integration.

Completion gate:
Extension loads in duckdb CLI; trigram extraction verified against a reference
implementation on unicode fixtures; CI green.

Testing plan:
- Unit + sqllogictests for `trigrams()`: ASCII, multibyte UTF-8, case folding, needles at
  gram boundaries, empty/NULL inputs.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Scope | Extension scaffold builds against pinned duckdb | Repo github.com/danthegoodman1/duckdb-ngram (private), duckdb submodule @ v1.5.5, local `make`/`make test` green. |
| Complete | Work | 1C: Re-verify Key Research Facts against pinned duckdb v1.5.5 and re-pin the plan's file:line references | Full verification pass 2026-08-08; Key Research Facts section rewritten with v1.5.5 pins. Notable: `RowIdOptimizerExtension` demo and `KEEP_ROW_IDS` are main-only; ILIKE reaches `table_filters` too; `DataTable::Fetch` handles transaction-local rowids natively; plan verification is DEBUG-build-only. |
| Complete | Work | 1A: `trigrams()` scalar function with shared normalization module | src/trigram.cpp + src/trigrams_function.cpp; positional optional args (named args unsupported for scalar functions); test/sql/trigrams.test. |
| Complete | Work | 1B: Needle decomposition + short-needle detection | DecomposeNeedle + too_short in src/trigram.cpp, shares ExtractGrams with 1A; O(k²) dedupe replaced with a set (first-occurrence order kept). Exercised directly through the Phase 3 surface: dedupe ('aaaa' probes one gram), short-needle fallback, gram-boundary lengths, and per-index gram/case options in test/sql/ngram_search*.test. |
| Complete | Gate | Loads in CLI; extraction matches reference on unicode fixtures | CLI load + generated fixture suite (scripts/gen_trigram_fixtures.py, incl. Cherokee/Deseret/Kelvin/ẞ/sigma); reviewer's full-codepoint sweep vs `lower()`: 0 divergences. CI green: run 31267278753. |
| Complete | Test | sqllogictest suite for extraction edge cases | test/sql/trigrams.test + generated test/sql/trigrams_fixtures.test. |

## Phase 2: Index Build and Shadow-Table Storage

Goal:
`PRAGMA create_ngram_index` materializes a durable posting-list index as ordinary tables;
survives close/reopen with or without the extension loaded.

Scope:
- Shadow schema per (table, column): segments table `(trigram, segment)` where `segment`
  is a blob of delta-compressed sorted rowids; metadata table (options, indexed column,
  rowid high-water mark, format version); per-trigram frequency stats.
- Build implemented as internal SQL (`unnest(trigrams(col))`, rowid, sort/group) so
  DuckDB provides parallelism, spilling, and compression.
- `PRAGMA create_ngram_index(...)` / `PRAGMA drop_ngram_index(...)`,
  `ngram_index_stats()` table function.

Out of scope:
- Incremental refresh, compaction, triggers (Phase 5); any query acceleration (Phase 3).

Completion gate:
Index builds on a 10 GB corpus without OOM under a constrained memory limit; database
close/reopen round-trips the index. The original extension-absent writeability guarantee
was true for shadow-only format 2 and is superseded by Phase 11: guarded format-3 tables
remain readable, but their DML requires the extension.

Testing plan:
- sqllogictests: create/drop lifecycle, duplicate create, stats output, options round-trip.
- Persistence tests incl. extension-absent reopen; build under `SET memory_limit` pressure.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Scope | Shadow schema (segments, metadata, stats) | Schema `ngram_<schema>_<table>` with `meta_/segments_/stats_<column>` tables; blob format documented in src/include/ngram/postings_codec.hpp. |
| Complete | Work | 2A: SQL-driven parallel build path | Two-pass build in src/index_pragmas.cpp: temp pairs table, external sort, streaming packer. 196 MB worst-case corpus builds in ~84 s under `memory_limit='1GB'`; differential identity vs brute force: 0 mismatches across 190M postings. |
| Complete | Work | 2D: Streaming postings packer (`ngram_pack_postings` table in-out fn) replacing grouped `list()` | Grouped `list()` OOMs under ANY memory_limit at scale (even states ≪ limit — verified up to 4GB); packer holds one run per thread, produced identical totals at 500MB. Memory floor is the sort's ~12-15 MB/thread. src/pack_postings.cpp + test/sql/pack_postings.test (now pack_segment.test) + build_scale.test. Superseded in Phase 7: the streaming packer was replaced by the `ngram_pack_segment` aggregate and deleted. |
| Complete | Work | 2B: create/drop pragmas + `ngram_index_stats` | src/index_pragmas.cpp + test/sql/create_index.test. Stats delivered as `PRAGMA ngram_index_stats` (pragma named args use `=`, not `:=`). |
| Complete | Work | 2C: Posting segment codec (delta + varint) | src/postings_codec.cpp + randomized round-trip tests in test/sql/postings_codec.test. Entropy compression deferred to Phase 6 tuning. |
| Complete | Gate | 10 GB build under memory limit; reopen w/ and w/o extension | Metered 10 GB run (51M rows hex, ~/duckdb-ngram-bench/gate10gb.db): `memory_limit='32GB'`, threads=16 → 1h48m48s wall, peak RSS 42.3 GB, ~600 GB NVMe sort spill (reclaimed on exit). 9.74B postings, 4,864 grams, 238,336 segment rows, 9.84 GB blobs. Spot verification exact: decoded-distinct = brute-force `contains` for 3 grams (2,191,010 / 2,195,219 / 986,849 rows). `memory_limit='8GB'` OOMs after 162 min in the final merge/CTAS phase (true RSS 19.4 GB — DuckDB meters buffer-pool allocations only); mitigations to evaluate in Phase 5/6: `preserve_insertion_order=false` for the CTAS sink, segment-range-partitioned builds. Extension-absent verified: stock duckdb 1.5.5 wheel read+wrote the indexed db, index intact on reopen with extension. In-suite restart round-trip in test/sql/index_persistence.test. |
| Complete | Test | Persistence + lifecycle sqllogictests | 8 test files, 931 assertions green (create_index, index_persistence, postings_codec, pack_postings, build_scale, trigrams, fixtures, ngram). |
| Complete | Review | Skeptical review of Phases 1-2 (13 findings, 2 blockers: rowid-column shadowing, shadow-schema collision) + fix pass | Reviewer APPROVE 2026-08-08 after re-verifying every fix with repros. Robustness fixes across index_pragmas.cpp (ownership guards, view/temp/NULL-param checks, CASCADE removal), postings_codec.cpp (varint domain checks), trigram.cpp. |
| Complete | Risk | Index size ratio unacceptable (>~60% of corpus) | Measured. Natural language (enwik9 line-per-row, 10.9M rows / 0.987 GB): 605,513 grams, 711M postings, blob ratio 0.867, on-disk ratio 1.014 (DuckDB adds ~15% storage overhead; varint blobs are high-entropy and do not lightweight-compress). Code (duckdb src line-per-row, 1.12M rows / 57 MB): 78,992 grams, blob ratio 0.718, on-disk 0.89. Hex worst case: 0.98. Disposition: exceeds the ~60% aspiration but is in pg_trgm-GIN territory (~0.5–1.0× is normal for exhaustive trigram indexes); accepted for v1, size-reduction work item added to Phase 6 scope. 1 GB builds complete under `memory_limit='8GB'` (peak RSS 11.5 GB, 9m02s for enwik9). |
| Complete | Doc | Build pragma cannot reference a table created in the same multi-statement batch | DuckDB expands pragmas before batch execution (statement_preprocessor.cpp); run `create_ngram_index` as its own statement. Noted here; user docs in Phase 6. |

## Phase 3: Explicit Query Path — Exhaustive Search with Recheck

Goal:
`ngram_search` / `ngram_candidates` return exact, complete results using the index,
including unindexed-tail and transaction-local rows.

Scope:
- `ngram_candidates(table, column, needle)`: rarest-first trigram selection capped by
  `ngram_max_grams_per_query`, posting-list intersection, rowid output.
- `ngram_search(table, needle)`: candidates → batched `DataTable::Fetch` (fetch driver
  modeled on `table_scan.cpp:109-271`) → recheck with real `contains` →
  union brute-force scan of rowids past the high-water mark → transaction-local storage
  scan phase → `SharedVacuumLock` held probe-through-fetch.
- Short-needle and missing-index fallbacks (full scan; still exhaustive).

Out of scope:
- Transparent rewrite (Phase 4); refresh/compaction (Phase 5).

Completion gate:
Differential test — `ngram_search` results identical to brute-force `contains` over
randomized corpora and needles (including "tent"/"often entered"-style false-positive
traps), with concurrent inserts and within an uncommitted writing transaction.

Testing plan:
- Property-based differential tests (randomized corpus + needle generator).
- Concurrency: search racing INSERT/DELETE/UPDATE and CHECKPOINT.
- Recheck-visibility tests: rows past HWM found; uncommitted own-transaction rows found.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Scope | Exhaustive explicit search path | src/ngram_search.cpp: `ngram_search(table, needle[, col := ...])` + `ngram_candidates(table, column, needle)` table functions, `ngram_max_grams_per_query` setting (default 3 since Phase 6 measured the curve; 8 as shipped in Phase 3). Named parameter is `col` because `column` is a reserved keyword. |
| Complete | Work | 3A: Rarest-first selection + intersection (`ngram_candidates`) | Stats scan supplies posting counts; K rarest probed via per-gram segment scans with an equality TableFilter (zone-map pruned, postings blobs of non-matching rows never read); sorted linear-merge intersection with live-segment and min/max-overlap pruning; a needle gram absent from the index short-circuits to empty. Short needle → "all indexed rowids" fallback. test/sql/ngram_search.test (superset + exact-content assertions). |
| Complete | Work | 3B: Fetch driver + recheck + vacuum lock (`ngram_search`) | Batched `DataTable::Fetch` in STANDARD_VECTOR_SIZE slices (driver modeled on table_scan.cpp:102-265 incl. the TASK_EXECUTOR HAVE_MORE_OUTPUT caveat); recheck folds through the shared `NormalizeString` (CI index) or raw bytes (CS index). `SharedVacuumLock` held unconditionally from before meta/segments reads until end of query; on v1.5.5 every rowid-moving checkpoint takes the exclusive vacuum lock or degrades to a concurrent (non-moving) checkpoint (storage_manager.cpp:672-683), so mid-query row motion is impossible by construction. |
| Complete | Work | 3C: HWM tail-scan union + transaction-local phase | One storage scan with a `rowid > hwm` ConstantFilter (rowid zone maps prune indexed row groups, RowGroup::CheckRowIdFilter) covers both the committed tail and transaction-local rows (local rowids >= MAX_ROW_ID always pass); shadow-table reads run through the caller's transaction, so an index created inside a still-open transaction is searchable there. test/sql/ngram_search_visibility.test. |
| Complete | Gate | Differential identity vs brute force under concurrency | 5 sqllogictest files, 525 new assertions (`make test`: 1456 across 13 files, green). scripts/differential_search.py property harness (randomized alphabet/gram/case/rows corpora; substring, splice-trap, case-flipped, absent, short, and empty needles; committed → tail → in-transaction → deleted → post-vacuum-subset phases; deterministic per seed, PYTHONHASHSEED-independent): recorded runs seed 1026533771 = 6458 checks, seed 555000 (3 trials) = 2370, seed 7 = 1520 — 0 failures, counts reproduced across independent runs. Concurrency: test/sql/ngram_search_concurrent.test, 6 threads × 4 rounds of INSERT/UPDATE/DELETE/CHECKPOINT racing snapshot-atomic differential queries plus searches inside uncommitted writing transactions (269 assertions). |
| Complete | Test | Property-based differential suite | scripts/differential_search.py (bounded, seed-reporting, exit-code gated) + CI-bounded test/sql/ngram_search_differential.test and ngram_search_scale.test (multi-segment corpus across the 2^20 boundary). |
| Complete | Risk | Rowid instability during vacuum if lock scope wrong | During a query: excluded by the shared vacuum lock (see 3B). Between queries: real and pinned by test — see the staleness row below. Race coverage: concurrent CHECKPOINTs against searches with deletes in flight (ngram_search_concurrent.test) and a deterministic post-vacuum check (ngram_search_scale.test). |
| Complete | Doc | Phase 3 staleness contract: misses-only, false positives never | Recheck + visibility make false positives impossible in every scenario (post-vacuum subset phase of the harness + scale test assert this against a genuinely compacted table: 916k of 917k rowids moved, results stayed ⊆ truth). Three misses-only gaps were documented on SearchBind and pinned in tests: (a) in-place UPDATE introducing the needle into a row <= hwm (v1.5.5 updates in place unless an ART index forces delete+insert, table_catalog_entry.cpp:327-336); (b) DELETE of indexed rows followed by checkpoint vacuum, which merges row groups, moves surviving rowids (invalidating postings) and shrinks the table end so even later INSERTs can land below the stale hwm; (c) DROP TABLE + re-CREATE under the same name binds the dead index. Phase 5 turned (b) and (c) into detected errors/refusals in every state it can prove, and (a) whenever a recorded row witness is hit; the residuals it cannot prove are listed in the Phase 5 ledger. |
| Complete | Work | 10GB latency benchmark (rare/moderate/dense needle vs brute scan) | gate10gb.db (51M rows / 10.05GB hex, 4864 grams, 9.74B postings), read-only, threads=16, warm cache (fits in RAM), median of 3 after 1 warmup; result counts identical between paths for all three needles (doubles as an at-scale differential check). Rare '4e732ced3463' (10 grams, K=8 probed, 2 candidates → 2 rows): search 0.168s vs brute 0.696s = 4.1x. Moderate 'abcd' (217,076 candidates → 135,887 rows, 0.27%): 0.368s vs 0.701s = 1.9x. Dense 'abc' (2,191,010 candidates ≡ matches, 4.3%): 2.868s vs 0.730s — search loses 3.9x. Crossover ≈ 0.55M candidates (~1.1% of rows) at the measured ~1.3µs/candidate fetch+recheck: the Phase 3 driver is single-threaded (user≈real), so the dense case is one core against a 16-thread scan that holds ~0.70-0.73s flat; parallel fetch/recheck (Phase 4/6) is the lever. Probe-only times: 0.185s rare (8 dense hex gram lists decoded — hex is the probe's worst case), 0.064s moderate, 0.023s dense. Cold-ish first touches: 0.302s first search, 2.948s first brute scan. |

## Phase 4: Transparent Optimizer Rewrite

Goal:
`WHERE col LIKE '%x%'` / `contains(col, 'x')` automatically use the index, with automatic
fallback, visible in EXPLAIN, and a kill switch.

Scope:
- `OptimizerExtension` (post-optimize): find `LogicalGet(seq_scan)` whose `table_filters`
  contain an `ExpressionFilter` `contains(col, const)` on an indexed column; swap in the
  `NGRAM_INDEX_SCAN` table function (pattern: `loadable_extension_demo.cpp:757`).
- `ILIKE` (`~~*`) matching against case-insensitive indexes; multi-pattern
  `LIKE '%a%b%'` via candidate intersection (built-in ART path cannot do this).
- Selectivity gate in `init_global`: candidates > `ngram_max_candidate_fraction` → fall
  back to seq scan. `SET ngram_auto_accelerate = false` disables rewrite entirely.
- EXPLAIN renders `NGRAM_INDEX_SCAN`.

Out of scope:
- Regex literal extraction; cost-model integration (none exists for extensions).

Completion gate:
Differential suite passes with rewrite enabled globally; EXPLAIN shows index scan for
accelerated shapes and seq scan for every fallback case (short needle, no index, high
selectivity, `ngram_auto_accelerate=false`); no plan-verification failures
(`ColumnBindingResolver::Verify` runs after extension optimizers).

Testing plan:
- Plan-shape sqllogictests (EXPLAIN) per accelerated + fallback shape.
- Rerun full Phase 3 differential suite through the transparent path.
- Composite predicates: `contains AND other_filter`, projections, joins over the scan.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Scope | Transparent acceleration with safe fallback | src/ngram_rewrite.cpp (+ ngram/ngram_rewrite.hpp, shared Phase 3 core exposed via ngram/search_core.hpp): post-optimize `OptimizerExtension` swaps qualifying `LogicalGet(seq_scan)` nodes for `NGRAM_INDEX_SCAN`. `ngram_auto_accelerate` defaults to **false** (opt-in): the transparent path inherits the misses-only staleness gaps (pinned in test/sql/ngram_rewrite.test with a live in-place-UPDATE miss), and silently rewriting standard `LIKE` into a path that can miss rows is not an acceptable default. Revisited and confirmed in Phase 5: the in-place-UPDATE gap cannot be closed on v1.5.5 (no triggers, no change feed), so the default stands for v1; Phase 5 did make the path decline instead of answering whenever a detector proves the index stale. |
| Complete | Work | 4A: OptimizerExtension matcher + scan swap | Matches `contains`/`~~`/`~~*` `ExpressionFilter`s inside `get.table_filters` (incl. inside ConjunctionAndFilter); swaps only `get.function`/`get.bind_data`, leaving table_index, returned types/names, column_ids, projection_ids, and the filters untouched, so column bindings survive verification unchanged. Execution reuses the Phase 3 pipeline (probe → batched `DataTable::Fetch` → recheck → `rowid > hwm` tail scan incl. transaction-local rows, `SharedVacuumLock` held probe-to-end). Recheck evaluates the pushed filters themselves via `TableFilter::ToExpression` (optional filters skipped per their contract), so query semantics never depend on index normalization and composite filters/dynamic join filters are enforced exactly; tail/fallback storage scans apply the same filters natively. Non-qualifying shapes decline the swap: `_`/escape patterns, prefix/suffix/anchored rewrites (those keep `~~` in a residual FILTER), OR-of-predicates, expressions over the column, generated-column tables, TABLESAMPLE, virtual/struct-child columns. RowGroupPruner ordering hints are dropped at swap (ordering-only when filters exist). |
| Complete | Work | 4B: ILIKE + multi-pattern LIKE handling | Case matrix enforced at both plan and init time: contains/LIKE probe either index flavor (CI folding is superset-preserving; recheck applies the CS predicate — pinned: 'TENT SHOUTING' not matched by `LIKE '%tent%'` over a CI index); ILIKE probes CI indexes only, never CS (needles carry `requires_ci`). Multi-pattern `LIKE '%a%b%'` decomposes into literal segments; one intersection over the union of all needles' grams equals the per-segment intersection; the original pattern is rechecked (segment order pinned: '%tail%tent%' vs '%tent%tail%'). Conservative bail on `_` and escape. |
| Complete | Work | 4C: Selectivity gate + settings + EXPLAIN rendering | `ngram_max_candidate_fraction` (double, default **0.01** since Phase 6 re-anchored it on measurement; **0.05** as shipped in Phase 3, when fetch was single-threaded and the anchor was a warm-cache crossover of ≈1.1% of rows hedged against cold scans). Gate runs in init_global after the probe: candidates > fraction × `GetTotalRows()` → full storage scan inside the same table function (filters applied natively, still exhaustive, no re-planning). EXPLAIN renders `NGRAM_INDEX_SCAN` + needles via `to_string`; EXPLAIN ANALYZE renders the runtime decision via `dynamic_to_string` ("index (N candidates)" / "full scan fallback: <reason>"). Kill switches tested: `ngram_auto_accelerate=false`, `SET disabled_optimizers='extension'`. |
| Complete | Gate | Differential + plan-shape suites green with rewrite on | scripts/differential_search.py `--transparent` mode: plain LIKE/contains/ILIKE (incl. synthesized `%`-multi-segment and `_` patterns) with `ngram_auto_accelerate=true` vs `disabled_optimizers='extension'` execution, phases committed/tail/in-txn/deleted + post-vacuum subset, plus a per-trial EXPLAIN assertion that the rewrite fires (no vacuous green). Recorded release runs: seed 902147 (8 trials) = 3496 checks / 0 failures; explicit-path seed 555000 (8 trials) = 6150 checks / 0 failures (explicit functions untouched). `make test` release: 1695 assertions / 15 files green. |
| Complete | Test | Composite-predicate and join plan tests | test/sql/ngram_rewrite_plan.test (88 assertions): EXPLAIN per accelerated shape (LIKE, contains, ILIKE, multi-pattern, regexp-literal, CS-index LIKE, composite AND, double-needle, projection-exclusion, rowid, count(*), join, both ORDER BY...LIMIT shapes incl. late materialization) and per fallback (short needle, `_`, ESCAPE, NOT LIKE, prefix/suffix, anchored, no index, wrong column, ILIKE-on-CS, OR, lower(col), '%%', TABLESAMPLE, both kill switches) + EXPLAIN ANALYZE mode strings. test/sql/ngram_rewrite.test (151 assertions, under `PRAGMA enable_verification` — every query also differentially checked against the unoptimized plan): correctness for all shapes, txn-local visibility, in-txn index build, DML (UPDATE/DELETE driven by accelerated scans), prepared statements across index drop/rebuild and setting flips, staleness pin. |
| Complete | Risk | Plan verifier rejects rewritten bindings | DEBUG build (`GEN=ninja make debug`) runs per-pass `ColumnBindingResolver::Verify` + AddressSanitizer: full sqllogictest suite (15 files, 1695 assertions) and a `--transparent` differential run (seed 31337, 4 trials, 1740 checks / 0 failures) green against the DEBUG build. The DEBUG runs earned their keep, catching two release-silent bugs: (1) ASAN stack-use-after-scope — a temporary string passed to `EntryLookupInfo`, which stores a reference (fixed in TryRewriteGet); (2) a D_ASSERT in `RowGroupCollection::InitializeScan` when scanning a table with no committed row groups, i.e. shadow tables created inside the current transaction — latent since Phase 3, fixed for both query paths via the shared `InitializeExhaustiveScan` helper (initializes only the transaction-local phase for committed-empty tables). `verify_serialization=false` on the injected function (never serialized; DEBUG serialization verification skips it). |

## Phase 5: Maintenance — Refresh, Compaction, Deletes

Goal:
The index tracks a changing table with bounded staleness cost and no correctness impact.

Scope:
- `PRAGMA ngram_refresh(table[, col])`: index the committed rows between the HWM and the
  table's last committed rowid into a new segment generation; advance the HWM
  transactionally with the segment write.
- `PRAGMA ngram_compact(table[, col][, purge])`: merge the segment rows that share a
  (gram, segment_no) — parallel-build splits and refresh generations — back into one row
  per key, and drop postings whose rowid no longer exists.
- Delete handling: stale candidates are eliminated by fetch/recheck until a checkpoint
  vacuums the deletes; vacuum merges row groups and MOVES surviving rowids
  (`row_group_collection.cpp` VacuumState: any table without native indexes has
  `can_change_row_ids = true`), stranding postings and leaving the recorded HWM above the
  table's new end, so refresh must detect row motion (or rebuild) — recheck keeps this
  misses-only, never false positives.
- Update handling: v1.5.5 updates rows IN PLACE (same rowid) unless the updated column is
  covered by an ART index (`table_catalog_entry.cpp:327-336`), and the engine offers no
  way to enumerate the rows an UPDATE touched: `CREATE TRIGGER` is a parser error and
  there is no change feed. There is therefore no sound incremental way to re-index
  updated rows — refresh covers the HWM tail only, and an in-place update below the HWM
  is repaired by drop + create, not by refresh. Decision 5C (`maintenance :=
  'incremental'` trigger mode) is resolved as unavailable on v1.5.5; revisit on an engine
  upgrade that adds triggers or a change feed.
- Recreation detection: DROP TABLE leaves the shadow schema behind, and the name-based
  ownership guard matches a recreated table of the same name, so queries silently use the
  dead index (pinned in test/sql/ngram_search_visibility.test). v1.5.5 has no persistent
  table identity token (catalog OIDs are per-process, and catalog ownership dependencies
  are in-memory only — verified). Store a schema fingerprint (ordered column names +
  types) plus the table's catalog oid, its database's catalog oid and an instance token in
  meta. When instance and database oid both match, the table oid is comparable: a changed
  one is proof of re-creation, and an unchanged one is proof the table was never
  re-created — which in turn makes ALTERs to other columns provably harmless, so only the
  indexed column's own shape is checked. When identity cannot be proven (another session,
  another attach incarnation) the full column list is the only recreation signal left and
  is enforced strictly. A same-shape re-creation performed in an earlier session stays a
  documented gap.
- Staleness detection generally: every verdict must be *proof*, because it gates errors
  and refusals. Metadata-only checks (O(1) rowid count, catalog oid, column list) run on
  every query; the maintenance pragmas additionally re-read a handful of recorded row
  witnesses, which is what catches a vacuum whose row motion later appends have papered
  over.

Out of scope:
- Automatic background scheduling (user/cron-driven).

Completion gate:
Long-run churn test (interleaved insert/delete/update/refresh/compact/checkpoint/reopen)
holds differential identity throughout; refresh cost proportional to tail size, not table
size.

Testing plan:
- Churn harness with periodic differential verification.
- Refresh/compact crash-interruption tests (kill mid-pragma, reopen, verify).

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Scope | Refresh + compaction lifecycle | src/ngram_maintenance.cpp (+ ngram/maintenance.hpp): `PRAGMA ngram_refresh(t[, col=...])`, `PRAGMA ngram_compact(t[, col=...][, purge=...])`, the staleness detectors, and the `ngram_unpack_postings` in-out function compaction needs (src/pack_postings.cpp). Meta is format_version 2 (schema_fingerprint, table_oid, instance_id, row_samples); every reader rejects other versions with a rebuild-required error (test/sql/ngram_maintenance.test pins search/refresh/compact/stats all erroring on a v1 meta row). |
| Complete | Work | 5A: HWM refresh with transactional segment publish | Tail rows (`rowid > hwm AND rowid < MAX_ROW_ID`) are packed and APPENDED as a new generation — the segments schema already tolerates several rows per (gram, segment_no) and readers union them, so no schema change was needed — then the HWM advances in the same multi-statement pragma expansion, which the preprocessor wraps in one BEGIN/COMMIT (`statement_preprocessor.cpp`: >1 statement and not already in a transaction; the has_select exemption applies to MULTI_STATEMENT, not to pragma expansion). Stats gets per-gram delta rows, which the probe already sums. A concurrency guard fails the script if the meta row changed between the callback reading it and the script running. |
| Complete | Work | 5B: Segment compaction + tombstone purge | Fragmented keys are decoded (`ngram_unpack_postings`), anti-joined against the table's live rowids, and re-packed through the existing streaming packer, then swapped in; stats is rebuilt exactly from the merged segments. `purge = true` widens the rewrite from fragmented keys to every key, so dead postings are dropped everywhere rather than only where a merge was rewriting the blob anyway. A posting is dropped only when the base table has no such row in the compacting transaction's snapshot, and MVCC keeps older readers on the pre-compact segment rows. test/sql/ngram_maintenance.test: merge, one-row-per-key, stats agreement, idempotence, purge, and a differential check after each. The packer is parallel, so in principle a key whose run straddles two packing threads can come back as two rows (readers union them, and the next compaction merges them); not observed in practice — the 700k-row crash-test corpus compacts 492 segment rows to exactly 164, one per key. |
| Complete | Work | 5C: Update/delete semantics + optional trigger mode decision | Resolved as unavailable: `CREATE TRIGGER trg AFTER INSERT ON t ...` → `Parser Error: syntax error at or near "TRIGGER"` on v1.5.5, and there is no change feed, so no sound incremental way to find in-place-updated rows exists. Refresh covers appends; updates need drop + create. Recorded in the scope text above, on `SearchBind`'s contract comment (src/ngram_search.cpp), and pinned in test/sql/ngram_search_visibility.test. DELETE without a vacuum stays refreshable (pinned) — deletes leave surviving rowids in place and recheck hides the dead postings. |
| Complete | Work | 5D: Staleness detection (row motion, re-creation, shape) | Detectors in src/ngram_maintenance.cpp, wired into both probe paths (error on the explicit path, decline on the transparent one) and into refresh/compact/stats (refuse/report). Query-time cost is O(1) + O(columns): allocated rowid count vs HWM, the identity triple (instance token, database oid, table oid), and the column-list check. The pragmas add the row witnesses. Measured: `pragma_storage_info` over a 20M-row/163-row-group table is 2 ms, but per query at TB scale it is not affordable, so the deep check is pragma-only. Identity is scoped to the attach incarnation because ATTACH renumbers oids: an earlier version compared table oids on the instance token alone and called a healthy index re-created after a DETACH + re-ATTACH (reviewer finding; regression-tested in ngram_maintenance_identity.test). Proven identity also relaxes the shape check to the indexed column's own name and type, so renaming, dropping or retyping any other column no longer forces an hours-scale rebuild; without proof the full column list stays strict. |
| Complete | Gate | Churn test green; refresh cost ∝ tail | scripts/churn_maintenance.py (INSERT/DELETE/UPDATE/CHECKPOINT/refresh/compact, a fresh duckdb process per step so every step reopens the database, small ROW_GROUP_SIZE so deletes trigger real vacuum merges): seed 90210 (120 rounds, 6000 rows) 1920 checks / 0 failures / 30 un-maintainable states, all 30 surfaced by a detector; seed 7 (120 rounds, 3000 rows) 1920 checks / 0 failures / 23 states, 19 surfaced; seed 20260809 (60 rounds, 5000 rows) 960 checks / 0 failures / 8 states, 7 surfaced; seed 20260810 (40 rounds) 640 checks / 0 failures / 13 states, 12 surfaced. The remainder are in-place updates that missed every witness — the documented gap; the harness rebuilds on the contract's advice and requires exhaustive results again afterwards. The harness also has a false-alarm gate (`--no-stale-expected`): only operations that keep an index valid (append, refresh, compact, checkpoint) run, and *any* detector verdict fails the run — seed 616 (40 rounds) 640 checks / 0 failures / 0 verdicts; seed 4711 (30 rounds) 480 checks / 0 failures / 0 verdicts. It is not vacuous: allowing deletes and updates back into the same strict run turns it into 3 failures naming the detector that fired. Refresh cost ∝ tail: the same 5000-row tail refreshes in 0.062 s on a 1M-row table and 0.065 s on an 8M-row table (whose build takes 5.3 s vs 56.1 s), and `EXPLAIN ANALYZE` of the generated tail scan over 8,005,000 rows reads 5,000 rows with `Filters: rowid>7999999 AND rowid<36028797018960000` in 0.014 s. An empty refresh is 0.025-0.032 s, essentially process start-up plus the witness reads. |
| Complete | Test | Crash-interruption recovery tests | scripts/crash_maintenance.py: runs the pragma in a child duckdb process, SIGKILLs it at offsets spread across the measured duration, reopens, and requires the recorded state to be exactly the pre- or post-operation one plus differential identity. Recorded runs: seed 555, 400k rows + 150k tail, 16 kills across a 0.99 s refresh and 16 across a 2.12 s compact (22 of the 32 landed before the commit) — 32/32 reopened clean, every state pre or post, 0 differential mismatches; seed 909 at 150k rows + 60k tail, 16 kills, 0 failures; seed 31337 at 40k rows, 16 kills, 0 failures. Local-only by nature (the kill offsets are timing-dependent); the deterministic part of the story is the single-transaction expansion. |
| Complete | Decision | Ship trigger-based incremental mode in v1? | No: v1.5.5 has no triggers (parser error, verified) and no change feed. duckdb-fts's trigger pattern is not available on the pinned engine. Revisit on an engine upgrade. |
| Complete | Decision | `ngram_auto_accelerate` stays default-false in v1 | The in-place-UPDATE gap survives Phase 5 (5C), so silently rewriting a plain `LIKE` into a path that can miss rows is still not an acceptable default. Phase 6 revisits the documentation, not the default. |
| Complete | Work | Deferred Phase 1-4 fix-ups folded in | (a) Orphaned-index drop with mismatched casing left an empty shadow schema — own-table recognition is now case-insensitive (pinned in create_index.test). (b) The ownership guard was vacuous on an empty meta table — it now compares a scalar row count, so an empty or foreign meta row fails every guard (pinned). (c) `ngram_index_stats` reported nothing through a colliding shadow schema — it now raises the collision error like the other pragmas (create_index.test flipped). |
| Complete | Risk | Building an index inside a transaction with uncommitted rows recorded transaction-local rowids | Found while verifying refresh semantics: `PRAGMA create_ngram_index` inside a transaction that had inserted rows recorded `hwm_rowid = 36028797018960000` (MAX_ROW_ID) and postings for local rowids; after COMMIT, `ngram_search` fetched one and duckdb raised `INTERNAL Error: LocalStorage::FetchChunk - local storage not found`, invalidating the database. Build and refresh now index committed rows only (`rowid < MAX_ROW_ID`); uncommitted rows are covered by the tail scan and picked up by a later refresh once they have real rowids. Pinned in test/sql/ngram_maintenance.test. |
| Complete | Test | Phase 3/4 differential harnesses still green | scripts/differential_search.py explicit seed 555000 6150 checks / 0 failures; `--transparent` seed 902147 3496 checks / 0 failures (reviewer-verified counts, identical to pre-fix runs — the stale-refusal change truncated nothing). Its post-vacuum phase learned the new outcome: where a vacuum leaves the table shorter than the high-water mark the explicit path now refuses instead of returning a subset, which the harness accepts as the better answer (the subset property is still enforced whenever the index does answer). |
| Complete | Test | Maintenance sqllogictests | test/sql/ngram_maintenance.test (164 assertions): refresh indexes the tail (candidates prove it, not just the tail scan), no-op refresh, refresh after an un-vacuumed DELETE, compaction merge/stats/idempotence/purge, two columns refreshed independently via `col =`, transaction-local rows never indexed, format_version mismatch on every reader, transparent-path decline via EXPLAIN, shape changes (appended columns tolerated; dropping another column stays healthy under proven identity; retyping the indexed column refused), ownership refusals, evil identifiers, and refresh atomicity inside a user transaction (visible inside, fully rolled back on ROLLBACK, durable on COMMIT). test/sql/ngram_maintenance_identity.test (72 assertions): benign ALTERs (rename, retype, drop of other columns) stay healthy while identity is proven, the indexed column's own rename does not, DETACH + re-ATTACH is healthy for search/candidates/refresh/compact/stats, a re-creation inside the new incarnation is still caught, and after a restart the strict column-list rule applies again. test/sql/unpack_postings.test (38 assertions): `ngram_unpack_postings` happy path, pack/unpack round trip, empty and multi-chunk blobs, malformed/truncated/garbage blobs, NULLs in any column, and wrong input schemas. Plus the flips in ngram_search_visibility.test and ngram_search_scale.test below. `make test`: 18 files, 2034 assertions. |
| Complete | Risk | New code paths hide memory or verification faults | DEBUG build (`GEN=ninja make debug`, AddressSanitizer + per-pass plan verification): full sqllogictest suite green (18 files, 2034 assertions), plus the churn harness against the DEBUG binary in both modes (seed 1234, 20 rounds, 320 checks / 0 failures; `--no-stale-expected` seed 4711, 15 rounds, 240 checks / 0 failures / 0 detector verdicts) — the witness fetches, the unpack/pack pipeline and the maintenance scripts all run clean under ASAN. |
| Complete | Doc | Phase 5 staleness contract: what is detected and what is not | Detected, and therefore an error on the explicit path / a decline on the transparent one / a refusal in the pragmas: a table shorter than the recorded HWM; a re-creation inside the instance and attach incarnation that recorded the identity (table oids survive every ALTER but are handed out afresh by CREATE — verified for ADD/DROP/RENAME COLUMN, ALTER TYPE, RENAME TABLE, CREATE OR REPLACE); the indexed column losing its name or type while identity is proven; a column list that is no longer a prefix of the recorded one when identity cannot be proven; and, in the maintenance pragmas, a recorded row witness whose value changed. Not detected, documented, misses-only: an in-place UPDATE that misses every witness; a checkpoint vacuum whose motion left every witness holding an equal value; and — where identity is proven, so the column list is not compared — dropping and re-adding the indexed column with the same name and type, which the query paths read as healthy while the witnesses still catch it in the maintenance pragmas and stats. Never affected: false positives, which recheck makes impossible in every state. |

## Phase 6: Scale Validation, Hardening, Release

Goal:
Meet the performance target at scale and ship as an installable community extension.

Scope:
- Benchmark suite: build time, index size ratio, p50/p95 query latency vs corpus size
  (100 GB sustained, built incrementally); memory behavior under `SET memory_limit`.
- Tuning defaults from measurement (`ngram_max_grams_per_query`,
  `ngram_max_candidate_fraction`, gram size, segment size, compression choice).
- Parallel fetch/recheck: Phase 3 measured the dense-needle case losing 3.9× to a
  16-thread scan with single-threaded fetch as the only cause.
- Index size reduction, measure-first against the Phase 2 ratio measurements (real text ≈
  0.87–1.01× corpus on disk): postings encoding beyond LEB128 deltas (bit-packing /
  roaring-style for dense grams), segment granularity, per-run overhead amortization.
  Implement only on a ≥20% measured on-disk win.
- Community-extensions packaging, versioned against a DuckDB release; docs with the
  staleness/maintenance contract and fallback semantics; recheck the duckdb-fts trigram
  sidecar for overlap before submission.

Out of scope:
- Native custom-index backend (future work if core fixes buffer pinning / hardens WAL
  replay — revisit `fixed_size_buffer.hpp` FIXME and the `KEEP_ROW_IDS` vacuum gate).
- Opening the community-extensions pull request. Everything is prepared and validated
  locally (`packaging/`); submitting is the maintainer's call.

Completion gate (revised in Phase 6 — the original "TB spot check" is hardware-bounded):
A monolithic TB build is impossible on the development machine — a 10 GB monolithic build
already needs ~600 GB of sort spill at `memory_limit='32GB'`, so a TB corpus plus its
~1× index does not fit at all on 1.7 TB of free NVMe. The gate is therefore:
(a) a 100 GB sustained benchmark built INCREMENTALLY (initial build on one chunk, then
append + `ngram_refresh` per chunk, with `ngram_compact` measured at the end), which
doubles as a real-workload validation of Phase 5;
(b) a measured scaling curve at 1/10/100 GB — build/refresh cost, index size, p50/p95
latency per needle-selectivity class, warm and cold — supporting an explicit TB
extrapolation with its assumptions stated;
(c) selective substring query ≤ hundreds of ms p95 at 100 GB with a cold cache;
(d) the extension installs and runs in a STOCK DuckDB build from an extension repository.
True-TB validation requires bigger hardware; whether to provision it is the user's call.

Testing plan:
- Reproducible benchmark scripts + recorded results per release.
- Full test matrix (Phases 1–5 suites) against the pinned DuckDB release build, and
  against a DEBUG + AddressSanitizer build.
- Parallelization must change nothing about results: differential (explicit and
  transparent), churn, and crash harnesses re-run with fresh seeds.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Decision | The plan's "TB spot check" is hardware-bounded; deliver 100 GB sustained + a measured scaling curve + an explicit TB extrapolation | A 10 GB monolithic build needs ~600 GB of sort spill at `memory_limit='32GB'`, so a TB corpus plus its ~1× index cannot fit on 1.7 TB of free NVMe, and a monolithic TB build cannot run at all. Gate text above rewritten accordingly. True-TB validation is deferred until bigger hardware exists; the user decides whether to provision it. |
| Complete | Scope | Scale benchmarks + tuned defaults | benchmarks/ holds the corpus generator, build/latency/sweep/encoding harnesses, a `run_all.sh` that reproduces every recorded number in order (scaled by `NGRAM_REPLICAS`), and RESULTS.md with the tables. Two defaults changed on measurement, three settings confirmed unchanged. |
| Complete | Work | 6A: 100 GB benchmark harness + corpus | Corpus is replicated-with-perturbation enwik9 (seed 20260809): replica r of source line i keeps the line but rewrites three characters at a seeded position with seeded letters, so gram distribution stays enwik9's while no two rows are byte-identical. It makes the index measurably *harder*, not easier — 788,596 distinct grams against enwik9's own 605,513, blob ratio 0.922 against 0.867. Plus the duckdb source tree as a code corpus (1,055,570 lines / 0.051 GiB) for the ratio table. **Built incrementally, 100 single-replica steps**: one `create_ngram_index` (208.0 s) and 99 `ngram_refresh` (median 210.3 s, min 206.7, max 267.7), total 5.95 h, spill 24.6-27.4 GB per step, peak RSS 52.4 GB. Final: 92.225 GiB of text, 1,092,042,300 rows, 72,434,741,725 postings, 85.289 GiB of blobs, 258,732,511 segment rows, 172.538 GiB database. Chunk size was itself measured: a 5-replica chunk costs 612 s per replica against 208 s for a 1-replica chunk, and 278 GB of spill against 25 GB, because the external sort starts needing multiple merge passes — extrapolating the 5-replica point to 100 GB gives ~3.4 TB of scratch, more than the machine has. |
| Complete | Gate | 100 GB sustained build, spot verification exact, detectors quiet, disk returned | Verification at 100 GB is exact on three grams spanning the frequency range: `ìa)` 98 = 98, `人ak` 5 = 5, `␠␠␠` 276,151,351 = 276,151,351 (decoded distinct vs brute-force `contains` over the indexed range). `ngram_index_stats` reported `stale_reason = NULL` before and after compaction at every scale — no false staleness after 100 appends and refreshes. Compaction at 100 GB: 9433.5 s, 53.3 GB RSS, 311.6 GB spill, postings byte-identical before and after (72,434,741,725), segment rows 258,732,511 → 248,445,026. The latency runs double as an at-scale differential check: every index/scan and explicit/brute-force pair returned identical counts, up to 37,102,213 matching rows. Bench artifacts removed afterwards; disk back to baseline. |
| Partial | Gate | Selective query ≤ hundreds of ms p95 at 100 GB, cold cache | **Met warm, missed cold — recorded, not rounded into a pass.** At the shipped defaults the rare needle (`supercalifragilistic`, 9.2 × 10⁻⁷ of 1.09 billion rows) is **0.641 s p50 / 0.643 s p95 warm** at 100 GB, inside "hundreds of milliseconds" at the top of the range, against 4.266 s for the parallel scan it replaces (6.66× faster). Cold, with the page cache evicted before every sample, it is 3.217 s against 17.625 s — a 5.5× win but an order of magnitude past the target. 10 GB meets both comfortably (0.073 s warm p95, 0.157 s cold). The cold gap is disk, not CPU: the 100 GB database is 182 GB against 125 GB of RAM, so a cold probe reads posting lists from NVMe. The residual cost is the probe itself — 0.564 s of the warm 0.641 s — which is single-threaded and costs Θ(corpus × K) however selective the needle is; a needle matching 88 rows in 1.09 billion still decodes three corpus-scale posting lists. Retuning K from 8 to 3 took this row from a clear miss (1.762 s warm / 7.653 s cold) to a warm pass, and is the whole of the improvement available from tuning; going further needs a structural change to the probe, which belongs to the throughput and incremental-refresh phases. **Superseded by Phase 7 (7C), which made that structural change.** Warm at 100 GB is now 0.125 s p50 / 0.133 s p95 — an order of magnitude inside the target rather than at the top of it — and cold is 0.714 s p50 / **0.816 s p95** against 16.387 s for the cold scan, 4.5× better than the 3.217 s recorded here and 20× better than scanning. Cold at 100 GB is therefore inside a second but still above a few hundred milliseconds; the floor is now attributed rather than inferred (0.30 s probe blob reads + ~0.46 s for 1,001 random row fetches from a 182 GiB file, both cold IO, neither CPU). 1 GB and 10 GB cold are 0.051 s and 0.110 s. See the Phase 7 gate row and benchmarks/RESULTS.md §2.
| Complete | Gate | Community install works in a stock DuckDB build | Proven against duckdb_cli-linux-amd64 v1.5.5 (`d8cdaa33fd`), not this repo's build, via a local extension repository in DuckDB's own layout — see the 6C row. The `INSTALL ngram FROM community` form itself waits on the submission, which is the maintainer's to open. |
| Complete | Work | 6B: Default tuning from measurements | Two changed, three confirmed, every sweep output in benchmarks/RESULTS.md §3 and the `*.sweep.json` files. **`ngram_max_grams_per_query` 8 → 3**: total query time against K is a shallow floor at 2-4 with a steep right arm — at 100 GB a rare needle costs 0.465 s at K=2, 0.640 s at K=3, 1.750 s at K=8, 5.211 s at K=16, measured identically at 10 GB and on a deliberately dense-gram (bigram) index. Re-measuring the full latency suite at the new default confirms it end to end: the rare needle goes 0.033/0.110/1.762 s → **0.026/0.059/0.641 s** at 1/10/100 GB and the moderate one 0.045/0.366/5.279 s → **0.031/0.125/1.770 s**, i.e. 2.7× and 3.0× at 100 GB, which is what turned the latency gate from a miss into a warm pass. Three rather than the measured-optimal two because K=2 leaves 0.89% of rows as candidates on the dense-gram index against K=3's 0.28%, close enough to the selectivity gate to risk abandoning the index entirely. Match counts were identical at every K, as the superset invariant requires. **`ngram_max_candidate_fraction` 0.05 → 0.01**: the gate runs after the probe, so its only job is fetch-versus-scan for the remaining work; measured fetch cost is 228/252/279 ns per candidate at 1/10/100 GB against parallel scans of 0.046/0.427/4.623 s, putting break-even at 1.6%/1.3%/1.1% of rows. Verified against a 21-needle ladder at both 10 and 100 GB: 0.01 leaves the index alone where the index is cheaper (`Wikipedia:` 5.36 s index vs 7.42 s fallback) and fires where it is not. **Confirmed unchanged**: gram size 3 (gram 2 is 37% smaller but yields 5,823 candidates instead of 8 for the same rare needle; gram 4 costs 37% more space and 5.6× the probe for one fewer candidate), `SEGMENT_SHIFT` 20, and the postings encoding. |
| Complete | Doc | The candidate ladder is not monotonic in selectivity | Worth recording because it bounds what any selectivity gate can do. `Ethelred` matches 1.5 × 10⁻⁵ of rows and costs 3.2× a scan, while `philosophy` matches 70× more rows and roughly ties. The difference is the grams: `Ethelred` has only six, so all of them are probed including `the` (196 million postings), whereas `photosynthesis` has twelve and rarest-first can drop the dense ones. Cost tracks the densest gram probed, not the rarity of the needle, and it is all spent before the gate is reached. |
| Complete | Work | 6D: Parallel fetch + recheck for `ngram_search` and `NGRAM_INDEX_SCAN` | src/ngram_search.cpp + src/ngram_rewrite.cpp: both scans gained `init_local`, a per-thread local state (fetch chunk, `ColumnFetchState`, `TableScanState`, recheck executor / fold scratch, selection vector), an atomic candidate-block cursor, and `DataTable::InitializeParallelScan`/`NextParallelScan` for the tail (which also covers the transaction-local rows, so exhaustiveness is untouched — the same rows are visited, once each). `MaxThreads()` reports the work actually available (candidate blocks + row groups past the high-water mark, or the whole table in fallback mode), so a two-candidate query still runs on two threads. The design stayed contained: the vacuum lock stays a single shared key in the global state (v1.5.5's own index scan does exactly this, `table_scan.cpp:127`), the `rowid > hwm` tail filter is unchanged, and the HAVE_MORE_OUTPUT protocol is unchanged. `get_partition_data` was added so output order survives: fetch blocks carry their block number, storage batches follow them, and ordered sinks reassemble the single-threaded order. Without it `UseBatchIndex` is false, DuckDB picks a non-parallel result collector, and `MaxThreads()` would have had no effect at all for a top-level SELECT (`physical_result_collector.cpp:30-51`). |
| Complete | Test | Parallelization changes nothing about results | test/sql/ngram_parallel.test (43 assertions): a 600k-row corpus where dense needles fill many fetch blocks; explicit and transparent paths compared to brute force in both directions; output-position equality against a rowid-ordered brute force (pinning the batch-index ordering contract); committed tail rows and this transaction's uncommitted rows under `PRAGMA verify_parallelism` (one work unit per vector); the in-scan full-scan fallback; `SET debug_physical_table_scan_execution_strategy='SYNCHRONOUS'`; ordered sinks (`CREATE TABLE AS`, `LIMIT`). `TASK_EXECUTOR_BUT_FORCE_SYNC_CHECKS` is deliberately not covered — DuckDB's own validator calls it "expected to throw on non-trivial workflows" and v1.5.5's built-in table scan violates it identically (`physical_table_scan.cpp:126-128`, `table_scan.cpp:212-216`). |
| Complete | Decision | Postings encoding: measure first, change only on a ≥20% on-disk win | Rejected on measurement. benchmarks/analyze_encoding.py reconstructs the posting stream through the codec's own inverse and re-derives the encoded size from first principles, matching the real blob total to 0.0000% on both corpora — then evaluates alternatives on exactly those postings. Delta widths are 77.1% one byte / 21.4% two / 1.5% three (enwik9 1 GB), per-segment headers are 1.6% of the total, so there is no per-run overhead or wide-delta tail to attack. Best alternative (per-block choice of bit-packed vs varint, B=32): **+7.8% on enwik9, +11.8% on code** — under half the bar, for a format flag, two decode paths, a format-version bump and full re-validation. Plain bit-packing peaks at +5.8%; roaring-style is 53.7% *worse* (lists are far too sparse per 2^16 chunk for bitmaps and a 2-byte array entry loses to a 1.26-byte varint). `format_version` stays 2; no migration code needed. |
| Complete | Decision | Segment granularity (`SEGMENT_SHIFT`) stays 20 | Same analysis, all shifts costed exactly on the 1 GB posting stream: shift 16 costs +3.88%, 18 +1.09%, 22 −0.43%, 24/26 −0.59%. Coarser buckets buy at most 0.6% of bytes while giving the probe's live-segment and min/max pruning less to work with; finer buckets cost real space. |
| Complete | Decision | Build-time `preserve_insertion_order=false` rejected | Matched pair at 1 replica (both 12 threads / 24 GB, same background load): order preserved 364.3 s / 2,549,366 segment rows / 913,035,911 blob bytes; order relaxed 484.8 s / 2,601,819 segment rows / 913,367,136 bytes. 33% slower and slightly larger — the packer wants one ordered pass. Postings were identical either way and spot verification was exact, so the option is safe, just not useful. |
| Complete | Risk | duckdb-fts trigram sidecar overlaps scope | Resolved: no collision. duckdb-fts merged PR #52 ("Add indexed wildcard and regex search") to main on 2026-08-04 and has **not released it** — no tags, no releases, last version bump 2026-02-16 — so `INSTALL fts` on v1.5.5 does not contain it. What it builds is a *dictionary* trigram sidecar (`term_grams`/`raw_term_grams`, keyed by termid) used to expand a whole-token wildcard or regex query term before BM25 scoring; its README states the sidecars "grow with the term dictionary rather than with the document corpus" and that a pattern is matched "as one whole-token pattern ... against the normalized raw-term dictionary". Character-level substring search across token boundaries over raw strings is a different structure with a different cost model. duckdb/duckdb #16071 (a real trigram *tokenizer*) is still open and unanswered; the only implementation of that idea is `tkys/duckdb-fts-trigram`, an unaffiliated single-author repo with 0 stars, one push, and no distribution. Registry survey: 306 community extensions, zero hits for trigram/ngram/substring/inverted index; the name `ngram` is unclaimed and no core extension uses it. Recorded in Key Research Facts. |
| Complete | Work | 6C: Community-extension packaging (prepared and validated, PR not opened) | packaging/community-extensions/extensions/ngram/description.yml is the exact single file a submission consists of; packaging/SUBMISSION.md carries the format research, the blocking checklist, the local validation transcript and the drafted PR text. community-extensions pins DuckDB centrally (`build.yml`: `DUCKDB_LATEST_STABLE: 'v1.5.5'`, ci_tools `v1.5-variegata`) — there is no `duckdb_version` field — so this repo's v1.5.5 pin is exactly right, and with no external dependencies neither `vcpkg_commit` nor `requires_toolchains` is needed. **Historical format-2 installability was proven against a STOCK binary**, not this repo's build: duckdb_cli-linux-amd64 v1.5.5 (`d8cdaa33fd`) + a local repository in DuckDB's own layout (`<repo>/v1.5.5/linux_amd64/ngram.duckdb_extension.gz`) + `SET custom_extension_repository`; `INSTALL ngram; LOAD ngram;` reported `install_mode = REPOSITORY`, and build/search/refresh/stats worked. Phase 11 supersedes the old extension-absent writeability result: format-3 guarded tables remain readable but require `LOAD ngram` for DML. Blocking on the maintainer: the repo is private, and the full CI matrix (Phase 9) has to be rerun on the final submission commit before a release SHA is pinned. |
| Complete | Doc | User docs: maintenance contract, fallbacks, settings | README.md rewritten from the extension template into user documentation: install, quick start, the correctness contract (superset + recheck, tail scan, transaction-local phase, case semantics), the full staleness contract as two tables in user language (detected → refused; not detected → misses only, with the repair for each) copied from the Phase 5 ledger, maintenance guidance (when to refresh, when to compact, when only a rebuild will do) and a column-by-column reading of `ngram_index_stats`, an API reference including which query shapes the transparent path accepts and declines, a settings table with defaults and the reason for each, performance expectations by selectivity class, limitations (including the pragma-in-a-batch caveat and the `current_transaction_invalidation_policy` reset), and platform support. Measured numbers live in benchmarks/RESULTS.md, which the README links. |
| Complete | Test | Parallel fetch/recheck measured before and after | `threads=1` reproduces the pre-parallel pipeline exactly (it ran on one thread whatever the thread setting was), compared against the same 24-thread scan both versions raced. Dense worst case: 10.016 s → 3.138 s at 10 GB (3.19×) and **218.887 s → 34.367 s at 100 GB (6.37×)**; moderate 23.082 s → 5.279 s at 100 GB (4.37×); rare 1.08× (it is all probe, and the probe was never the parallel part). Against the scan it races, the dense case went from 47.4× slower to 7.43× at 100 GB. In gate terms: fetch fell from ~2,970 ns to ~279 ns per candidate at 100 GB, moving the fetch-versus-scan crossover from 0.14% to 1.1% of rows — the band of selectivities where the index is the right choice is about eight times wider. The packet's predicted "~0.2 s worst case" did not materialise and is corrected in RESULTS.md: fetching tens of millions of rows is expensive on any thread count, and the gate, not parallelism, is what keeps the default path off that number. The in-scan fallback also stopped being a single-threaded crawl — it is now a parallel scan, measured at 1.3-4.3× a plain scan with nearly all the excess being the already-paid probe. |
| Complete | Test | Phase 1-5 harnesses re-run with fresh seeds after the Phase 6 code changes | Release build: `make test` 19 files / 2081 assertions green (2074 before test/sql/ngram_parallel.test was strengthened to 43 assertions); scripts/differential_search.py explicit seed 20260901 = 6281 checks / 0 failures, `--transparent` seed 20260902 = 3472 checks / 0 failures; churn seed 20260903 (60 rounds, 5000 rows) = 960 checks / 0 failures / 9 un-maintainable states (8 detector-reported, 1 the documented in-place-update gap), churn `--no-stale-expected` seed 20260904 (30 rounds) = 480 checks / 0 failures / 0 detector verdicts; crash seed 20260905 (400k rows + 150k tail, 16 kills across a 0.84 s refresh and 16 across a 1.79 s purging compact) and seed 20260906 (150k + 60k) = 0 failures, every reopen landing on exactly the pre- or post-operation state. DEBUG + AddressSanitizer build: full suite 19 files / 2081 assertions green, churn seed 20260907 (20 rounds) = 320 checks / 0 failures, churn strict seed 20260908 (15 rounds) = 240 checks / 0 failures / 0 verdicts, `--transparent` differential seed 20260909 (4 trials) = 1732 checks / 0 failures. |
| Open | Risk | Two unreproduced full-suite failures under heavy load | Twice during Phase 6 a full-suite run reported one failed assertion where every isolated and every captured re-run passed. Both happened while the machine was running two or three other heavy jobs (the 10 GB or 100 GB corpus build, plus another suite), both were piped through `tail`, so neither preserved the assertion text, and `test/sql/ngram_parallel.test` was the last file the progress line named in one of them. It passed in isolation against the same DEBUG binary immediately afterwards, twice, and a captured full-suite re-run of the same tree passed with 2081 assertions. Closing evidence: on the quiet machine, with the shipped defaults, **13 consecutive captured full-suite runs — 10 release and 3 DEBUG + AddressSanitizer — all "All tests passed (2081 assertions in 19 test cases)"**, each written to its own file rather than a pipe. The state is therefore: unexplained, load-correlated, never reproduced when isolated, and never named. Not claimed as fixed. If it recurs, the run must be captured whole — a `tail` pipe is what cost the diagnosis both times. |
| Complete | Doc | Deferred cosmetics: pragma outputs surfaced internal guard rows | Closed. The generated scripts guarded themselves with `SELECT CASE WHEN ... END AS ngram_ownership_check` / `ngram_meta_guard`, and the CLI printed one NULL row per guard for every create/drop/refresh/compact. The guard is now `SET VARIABLE __ngram_guard = (SELECT <same expression>)`, which evaluates the expression, raises the same errors, and returns no rows. Statement count is unchanged, so the preprocessor still wraps the expansion in exactly one transaction — no transaction semantics were traded for cosmetics. Pinned in test/sql/create_index.test: each pragma returns zero rows, the guard variable exists and is NULL, and a duplicate create still fails with the same message. |

## Phase 7: Build, Refresh, and Probe Throughput

Goal:
Index construction, refresh, and the query-side probe run at a speed the hardware
justifies. The Phase 6 build baseline is ~208 s per 1 GB refresh chunk (~5 MB/s of
corpus) with the box neither CPU-saturated (24 threads available, well under full
utilization) nor disk-saturated (~250 MB/s against multi-GB/s NVMe): the single global
`ORDER BY gram, segment_no` feeding the packer is the bottleneck. Target ≥3× measured
improvement, aiming for 5–10×. Phase 6 also measured the probe as the remaining query
latency lever: posting-list decode + intersect is single-threaded and Θ(corpus × K)
regardless of selectivity — 0.564 s of a 0.641 s rare-needle query at 100 GB warm, and
the reason the Phase 6 latency gate closed Partial (3.217 s cold against a
hundreds-of-ms target). This phase owns that lever (user decision, 2026-08-09).

Scope:
- Parallelize the pair-stream grouping. Measured in-phase: the global sort is CPU-bound
  (~640 ns/row, ~5–6 of 24 threads busy), not spill-bound — gram-hash partitioning into
  sequential per-partition sorts eliminated 100% of the spill, was byte-identical, and
  still ran 1.2× SLOWER, so partitioning alone cannot reach the gate. Revised design (as
  built): replace sort+pack with grouped aggregation — a custom aggregate
  (`ngram_pack_segment`) producing the encoded postings per (gram, segment_no) on
  DuckDB's radix-partitioned hash aggregate (its most parallel operator), with
  partitioning retained as the memory-bounding enabler (aggregate states cannot spill;
  per-partition they are bounded; `ngram_build_partitions`, 0 = auto from
  `memory_limit`). Partitions are segment-aligned ROWID RANGES, not gram hashes —
  measured: a `hash(gram) % N` predicate cannot be pushed below the unnest, so every
  partition re-tokenizes the whole corpus (cost linear in N), while rowid ranges prune
  by zone map and read the corpus once in total; both hold whole (gram, segment_no)
  keys since `segment_no = rowid >> 20`. Grouped output is rowid-ordered, so every
  write into the segments table carries `ORDER BY gram, segment_no` to preserve the
  gram-clustered physical layout the probe's zone-map pruning depends on (test-pinned).
  The segments table still receives one row per (gram, segment_no) per generation, the
  on-disk format and `format_version` are untouched, and postings must come out
  byte-identical to the previous pipeline's.
- Apply the same restructuring to all three consumers of the sorted-pairs shape: initial
  build, refresh, and compact's decode→union→re-encode pass.
- Re-measure the memory ladder: per-partition sorts are smaller, so tight memory limits
  should stop OOMing — report whether the 10 GB monolithic build now completes at
  `memory_limit='8GB'` (the Phase 2 gate's open finding).
- Re-measure build/refresh/compact cost at the 1/10 GB scale points, refresh-chunk cost
  at 100 GB scale if a re-run is affordable, and update benchmarks/RESULTS.md and the
  README's maintenance-cost guidance with the new numbers.
- Parallelize the probe: decode + intersect posting lists across parallel work units
  (segment rows are independent decode units; the probe already prunes by live-segment
  bounds). The probe's output — the candidate rowid set — must be identical; only its
  latency changes. Target: 100 GB rare-needle cold p95 inside the Phase 6 gate's
  "hundreds of ms", or a measured statement of the remaining floor and its cause (e.g.
  cold-IO bound). Re-measure warm and cold at 1/10/100 GB, update the RESULTS.md latency
  tables, and annotate the Phase 6 Partial gate row with a pointer to the new numbers.
  A 100 GB re-measure needs a rebuilt corpus — affordable once the build speedup lands
  (~1 h at target throughput), so sequence the probe re-measurement after the build work.

Out of scope:
- Postings encoding changes (measured and rejected in Phase 6, +7.8–11.8% vs a ≥20% bar).
- `preserve_insertion_order=false` (measured in Phase 6: 33% slower — the packer wants
  ordered passes; partitioning must not resurrect this).
- Changes to query results or semantics: the probe work is a latency change only —
  identical candidates, identical output, exhaustiveness contract untouched. On-disk
  format untouched; `format_version` stays 2.

Completion gate:
Refresh throughput improved ≥3× at the 10 GB scale point; postings byte-identical to the
unpartitioned pipeline on a differential corpus; probe latency re-measured at 100 GB
cold with the parallel probe — inside the hundreds-of-ms target or the measured floor
recorded with its cause; full differential (explicit + transparent), churn, and crash
suites green with fresh seeds; release and DEBUG/ASAN suites green; the memory-limit
ladder re-measured and recorded.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 7A: Grouped-aggregate packing for the initial build | src/pack_postings.cpp gained `ngram_pack_segment(rowid)`, an aggregate returning `STRUCT(postings, rowid_count, min_rowid, max_rowid)` built on the existing `EncodePostings`, and src/index_pragmas.cpp now generates `GROUP BY gram, segment_no` over it instead of `ORDER BY gram, segment_no` feeding the streaming packer. **1 GB `create_ngram_index`: 206.2 s → 8.1 s (25×)**, peak RSS 50.3 → 15.9 GB, spill 25.2 GB → 0. The aggregate runs at 2111 % CPU where the sort managed 585 %. Because `EncodePostings` sorts and dedupes, a group's payload is a function of its rowid set alone, so arrival order cannot reach the output — that is what makes partitioning byte-identical rather than merely equivalent. The streaming packer `ngram_pack_postings` is deleted; nothing used it any more. |
| Complete | Work | 7B: Same restructuring for refresh and compact | src/ngram_maintenance.cpp: both pragmas build the same partitioned `packed` temp table and then write it out. **10 GB `ngram_refresh` (5-replica tail): 3062.4 s → 49.0 s (62×)**; 1-replica tail 210.3 s median → 8.9 s (24×), still flat with index size (8.3/8.8/8.5/8.9/10.1/10.2/8.5/8.8/9.0/9.0 s over ten chunks into an index growing to 8.3 GiB). `ngram_compact` at 10 GB 15.8 s → 8.9 s. Compact partitions on `segment_no`, the same boundary; its rowid-liveness anti-join is rebuilt per partition, which is the one cost the restructuring adds. |
| Complete | Decision | Partitions are rowid ranges, not gram hashes | The packet specified gram-hash partitioning; measurement chose otherwise, and the plan text above is amended to match. Both keep a whole `(gram, segment_no)` inside one partition — `segment_no = rowid >> SEGMENT_SHIFT`, so a segment-aligned rowid range holds every rowid of every key it touches — and both are byte-identical. They differ in what a partition reads: `hash(gram) % N = i` cannot be pushed below the `unnest`, so every partition re-reads and re-tokenizes the whole corpus, while `rowid >= lo AND rowid <= hi` prunes row groups by zone map. Measured at 1 GB (all byte-identical): rowid ranges 6.9/7.2/8.6 s at N=4/8/16 against gram hash 8.0/10.8/17.1 s at N=2/4/8. Gram-hash cost grows linearly in N; rowid ranges are nearly flat, which matters precisely in the low-memory case where N is large. The count comes from `memory_limit` and a 512-row sample of the range being indexed (byte lengths, deleted rows counted — biased to overestimate), budgeting 32 B/pair against half the limit; `ngram_build_partitions` overrides it. |
| Complete | Decision | The sort was CPU-bound, not spill-bound — partitioning it was measured and rejected | The packet's premise was that the global sort was slow because it spilled ~26 GB. It was not. Splitting the 1 GB pair stream into eight gram-hash partitions and sorting each independently removed **100 % of the spill** (22.5 GB → 0, peak RSS 53.0 → 9.5 GB), was byte-identical, and ran **slower**: 149.3 s against 121.9 s, with per-row throughput unmoved (8.1M → 6.5M pairs/s). A thread ladder over one eighth of the stream shows why: extraction scales 14.7× from 1 to 24 threads, sort-only 6.5×, but sort+pack only **2.9× and flat past eight threads**. Operator CPU in the old build: `ORDER_BY` 616.6 s of 686.8 s total, the packer 34.4 s. The sort was 90 % of the cost and could not use the machine; no partitioning of it could have reached the gate. |
| Complete | Decision | A cheaper sort key was measured and rejected | Sorting the 966M-row pair stream on a fixed-width 64-bit key rather than the VARCHAR gram costs 17.4 s against 31.7 s (no sort at all: 1.2 s), so dictionary-encoding the gram could save ~14 s of a 206 s pragma. Real, and an order of magnitude short of the gate — recorded so the option is not revisited. |
| Complete | Doc | The 206 s pragma versus 122 s of hand-run SQL was the transaction wrap | A pragma expansion runs inside one transaction (statement_preprocessor.cpp wraps any expansion of more than one statement). The old sort pipeline cost 126.2 s in autocommit and 205.9 s wrapped — the entire gap, and the reason ledger numbers must be pragma-to-pragma. The grouped shape is indifferent to it (7.6 s → 8.3 s), so no separate fix was needed. Statement counts still expand to more than one statement, so the auto-wrap and its crash atomicity are untouched. |
| Complete | Risk | Dense-needle fetch drift at 10 GB — raised, then closed by measurement | Carried Open through the build half: probe-only matched Phase 6 within 3% at every class and the brute-force scans within 1%, but dense `ngram_search` read 2.86-2.90 s against Phase 6's 2.600 s across two back-to-back passes, with identical candidate sets and nothing in the build change touching fetch. The one fully controlled comparison — the two 1 GB indexes built by the two pipelines, queried back to back in one session — showed parity (0.272 s vs 0.274 s), so it was recorded rather than explained away. **Re-measured on the shipped code it is 2.418 s, below Phase 6's 2.600 s.** It was session-level buffer-pool state for millions of scattered row fetches, not a regression; the parallel probe more than covers it. |
| Complete | Work | Gram-ordered writes are now an explicit contract | Partitioned grouping emits rows in rowid order, which would have destroyed the zone-map pruning behind the probe's `gram = ?` filter — the segments table's physical order is what makes that filter skip row groups. Every write into the segments table (build, refresh generation, compact re-insert) now carries `ORDER BY gram, segment_no`, and test/sql/build_scale.test pins it by checking no row's gram precedes its predecessor's. The sort is over segment rows (2.5 M per 1 GB indexed), not over the pair stream (966 M), so it costs nothing measurable. |
| Complete | Work | `ngram_build_partitions` setting | `SET ngram_build_partitions = N` (0 = size it from `memory_limit`) overrides the partition count for build, refresh and compact. It exists because the sizing estimate is a sample and an adversarial row-length distribution could under-partition; it is also what makes the memory ladder measurable. Negative values are refused. |
| Complete | Doc | README documented an invalid pragma syntax (pre-existing bug) | Every `PRAGMA` example using `name := value` — `create_ngram_index(..., gram := 3, case_insensitive := true)`, `ngram_compact(..., purge := true)` — fails with a binder error on v1.5.5: pragma named parameters take `=`, and only table functions take `:=`. Found while writing the Phase 7 harness, which hit the same error. Corrected in README.md; the `ngram_search(..., col := 'column')` example is a table function and was already right. |
| Complete | Work | 7C: Parallel probe (decode + intersect), identical candidate sets | Contained to `ProbeIndex` in src/ngram_search.cpp; both call sites are untouched. Grams are still probed in sequence because each one's live-segment and min/max pruning depends on the last one's survivors, but the work inside a gram — the filtered segment scan, the blob decode, the stats scan behind rarest-first selection, and the intersection — now runs across the scheduler's threads through a `TaskExecutor`. **The bigger win was structural, not parallel.** The old code decoded every blob of a gram into one flat vector and `std::sort`ed it, up to ~200 M elements single-threaded for a dense gram at 100 GB; but the codec already writes each blob sorted and deduplicated, and every rowid of a `(gram, segment_no)` blob lies inside that segment's 2^SEGMENT_SHIFT window, so segments never overlap and concatenating them in segment order is already globally sorted. The sort is gone; only a key carrying more than one blob (a refresh generation, or a partial segment from an index an older version built) needs merging. The intersection is per-segment for the same reason — each segment's slice of the candidate list is a contiguous range found by binary search — so it parallelizes and concatenates. Two smaller wins fell out: `live_segments` was an `unordered_set` built by hashing every surviving candidate (200 M inserts on a dense probe) and is now one no-hash pass over the already-sorted list, and the first gram moves its per-segment lists into the result instead of copying them. Determinism is structural: the result is the per-segment lists concatenated in segment order, each a deterministic function of that segment's blobs, so thread count and row-group claim order cannot reach the output. |
| Complete | Test | Probe candidate sets identical on both paths at every thread count | test/sql/ngram_parallel.test grew from 43 to 83 assertions. A single-threaded probe of a 600k-row index is snapshotted per needle, then compared at threads 2/3/8/24 as a multiset **and** position by position (the probe returns sorted output and callers binary-search it, so order is part of the contract), for five needles spanning selectivities. At each thread count the explicit path is compared to brute force and the transparent path to the same query with the rewrite disabled, both directions. A final block appends 100k rows, refreshes to create a second generation, and re-checks candidate identity at two thread counts — that is the multi-blob per-segment merge path, which the single-generation cases never reach. Writing it caught a real distinction: comparing candidates against *all* matching rows fails, because `ngram_candidates` covers indexed rows only and rows past the high-water mark belong to the tail scan; the superset check is scoped to `rowid <= hwm`. |
| Complete | Test | Postings byte-identical to the pre-Phase-7 pipeline, for build, refresh, compact and purging compact | The pre-Phase-7 binary was rebuilt from this tree and run through the whole sequence on the same corpus as the new one; every column of every segment row was compared in both directions, `generation` included. **0 mismatches at all four stages** (657,843 → 1,001,250 → 935,947 → 912,324 rows), and the stats tables are equal. The refresh boundary is deliberately mid-segment: with a segment-aligned one the tail lands in fresh segments, nothing shares a key and the compaction stage proves nothing — this run gave compaction 65,303 genuinely fragmented keys to merge. At 1 GB the two pipelines' full indexes differ in 0 of 2,549,366 rows. At 10 GB, a chunked build (create + refresh + purging compact over two 5-replica chunks) and a monolithic one produce the same 24,914,936 rows with **0 mismatches** — refresh and compaction reproduce a build rather than approximating it. |
| Complete | Test | All Phase 1–6 suites re-run with fresh seeds, release and DEBUG/ASAN | Release: `make test` **2,169 assertions / 19 files** green (2081 before the new partition-identity and aggregate tests). [Corrected during Phase 8: this row first recorded 2118, which no run of this tree produces; the suite at 47d2d06 is 2,169 in 19 files, re-measured by running Phase 8's two new test files aside.]; differential explicit seed 20260910 = 6569 checks / 0 failures, `--transparent` seed 20260911 = 3480 / 0; churn seed 20260912 (60 rounds) = 960 checks / 0 failures / 16 un-maintainable states, all detector-reported, 0 undetected in-place updates; churn `--no-stale-expected` seed 20260913 = 480 / 0 / 0 verdicts; crash seeds 20260914 (400k + 150k tail, 16 kills) and 20260915 (150k + 60k) = 0 failures, every reopen on exactly the pre- or post-operation state — the auto-wrap still gives each pragma one transaction despite the higher statement count. DEBUG + AddressSanitizer: full suite **2118 assertions** green, `--transparent` differential seed 20260916 = 1748 / 0, churn seed 20260917 = 320 / 0, churn strict seed 20260918 = 240 / 0 / 0. Format check clean. |
| Complete | Test | New tests for the aggregate and for partitioning | test/sql/pack_segment.test replaces pack_postings.test: grouping, deduplication, order-independence of the encoded payload (the property the whole design rests on), a rowid range spanning several segments, an empty group under `FILTER`, NULL and negative rowids, wrong argument type. test/sql/build_scale.test gains a 3×2²⁰-row corpus indexed at one partition, at three, and on a single thread, with all three compared column by column (0 differences), one-row-per-key asserted, the gram-order contract asserted, and a negative `ngram_build_partitions` refused. |
| Complete | Bench | Before/after build throughput at 1/10 GB, memory ladder, RESULTS.md and README | benchmarks/RESULTS.md §2 rewritten: the before/after table, the thread ladder and per-operator CPU that show why the sort had to go, the two measured-and-rejected alternatives, the rowid-range versus gram-hash comparison, the memory ladder, the identity tables and the probe-unchanged table. README: build-cost guidance replaced (≈1 GB of text per second, memory as the constraint rather than disk, the structural floor and the clean out-of-memory failure), compaction wording corrected, `ngram_build_partitions` documented, and the invalid `:=` pragma examples fixed. |
| Complete | Bench | 100 GB corpus rebuilt; probe latency at 1/10/100 GB warm+cold; RESULTS.md tables and Phase 6 gate row updated | The 100 GB corpus was rebuilt from the recorded seeds under Phase 6's exact protocol and is the same index — 72,434,741,725 postings, 248,445,026 segment rows, 4,509,928 distinct grams, 85.25 GiB of blobs, all equal, and spot verification exact at 276,151,351 decoded rowids for `␠␠␠`. Indexing plus compaction went from **8.6 h to 25 min**: create 208.0 → 8.4 s, 99 refreshes median 210.3 → 9.3 s (p95 10.6), 100 index steps 5.95 h → 15.8 min, compact 2 h 37 m → 9.7 min, peak spill 27.4/311.6 GB → **0**. Warm probe fell 12.5× at 100 GB (0.564 → 0.045 s), 10.4× on the dense needle (15.245 → 1.464 s), with candidate counts identical in every cell (1,001 / 2,335,591 / 68,555,361). RESULTS.md §2 latency tables now carry warm and cold at all three scales with serial-vs-parallel probe columns, §4 gained a probe subsection, and the Phase 6 Partial gate row is annotated with the new numbers. |
| Complete | Bench | Memory ladder incl. the 8 GB monolithic-build retry | Recorded in the build half: 48/32/16 GB complete in 78.0/97.8/91.2 s, **8 GB completes in 136.6 s** where Phase 2 could not, 4 GB and 2 GB fail with a clean out-of-memory rollback. Identical digest at every rung that completes. |
| Complete | Decision | Stats-table gram ordering deferred | The stats table is written in hash-aggregate order and the probe full-scans it for rarest-first gram selection — ~0.07 s of a 100 GB probe. Ordering it at write time would bound that cost but is another build-side change requiring identity re-validation, the win is tens of milliseconds at the largest scale, and refresh appends unordered delta rows between compactions so the benefit decays in exactly the workloads that refresh often. Deferred; revisit only if a future probe profile shows the stats scan dominating. |
| Complete | Gate | ≥3× refresh throughput at 10 GB with identity and suites green | **62×** at the 10 GB scale point (5-replica tail, 3062.4 s → 49.0 s) and **24×** per 1-replica chunk (210.3 s → 8.9 s), against a ≥3× bar. Postings byte-identical at every stage, release and DEBUG/ASAN suites green with fresh seeds, format clean, memory ladder recorded including the 8 GB monolithic build Phase 2 could not complete. |
| Complete | Gate | 100 GB cold rare-needle re-measured with the parallel probe — **floor recorded with its cause** | **Warm: met with room.** 0.125 s p50 / **0.133 s p95** at 100 GB against the gate's "hundreds of milliseconds", where Phase 6 sat at 0.641/0.643 s — and 33× the 4.173 s scan it replaces, against Phase 6's 6.7×. **Cold: 0.714 s p50 / 0.816 s p95**, 4.5× better than the 3.217 s Phase 6 recorded and 20× better than the 16.387 s cold scan, but above a few hundred milliseconds — so the gate closes on the floor-with-cause branch rather than the target branch. The attribution is measured, not inferred, because `bench_latency.py` was extended to time the probe and the fetch cold separately: of the 0.71 s, **0.296 s is the probe reading posting blobs off disk and ~0.46 s is fetching 1,001 candidate rows** at ~0.46 ms each — random reads from a 182 GiB database against 125 GB of RAM. Both halves are cold IO scaling with candidate count rather than corpus size; neither is CPU and neither responds to more threads. Warm, the identical query is 0.125 s. Cold at the smaller scales is comfortably inside: 0.051 s at 1 GB, 0.110 s at 10 GB. |

## Phase 8: Bounded Incremental Refresh

Goal:
A large write burst never turns catch-up into a single monster transaction. Refresh
gains a bound: one invocation indexes at most a caller-chosen amount of tail and commits
that progress durably, so a crash during catch-up loses only the current increment —
today it rolls the entire refresh back to the starting high-water mark. Combined with
Phase 7 throughput, a 50 GB ingest burst becomes tens of minutes of catch-up committed
in minute-scale steps instead of one multi-hour all-or-nothing transaction.

Scope:
- `PRAGMA ngram_refresh(table, max_rows)` (optional second argument): index at most
  ~max_rows of committed tail in rowid order, advance the high-water mark to the highest
  indexed rowid, commit. Callers loop until stats report the tail empty. Exactness holds
  at every intermediate state: rows past the new high-water mark are simply still tail,
  covered by every query's tail scan.
- One invocation stays one transaction. The statement-preprocessor auto-wrap is what
  gives refresh its crash atomicity; an in-pragma loop would need multiple transactions
  inside one expansion, which that wrap forbids. The loop lives in the caller.
- The bound must land on a rowid boundary that preserves the invariant "every committed
  row ≤ hwm is indexed" after each increment (deletes leave rowid gaps, so max_rows is
  an approximate count; the boundary itself is exact).
- Each bounded invocation RETURNS a one-row summary — rows indexed this call, the new
  high-water mark, and remaining-tail rows — as the final SELECT of the pragma
  expansion (user request, 2026-08-09: callers driving the loop read progress from the
  call itself, not a separate stats query). Adding a result statement keeps the
  expansion multi-statement, so the transaction auto-wrap is unaffected.
- Stats: also expose remaining-tail rows so progress is visible outside a refresh call.
- Staleness detectors unchanged: a bounded refresh refuses on detected row motion or
  recreation exactly as an unbounded one does.

Out of scope:
- Bounding `ngram_compact` (whole-index by nature; revisit only on concrete need).
- Background/automatic scheduling (the Phase 5 decision stands — callers drive the loop).
- Query-semantics or on-disk format changes; `format_version` stays 2.

Completion gate:
A bounded-refresh loop over a large tail yields an index identical to one unbounded
refresh (differential + postings comparison); kill -9 between and during increments
resumes from the last committed high-water mark, never from zero, with correctness
intact at every reopen; churn harness extended with bounded-refresh steps; release and
DEBUG/ASAN suites green with fresh seeds.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 8A: `max_rows` bound on refresh with the exact hwm-boundary invariant | `PRAGMA ngram_refresh(t, max_rows)` takes the bound positionally or as `max_rows = N` (two overloads in a `PragmaFunctionSet`; `=`, not `:=`), and returns a progress row. src/ngram_maintenance.cpp gained `BoundedRefreshEnd`, which turns the bound into the last rowid the call may cover, and the generated script changes in exactly three places: `EstimateGramCount` and `SegmentAlignedRanges` are given that rowid instead of the table's committed end, `SegmentAlignedRanges` gained an `open_ended` flag so the last partition stops at it rather than running to `LOCAL_ROWID_START - 1`, and the `UPDATE` records the bound instead of the highest rowid seen. Everything else — both guards, `ngram_pack_segment`, the gram-ordered segment write, the stats delta, the detector refusals — is byte-for-byte the code the unbounded path runs, and a bound that reaches past the tail generates the unbounded script verbatim (plus the progress row), so "loop until `remaining_tail` = 0" costs one call on a short tail. Bounding the rows also bounds the partitioner's estimate, so peak memory follows the bound rather than the tail. |
| Complete | Work | 8B: Remaining-tail exposure in `ngram_index_stats` | New `remaining_tail` column after `table_max_rowid`: committed rows past this index's own mark, counted against the meta row rather than derived from `table_max_rowid` (the rowid gap counts deleted rows; this counts work). **This is an output-shape change and it forced the only edits made to existing tests in this phase**: nine pinned `PRAGMA ngram_index_stats` rows across create_index.test (4), index_persistence.test (2), ngram_maintenance_identity.test (2) and ngram_maintenance.test (1) gained a column, and scripts/churn_maintenance.py's stats parser moved from `len(row) == 12` to 13. No assertion changed meaning; every other existing test is untouched. |
| Complete | Work | Fix found in review: a bounded refresh recorded staleness witnesses for rows above its own mark | The index records 32 (rowid, value hash) witnesses so maintenance can catch a checkpoint vacuum that moved indexed rows. They were sampled over `[0, table's last rowid]`, which was safe only because an unbounded refresh ends at the table's end — a bounded one stops short, so **27 of 32 witnesses landed in the tail** (measured: mark 149, witnesses up to 999). `SampleStaleReason` had no mark check, so an ordinary in-place `UPDATE` of a tail row, or a plain tail `DELETE` that the next checkpoint vacuumed, then made `ngram_index_stats` report the index stale and made `ngram_refresh` (bounded **and** unbounded) and `ngram_compact` refuse with rebuild-required — killing the very catch-up loop this phase exists for. Queries stayed exact throughout (the query paths use `CertainStaleReason` only), so it was a maintenance-availability defect, not a wrong-answer one. **Fixed on both sides.** Recording: witnesses are drawn over `range_end`, the last rowid the call may cover, matching what `ngram_compact` already did; unbounded is bit-identical because there `range_end` *is* the table's last rowid. Checking: `SampleStaleReason` skips any witness past `meta.hwm_rowid`. The second half is what makes it airtight — `range_end` is only an upper bound on the mark (the mark falls back below it when nothing committed is visible past the bound), and a witness can reach the gap between them if a row was visible to the pragma callback and deleted before the script's snapshot; it also disarms above-mark witnesses already written into an index by an earlier build, which no recording-side change can retract. It costs no detection: past the mark there are no postings, and a vacuum can only move an indexed row when a deleted gap sits at or below the mark, which disturbs the witnesses that remain. One consequence worth recording so it is not later rediscovered as a discrepancy: a bounded loop's final `row_samples` can now legitimately differ from an unbounded refresh's, because sample positions are derived from the `max_rowid` passed in, and the last increment's is `bound_end` rather than `total_rows - 1` whenever nothing is visible past the bound. Both witness sets are valid, and identity here has always been claimed over segment rows, postings, stats and the mark — never over `row_samples`. |
| Complete | Decision | The bound is a rowid span, snapped down to a segment boundary | Three constraints pick this. (1) A live-row count could only be evaluated at run time, so partition ranges would become `rowid <= getvariable(...)` instead of literals — and literal rowid ranges are what makes a partition's scan skip row groups by zone map, the whole reason Phase 7 chose rowid-range partitions over gram-hash. (2) A span bounds work in the safe direction: deletes leave gaps, so a call indexes at most `max_rows` rows, never more (measured: a 250,000-row bound over a tail with two delete patterns indexed 250,000 / 220,390 / 172,466 rows in its three calls). (3) `segment_no = rowid >> 20`, so an increment ending on a segment boundary produces each `(gram, segment_no)` whole, and the loop then writes the segment rows one unbounded refresh would have written. Snapping down never overspends the bound; a bound too small to reach the next boundary is honoured exactly rather than rounded up to 2²⁰ rowids, because on long rows one segment can be far more text than the caller is willing to put in one transaction — that is the case the phase exists for. **Measured consequence, both directions**: with an aligned bound (1,048,576) the loop's index is byte-identical to the unbounded one, 3,514 segment rows each way, 0 differences in either direction over all columns but `generation`; with a sub-segment bound (250,000) the postings are still identical (40.2 M / 31.4 M postings compared row by row, 0 differences, 0 duplicated `(gram, rowid)`) but the loop leaves 4,590 segment rows against 2,406 — and one `ngram_compact(purge = true)` on each collapses both to the same 1,279 rows with **0 differences including `generation`, and equal stats tables**. |
| Complete | Decision | The progress row belongs to the bounded form alone | `PRAGMA ngram_refresh('t')` returns nothing, exactly as before; `PRAGMA ngram_refresh('t', N)` returns `(column_name, rows_indexed, hwm_rowid, remaining_tail)`, one row per index it advanced. The plan asks for a summary from the bounded call and for the unbounded form to be unchanged, and this is the only reading that satisfies both — it also leaves create_index.test's guard-output block (which asserts a refresh prints nothing) passing unmodified. A caller who wants a summary from a full catch-up passes a bound wider than the tail and gets the unbounded script plus the row; the README documents that idiom. |
| Complete | Decision | Two costs of spending the bound as a rowid span, documented rather than engineered away | (1) Because the snap rounds down, a bound just short of a segment alternates a full increment with a tiny one: `max_rows = 1048575` from an aligned mark measures 1,048,575 / 1 / 1,048,575 / 1. Every call is correct and the loop covers the same rows; half of them are near-empty transactions. Removing it would mean either overshooting `max_rows` — the one property the bound guarantees — or shortening increments further, so the README says to prefer a bound that is a whole number of 2²⁰-rowid segments. (2) A run of deleted rowids costs one no-op call per bound: a 1,000-rowid gap at `max_rows = 100` measures ten calls reporting `rows_indexed = 0` before live rows resume. A fix exists and is deliberately deferred: the mark could jump to `min(rowid) - 1` of the first live row past the bound, which the same commit-order argument makes safe — a live row there proves every slot below it settled — and which would *replace* the `EXISTS` rather than add to it. It is deferred because it perturbs the single expression that carries this phase's concurrency proof, for a performance-only win in a case that is already correct, terminating, accurately reported, and cheap (nothing to index means nothing to pack). Raising `max_rows` walks the gap in fewer steps. |
| Complete | Decision | Why the mark may advance past rows the transaction never read, and the proof it is safe | A bounded call records the bound itself, not the highest rowid it saw, or a loop would stall forever on an increment whose rows were all deleted (`max(rowid)` over it is NULL). That is only sound if no rowid at or below the bound can still belong to an append this transaction cannot see — such a row would be indexed by nobody, since every later refresh starts above the mark. The generated `UPDATE` therefore advances to the bound **only when it can see a committed row past it**, and falls back to the unbounded rule inside the increment otherwise (in which case the remaining tail is 0 anyway, so the loop still ends). The proof that seeing one row past the bound settles everything below it: rowids are handed out to a transaction's rows while it commits, under the table's append lock (`DataTable::AppendLock`, src/storage/data_table.cpp, takes `row_start` from the table's current row count), reached through `LocalStorage::Commit` → `LocalStorage::Flush`. On a database with a WAL that runs inside `DuckTransaction::WriteToWAL`, which `DuckTransactionManager::CommitTransaction` calls with `transaction_lock` **released** and the **WAL lock** held; `info.commit_id` is taken after it returns, inside that same WAL-lock critical section, which ends only when the commit does. On a database without a WAL (in-memory, or `NO_WAL_WRITES`) the allocation happens in `DuckTransaction::Commit` instead, under the `transaction_lock` that already covers the commit id. Either way one lock serializes both events for every committing transaction, and `ShouldWriteToWAL` is a per-database property so a database never mixes the two regimes. Commit-id order and rowid order are therefore one order, so a visible row at rowid W proves every rowid below W belongs to a transaction that committed before this one started — visible-or-deleted, and covered by the partitions. (The first version of this row named `transaction_lock` as what serializes the allocation; it is the WAL lock wherever there is a WAL, and the conclusion is unchanged.) Citation carried in the code comment on the `UPDATE`; the observable half is tested (next row). |
| Complete | Test | Loop-vs-unbounded identity, and the bounded contract in 315 assertions | New test/sql/ngram_refresh_bounded.test: the progress row's exact values for positional and named bounds; a loop to `remaining_tail = 0` with a differential check at intermediate states and `max(rowid)` of the candidate set pinned to the mark; a bound wider than the tail; `9223372036854775807` (the clamp); the unbounded form still returning no rows; the guard variable still NULL; deletes in the tail (an increment of nothing but deleted rows still advances the mark, a half-deleted one reports 5 of 10, an all-deleted tail leaves the mark on the last live row); a 1.2 M-row table where a 1,100,000-row bound is snapped to 1,048,575 and **no key above segment 0 is fragmented** while segment 0 — shared with the build, bound or no bound — is; a twin-table identity (one unbounded call against eleven bounded ones over the same corpus with deletes) compared as decoded `(gram, rowid)` postings both directions and then as segment rows after compaction; `BEGIN`/`ROLLBACK` and `BEGIN`/`COMMIT` around an increment; `ngram_index_stats` reporting the same remaining tail; argument errors (0, negative, NULL, both forms at once, unknown parameter); the re-created-table refusal firing identically with and without a bound; two indexed columns (one row each, `col =` narrowing to one); and a quoted `"my.table"` with a spaced column name. **Regression block for the witness-scope defect above**: the mark, the highest rowid witnessed and the witness count are pinned together (`149 / 149 / 32`); an in-place `UPDATE` of a tail row leaves `stale_reason` NULL and the loop running, while one below the mark still makes both `ngram_refresh` and `ngram_compact` refuse by name; and a tail-only `DELETE` followed by a `CHECKPOINT` that visibly vacuums (the table's last rowid drops 59999 → 51807 under `ROW_GROUP_SIZE 2048`) also leaves the index maintainable and exact. Verified to discriminate: with the fix reverted and release rebuilt, this block fails on the witness bound (999 against 149). |
| Complete | Test | Crash resume: 58 bounded-loop kills across five sweeps, every one on an increment boundary | scripts/crash_maintenance.py gained a bounded scenario: it records every state the uninterrupted loop passes through, kills a child running the whole loop at offsets spread across its measured duration, and then requires that the reopened state **is** one of those states, that queries still equal brute force there, and that resuming the loop lands on the uninterrupted loop's exact index — same `(hwm, segment rows, postings, stats rows)` and the same digest over every decoded `(gram, rowid)`. Release seed 20260814 (300k + 150k tail, bound 15,000, 11 states): 16 kills survived at increments 0,1,1,2,3,4,5,4,5,5,6,7,7,9,9,10 — never partial, never behind the previous commit, all resumed identical. Seed 20260815 (800k + 500k tail, bound 60,000): 14 kills at increments 0,1,2,2,2,5,4,6,7,7,8,10,10,10, all identical after resume. Under DEBUG/ASAN, where an increment costs ~1.6 s instead of ~30 ms so a kill is far likelier to land mid-increment than between two, seed 20260823 survived at increments 1, 4, 6, 8, 8, 8 and seed 20260843 at 2, 4, 6, 8, 8, 8 of a 14.5 s loop — again never partial, all resumed identical. 58 bounded-loop kills in total: 36 on the tree as first written (release 20260814/20260815, ASAN 20260823) and **22 re-run on the shipped tree after the witness fix** (release 20260848 = 16, ASAN 20260843 = 6). Every sweep also re-ran the pre-existing unbounded-refresh and purging-compact kill sets — the ASAN run alone is 18 kills across all three scenarios — 0 failures anywhere. |
| Complete | Test | A bounded mark never buries a concurrent writer's rows | New test/sql/ngram_refresh_concurrent.test. Deterministic half: one connection holds an uncommitted 20-row append while another runs a bounded refresh with a bound far past the committed end; the mark stops at the last committed row, the held rows are answered by the tail scan, and the next call indexes them. Racing half: four `concurrentloop` threads each append 200 rows and then try a bounded refresh (137 rows) six times over, so appends commit underneath refreshes that are in flight. 24 calls of at most 137 rowids cannot cover 4,800 appended rows, so the state reached is provably mid-catch-up — and there the invariant the mark exists for is asserted directly: **no committed row at or below the mark that matches the needle is missing from `ngram_candidates`**, plus the usual differential. Then the catch-up is finished and both checks re-run. Run repeatedly with no failures; the concurrent refreshes did not observably collide in this workload, so the `statement maybe` on them is insurance rather than the tested path. |
| Complete | Test | Full suites, fresh seeds, release and DEBUG/ASAN | Release `make test`: **2,559 assertions / 21 files** green (2,169 / 19 at HEAD 47d2d06, the end of Phase 7; the two new files are the bounded and concurrent tests, and 2,559 − 315 − 75 reconciles with it exactly). Differential explicit seed 20260810 = 6,036 checks / 0 failures; `--transparent` seed 20260811 = 3,464 / 0. Churn seed 20260812 (60 rounds, bounded steps mixed in) = 960 checks / 0 failures / 10 un-maintainable states, all detector-reported, 0 undetected in-place updates; churn `--no-stale-expected` (the false-alarm gate) = 640 / 0 / **0 verdicts**. DEBUG + AddressSanitizer/UBSan on the fixed tree, in **two independent runs of the same binary**: this phase's pipeline (seeds 20260840-3) reports full suite **2,559 assertions / 21 files** green, `--transparent` differential 20260840 = 3,496 / 0, churn 20260841 (40 rounds) = 640 / 0 with 7 un-maintainable states all detector-reported and **0 undetected in-place updates**, churn strict 20260842 (30 rounds) = 480 / 0 / **0 verdicts** with `tail_update` exercised 3 times, and crash 20260843 = **18 kills / 0 failures**; the reviewer's own ASAN suite, run separately, reports the same **2,559 assertions / 21 files**, `TEST_EXIT=0`, no sanitizer reports, having checked binary mtimes and source hashes across the run to confirm no relink. The pre-fix tree's earlier ASAN run (2,507 assertions, seeds 20260820-3, all green) is superseded by these. Format check clean. |
| Complete | Test | What the churn harness's false-alarm gate does and does not cover | `--no-stale-expected` was the mode that should have caught the witness defect and could not: it drops deletes and in-place updates outright, so it structurally never reaches a state where a false alarm lives, and the ordinary mode rebuilds on every verdict, so it cannot tell a false alarm from a true one. It now also runs an in-place `UPDATE` **confined to rows past the mark** — rows nothing indexes, which v1.5.5 updates in place (verified: row count, max rowid and the updated row's own rowid unchanged across the update and the checkpoint after it), so no detector may fire and any verdict fails the run. That op reproduces the defect's class directly. A tail-confined **DELETE** was tried and deliberately removed: refresh advances the mark over deleted rowids on purpose (or a bounded increment of nothing but deleted rows would stall the loop), so the gap ends up below the mark and the next checkpoint's vacuum moves indexed rows — genuine staleness, the documented rebuild case. The finding rests on a deterministic reproduction, not on a failure rate. The reproduction, in full so the figures can be re-derived: a 2,000-row corpus indexed at build, then **five rounds** of {append 2,000 rows; `DELETE ... WHERE rowid > hwm AND id % 4 = 1`; `PRAGMA ngram_refresh`; re-query}, every statement in its own duckdb process so each close checkpoints, default row group size. It goes stale in **round 3** — brute force 6,750, index 5,750, **1,000 real misses** — with the detector refusing on `row id 2128` at that same moment, which is a true positive by definition. Re-run verbatim it reproduces exactly, round for round (3500/3500, 5000/5000, 6750/**5750**, 8250/7250, 9750/8750). The *round* at which it first bites is parameter-dependent, not fixed: it depends on when a checkpoint first finds a row group worth compacting, so it moves with delete density and append size — the reviewer's two independent shapes (`id%3=0` and `id%4=0`, other corpus sizes) both bit in round 1, with 999 and 750 misses. The phenomenon is what is being claimed, and it is what all three runs agree on. The randomised run agrees — with the op present, seed 88880011 reports `row id 6003` (op mix `tail_delete` 5, `tail_update` 8) — but only that one seed ever ran with the op, so no rate is claimed from it; with the op removed the same seed is 960 checks / 0 failures. **What the gate now covers**: append, refresh bounded and unbounded, compact, and in-place mutation of rows past the mark — release seed 20260847 = 640 / 0 / 0 with `tail_update` 6 times, ASAN seed 20260842 = 480 / 0 / 0 with it 3 times. **What it does not**: deletes and below-mark updates, both of which legitimately invalidate an index and therefore belong to the ordinary mode, where they already are. |
| Complete | Doc | README and the API reference | README gained "Catching up on a large tail, one bounded step at a time" (the progress row, a Python loop driving it to `remaining_tail = 0`, the crash-exposure argument, the at-most-never-more semantics of the span, the segment snapping and the fragmentation a sub-segment bound leaves), a `remaining_tail` entry in the `ngram_index_stats` table, a new "Maintenance" block in the API reference with every pragma signature and the progress row's columns, and a pointer from the incremental-build guidance to the bounded loop as the other way to bound peak memory. |
| Complete | Bench | 10 GB burst caught up by a bounded loop, at no throughput cost | A 1 GB indexed base, then ten more enwik9 replicas appended with no refresh: 120,124,653 rows / 10.89 GB of text, the mark at 10,920,422, a **9.90 GB / 109,204,230-row tail**. Caught up twice from that same state. One unbounded refresh: **93.9 s**, one transaction, all of it lost to a crash. A loop at `max_rows` = tail/10: **11 calls, 8.3–8.9 s each (the last 3.7 s), 88.6 s in total** — so bounding costs nothing, and the crash exposure drops from 94 s to ~9 s. The mark advanced exactly 10 segments per call (20971519, 31457279, … each 10 × 2²⁰ − 1), which is the snapping working at scale: the first call spent 10,051,097 of its 10,920,423-row bound to land on the boundary and every later one spent 10,485,760. The two indexes are the same index: **27,495,963 segment rows each with 0 differences in either direction** over every column but `generation`, 7,967,815,476 postings, 10,048,422,528 postings bytes, identical per-gram stats sums (0 differences both ways), the same mark, and the same 13,652 candidates for a spot-checked needle. The one place they differ is the shape of the stats table before compaction — the loop appended 11 delta generations, 8,965,994 rows against 2,468,943 — which sums to the same values the probe reads and is what `ngram_compact` folds back to one row per gram. |
| Complete | Gate | Identity + crash-resume + suites green | **Identity**: a bounded loop's index equals one unbounded refresh's, proven at three scales and both granularities — byte-identical segment rows at 1.2 M rows and at 120 M rows (0 differences either direction, `generation` excepted) with a segment-aligned bound, and identical decoded postings with a sub-segment bound, which one `ngram_compact(purge = true)` then makes byte-identical including `generation`. **Crash resume**: 58 bounded-loop kills over five seeds, release and ASAN, 22 of them re-run on the shipped tree, every reopen exactly on an increment boundary of the uninterrupted loop, correct against brute force there, and identical to the uninterrupted final index after resuming — the loss window is one increment, measured. **Exactness mid-catch-up**: differential checks at intermediate states in the test file, in the churn harness (which now stops refreshes part-way on purpose), and under concurrent writers, where the invariant is asserted directly as "no committed row at or below the mark is missing from the index". **Suites**: release **2,559 assertions / 21 files** green on the shipped tree, and DEBUG/ASAN the same 2,559 in two independent runs of that binary (this phase's pipeline and the reviewer's own, the latter with no sanitizer reports and verified binary/source hashes), differential/churn/strict-churn/crash green with fresh seeds on both, format clean. Unbounded refresh is unchanged in behaviour and output, and every existing test passes unmodified except the nine stats rows and one parser that 8B's new column forced. **One defect was found in review and fixed**: a bounded refresh recorded staleness witnesses over rows above its own mark, so an ordinary tail `UPDATE` or a vacuumed tail `DELETE` made the index refuse all maintenance; it is fixed on both the recording and the checking side, pinned by a regression block proven to fail on the reverted build, and it exposed a real gap in the churn harness's false-alarm gate, which now carries an above-mark mutation. **Review lane**: this phase was reviewed on Opus rather than the usual reviewer model, which was unavailable; the reviewer re-ran the release suite, a DEBUG/ASAN suite, a fresh-seed crash sweep, three independent strict-churn seeds and the format check on the shipped tree, and audited the fix's reachability rather than accepting the ledger's reasoning. |

## Phase 9: Restore Full Platform CI Matrix

Goal:
This repo's CI builds and tests the extension on every distribution platform, macOS
first among them. (Community-extensions CI builds all platforms on duckdb-org
infrastructure regardless; this phase makes platform breakage visible here, pre-merge.)

Scope:
- Remove the `exclude_archs` trim from `.github/workflows/MainDistributionPipeline.yml`
  (added during development to conserve private-repo Actions minutes; macOS runners bill
  at 10x). Restore macOS (osx_amd64, osx_arm64), then Windows and Wasm targets.
- Fix any platform-specific build or test failures the wider matrix surfaces.

Out of scope:
- New functionality; this phase only widens build/test coverage.

Completion gate:
Full distribution matrix green on the pinned DuckDB release, including macOS.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 9A: Re-enable macOS archs in CI | `osx_amd64;osx_arm64` dropped from `exclude_archs` in .github/workflows/MainDistributionPipeline.yml (commit `abeef27`), leaving Windows and Wasm excluded so a red run would name one toolchain. **Run [31412998562](https://github.com/danthegoodman1/duckdb-ngram/actions/runs/31412998562): success, first try, no source change needed.** Both macOS jobs green — `MacOS (osx_arm64, macos-15, arm64, arm64-osx-release)` and `MacOS (osx_amd64, macos-15, x86_64, x64-osx-release, host arm64-osx-release)` — alongside both Linux archs and `format;tidy`; Windows and DuckDB-Wasm skipped as intended. So AppleClang compiled eight phases of previously-Linux-only C++ (disk format, custom aggregate, parallel probe, pragma-generated SQL) with no source change and no compiler warning citing a file under src/, and `osx_arm64` ran the suite to **2,559 assertions / 21 test cases**, the identical figure Phase 8 recorded on Linux. (The job log is not warning-free: `osx_arm64.log:820` carries the `cmake_minimum_required` deprecation warning against this repo's own CMakeLists.txt, which 9B's row shows is universal and predates this phase.) **What a green macOS check does not cover, because upstream's matrix gates the test step:** `_extension_distribution.yml@v1.5.5:716` runs tests only when `matrix.osx_build_arch == 'arm64'`, so `osx_amd64` is a build-only cross-compile from the same arm64 `macos-15` runner and never executes an assertion; `:462` likewise skips tests on `linux_arm64`. Of the four archs green here, two ran the suite and two only proved they compile and link. The `macos` job also carries `needs: [generate_matrix, linux]`, so macOS never starts until both Linux archs pass — a macOS failure surfaces ~10 minutes into a run, not at the start. |
| Complete | Work | 9B: Re-enable Windows + Wasm archs | `exclude_archs` deleted outright, along with the comment explaining the trim, and `opt_in_archs: "windows_arm64"` added in its place (commit `ccac5b0`) — the workflow now passes `duckdb_version`, `ci_tools_version`, `extension_name` and `opt_in_archs` and nothing else. **Run [31415223698](https://github.com/danthegoodman1/duckdb-ngram/actions/runs/31415223698): success, first try, no source change needed on any toolchain.** All 13 jobs green: `Windows (windows_amd64, windows-latest, x64-windows-static-release)`, `Windows (windows_amd64_mingw, windows-latest, x64-mingw-static)`, `Windows (windows_arm64, windows-11-arm, arm64-windows-static-release)`, `DuckDB-Wasm (wasm_mvp / wasm_eh / wasm_threads, wasm32-emscripten, host x64-linux)`, both MacOS archs, both Linux archs, and `format;tidy`. Windows and Wasm are sibling jobs that both fan out from `linux`, so pushing them together still attributes a failure to one toolchain by job boundary; that is why they went in one commit. **`windows_arm64` was opted in rather than un-excluded, because the exclude entry never did anything.** It carries `"opt_in": true` in extension-ci-tools' `config/distribution_matrix.json`, and `scripts/modify_distribution_matrix.py`'s `should_run()` drops any opt-in arch not named in `--opt_in`. Verified by running that script from the `v1.5.5` ref against the config it carries: `--exclude "windows_arm64" --opt_in ""` and `--exclude "" --opt_in ""` produce byte-identical Windows matrices, both exactly `[windows_amd64, windows_amd64_mingw]`. **`linux_amd64_musl` / `linux_arm64_musl` remain unbuilt**: they are opt-in too, but they were never part of the trim this phase reverses, so switching them on would add coverage the extension has never had rather than restore coverage it lost — a separate decision with its own cost, not this phase's. It is worth being precise about what that leaves uncovered: musl is a distinct libc, an axis no glibc arch exercises, so the ten archs below are the twelve in v1.5.5's config minus those two. Computed matrix, from the same script: linux_amd64, linux_arm64, osx_amd64, osx_arm64, windows_amd64, windows_arm64, windows_amd64_mingw, wasm_mvp, wasm_eh, wasm_threads. **A pre-existing warning recorded rather than patched**: this repo's `cmake_minimum_required(VERSION 3.5)` (CMakeLists.txt:1, inherited from the extension template) draws a deprecation warning — "Compatibility with CMake < 3.10 will be removed from a future version of CMake" — from **nine of the ten job logs**: `linux_amd64:4523`, `linux_arm64:4550`, both macOS at `:820`, `windows_amd64:857`, `windows_arm64:1038`, and all three wasm jobs around `:698-701`. The body is byte-identical everywhere; only the header differs, because newer CMake reclassifies the same policy warning as a dev warning ("CMake Deprecation Warning at" versus "CMake Warning (deprecated) at"). windows_amd64_mingw is the sole job without it, since rtools ships an older CMake. It is not a Windows finding and not something the widened matrix surfaced: run 31411718292, the pre-Phase-9 tip, already carries it on linux_amd64. Deferred because it is its own item — a universal, long-standing build-config warning that breaks nothing today and becomes a build error only when CMake drops 3.5 compatibility, at which point the one-line floor raise belongs in a commit that can be tested on its own. |
| Complete | Gate | Full matrix green incl. macOS | **Run [31415223698](https://github.com/danthegoodman1/duckdb-ngram/actions/runs/31415223698) at `duckdb_version: v1.5.5`: 13 of 13 jobs success**, covering all ten default archs of the distribution matrix — linux_amd64, linux_arm64, osx_amd64, osx_arm64, windows_amd64, windows_arm64, windows_amd64_mingw, wasm_mvp, wasm_eh, wasm_threads — plus `format;tidy`. **Five archs executed the test suite, each reporting `All tests passed (2559 assertions in 21 test cases)`**, pulled from the job logs: linux_amd64, osx_arm64, windows_amd64, windows_amd64_mingw, windows_arm64. The same 2,559 across MSVC, MinGW/GCC, AppleClang and Linux GCC is the phase's strongest single fact: `duckdb/CMakeLists.txt:677` suppresses MSVC's C4244/C4267 conversion warnings, so a genuine narrowing bug in the `SEGMENT_SHIFT` arithmetic or the LEB128 varint codec would have compiled silently and surfaced as a wrong answer — instead every assertion agrees with Linux, including the byte-exact postings-blob and index-identity tests. **Five archs proved compile-and-link only**, because upstream gates the test steps: `_extension_distribution.yml@v1.5.5:462,467` skip tests on linux_arm64; `:716` runs the macOS test step only when `osx_build_arch == 'arm64'`, so osx_amd64 is a build-only cross-compile from an arm64 runner; and the `wasm` job (`:1038-1224`) defines no test step at all, ending at "Build Wasm module" — Emscripten linked and uploaded `build/wasm_mvp/extension/ngram/ngram.duckdb_extension.wasm`, which is the whole of what Wasm coverage asserts here. The Windows test step (`:995`) is gated only on `skip_tests`, which is why all three Windows archs ran assertions. So this green means: all ten default archs build and link, and half of them — including all three Windows toolchains and one macOS — execute the complete suite to the identical count. Two further boundaries a reader should have without digging. The two opt-in musl archs (`linux_amd64_musl`, `linux_arm64_musl`) are unbuilt by choice, so a distinct libc remains unexercised; the ten here are v1.5.5's twelve minus those. And "pinned" means two different things across the two refs this workflow names: `duckdb/duckdb` `v1.5.5` is a genuine immutable tag (`refs/tags/v1.5.5` → `d8cdaa33`), while `duckdb/extension-ci-tools` `v1.5.5` exists only as `refs/heads/v1.5.5` — a **branch**, resolved by this run at `72e76e9`. Upstream can move that branch and change this repo's CI with no commit here, so the gate's green is reproducible against a mutable ref on the tooling side. Pre-existing and out of this phase's scope to change, but the word "pinned" should not be read as immutable for the CI tooling. **No source change was needed on any platform across both stages.** That is a property of the code rather than luck: it reaches platform behaviour only through DuckDB's own abstractions (`idx_t`/`row_t`, `NumericCast`, `MinValue`/`MaxValue`, `StringUtil::Format` over the bundled fmt rather than the C library, `TaskExecutor`, `ArenaAllocator`), and a pre-push audit of the 4,756 lines under src/ confirmed the absence of every predicted failure class — no POSIX headers, no `__attribute__`/`__builtin_*`, no VLAs, no alternative operator spellings, non-ASCII confined to comments covered by DuckDB's `/utf-8`. Two portability questions the audit answered by reading the source, **and which CI has compiled and linked but never executed**, because Wasm runs no tests: the parallel probe should degrade correctly on a single-threaded Wasm build, since `ParallelForEachUnit` (src/ngram_search.cpp:494-511) runs its body inline when `workers <= 1 || units == 1` and `ProbeThreads` clamps `NumberOfThreads()` to a minimum of 1; and the aggregate's arena block header should stay 8-aligned on wasm32's 4-byte pointers because it stores an explicit `int64_t *data` rather than a flexible array member (src/pack_postings.cpp:124-128). Neither property has been exercised on any 32-bit-pointer target; both rest on code reading alone. No compiler warning in any Windows job cites a file under src/ — the only C++ warnings are in DuckDB's own `roaring.hpp` and MSVC's `<algorithm>`. **No test was weakened, skipped, or platform-gated to obtain any of these greens**, and no test file was touched in this phase: the only changes are the workflow and this ledger. **Review lane**: reviewed on Opus rather than the usual reviewer model, which was unavailable; the reviewer re-derived the evidence from the CI logs rather than the ledger's prose — pulling the assertion lines from all ten job logs, checking the suite for platform-conditional directives, reproducing the distribution-matrix result byte-for-byte, and confirming every upstream line citation — and corrected three claims, chief among them that the CMake deprecation warning is universal and pre-existing rather than a Windows-specific finding. **Confirmation**: run [31422576000](https://github.com/danthegoodman1/duckdb-ngram/actions/runs/31422576000) on the corrected tree is 13 of 13 success against a byte-identical workflow. |

## Post-completion Review Objective (2026-08-11)

Preserve the ordinary-table, superset-candidate, recheck, and tail-scan architecture,
while removing the conditions under which it can return an incomplete result or consume
unbounded resources. Phases 10 and 11 are correctness release blockers; Phase 12 bounds
and consolidates query execution; Phases 13 and 14 then address read amplification,
lifecycle safety, observability, and release evidence.

## Phase 10: Fence Rowid-Dependent Maintenance

Goal:
No completed create, refresh, or compaction can publish postings or advance a high-water
mark from a different rowid epoch than the base-table snapshot it validated.

Scope:
- Add an execution-time shared vacuum fence that is acquired before maintenance validates
  rowid-derived state and held across every generated statement until commit, rollback,
  cancellation, or error.
- Re-run ownership, identity, high-water-mark, and index-option validation after acquiring the
  fence. Pragma-callback validation may reject obvious failures early, but is not the
  correctness boundary.
- Make owner identity comparisons case-insensitive in every reader and generated guard;
  replace deterministic temp-table names and the shared `__ngram_guard` session variable
  with invocation-scoped internal state that cannot overwrite user state.
- Give the fence and all scratch state one cleanup owner whose lifetime covers success,
  fallback, cancellation, and exceptional exits.

Out of scope:
- Detecting and overlay-scanning updated row groups; Phase 11 closes that independent
  correctness gap.

Completion gate:
In the deterministic preprocessing-to-execution race, fence-first maintenance makes an
overlapping checkpoint use a non-rowid-moving path and commits metadata and postings from
that fenced snapshot. If a rowid-moving checkpoint completes first, execution-time
identity, HWM, and option validation refuses refresh or compaction and leaves the
previously published index unchanged; a create may instead build wholly from the fenced
post-checkpoint snapshot using a runtime HWM. No schedule may commit
maintenance state assembled from two rowid epochs. Exact search through an index that a
checkpoint invalidated before the fence is acquired remains Phase 11's responsibility.

Testing plan:
- Add a deterministic in-process three-connection harness that freezes a pragma after
  preprocessing but before its generated transaction executes. Exercise both external
  schedules (fence first and rowid-moving checkpoint first), checkpoint-first create,
  and the automatic checkpoint run by the maintenance transaction's own commit.
- Prove rollback, error, and commit cleanup by causing real rowid movement in a following
  checkpoint, and assert failed work leaves the previous HWM and temp catalog unchanged.
- Use functional SQL tests for ordinary create, bounded and unbounded refresh, purging and
  non-purging compaction, case-only renames, ASCII/non-ASCII owner identity, pre-existing
  user temp tables and the `__ngram_guard` session variable, and macro shadowing.
- This is mechanism coverage, not a timing matrix for every operation and exit. Compact
  and bounded refresh enter through the same first-statement fence and existing-index
  guard as unbounded refresh; cancellation reaches the same DuckDB rollback callback as
  an error. No cancellation injection or concurrent-maintenance stress is claimed here.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Scope | Maintenance holds one execution-time rowid-stability fence through publication | `AcquireMaintenanceFence` takes DuckDB's normal transaction vacuum lock before any generated write. `MaintenanceFenceState` keeps a second shared lock in connection state until DuckDB's post-commit or rollback callback; that bridge is necessary because DuckDB releases the transaction-owned lock before running the same commit's automatic checkpoint. The holder is mutex-protected for direct parallel invocation and stores each manager/lock as one exception-safe aggregate. |
| Complete | Scope | Validation runs after the fence and against the publishing snapshot | The volatile, fallible `system.main.__ngram_maintenance_guard` is the first generated body expression for create, refresh, and compact. After fencing it re-resolves the table and verifies table OID, schema fingerprint, ASCII-case-insensitive ownership, indexed column, HWM, gram size, and case option. Create separately enumerates current metadata owners, rejects an existing same-column index, and derives HWM from the runtime committed maximum rowid. Phase 11 later removed format-3 row witnesses. |
| Complete | Scope | Identifier, scratch, and guard state cannot collide with user state | Scratch/guard tables use random UUID names and transactional `CREATE TEMP TABLE`, and maintenance no longer reads or writes a session variable. Generated calls are `system.main` qualified so user macros cannot intercept them. SQL owner guards compare encoded ASCII-folded bytes, matching C++ `StringUtil::CIEquals` even under `default_collation='nocase'`; `ExistingMetaTables` and stats discovery use case-insensitive prefix matching. |
| Complete | Scope | Fence and scratch cleanup covers every exit | The lock has one `ClientContextState` owner and is cleared by DuckDB's commit/rollback callbacks; temp scratch belongs to the generated transaction. The deterministic harness proves rollback after an execution-time error and success both permit a later checkpoint to move rowids, and SQL regressions prove failed work leaves no invocation scratch. Cancellation is not separately injected; it follows DuckDB's same automatic rollback callback. |
| Complete | Work | 10A: Introduce a maintenance execution wrapper with transaction-lifetime vacuum fencing | Implemented by the shared guard scalar plus `MaintenanceFenceState`, covering every statement in each generated transaction and the otherwise-unprotected automatic-checkpoint interval inside commit. No production scheduling hook or custom lock protocol was added. |
| Complete | Work | 10B: Move definitive identity/HWM checks behind the fence | Callback checks remain only as early rejection. The generated body repeats all publication-critical checks after its fence and includes explicit create/existing mode and index-option comparisons. Phase 11 superseded and removed the witness mechanism from unreleased format 3. |
| Complete | Work | 10C: Replace name-derived scratch objects and shared session variables | Create, refresh, and compact now use UUID temp names; the fixed `__ngram_guard` variable is gone. Owner semantics are shared across create/search/maintenance/drop/stats, including case-only renames and distinct non-ASCII identifiers. |
| Complete | Gate | Checkpoint/maintenance schedules cannot mix rowid epochs | The old transaction-only build failed the same-commit discriminator 3/3 with `max(rowid)=127119`, `HWM=124999`, brute-force count 1, and indexed count 0. The final deterministic harness passes repeatedly: fence-first checkpoint is non-moving; checkpoint-first refresh refuses without publishing; checkpoint-first create builds exactly from the moved snapshot; and same-commit automatic checkpoint leaves rowids stable until publication, after which a fresh delete plus checkpoint proves release. The full release gate passed the harness and all 22 SQL tests (2,668 assertions). |
| Complete | Test | Deterministic checkpoint/fence mechanism coverage | `test/cpp/ngram_maintenance_checkpoint_gap.cpp`, wired into normal release/debug/reldebug test targets, freezes expanded ASTs at the real preprocessing/execution seam and covers both external schedules, the distinct create branch, same-commit automatic checkpoint, exact search/HWM outcomes, and real lock release. Ordinary SQL tests cover bounded/unbounded refresh and both compact modes, which share the same first-statement existing-index guard. No full operation-by-schedule matrix is claimed. |
| Complete | Test | Cleanup and namespace regressions | The C++ harness covers commit, explicit rollback after a generated execution error, temp-catalog cleanup on the maintenance connection, and subsequent real rowid movement. SQL covers case-only table/meta renames, non-ASCII owner separation under `nocase`, preserved legacy temp names and user `__ngram_guard`, and user macros shadowing internal names. Cancellation and concurrent-maintenance stress were not injected; cleanup there follows the already-exercised rollback owner. |

## Phase 11: Make Exhaustiveness Unconditional

Goal:
Close the in-place-update and post-vacuum miss paths so the extension's primary promise
is literal: every accelerated result is identical to the same predicate's brute-force
result, without requiring callers to remember a rebuild rule.

Scope:
- Create one zero-posting `NGRAM_ROWID_GUARD` native index per ngram index. Its physical
  column dependencies make covered updates delete and append rather than mutate a rowid in
  place; its non-ART type makes DuckDB's live-rowid-moving vacuum path ineligible. Persist
  an allocated-rowid high-water mark, permanent unsafe-reuse latch, random incarnation
  token, compatibility bit, and database-header checkpoint seal without scanning the base
  table to build the guard.
- Make guard installation atomic with the postings build. Under the Phase 10 vacuum fence,
  exclude old writers, replace the base `DataTable` with an ADD/DROP of a UUID-named nullable
  column, create the scan-free guard while its build state holds the table append lock across
  baseline and physical installation, then validate its internally minted token and release
  the exclusive checkpoint lock before the long postings scan. Existing format-3 guard
  coverage or a strictly old explicit native ART can replace the ADD/DROP barrier. Reject
  transaction histories that cannot be made safe and make every race fail or retry before
  publication.
- Record the exact guard name and token in meta format 3 and validate type, table, physical
  column, token, persisted state, and pinned DuckDB runtime on every query and maintenance
  path. A missing, incompatible, replaced, or unsafe guard makes explicit and transparent
  search scan; `ngram_candidates` emits every visible covered-prefix rowid; refresh and
  compact refuse until rebuild.
- Allow checkpoint vacuum to discard fully deleted trailing row groups while keeping live
  rowids stable. The first committed append that reuses any previously allocated rowid
  latches the guard unsafe, so no stale posting can cause a miss. Seal guard state to the
  database-header checkpoint iteration so an unbound checkpoint cannot launder buffered WAL
  replay; eagerly bind only at safe startup/last-close boundaries and quarantine malformed
  persisted state without poisoning DuckDB's bind state. Make public DROP recover
  exact bound or unbound guards and guard-less format 2 indexes without deleting a
  same-named or re-created unrelated index.
- Pin the custom index implementation to the exact host-reported DuckDB version/source whose
  transaction, index, and checkpoint behavior was audited. Read the qualified host built-in
  `pragma_version()` before registration and accept only the exact 8-character local or
  10-character official ID for the pinned commit; a mismatch latches fail-closed without
  blocking inspection/drop. Document the material costs: writes require
  the extension while a guard exists, creation invalidates the reservoir sample and makes
  concurrent writers retry, native index dependencies restrict some ALTER operations, and
  conservative commit-conflict handling may require rebuild even when no wrong answer was
  possible.

Out of scope:
- A dirty-rowgroup overlay, surgical repair, or clearing an unsafe guard in place: a full
  scan plus explicit rebuild is the deliberately smaller recovery path.
- Supporting the custom guard across unaudited DuckDB internals; incompatible runtimes fail
  closed and retain a safe removal path.
- Enabling transparent acceleration by default. Exactness is no longer the blocker after
  this phase, but Phase 12 must first bound its memory and work.
- Fuzzy matching, ranking, or storing ngram postings in the native guard.

Completion gate:
For committed inserts, indexed and non-indexed updates, deletes, rowid-preserving and
trailing-rowgroup checkpoints, actual trailing-rowid reuse, close/reopen, WAL replay, and
mixed snapshots, explicit and transparent results equal brute force. Every uncertain guard
state scans or refuses before returning; every creation interleaving either publishes one
atomic guard/postings snapshot or publishes nothing; and public DROP remains a safe recovery
path without deleting an index whose identity it cannot prove.

Testing plan:
- Add a deterministic in-process barrier harness for active and recently committed writers,
  pre-bound updates, staged append rollback, creator commit failure, retry, explicit rollback,
  connection close, post-barrier build error, and proof that the custom guard's
  physical build plan never scans the base table.
- Exercise indexed-column updates that introduce and remove matches, NULL/equal-value cases,
  non-indexed controls, local inserts in the creator transaction, WAL-only crash/replay,
  checkpoint/reopen, harmless trailing shrink, actual reused rowids, and
  `vacuum_rebuild_indexes` under both explicit and transparent search.
- Verify unsafe/missing/re-created/incompatible guards choose exact full-scan or covered-prefix
  behavior, maintenance refuses, stats explains why, rebuild recovers, and the candidate API's
  prefix remains disjoint from its caller's tail scan.
- Cover format-2 cleanup, malformed format/version combinations, missing and unbound format-3
  guards, same-name indexes on another table, direct guard/query concurrency, unsupported
  runtime behavior, extension-absent reads and writes, and the documented ALTER restrictions.
- Characterize creation time/RSS on representative wide and cold tables, then pass the full
  release and DEBUG/ASAN suites with differential exactness checks.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Scope | A zero-posting non-ART guard makes update/vacuum histories exhaustive | `src/rowid_guard.cpp` stores no keys, broad normal guards cover every then-existing physical `VARCHAR`, the ordinary sequence-rowid append path is O(1), and non-ART presence makes v1.5.5's ART-only moving-vacuum predicate false. `ngram_rowid_guard.test` covers clustered/scattered below-HWM updates, NULL/equal-value transitions, a non-covered control, delete/checkpoint, harmless trailing shrink, committed reuse, candidates-prefix disjointness, and explicit/transparent parity. Bulk/all-NULL append is source-audited rather than given a separate fixture: pinned v1.5.5 `DataTable::AppendToIndexes` passes every row and the guard consumes only its value-independent rowid sequence. The C++ harness first proves an ART-only control moves rowid 122880→0 with `vacuum_rebuild_indexes=500000`, then proves the identical guarded survivor remains 122880 and accelerated. |
| Complete | Scope | Creation atomically excludes pre-guard writers without holding EXCLUSIVE through postings build | The generated transaction upgrades the checkpoint lock, rejects active/recent writers and prior global update/delete/catalog undo, performs the UUID ADD/DROP barrier, installs the scan-free guard while its global state retains `TableAppendState` through `PhysicalCreateIndex::AddIndex`, and releases through one validating/token-returning finish scalar. The deterministic harness covers pre-bound UPDATE/INSERT/DELETE, active/recent refusal, local INSERT, post-finish build error and rollback, connection-close release, physically staged append + `RevertAppend` restoring old storage and forcing creator COMMIT failure, and retry. Cancellation is not injected; it uses the same DuckDB rollback owner as the exercised error/close paths. |
| Complete | Scope | WAL, checkpoint, reopen, and incompatible persisted state fail closed | WAL serialization records current commit-time guard state, distinguishing creator-local from later replayed appends. Binding carries the persisted header-iteration seal without reading DuckDB's non-atomic live counter; query validation compares it under a temporary shared checkpoint lock (or the context's existing creation EXCLUSIVE), while disk/WAL serialization consumes and latches it under their existing checkpoint ordering before resealing. This detects an unbound checkpoint that discards buffered replay, including bind followed by checkpoint before first query. Strict eager bind runs only before a first connection or on safe last close; a tolerant incompatible quarantine avoids v1.5.5's stuck-`BINDING` path while query validation stays strict. Runtime compatibility is derived from the host-executed `system.main.pragma_version()`, not the DSO's statically linked `DuckDB::SourceID()`, and accepts only v1.5.5 `d8cdaa33`/`d8cdaa33fd`. Harness cases cover crash WAL, actual reuse then stock extension-free shutdown followed by eager bind and FORCE-before-query, startup/dynamic-FORCE/last-close seal paths, repeated source-mismatch query/write/reopen/drop, and one malformed unrelated guard not poisoning a valid index. |
| Complete | Scope | Uncertain state preserves every public query contract | Missing, replaced, incompatible, replay-buffered, max-behind, seal-mismatched, and unsafe guards select one full live scan in `ngram_search` and transparent execution; candidates enumerate visible `rowid <= HWM`, leaving tail/local rows disjoint; maintenance refuses; stats exposes the reason. State is one fixed-size latch, not dirty ranges, so 1/10/100/1000 uncertain events cannot add overlay work: the SQL plan gate shows exactly one `SEQ_SCAN` after the latch. |
| Complete | Scope | Public DROP is an identity-safe recovery path | The execution scalar pins meta catalog OID, layout, format, guard name/token, table/type/column, and physical state before unconditional `DROP INDEX IF EXISTS`. A present unbound v3 guard is structurally pre-screened with every same-type guard, bound through the tolerant quarantine path, and re-read as the exact BOUND incarnation before validation succeeds; a current `BINDING` state refuses with retry instead of racing DuckDB's retained raw entry pointer. It accepts a genuinely missing expected guard even when another broad guard overlaps, rejects same-name replacement/cross-table collisions, and conservatively refuses malformed format-2 hybrids. Harness cases cover both preprocessing/execution races, forced BINDING refusal then UNBOUND→BOUND validation, same-name token replacement, missing overlapping guards, unbound incompatible cleanup, and rendered option-free guard DDL. |
| Complete | Work | Format 3 supersedes probabilistic row witnesses | `row_samples`, 32 random fetches, digest/hash helpers, scalar registration, generated SQL, and tests were deleted from the unreleased format-3 layout. The Phase 10 fence plus deterministic guard/barrier proof covers the schedules witnesses attempted to sample. Historical Phase 5/8 ledger rows remain history; they do not describe current metadata. |
| Complete | Test | Adversarial SQL and internal-schedule harness | `test/sql/ngram_rowid_guard.test` and the repurposed `ngram_maintenance_checkpoint_gap.cpp` cover the required result and engine schedules, including stock v1.5.5's exact boundary: SELECT and unrelated ADD work, INSERT/UPDATE/guard ALTER fail, and DELETE busy-spins in the host binder under a bounded child timeout without committing. Added-VARCHAR lifecycle proves the old index remains usable, the uncovered new target refuses, and an explicit old ART permits a target-only guard. |
| Complete | Test | Guard/barrier cost characterized without a benchmark subsystem | Release, one thread, 200k-row base plus 500k appended rows: `/usr/bin/time` measured 0.21 s unguarded, 0.22 s with one broad guard, and 0.22 s with two; peak RSS was 60.1/60.2/60.5 MB. Updating 100k covered rows was 0.04/4.73/4.76 s and 40.0/47.9/48.0 MB: the cost is DuckDB's required delete+insert rewrite, while a second zero-data guard is negligible. Source audit adds a distinct wide-table DELETE/rollback cost: v1.5.5 fetches the union of every guard-covered `VARCHAR` before value-independent `TryDelete`; multiple broad guards deduplicate that union. On a cold 57,946,112-byte table with 200k rows × 16 varied `VARCHAR`s, cold open/count and UUID ADD+DROP+16-column guard both rounded to 0.01 s; peak RSS was 26,372 vs 26,468 KiB and file bytes were identical. These are one-run mechanism characterizations, not general throughput claims. |
| Complete | Gate | Full release, randomized differential/churn, DEBUG/ASAN, and skeptical approval | Exact-source `timeout 1800s env CCACHE_DISABLE=1 make test_release` and `timeout 3600s env ASAN_OPTIONS=detect_leaks=0 CCACHE_DISABLE=1 make test_debug` both pass the deterministic C++ harness and all **2,674 assertions / 22 SQL cases**, with no sanitizer finding. The corrected exhaustive differential harness passes explicit `--seed 110311 --trials 2 --rows 1500` (1,752 checks) and transparent seed 110312 (886); both exercise introducing/removing a below-HWM marker and exact post-vacuum parity. Churn verifies before any stats-triggered rebuild and passes ordinary seed 110313 plus `--no-stale-expected` seed 110314 (`--rounds 20 --rows 3000`, 320 checks each, zero failures/verdicts). The official stock v1.5.5 `d8cdaa33fd` CLI also passes a fresh local-repository `FORCE INSTALL`/`LOAD` format-3 lifecycle: covered update + checkpoint stays healthy/exact, extension-free SELECT works while INSERT fails closed, loaded reopen finds a tail append, and public drop removes guard/meta without touching two base rows. The skeptical reviewer independently passed the focused harness, `make test_release` (2,674/22), the same four seeds (1,752/0, 886/0, 320/0, strict 320/0/0 verdicts), the exact current DSO (SHA-256 prefix `5eb4797d`) on the official v1.5.5 lifecycle, and `git diff --check`, then approved the phase with no remaining production blocker or safe high-leverage LOC cut. |
| Complete | Docs | User-visible contract and costs replace the blanket ART requirement | `README.md` and `docs/stale-updates.md` now describe unconditional scan/refusal semantics, creation writer retries and sample loss, broad update rewrite, the narrow added-column/native-ART protector path, dependency-limited ALTER, conservative conflict/DETACH degradation, source pinning, and the extension-absent read-only boundary. |

## Phase 12: Bound and Consolidate Query Execution

Goal:
Make query memory and work predictable before decoding starts, while replacing the two
nearly parallel explicit/transparent engines with one projection-aware implementation.

Scope:
- Replace the post-materialization candidate gate with admission checks based on posting
  counts, estimated decoded bytes, DuckDB's memory limit, and an explicit hard work budget.
  Full-result functions fall back to a scan; candidate-only APIs stream or return a clear
  resource-limit error before oversized allocation.
- Change `ProbeIndex` from a whole-query `vector<row_t>` result to a segment/chunk-oriented
  candidate source. Intersect, fetch, recheck, and emit bounded chunks without retaining
  an all-row posting list or final candidate set in extension memory.
- Factor candidate probing, fetched-row recheck, tail/dirty scan, partition scheduling,
  and fallback into one execution core used by `ngram_search` and `NGRAM_INDEX_SCAN`.
- Add projection pushdown to `ngram_search`: fetch the indexed column for recheck plus
  only the output columns the parent requested, including the zero-output `count(*)` case.
- Catch only expected index-availability/staleness conditions for fallback. Propagate OOM,
  cancellation, corruption, and internal errors, and release the vacuum fence before a
  full-scan fallback begins.

Out of scope:
- Approximate result modes, ranking, or changing the exact semantics of either result API.
- Changing postings encoding; Phase 6 measured and rejected that trade.

Completion gate:
No query path allocates memory proportional to the full posting count or candidate count
without a pre-approved reservation. A corpus-scale dense gram under a tight memory limit
streams within budget or selects a full scan before materialization; explicit and
transparent results and output order remain identical to their current exact oracles.

Testing plan:
- Add dense single-gram and common-gram-conjunction cases with posting counts extrapolated
  through the 1.09-billion-row benchmark shape; run under tight memory limits and record
  peak RSS, decoded rowids, and selected mode.
- Compare candidate chunks and final results at thread counts 1/2/8/24, across refresh
  generations, partial segments, fallback decisions, cancellation, and corruption/OOM
  injection.
- Add wide and nested tables and assert physical fetch columns for `count(*)`, one-column,
  reordered, duplicate, and all-column projections on both explicit and rewritten paths.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Scope | Probe admission happens before decoded candidate allocation | `PlanIndexProbe` reads checked multi-generation stats, builds a flat descriptor manifest from visible shadow rowids, validates structural invariants, and computes hard-work, candidate, per-segment, and per-worker bounds before `DecodePostings` is reachable. `ProbeMemoryReservation` reserves the admitted planning/manifest/workspace headroom through `BufferManager`; the query budget is `min(memory_limit / 4, 256 MiB)`. |
| Complete | Scope | Candidate decode/intersection/fetch is segment-streaming and bounded | `NextCandidateSegment` owns only one admitted segment's posting/intersection vectors. `ExecuteSearchCore` fetches and emits it in `STANDARD_VECTOR_SIZE` chunks before claiming another; `ColumnFetchState` is reset at safe chunk boundaries so base/shadow block pins do not accumulate query-wide. Blobs are fetched by manifest rowid; single-blob codec order is retained, while multi-generation rows are merged, sorted, duplicate-checked, and intersected without a global candidate vector. |
| Complete | Scope | Explicit and transparent paths share one execution core | `SearchCoreGlobal`, `SearchCoreLocal`, and `ExecuteSearchCore` now own candidate fetch, exact recheck selection, projected emission, tail/full scan, async yield, deterministic batch numbering, and thread scheduling for both adapters. `src/ngram_rewrite.cpp` delegates to that core and is 223 production lines smaller than the phase baseline; total production source is net +390 lines. |
| Complete | Scope | Explicit search supports projection pushdown | `ngram_search` enables projection and nested-extract pushdown, maps requested output columns to storage fetch columns, appends only the hidden indexed column needed for recheck, and exposes an EMPTY boolean virtual column so `count(*)` fetches only the indexed `VARCHAR`. Dynamic profiles pin count/one/nested physical fetch layouts on both adapters; result assertions pin reordered/duplicate/all-column correctness. |
| Complete | Scope | Fallback classification preserves fatal errors and releases locks | Missing shadow tables and valid-but-stale identity select an exact full scan; malformed/colliding/wrong-kind metadata, decode errors, cancellation, OOM/reservation failure, and internal errors propagate. Both exact adapters reset their `SharedVacuumLock` before an unfiltered full scan, and EXPLAIN/ANALYZE reports the concrete decline reason. |
| Complete | Work | 12A: Define probe admission and resource-budget policy | Added `ngram_max_probe_rowids` (default 100M), applied the existing candidate-fraction policy consistently to exact adapters, retained rarest-K selection across the full needle, and made candidate-only admission use its actual one-worker cap. Exact paths scan on policy decline; `ngram_candidates` returns the governing resource error. |
| Complete | Work | 12B: Implement a bounded segment candidate source | Flat `(segment, gram, shadow-rowid, count)` descriptors avoid segment×shadow rescans; same-snapshot manifest fetch is protected by the vacuum fence. Per-segment generation union/intersection remains globally rowid-sorted, decoder work is counted, cancellation is checked between descriptor batches, and retained row/pin state is bounded per admitted worker. |
| Complete | Work | 12C: Build one projection-aware search executor and migrate both entry points | Explicit normalized-string recheck and transparent `ExpressionExecutor` recheck are thin callbacks over the same fetch/scan/emission state machine. The core handles projected storage types, rowid/tail filters, local rows, fallback scans, ordered sink partitions, and bounded probe workers. |
| Complete | Work | 12D: Replace broad exception fallback with explicit outcomes | Missing-only catalog resolvers no longer swallow wrong-kind or malformed objects. Expected availability, staleness, density, work, and memory declines are explicit; all other exceptions propagate. Dynamic profiles expose mode/reason, selected storage columns, candidates/decoded work, and candidate worker count. |
| Complete | Gate | Dense probes are bounded before materialization and both APIs retain exact parity | The release smoke in `benchmarks/RESULTS.md` uses a persisted 2.2M-hit dense index: admitted exact/candidate queries peak at 39.5/39.0 MB under 64 MB, while 16 MB memory and 1M-row work limits choose a scan before decode at 28.1/27.7 MB; candidate-only returns the same predecode memory error. The manifest extrapolates to about 1,042 independent bounded segments at 1.09B rows rather than one ~8.1 GiB rowid vector. |
| Complete | Test | Dense-probe memory/work matrix | `ngram_query_bounds.test` covers dense single/conjunctive probes, rarest-K, 8/16 MB limits, hard-work declines, partial/final and cross-boundary segments, one-worker candidate admission under `threads=24`, actual decoded counters, explicit/transparent parity, and typed malformed stats/manifests/blobs. Candidate-fraction fallback remains covered by the existing parallel/rewrite suites. The Phase 12 smoke records wall time, RSS, DuckDB buffer use, mode, and work. |
| Complete | Test | Shared-engine projection, fallback, ordering, and fault suite | Wide/nested projection and physical-column assertions, missing-storage fallback, wrong-kind/incompatible/corrupt propagation, multi-generation and two-segment `threads=24` global-order cases, plus existing 1/2/3/4/8/24 differential suites are green. The C++ harness interrupts a 2.2M-row dense exact query, proves the connection remains usable, and then completes `FORCE CHECKPOINT`, covering cancellation plus reservation/vacuum-fence release. `make test_release` and `make test_debug` each pass the deterministic harness and **2,821 assertions / 23 SQL cases**; DEBUG reports no ASAN/UBSan finding. Their native prerequisites explicitly relink `unittest` alongside the harness so stale statically linked extension code cannot be tested. Independent fixed-seed review runs also pass: explicit `120121` (1,764/0), transparent `120122` (886/0), churn `120123` (320/0), and strict churn `120124` (320/0 failures/0 verdicts). The skeptical reviewer independently repeated focused, release, DEBUG/sanitizer, randomized, dense-smoke, diff, and documentation gates and **APPROVED Phase 12** with no remaining blocker. `git diff --check` is clean. |

## Phase 13: Remove Maintenance and Stats Read Amplification

Goal:
Make compaction cost proportional to data actually rewritten and make rarest-gram lookup
proportional to the requested grams, not to the accumulated global stats history.

Scope:
- Make non-purging compaction a shadow-table-only merge. Retaining dead postings is safe
  because fetch/recheck removes them; only `purge = true` should pay to consult the base
  table for liveness.
- Make purging compaction scan the indexed base prefix once per selected indexed column,
  retain only relevant live rowids, and range-scan that bounded source from the packing
  statements. Direct rowid range predicates do not physically prune the v1.5.5 base scan
  and are therefore not the chosen implementation.
- Materialize selected encoded segment rows once in segment order so packing ranges do not
  multiply persistent gram-ordered segment-table scans.
- Keep byte-sorted one-row-per-gram stats through a validated stats-only fold on every
  refresh; compaction rebuilds them in byte order from segment metadata. Use a
  requested-gram zone-map hint in the raw stats reader.

Out of scope:
- Changing exact query semantics or postings encoding.
- A background compaction scheduler; this phase makes each requested operation efficient.

Completion gate:
Non-purging compaction performs zero base-table row reads. Purging compaction performs one
relevant base-membership pass per selected indexed column, and persistent segment-table
reads are independent of the memory-driven packing range count. Query gram-count lookup
reads data proportional to the needle grams and their bounded physical stats layout, with
output identical to current stats sums.

Testing plan:
- Profile compact at partition counts 1/8/auto over low/high fragmentation and purge
  on/off; record base/segments rows, operator I/O, peak RSS, spill, and wall time.
- Compare postings and stats before/after each compaction in both directions, including
  deleted rows, partial segments, many generations, rollback, crash, and reopen.
- Benchmark gram-count lookup over 1/10/100 refresh generations and rare/common needle
  grams; require identical gram choices, candidates, and final results.
- The scale gate is amended to a real 0.984 GB BLOB-heavy, 100-refresh corpus plus an
  actual 1.09B-row sparse prefix. The former exercises selected-BLOB/output coexistence
  and spill; the latter exercises roughly 1,042 generated ranges and a billion-row base
  pass. Neither is presented as a 100 GB BLOB benchmark, and a 10 GB rebuild was not
  required because the 1 GB runs already spilled nonlinearly sized temp state.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Scope | Non-purging compaction does not read the base table | The merge plan derives keys and its conservative auto range count entirely from HWM/shadow state, materializes only selected encoded rows, and omits the live-row source. Profiles report **0 base rows** at p1/p8/auto on both 0- and 8-fragmented-key fixtures and on the 4.64M-segment real corpus. A deleted posting remains a candidate after merge and exact recheck removes it; purge removes it. |
| Complete | Scope | Purging base reads are not multiplied by packing ranges | A measured direct semi-join read 8,388,608 base rows at p1 and 67,108,864 at p8: rowid bounds did not prune the physical v1.5.5 scan. The chosen spillable one-BIGINT live temp scans the base once, restricted to HWM, selected segments, and non-NULL indexed values; valid v3 postings can only originate from those rows. Final real auto-purge scans **11,920,423 base rows once** while 12 range statements read the ordered temp. The sparse 1.09B-row run likewise scans the base exactly once and retains only 1,042 relevant rowids. |
| Complete | Scope | Segment-source reads do not become persistent full-table scans per partition | Selected encoded rows are copied once into a spillable `(segment_no, byte-gram)`-ordered temp. On the real auto-purge profile, its 12 range scans consume 5,769,777 logical rows for 4,637,233 selected rows (1.244×), rather than 12 persistent scans. The persistent segment table's 16,121,746 cumulative rows are broken across four unavoidable statements: key discovery, selected-source materialization, selected deletion, and stats rebuild; this total is independent of packing range count and is not claimed as one source pass. |
| Complete | Scope | Rarest-gram counts avoid accumulated global history | `ReadGramStats` applies `OptionalFilter(InFilter)` as a zone-map hint and retains checked byte-exact C++ filtering. Refresh validates every historical stats row it folds, aggregates by encoded gram, and persists one byte-sorted row per gram; compaction instead rebuilds byte-sorted stats from resulting segment metadata. Refresh therefore upgrades an old v3 history without marker/schema state. On the 605,513-gram real history, filtered work after 1/10/100 same-connection folds stays at **122,880 rows/60 chunks** for one ordinary gram and **245,760/120** for the many-gram case; K=1 rare latency is 0.030/0.032/0.047 s and K=8 is 0.196/0.190/0.199 s. Fully deleted MVCC rowgroups remain allocated until checkpoint (about 2.3 GB after 100 folds), but the scanner skips them; the phase does not claim physical history reclamation. |
| Complete | Work | 13A: Split merge-only and purging compaction plans | Merge uses only the selected shadow source; purge adds one live-rowid temp. Checked unpack validates NULL/key/descriptor/generation/count/min/max, rowid bucket, and HWM before liveness filtering, including an empty-live-temp regression. Selected-only corruption is inspected by merge; purge selects and validates every key. v2 remains drop-only; maintenance requires the v3 guard. |
| Complete | Work | 13B: Eliminate partition-multiplied segment scans | The ordered selected-source and live-row temps were chosen over direct persistent range scans after A/B profiling. The real p8/auto request emits **12**, not 8, ranges because the segment-aligned floor split may overshoot; auto requests 4096 and emits one range for each of the 12 covered segments. At 1,024 MB, merge spills 1,001,521,184 B and completes in 4.81 s / 2.11 GB RSS; at 1,536 MB, purge spills 4,576,434,848 B and completes in 40.0 s / 2.43 GB RSS. |
| Complete | Work | 13C: Prototype filtered stats generations versus segment-derived counts | At 1M distinct grams and eight segment rows per gram, K=1 was 2.6–2.9 ms for either source; K=8 was 18.3 ms from stats versus 11.3 ms from segments, but K=1000 was 23 ms/1M stats rows versus 81.5 ms/8M segment rows. Retaining stats wins the unbounded-K shape and preserves existing validation. Per-delta ordering alone was rejected after a real 101-generation history still read nearly every broad coalesced rowgroup. An unconditional validated fold was the smaller bounded solution: a 605,513-distinct real fold is 0.178 s generated-statement time normally and 0.421 s at 96 MB with 141,361,152 B spill; a 4.5M-distinct synthetic aggregate/temp creation is 1.255 s at 96 MB with 296,419,328 B spill (the latter does not measure persistent delete+insert). |
| Complete | Gate | Maintenance and stats reads scale with selected work, not partition/history count | The final-tree 0.984 GB/100-refresh auto-purge artifact records 22 statements, 12 actual ranges, 33.16 s wall, 9,677,436 KiB max RSS, 16,326,545,408 B DuckDB peak buffer, no spill, 36,962,304/254,541,824 B profiler storage read/write, and one 11,920,423-row base scan. OS input bytes were zero on a warm page cache and are not treated as physical-I/O proof. Merge p1/auto is 3.32/3.18 s with zero base rows. A 1.09B-row, 1,042-segment sparse proxy isolates auto statement overhead: no-key merge emits 1,051 statements in 0.57 s / 68,784 KiB RSS; fragmented merge is 1.19 s / 229,812 KiB. Purge completes in 1.76 s / 311,036 KiB after one 1.09B-row base pass and retains 1,042 live rowids; this is explicitly not a 100 GB BLOB/temp claim. |
| Complete | Test | Compaction amplification and identity matrix | A preserved 8,388,608-row profile/identity matrix covers low/high fragmentation × p1/p8/auto × merge/purge. Requested p1/8/auto emit 1/8/8 actual ranges on its eight segments; all 12 runs have 0 bidirectional posting differences, 0 stats↔segments differences, and identical 8,388,608 candidates/final rows. High-fragmentation max RSS at p1/p8/auto is 185,484/141,656/135,728 KiB for merge and 728,664/608,044/628,280 KiB for purge; low-fragmentation values are 60,320/61,468/60,336 and 742,144/631,324/604,844 KiB. It also pins deletes, partial/empty/HWM=-1 indexes, NULL transitions, unrelated updates, nocase/case-distinct and multibyte grams, malformed keys/blobs/descriptors/stats, explicit rollback, cancellation with digest/temp cleanup/checkpoint, and reopen. Crash seed 20261317 kills refresh, fragmented merge, purge, and bounded catch-up at four offsets each; the stats count plus two order-independent value digests and posting/search checks accept only exact pre/post boundaries, with 0 failures. |
| Complete | Test | Stats-history scaling and query parity matrix | The 1/10/100-generation curves cover K=1/default/high, rare/common/missing grams, selected-gram/candidate/final-result parity, high-K memory accounting, binary order under nocase and disabled insertion-order preservation, and relevant corruption. Unrelated malformed stats rows are intentionally outside the filtered probe; refresh's full fold rejects every malformed input atomically, while compact retains its historical behavior of replacing stats from segment metadata. Independent final-tree commands pass: explicit differential `--trials 2 --rows 5000 --seed 20261314` (**1,764/0**), transparent with the same budget and seed 20261315 (**886/0**), normal churn `--rounds 20 --rows 4000 --seed 20261316` (**320/0**, no unmaintainable state or detector report), and strict churn with the same budget, seed 20261318, and `--no-stale-expected` (**320/0/0 detector verdicts**). This phase supersedes the Phase 7/8 historical claims that stats remain unordered/appended until compaction; `README.md` and `benchmarks/RESULTS.md` now describe refresh-time folding and the merge/purge temp-disk tradeoff. |
| Complete | Test | Final release and sanitizer suites | Final-tree `CCACHE_DISABLE=1 make test_release` passes the deterministic C++ harness and **2,944 assertions / 23 SQL cases** (14.52 s wall, 1,328,720 KiB max RSS). `ASAN_OPTIONS=detect_leaks=0 CCACHE_DISABLE=1 make test_debug` passes the same harness/cases with no ASan/UBSan finding (37:08 wall, 6,400,040 KiB max RSS; leak detection is disabled because LSan cannot run under the ptrace sandbox). Focused cancellation/rollback gates are green. `git diff --check` is clean. |

## Phase 14: Lifecycle, Observability, and Release Evidence

Goal:
Make every index discoverable and removable throughout its lifecycle, and make release
claims reproducible from artifacts and correctness checks rather than hand-maintained
prose or a portability-only CI matrix.

Scope:
- Add a durable ordinary-table registry with opaque index IDs. Derive new shadow object
  names from the ID rather than user identifiers, and expose list/status/drop-by-ID APIs
  that continue to work when the base table or indexed column is absent.
- Define rename and orphan handling around DuckDB v1.5.5's host constraints. A physical
  rowid-guard dependency refuses table and column rename (including case-only rename),
  while moving a table between schemas and renaming a schema are not implemented by the
  host. The supported workflow is drop by stable ID, rename, then rebuild; retain
  case-insensitive API lookup and legacy-index discovery/drop during the format transition.
- Emit versioned machine-readable benchmark artifacts containing commit, DuckDB version,
  corpus manifest, settings, hardware, commands, and raw samples. Generate README and
  `benchmarks/RESULTS.md` tables/claims from those artifacts with consistency checks.
- Add a Linux correctness CI lane: release suite, DEBUG + AddressSanitizer/UBSan, the
  deterministic Phase 10 race, and bounded fixed-seed differential/churn tests. Keep the
  existing distribution workflow as a separate build/portability lane.

Out of scope:
- Automatically migrating postings to a native DuckDB index backend.
- Running the 100 GB benchmark on every pull request; release artifacts may be produced on
  dedicated hardware and verified cheaply in CI.

Completion gate:
Every structurally valid allocation created by the extension can be listed and dropped by
stable ID even after base drop; stable-ID drop removes the host dependency before rename,
after which the index can be rebuilt. No pair of valid identifiers can alias a new index's
storage. Identifiable malformed, dangling, or foreign-object corruption is listed with a
reason but drop fails closed with manual-repair guidance. Every numeric
README/RESULTS claim is traceable to a checked artifact for the current implementation,
and the correctness CI lane is green in addition to the platform matrix.

Testing plan:
- Cover host refusal of table/column rename (including case-only), the distinct
  not-implemented schema-move/schema-rename errors, drop, drop-and-recreate,
  `CREATE OR REPLACE`, detach/attach, reopen, colliding legacy names, two indexed columns,
  orphan listing/drop, case-varied API spelling, and drop-by-ID/rename/rebuild behavior.
- Add golden tests for artifact schema and generated Markdown; deliberately mix commits,
  defaults, or old/new benchmark stages and require generation to fail.
- Run the new CI commands locally and pin their exact bounded seed/corpus budgets; retain a
  scheduled or release-only lane for longer sanitizer and scale checks.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Scope | Opaque registry makes indexes discoverable and removable independently of the base name | Commit `b6a388c` adds the ordinary `__ngram.registry` table, UUIDv4 index identity, opaque per-index schemas, catalog-qualified list/status APIs, and stable-ID drop. The lifecycle matrix proves distinct owner/index identities for colliding names and keeps dropped-base storage addressable as `ORPHAN`. |
| Complete | Scope | Rename, orphan, and legacy semantics are explicit and safe | The README state table and lifecycle suites pin host refusal of table/column rename (including case-only), distinct not-implemented schema moves, stable-ID drop/rename/rebuild, `ORPHAN` and `REPLACED` cleanup, and fail-closed `LEGACY_REBUILD`, `UNREGISTERED`, and malformed states. |
| Complete | Scope | Performance documentation is generated from versioned artifacts | `benchmarks/artifacts/enwik9-current-v1.json` is the strict raw evidence source for the uniquely owned README/RESULTS blocks. `benchmarks/release_evidence.py` validates provenance and every derived value, renders both blocks, and rejects stale, mixed, malformed, noncanonical, or hand-edited evidence. |
| Incomplete | Scope | CI has a dedicated correctness/sanitizer/race lane | `.github/workflows/MainDistributionPipeline.yml` invokes distribution and format/tidy workflows only. Missing: Linux DEBUG/ASAN/UBSan and focused differential/churn/race jobs. |
| Complete | Work | 14A: Introduce opaque index identity, registry, and lifecycle APIs | Commit `b6a388c` implements the registry schema and transactional bootstrap, opaque UUID-backed storage, catalog-qualified discovery/status/drop-by-ID, persistent reopen and detach/attach behavior, host rename refusal, and drop-only legacy/unregistered recovery. A public rebind API remains deliberately omitted because v1.5.5 exposes no reachable safe rename transition while the guard exists. |
| Complete | Work | 14B: Add benchmark artifact schema and documentation generator | The checked schema-v1 enwik9 artifact, self-contained standard-library collector/validator/generator, and generated README/RESULTS blocks bind engine, tool, build, binary, corpus, machine, protocol, raw samples, and exact-result parity. Independent final4 review reproduced all hashes, invariants, summaries, rendering, and live database checks with no findings. |
| Incomplete | Work | 14C: Add bounded correctness CI and release-evidence checks | Missing: workflow/jobs, runtime budgets, artifact-consistency check, deterministic race target, and fixed seeds. |
| Partial | Gate | Lifecycle is leak-free/collision-free and release claims are traceable | The Phase 14A lifecycle/collision matrix and Phase 14B provenance, parity, and generated-doc gates are complete. The dedicated bounded correctness/sanitizer/race CI lane remains Phase 14C work. |
| Complete | Test | Registry and lifecycle transition matrix | `test/sql/ngram_lifecycle.test`, the expanded create/maintenance suites, and the deterministic C++ harness cover host rename refusal, drop/recreate, detach/reopen, colliding and Unicode names, one and multiple indexed columns, orphan/replaced/legacy/unregistered states, and stable-ID drop/rename/rebuild. |
| Partial | Test | Artifact-generation and correctness-CI gates | Phase 14B includes a literal full-block golden, strict parser/provenance/cross-field negative cases, normalization and real-CLI smoke tests, exact bidirectional result parity, canonical rendering checks, and an independently audited final4 run. The DEBUG/ASAN/UBSan, deterministic race, and bounded differential/churn CI evidence remains Phase 14C. |

## Phase 15: Benchmark Against ClickHouse on the Same Machine

Goal:
Produce a reproducible, apples-as-practical comparison against ClickHouse so users can
see which system wins on load time, index cost, storage, and selective substring queries
on this machine, without hiding semantic or architectural differences.

Scope:
- Use the existing 1 GB `enwik9` line-per-row corpus and one frozen query manifest shared
  by both systems. Freeze equivalent query templates, bytewise case-sensitive semantics,
  escaping, NULL handling, and a checksum over normalized `(row_id, text)` rows before
  loading either engine. Needles shorter than 3 bytes are scan controls, not indexed wins.
- Configure DuckDB for bytewise case-sensitive 3-grams and pin one exact ClickHouse
  `ngrambf_v1` expression (n-gram size, Bloom-filter bytes, hash count, and GRANULARITY)
  for the whole campaign. Freeze the parameter-selection rule before any timed run. No
  per-query settings or index tuning are allowed; record the unavoidable difference
  between data skipping and DuckDB's exact postings.
- Pin DuckDB, extension, and ClickHouse versions; CPU count/governor, RAM limit, storage
  device, filesystem, compression, threads, row/block granularity, cache state, and every
  non-default setting. Run both engines under the same resource envelope and on the same
  local storage class.
- Define executable timing boundaries: fresh process and fresh database/table per measured
  load, explicit cgroup/resource envelope covering the ClickHouse server, client, and all
  measured background workers, durability/checkpoint/merge completion, cold cache
  preparation, warm-up, and measured query loop. Isolate ClickHouse data and system-log
  state per repetition or freeze an explicit exclusion rule. Measure base-table ingest,
  index construction/materialization, end-to-end indexed ingest, peak RSS, CPU time, and
  spill or temporary bytes. CPU/RSS/spill are diagnostics; no profiler framework is required.
- Measure storage from paired fresh states (base-only and indexed) after DuckDB checkpoint
  and ClickHouse merge/materialization. Record logical bytes, allocated filesystem bytes,
  free blocks, inactive-part cleanup, incremental index bytes, and final totals so neither
  engine receives credit for deferred or merely unlinked work.
- Measure exact substring counts for short, rare, medium, and common needles at several
  selectivities. For each engine/query mode collect at least 5 independently prepared cold
  observations and report median plus full range (not p95); collect at least 20 warm
  observations and report p50/p95. Record rows/bytes read, CPU time, and result parity
  with each engine's unindexed scan. Include a no-index scan baseline for both engines.
- Check in the corpus/query manifest, runnable commands, raw machine-readable samples,
  and a generated comparison table. Every cell applies one frozen winner rule: winner only
  when uncertainty ranges do not overlap and the ratio clears the declared practical
  threshold; otherwise tie or inconclusive. CV above 10% requires more repetitions or an
  inconclusive result. Include the end-to-end indexed-ingest p50 ratio explicitly.

Out of scope:
- Distributed ClickHouse, cloud services, replication, concurrency saturation, or a
  broad tuning competition.
- Claiming the two index structures are identical: ClickHouse's Bloom/data-skipping
  behavior and DuckDB's posting-list candidates must be described explicitly.
- Repeating the existing 100 GB scale campaign unless the 1 GB result is too noisy or
  reveals a crossover that needs one bounded 10 GB confirmation.
- Adding a profiler/telemetry subsystem; process metrics and engine-reported counters are
  enough for this bounded comparison.

Completion gate:
At least 5 independently prepared cold observations and 20 warm observations per query
cell produce exact matching result counts and complete raw provenance. Base ingest, index
build, and end-to-end indexed ingest each use at least 5 paired fresh-state repetitions
and report median plus full range, including the end-to-end indexed-ingest median (p50) ratio.
The generated summary applies the frozen winner/tie/inconclusive rule to
ingest, build, incremental storage, total storage, cold median/range, and warm p50/p95;
CV above 10% is resolved with more repetitions or labeled inconclusive. No headline number
may mix deferred ClickHouse work, cache state, process lifetime, or resource limits.

Testing plan:
- Validate the shared corpus manifest (row count, raw bytes, and normalized-row checksum),
  NULL/escaping/case-sensitive query templates, and query manifest before timing; compare
  every timed query count to an untimed full-scan oracle in both engines.
- Run randomized query order and alternating engine order within the frozen cold/warm
  process and cache protocol. Retain every sample; cold reports median/range from at least
  5 preparations, warm reports p50/p95 from at least 20 observations, and CV above 10%
  triggers more samples or an inconclusive label.
- Verify storage only after DuckDB checkpoint and ClickHouse merge/materialization have
  completed and inactive parts are cleaned; record engine logical bytes plus filesystem
  allocated bytes/free blocks for paired fresh base-only/indexed states.
- Capture EXPLAIN for every indexed query and require proof that the intended index is used.
  Re-run every query with index use disabled; sub-3-byte needles must remain scan controls.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Scope | One frozen corpus, query workload, semantics, and resource envelope | Missing: checked normalized-row/corpus/query manifests; exact case/escaping/NULL templates; pre-timing parameter-selection rule and fixed `ngrambf_v1` parameters; engine versions; and machine/cgroup/process/cache protocol covering server/client/background workers. |
| Incomplete | Scope | Ingest, build, memory, and storage costs are separated and comparable | Missing: scripts and ≥5 paired fresh-state median/range measurements separating base ingest, index build, end-to-end indexed ingest, deferred materialization/merge, isolated/excluded system logs, and logical/allocated storage after cleanup. |
| Incomplete | Scope | Query comparison covers selectivity and cache state with exact result parity | Missing: short scan controls, rare/medium/common needles, scan oracles, EXPLAIN proof for every indexed query, ≥5 cold median/range samples, and ≥20 warm p50/p95 samples. |
| Incomplete | Work | 15A: Build reproducible same-machine benchmark harness | Missing: ClickHouse provisioning, shared loader/query driver, resource controls, randomized run order, and provenance capture. |
| Incomplete | Work | 15B: Choose and document the closest stable ClickHouse n-gram configuration | Missing: one version-pinned `ngrambf_v1` expression with fixed n-gram/Bloom/hash/granularity parameters, no per-query tuning, and explicit comparability limits. |
| Incomplete | Work | 15C: Generate raw artifacts and decision-oriented comparison | Missing: versioned JSON/CSV samples, aggregation script, generated Markdown table, fixed practical-threshold and overlap rule, ratios, winner/tie/inconclusive labels, variability, and caveats. |
| Incomplete | Gate | Clear same-machine winner by metric with reproducible numbers | Missing: required cold/warm and ≥5 fresh-state load/build repetitions, exact count parity, complete provenance, end-to-end indexed-ingest median (p50) ratio, and generated ingest/storage/query decisions; noisy cells must expand or remain inconclusive. |
| Incomplete | Test | Corpus, oracle, plan-use, storage-attribution, and variance checks | Missing: normalized manifest checksums, semantics templates, bidirectional result checks, per-query EXPLAIN and disabled-index controls, paired post-materialization allocated/logical size checks, and enforced >10% CV handling. |
