# Development Plan: DuckDB `ngram` Extension — Exhaustive Substring Search

## Overarching Goal

Build a C++ DuckDB extension providing a disk-persisted trigram index that accelerates
substring search (`LIKE '%needle%'` / `contains(col, 'needle')`) over large text columns,
returning **every** matching row (exact search, 100% recall). Postings live in ordinary
DuckDB shadow tables (the duckdb-fts model), so the index is durable, buffer-managed,
WAL-recovered, and inert-but-readable when the extension is not loaded. Query
acceleration is a transparent optimizer rewrite plus explicit table functions; every
result is rechecked against the real string, so index imprecision or staleness can only
cost performance, never correctness.

Performance target: hundreds of milliseconds per selective substring query over TB-scale
corpora. Interactive (sub-ms) latency is a non-goal, which is why the shadow-table
architecture is chosen over a native `BoundIndex` custom index type: DuckDB's custom-index
path (as of Aug 2026 main) pins index buffers permanently after first touch, writes the
whole index image to the WAL at `CREATE INDEX`, forces `KEEP_ROW_IDS` vacuum on the whole
table, and its WAL-replay path is lightly exercised (duckdb-vss still gates persistence
behind an experimental flag; see vss issue #81). Non-goals: similarity/fuzzy ranking,
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
- **Shadow tables** persist/recover as ordinary tables; a database opened without the
  extension reads and writes normally (verified against the stock duckdb 1.5.5 wheel).
  This beats the custom-index route, which on v1.5.5 still pins fixed-size buffers after
  first touch (`fixed_size_buffer.hpp:120-144` FIXME), writes the whole index image to
  the WAL at CREATE INDEX (`write_ahead_log.cpp:372-408`; the default
  `BoundIndex::SerializeToWAL` throws, `bound_index.cpp:177-179`), forces rowid-stable
  non-compacting vacuum for any non-ART index (`row_group_collection.cpp:28-39`,
  `:1359-1369`; main's `KEEP_ROW_IDS` naming does not exist on v1.5.5), and raises
  `MissingExtensionException` on write when the extension is absent (`index_binder.cpp:27`).
- **Ecosystem**: no trigram/ngram/substring index exists among ~290 community extensions;
  duckdb/duckdb discussion #16071 (trigram tokenizer) is open, unanswered. duckdb-fts main
  branch is adding a token-level trigram sidecar (not general `LIKE '%x%'` over raw
  strings) — monitor for scope collision.

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
PRAGMA ngram_refresh('logs');   PRAGMA ngram_compact('logs');
SELECT * FROM ngram_index_stats('logs');

SET ngram_auto_accelerate = true;
SET ngram_max_candidate_fraction = 0.05;
SET ngram_max_grams_per_query = 8;   -- probe only the K rarest trigrams of the needle
```

## Implementation Principles

- Recall is structurally protected: candidate generation may only ever over-approximate;
  every optimization must be provably superset-preserving; recheck always runs.
- The index is an optimization, never a correctness dependency: rows past the refresh
  high-water mark are covered by a brute-force tail scan unioned into results;
  transaction-local rows are scanned separately.
- Build and maintenance run through DuckDB's own SQL engine (parallel, out-of-core,
  compressed) rather than custom pipelines; the extension orchestrates queries.
- Normalization (case folding, unicode) is defined in exactly one place and shared by
  index build and needle decomposition — a mismatch is the one bug class that drops rows.
- The public surface (pragmas, functions, settings) hides the storage backend so a future
  native custom-index backend can be swapped in without breaking users.
- Pin the DuckDB version per release; the C++ extension ABI couples to internals
  (`OptimizerExtension`, `DataTable::Fetch`, `ExpressionFilter`).

## Testing Strategy

- Differential testing as the backbone: every accelerated query path is compared against
  brute-force `LIKE`/`contains` on randomized corpora (unicode, NULLs, empty strings,
  needles spanning gram boundaries) — results must be identical, always.
- sqllogictest suites for API surface, EXPLAIN plan shape, fallback behavior, and
  persistence across close/reopen (with and without the extension loaded).
- Concurrency tests: index probes racing inserts/deletes/updates and checkpoints;
  uncommitted-row visibility inside a writing transaction.
- Scale benchmark tracked per release: index build time, index size ratio, and query
  latency percentiles on a ≥100 GB corpus (TB-scale spot checks before release).

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
close/reopen round-trips the index; reopening WITHOUT the extension leaves the base table
fully usable and the index schema inert.

Testing plan:
- sqllogictests: create/drop lifecycle, duplicate create, stats output, options round-trip.
- Persistence tests incl. extension-absent reopen; build under `SET memory_limit` pressure.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Scope | Shadow schema (segments, metadata, stats) | Schema `ngram_<schema>_<table>` with `meta_/segments_/stats_<column>` tables; blob format documented in src/include/ngram/postings_codec.hpp. |
| Complete | Work | 2A: SQL-driven parallel build path | Two-pass build in src/index_pragmas.cpp: temp pairs table, external sort, streaming packer. 196 MB worst-case corpus builds in ~84 s under `memory_limit='1GB'`; differential identity vs brute force: 0 mismatches across 190M postings. |
| Complete | Work | 2D: Streaming postings packer (`ngram_pack_postings` table in-out fn) replacing grouped `list()` | Grouped `list()` OOMs under ANY memory_limit at scale (even states ≪ limit — verified up to 4GB); packer holds one run per thread, produced identical totals at 500MB. Memory floor is the sort's ~12-15 MB/thread. src/pack_postings.cpp + test/sql/pack_postings.test + build_scale.test. |
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
| Complete | Scope | Exhaustive explicit search path | src/ngram_search.cpp: `ngram_search(table, needle[, col := ...])` + `ngram_candidates(table, column, needle)` table functions, `ngram_max_grams_per_query` setting (default 8). Named parameter is `col` because `column` is a reserved keyword. |
| Complete | Work | 3A: Rarest-first selection + intersection (`ngram_candidates`) | Stats scan supplies posting counts; K rarest probed via per-gram segment scans with an equality TableFilter (zone-map pruned, postings blobs of non-matching rows never read); sorted linear-merge intersection with live-segment and min/max-overlap pruning; a needle gram absent from the index short-circuits to empty. Short needle → "all indexed rowids" fallback. test/sql/ngram_search.test (superset + exact-content assertions). |
| Complete | Work | 3B: Fetch driver + recheck + vacuum lock (`ngram_search`) | Batched `DataTable::Fetch` in STANDARD_VECTOR_SIZE slices (driver modeled on table_scan.cpp:102-265 incl. the TASK_EXECUTOR HAVE_MORE_OUTPUT caveat); recheck folds through the shared `NormalizeString` (CI index) or raw bytes (CS index). `SharedVacuumLock` held unconditionally from before meta/segments reads until end of query; on v1.5.5 every rowid-moving checkpoint takes the exclusive vacuum lock or degrades to a concurrent (non-moving) checkpoint (storage_manager.cpp:672-683), so mid-query row motion is impossible by construction. |
| Complete | Work | 3C: HWM tail-scan union + transaction-local phase | One storage scan with a `rowid > hwm` ConstantFilter (rowid zone maps prune indexed row groups, RowGroup::CheckRowIdFilter) covers both the committed tail and transaction-local rows (local rowids >= MAX_ROW_ID always pass); shadow-table reads run through the caller's transaction, so an index created inside a still-open transaction is searchable there. test/sql/ngram_search_visibility.test. |
| Complete | Gate | Differential identity vs brute force under concurrency | 5 sqllogictest files, 525 new assertions (`make test`: 1456 across 13 files, green). scripts/differential_search.py property harness (randomized alphabet/gram/case/rows corpora; substring, splice-trap, case-flipped, absent, short, and empty needles; committed → tail → in-transaction → deleted → post-vacuum-subset phases; deterministic per seed, PYTHONHASHSEED-independent): recorded runs seed 1026533771 = 6458 checks, seed 555000 (3 trials) = 2370, seed 7 = 1520 — 0 failures, counts reproduced across independent runs. Concurrency: test/sql/ngram_search_concurrent.test, 6 threads × 4 rounds of INSERT/UPDATE/DELETE/CHECKPOINT racing snapshot-atomic differential queries plus searches inside uncommitted writing transactions (269 assertions). |
| Complete | Test | Property-based differential suite | scripts/differential_search.py (bounded, seed-reporting, exit-code gated) + CI-bounded test/sql/ngram_search_differential.test and ngram_search_scale.test (multi-segment corpus across the 2^20 boundary). |
| Complete | Risk | Rowid instability during vacuum if lock scope wrong | During a query: excluded by the shared vacuum lock (see 3B). Between queries: real and pinned by test — see the staleness row below. Race coverage: concurrent CHECKPOINTs against searches with deletes in flight (ngram_search_concurrent.test) and a deterministic post-vacuum check (ngram_search_scale.test). |
| Complete | Doc | Phase 3 staleness contract: misses-only, false positives never | Recheck + visibility make false positives impossible in every scenario (post-vacuum subset phase of the harness + scale test assert this against a genuinely compacted table: 916k of 917k rowids moved, results stayed ⊆ truth). Three misses-only gaps remain until Phase 5, documented on SearchBind and pinned in tests: (a) in-place UPDATE introducing the needle into a row <= hwm (v1.5.5 updates in place unless an ART index forces delete+insert, table_catalog_entry.cpp:327-336); (b) DELETE of indexed rows followed by checkpoint vacuum, which merges row groups, moves surviving rowids (invalidating postings) and shrinks the table end so even later INSERTs can land below the stale hwm; (c) DROP TABLE + re-CREATE under the same name binds the dead index (see the Phase 5 recreation-detection bullet). |
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
| Complete | Scope | Transparent acceleration with safe fallback | src/ngram_rewrite.cpp (+ ngram/ngram_rewrite.hpp, shared Phase 3 core exposed via ngram/search_core.hpp): post-optimize `OptimizerExtension` swaps qualifying `LogicalGet(seq_scan)` nodes for `NGRAM_INDEX_SCAN`. `ngram_auto_accelerate` defaults to **false** (opt-in): the transparent path inherits the Phase 3 misses-only staleness gaps (pinned in test/sql/ngram_rewrite.test with a live in-place-UPDATE miss), and silently rewriting standard `LIKE` into a path that can miss rows is not an acceptable default until Phase 5 maintenance; revisit then. |
| Complete | Work | 4A: OptimizerExtension matcher + scan swap | Matches `contains`/`~~`/`~~*` `ExpressionFilter`s inside `get.table_filters` (incl. inside ConjunctionAndFilter); swaps only `get.function`/`get.bind_data`, leaving table_index, returned types/names, column_ids, projection_ids, and the filters untouched, so column bindings survive verification unchanged. Execution reuses the Phase 3 pipeline (probe → batched `DataTable::Fetch` → recheck → `rowid > hwm` tail scan incl. transaction-local rows, `SharedVacuumLock` held probe-to-end). Recheck evaluates the pushed filters themselves via `TableFilter::ToExpression` (optional filters skipped per their contract), so query semantics never depend on index normalization and composite filters/dynamic join filters are enforced exactly; tail/fallback storage scans apply the same filters natively. Non-qualifying shapes decline the swap: `_`/escape patterns, prefix/suffix/anchored rewrites (those keep `~~` in a residual FILTER), OR-of-predicates, expressions over the column, generated-column tables, TABLESAMPLE, virtual/struct-child columns. RowGroupPruner ordering hints are dropped at swap (ordering-only when filters exist). |
| Complete | Work | 4B: ILIKE + multi-pattern LIKE handling | Case matrix enforced at both plan and init time: contains/LIKE probe either index flavor (CI folding is superset-preserving; recheck applies the CS predicate — pinned: 'TENT SHOUTING' not matched by `LIKE '%tent%'` over a CI index); ILIKE probes CI indexes only, never CS (needles carry `requires_ci`). Multi-pattern `LIKE '%a%b%'` decomposes into literal segments; one intersection over the union of all needles' grams equals the per-segment intersection; the original pattern is rechecked (segment order pinned: '%tail%tent%' vs '%tent%tail%'). Conservative bail on `_` and escape. |
| Complete | Work | 4C: Selectivity gate + settings + EXPLAIN rendering | `ngram_max_candidate_fraction` (double, default **0.05**; Phase 3 anchor: warm-cache crossover ≈1.1% of rows on the 16-thread 10 GB box, cold-cache far higher — 0.05 splits the difference toward not regressing cold scans; tune in Phase 6). Gate runs in init_global after the probe: candidates > fraction × `GetTotalRows()` → full storage scan inside the same table function (filters applied natively, still exhaustive, no re-planning). EXPLAIN renders `NGRAM_INDEX_SCAN` + needles via `to_string`; EXPLAIN ANALYZE renders the runtime decision via `dynamic_to_string` ("index (N candidates)" / "full scan fallback: <reason>"). Kill switches tested: `ngram_auto_accelerate=false`, `SET disabled_optimizers='extension'`. |
| Complete | Gate | Differential + plan-shape suites green with rewrite on | scripts/differential_search.py `--transparent` mode: plain LIKE/contains/ILIKE (incl. synthesized `%`-multi-segment and `_` patterns) with `ngram_auto_accelerate=true` vs `disabled_optimizers='extension'` execution, phases committed/tail/in-txn/deleted + post-vacuum subset, plus a per-trial EXPLAIN assertion that the rewrite fires (no vacuous green). Recorded release runs: seed 902147 (8 trials) = 3496 checks / 0 failures; explicit-path seed 555000 (8 trials) = 6150 checks / 0 failures (explicit functions untouched). `make test` release: 1695 assertions / 15 files green. |
| Complete | Test | Composite-predicate and join plan tests | test/sql/ngram_rewrite_plan.test (88 assertions): EXPLAIN per accelerated shape (LIKE, contains, ILIKE, multi-pattern, regexp-literal, CS-index LIKE, composite AND, double-needle, projection-exclusion, rowid, count(*), join, both ORDER BY...LIMIT shapes incl. late materialization) and per fallback (short needle, `_`, ESCAPE, NOT LIKE, prefix/suffix, anchored, no index, wrong column, ILIKE-on-CS, OR, lower(col), '%%', TABLESAMPLE, both kill switches) + EXPLAIN ANALYZE mode strings. test/sql/ngram_rewrite.test (151 assertions, under `PRAGMA enable_verification` — every query also differentially checked against the unoptimized plan): correctness for all shapes, txn-local visibility, in-txn index build, DML (UPDATE/DELETE driven by accelerated scans), prepared statements across index drop/rebuild and setting flips, staleness pin. |
| Complete | Risk | Plan verifier rejects rewritten bindings | DEBUG build (`GEN=ninja make debug`) runs per-pass `ColumnBindingResolver::Verify` + AddressSanitizer: full sqllogictest suite (15 files, 1695 assertions) and a `--transparent` differential run (seed 31337, 4 trials, 1740 checks / 0 failures) green against the DEBUG build. The DEBUG runs earned their keep, catching two release-silent bugs: (1) ASAN stack-use-after-scope — a temporary string passed to `EntryLookupInfo`, which stores a reference (fixed in TryRewriteGet); (2) a D_ASSERT in `RowGroupCollection::InitializeScan` when scanning a table with no committed row groups, i.e. shadow tables created inside the current transaction — latent since Phase 3, fixed for both query paths via the shared `InitializeExhaustiveScan` helper (initializes only the transaction-local phase for committed-empty tables). `verify_serialization=false` on the injected function (never serialized; DEBUG serialization verification skips it). |

## Phase 5: Maintenance — Refresh, Compaction, Deletes

Goal:
The index tracks a changing table with bounded staleness cost and no correctness impact.

Scope:
- `PRAGMA ngram_refresh`: index rows between HWM and current max rowid into a new segment
  generation; advance HWM transactionally with the segment write.
- `PRAGMA ngram_compact`: LSM-style merge of segment generations; purge tombstoned rowids.
- Delete handling: stale candidates are eliminated by fetch/recheck until a checkpoint
  vacuums the deletes; vacuum merges row groups and MOVES surviving rowids
  (`row_group_collection.cpp` VacuumState: any table without native indexes has
  `can_change_row_ids = true`), stranding postings and leaving the recorded HWM above the
  table's new end, so refresh must detect row motion (or rebuild) — recheck keeps this
  misses-only, never false positives. Update handling: v1.5.5 updates rows IN PLACE
  (same rowid) unless the updated column is covered by an ART index
  (`table_catalog_entry.cpp:327-336`), so an update that introduces a match into an
  indexed row is invisible until refresh — refresh must re-index updated rows, not just
  the HWM tail; optional `maintenance := 'incremental'` trigger mode (evaluate DuckDB
  trigger maturity; duckdb-fts main uses this pattern).
- Recreation detection: DROP TABLE leaves the shadow schema behind, and the name-based
  ownership guard matches a recreated table of the same name, so queries silently use the
  dead index (misses-only; pinned in test/sql/ngram_search_visibility.test). v1.5.5 has no
  stable cross-restart table identity token (catalog OIDs are per-process) and no DDL
  hooks. Store a schema fingerprint (column names + types) in meta to catch recreations
  that change shape; a same-shape recreation stays a documented gap unless a stronger
  token turns up.

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
| Incomplete | Scope | Refresh + compaction lifecycle | Missing: implementation. |
| Incomplete | Work | 5A: HWM refresh with transactional segment publish | Missing: implementation + crash test. |
| Incomplete | Work | 5B: Segment compaction + tombstone purge | Missing: implementation + tests. |
| Incomplete | Work | 5C: Update/delete semantics + optional trigger mode decision | Missing: trigger-maturity evaluation + doc. |
| Incomplete | Gate | Churn test green; refresh cost ∝ tail | Missing: harness + timing evidence. |
| Incomplete | Test | Crash-interruption recovery tests | Missing: test files. |
| Incomplete | Decision | Ship trigger-based incremental mode in v1? | Missing: evaluation vs duckdb-fts trigger approach. |

## Phase 6: Scale Validation, Hardening, Release

Goal:
Meet the performance target at scale and ship as an installable community extension.

Scope:
- Benchmark suite: build time, index size ratio, p50/p95 query latency vs corpus size
  (100 GB sustained, TB spot check); memory behavior under `SET memory_limit`.
- Tuning defaults from measurement (`ngram_max_grams_per_query`,
  `ngram_max_candidate_fraction`, segment size, compression choice).
- Index size reduction, targets from the Phase 2 ratio measurements (real text ≈
  0.87–1.01× corpus on disk): postings encoding beyond LEB128 deltas (bit-packing /
  roaring-style for dense grams), segment granularity, per-run overhead amortization.
- Community-extensions packaging, versioned against a DuckDB release; docs with the
  staleness/maintenance contract and fallback semantics; monitor duckdb-fts trigram
  sidecar for overlap before submission.

Out of scope:
- Native custom-index backend (future work if core fixes buffer pinning / hardens WAL
  replay — revisit `fixed_size_buffer.hpp` FIXME and the `KEEP_ROW_IDS` vacuum gate).

Completion gate:
Selective substring query ≤ hundreds of ms p95 on the TB spot-check corpus with cold-ish
cache; extension installs via `INSTALL ngram FROM community` in a stock DuckDB build.

Testing plan:
- Reproducible benchmark scripts + recorded results per release.
- Full test matrix (Phases 1–5 suites) against the pinned DuckDB release build.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Scope | Scale benchmarks + tuned defaults | Missing: benchmark suite + results. |
| Incomplete | Work | 6A: 100 GB / TB benchmark harness + corpus | Missing: scripts + corpus source. |
| Incomplete | Work | 6B: Default tuning from measurements | Missing: recorded sweeps. |
| Incomplete | Work | 6C: Community-extension packaging + docs | Missing: PR to community-extensions. |
| Incomplete | Gate | p95 target met at TB spot check; community install works | Missing: benchmark evidence + install run. |
| Incomplete | Risk | duckdb-fts trigram sidecar overlaps scope | Missing: recheck of duckdb-fts state before submission. |
| Incomplete | Doc | User docs: maintenance contract, fallbacks, settings | Missing: docs. |

## Phase 7: Restore Full Platform CI Matrix

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
| Incomplete | Work | 7A: Re-enable macOS archs in CI | Missing: workflow change + green run. |
| Incomplete | Work | 7B: Re-enable Windows + Wasm archs | Missing: workflow change + green run. |
| Incomplete | Gate | Full matrix green incl. macOS | Missing: CI evidence. |
