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

Findings from reading duckdb main (checkout `e500d778`, Aug 2026) and ecosystem survey:

- **Correctness invariant**: any row containing the needle contains every trigram of the
  needle, so intersecting posting lists yields a superset of true matches; recheck with
  the original predicate filters to the exact answer. Dropping constraints (rarest-K
  trigram selection, hashing, skipped hot trigrams, index lag) only grows candidates.
- **Rewrite recognition is easy**: the optimizer normalizes `col LIKE '%x%'` to
  `contains(col, 'x')` (`src/optimizer/rule/like_optimizations.cpp`) *before* post-optimize
  extension hooks run, and pushes it into `LogicalGet.table_filters` as an
  `ExpressionFilter`, deleting the `LogicalFilter`. `ILIKE` (`~~*`) is NOT normalized and
  must be matched manually. `regexp_matches` with extractable literals also normalizes to
  `contains` where possible.
- **No planner hook for custom indexes**: `table_scan.cpp` index scans are hardcoded to
  ART (`TableScanInitGlobal`, gate at `src/function/table/table_scan.cpp:817`).
  `IndexType::create_plan` is CREATE-INDEX-build only. The blessed pattern is an
  `OptimizerExtension` (post-optimize) swapping `get.function`/`get.bind_data` for a
  custom table function — this is duckdb-vss's `hnsw_index_scan` shape. In-tree demo of
  rewrite + rowid `ExpressionFilter` + recheck: `test/extension/loadable_extension_demo.cpp:757`
  (`RowIdOptimizerExtension`).
- **Rowid fetch**: `DataTable::Fetch` is public, but the batched fetch driver must be
  reimplemented (~40 lines; template at `table_scan.cpp:109-271`), including the
  transaction-local-storage second phase (or uncommitted rows are silently dropped) and
  holding `DuckTransactionManager::SharedVacuumLock()` from probe through fetch
  (`table_scan.cpp:807-815`) so rowids cannot shift mid-query.
- **The fetched-storage phase does not re-apply filters** — recheck is the extension's
  responsibility, which the design requires anyway.
- **Shadow tables** persist/recover as ordinary tables; a database opened without the
  extension reads and writes normally (index schema is inert). This beats the custom-index
  route's `MissingExtensionException` on write.
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
| Incomplete | Work | 1C: Re-verify Key Research Facts against pinned duckdb v1.5.5 and re-pin the plan's file:line references (research used main `e500d77`, kept as submodule tag `research-e500d778`) | Missing: verification pass over hook ordering, `table_scan.cpp` gates, `KEEP_ROW_IDS` vacuum behavior. |
| Complete | Work | 1A: `trigrams()` scalar function with shared normalization module | src/trigram.cpp + src/trigrams_function.cpp; positional optional args (named args unsupported for scalar functions); test/sql/trigrams.test. |
| Partial | Work | 1B: Needle decomposition + short-needle detection | DecomposeNeedle + too_short in src/trigram.cpp, shares ExtractGrams with 1A; direct tests come with the Phase 2/3 SQL surface that exposes it. |
| Partial | Gate | Loads in CLI; extraction matches reference on unicode fixtures | CLI load + 551 passing assertions incl. generated fixture suite (scripts/gen_trigram_fixtures.py); remaining: CI green on GitHub. |
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
| Partial | Work | 2A: SQL-driven parallel build path | Two-pass build in src/index_pragmas.cpp (temp pairs table, then grouped encode): 196 MB corpus builds in ~7 s unmetered. Metered runs blocked on 2D. |
| Incomplete | Work | 2D: Streaming postings packer to replace grouped `list()` | Grouped `list()` OOMs instantly under ANY `memory_limit` at 190M-pair scale (duckdb 1.5.5; non-list aggregates spill fine — verified). Candidates: table in-out packer over sorted pairs; bitmap-span format; upstream fix. Required for the memory-limit gate. |
| Complete | Work | 2B: create/drop pragmas + `ngram_index_stats` | src/index_pragmas.cpp + test/sql/create_index.test. Stats delivered as `PRAGMA ngram_index_stats` (pragma named args use `=`, not `:=`). |
| Complete | Work | 2C: Posting segment codec (delta + varint) | src/postings_codec.cpp + randomized round-trip tests in test/sql/postings_codec.test. Entropy compression deferred to Phase 6 tuning. |
| Partial | Gate | 10 GB build under memory limit; reopen w/ and w/o extension | Extension-absent verified: stock duckdb 1.5.5 wheel read+wrote the indexed db, index intact on reopen with extension. In-suite restart round-trip in test/sql/index_persistence.test. Memory-limited large build blocked on 2D. |
| Complete | Test | Persistence + lifecycle sqllogictests | test/sql/create_index.test, index_persistence.test, postings_codec.test (649 assertions green). |
| Incomplete | Risk | Index size ratio unacceptable (>~60% of corpus) | Worst-case hex corpus (every trigram in ~every row): 205 MB blobs / 196 MB corpus ≈ 105%. Needs real-text corpus measurement; hex is maximally dense. |
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
| Incomplete | Scope | Exhaustive explicit search path | Missing: implementation. |
| Incomplete | Work | 3A: Rarest-first selection + intersection (`ngram_candidates`) | Missing: implementation + tests. |
| Incomplete | Work | 3B: Fetch driver + recheck + vacuum lock (`ngram_search`) | Missing: implementation + tests. |
| Incomplete | Work | 3C: HWM tail-scan union + transaction-local phase | Missing: implementation + visibility tests. |
| Incomplete | Gate | Differential identity vs brute force under concurrency | Missing: passing property-test run. |
| Incomplete | Test | Property-based differential suite | Missing: harness. |
| Incomplete | Risk | Rowid instability during vacuum if lock scope wrong | Missing: targeted race test. |

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
| Incomplete | Scope | Transparent acceleration with safe fallback | Missing: implementation. |
| Incomplete | Work | 4A: OptimizerExtension matcher + scan swap | Missing: implementation. |
| Incomplete | Work | 4B: ILIKE + multi-pattern LIKE handling | Missing: implementation + tests. |
| Incomplete | Work | 4C: Selectivity gate + settings + EXPLAIN rendering | Missing: implementation + plan tests. |
| Incomplete | Gate | Differential + plan-shape suites green with rewrite on | Missing: passing runs. |
| Incomplete | Test | Composite-predicate and join plan tests | Missing: test files. |
| Incomplete | Risk | Plan verifier rejects rewritten bindings | Missing: coverage across projection shapes. |

## Phase 5: Maintenance — Refresh, Compaction, Deletes

Goal:
The index tracks a changing table with bounded staleness cost and no correctness impact.

Scope:
- `PRAGMA ngram_refresh`: index rows between HWM and current max rowid into a new segment
  generation; advance HWM transactionally with the segment write.
- `PRAGMA ngram_compact`: LSM-style merge of segment generations; purge tombstoned rowids.
- Delete handling: stale candidates are eliminated by fetch/recheck; compaction reclaims.
  Update handling: updates to indexed columns produce new rowids (delete+insert) — detect
  and document; optional `maintenance := 'incremental'` trigger mode (evaluate DuckDB
  trigger maturity; duckdb-fts main uses this pattern).

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
