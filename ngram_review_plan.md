# Development Plan: ngram Review Follow-up (Phases 16 to 23)

This is the active continuation of `ngram_index_plan.md`. Independently audited on
2026-09-04 against HEAD `d3ecccf5a9f95032414ddc4b2360ab3065efb50b` **plus the dirty
Phase 19 implementation**. This audit updates the plan; it does not implement the fixes.
Phase and item identifiers are stable. Phases 16–18 retain historical completion evidence;
that evidence does not certify the current source or current CI.

## Overarching Goal

Release an exhaustive, durable substring index whose cost and failure behavior are
predictable. Preserve exact results as a **multiset** under supported DML, maintenance,
transactions, checkpoints, reopen, and index replacement. An unavailable optimization must
fall back to a correct scan; recognized corruption may fail the statement cleanly. A query
must not invalidate the database because of an extension scheduling or validation error.

Keep the useful architecture: ordinary DuckDB tables own postings, DuckDB owns persistence
and visibility, and a small native guard protects the rowid mapping. Prefer a narrow adapter
for the pinned host's storage operations and one exact predicate/execution path. Do not
replace this with a native postings engine without evidence that smaller changes cannot meet
the goals. The codec and whole storage design remain revisable; source line count is a
maintenance signal, not a correctness or release gate.

The guarantee assumes extension-owned registry and postings objects have not been manually
edited. Recheck prevents false positives; it cannot recover a missing posting after arbitrary
internal-table deletion. Keep this boundary explicit rather than promising recovery from all
possible corruption. No migration reader for unreleased formats is required, but advertised
safe drop/rebuild must actually remove the known owned objects.

## Implementation Principles

- Separate semantic correctness, resource admission, and profitability. Keep an independent
  bound on cumulative decode work as well as peak memory and candidate fetch cost.
- Admit extension allocations before allocating them, including temporary capacity, retained
  decode buffers, query normalization, and queued candidates. Distinguish this accounted
  memory from DuckDB-managed storage, result buffers, and total process RSS.
- Use the caller's transaction and row visibility for every read. Preserve the guard,
  high-water mark, transaction-local tail, and maintenance fence invariants across refactors.
- A rowid predicate is not proof of physical pruning on the pinned host. Measure visited
  storage and validate the actual scan API, including delete visibility and local rows.
- Reuse native predicates and DuckDB scheduling where possible. Avoid a second scheduler or
  cache whose ordering, cancellation, invalidation, and lifetime need another proof.
- Query semantics should not depend on which optional index exists. Keep semantic options
  explicit; use an index only when its normalization can produce a safe candidate superset.
- Architecture and APIs are open to revision in this audit. New recommendations supersede
  earlier recommendations where stated; prior approval of an idea is not implementation proof.
- Record commands, source identity, settings, corpus, thread count, and artifacts for measured
  claims. Synthetic experiments diagnose mechanisms; they do not replace release benchmarks.

## Testing Strategy

- Compare both directions with `EXCEPT ALL`, with an unaccelerated oracle. Unique input IDs
  do **not** make `EXCEPT` sensitive to duplicate output. Include duplicate base rows too.
- Assert the executed mode using `EXPLAIN ANALYZE`; an injected `NGRAM_INDEX_SCAN` can still
  fall back. Tests of probe scheduling must actually admit the probe.
- Exercise ordered collectors, `CREATE TABLE AS`, `INSERT SELECT`, and streaming results in
  addition to counts and set operations. Test unequal work in multiple posting segments at
  threads 1, 2, 8, and 24; preserve DuckDB's batch-index contract even without SQL `ORDER BY`.
- Turn every reproduced defect into a small regression; make race schedules deterministic in
  the C++ harness. Use counters/barriers for invariant tests and timing only for benchmarks.
- Check extension allocation high-water marks and interruption during normalization, manifest
  collection, decode, waiting, and fetch. Test a query alone and concurrent admitted queries.
- Run `make test_release`, relevant sanitizer tests, and fixed-seed differential/churn tests
  for changed mechanisms. A phase is complete only with its gates and a green Correctness run
  on its committed source. Distribution and full release evidence gate the release candidate.
- Keep crash/reopen coverage operational with every format change, rather than deferring it
  to a later test-infrastructure phase. Run broader nightly tests after focused regressions.

Audit evidence is reproducible with:

```sh
python3 docs/review/2026-09-04/reproduce.py --binary build/release/duckdb
# One mechanism, for example:
python3 docs/review/2026-09-04/reproduce.py --case batch_order
```

[Recorded observations](docs/review/2026-09-04/observations.json) include binary and source
SHA-256 identities. The audited `src/` digest is
`447847999fae6ca29f4ae47e9081b0d50f366a508ec7fe73f4c192807a714af1`.
The script uses disposable databases. Its format-4 and registry-scale cases are explicitly
synthetic. It reports defects rather than asserting that the current behavior is correct.

Recommended order: repair 19D and resource admission (19E/19H), then 19I/19F and the format
closure in 19J. Implement 22A–22C regressions alongside those changes. Rebaseline performance
before Phase 20; settle semantics in 21H before API renames; finish release infrastructure and
Phase 23 last. Do not enable acceleration by default while these gates are open.

## Phase 16: Fix the Refresh Defect and Restore CI Truth

Goal:
Maintenance and guard failures do not poison future queries.

Scope:
- Preserve the empty-insert fix, rejected-commit guard behavior, read-path fallbacks, and CI
  wiring. Historical findings F1–F5 were addressed here.

Completion gate:
Historical gate satisfied on the commits below; current behavior is rechecked by later phases.

Testing plan:
Keep no-op refresh, rejected commit, replacement, checkpoint, and fixed-seed regressions.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 16A: Remove the empty-insert-then-delete shape | `ac55e30`; `test/sql/ngram_refresh_noop.test`, [host reproduction](docs/upstream/duckdb-empty-batch-insert.md). The format-5 adaptation is checked again in 19J. Filing the upstream issue was not part of this completed code item. |
| Complete | Work | 16B: Latch only on possible reuse | `ac55e30`; `RowIdGuard::Observe`, `TestRejectedCommitKeepsGuard`. File checkpoint iteration distinguishes rejected appends from possible trailing-rowid reuse; unknown/in-memory histories remain conservative. |
| Complete | Decision | 16B-2: Reuse above the high-water mark | Historically deferred on 2026-09-01. No relaxation is justified by this audit. |
| Complete | Work | 16C: Read-path fallbacks and small fixes | `ac55e30`; replacement cases `swapped_part`/`registry_swap`, unbound replay read under `entry.lock`, extension-owned internal errors removed. This does not cover the new host internal error in 19D. |
| Complete | Work | 16D: CI completes on phase head | `20b864a`; pinned host/configuration and cache/fixture/pin checks. [Recorded Correctness run](https://github.com/danthegoodman1/duckdb-ngram/actions/runs/33584571329). Historical run, not reverified as current CI. |
| Complete | Gate | In-process maintenance and historical CI | Prior ledger records reviewer approval and [distribution matrix](https://github.com/danthegoodman1/duckdb-ngram/actions/runs/33584571470) on `20b864a`. |
| Complete | Test | Regression and workflow evidence | Historical `make test_release`: 2993 assertions/25 cases; explicit/transparent/churn/strict-churn seeds: 1764/886/320/320 checks, zero failures. |
| Incomplete | Doc | 16A-2: Upstream report | The reproduction note exists; no submitted DuckDB issue URL is recorded. Optional follow-up, not a local correctness gate. |

## Phase 17: One Identity Anchor, One Storage Layout (Format 4)

Goal:
Keep one guard identity per table and one registry row per index, with understandable lifecycle
states. This phase describes the completed format-4 restructuring, not the current format.

Scope:
- Preserve the removal of redundant fingerprints and legacy readers, shared guards, and safe
  ownership checks. Historical findings F10/F11 belong here; format-5 cleanup belongs to 19J.

Completion gate:
The historical restructuring is complete; later changes must preserve ownership and visibility.

Testing plan:
Keep shared-guard, replacement, concurrent drop, read-only old-format, and drop-by-id tests.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 17A: Delete legacy v2 | `4243564`, `a4e64ba`, `ef4d88c`; old read branches, naming helpers, and extra statuses deleted. |
| Complete | Work | 17B: Guard as sole identity | Same commits; `RowIdGuardReason` validates name/token, physical column, host compatibility, observed mark, and reuse state. The fingerprint was deleted. |
| Complete | Work | 17C: Registry row is metadata | Same commits; twelve-column `__ngram.registry`, UUID storage names, four lifecycle statuses. Format 5 subsequently removes the stats table. |
| Complete | Work | 17D: One guard per table | `TestSharedGuardAndDrop`, `TestConcurrentDropsStrandGuard`; first-index create cleans unreferenced guards after the ownership/fence checks. |
| Complete | Decision | 17D-2: Guard coverage at first creation | Historical choice: cover every existing VARCHAR. Retain for now; updates to otherwise unindexed covered columns also become delete+insert, and later-added VARCHARs need a rebuild. |
| Complete | Work | 17E: Format 4 and drop-only path | `test/fixtures/format3.duckdb`, `TestFormat3Fixture`; historical format-3 cleanup works. This proves nothing about the format-4-to-5 cleanup newly broken in 19J. |
| Complete | Doc | 17F: Vacuum and identity documentation | `4243564`; README and `docs/stale-updates.md`. Physical rowid-filter pruning claims still need correction under 19I/23A. |
| Complete | Gate | Four states and one identity anchor | Historical net reduction 1136 production lines; [Correctness](https://github.com/danthegoodman1/duckdb-ngram/actions/runs/33596217147), [matrix](https://github.com/danthegoodman1/duckdb-ngram/actions/runs/33596215795) on `ef4d88c`. |
| Complete | Test | Lifecycle and shared-guard evidence | Historical 3046 assertions/25 cases, sanitizer harness, four fixed seeds with zero failures. |

## Phase 18: Code Structure

Goal:
Keep ownership and invariant boundaries legible without treating function length as a design
objective. Historical finding F12 was addressed by this restructuring.

Scope:
- Preserve centralized validation, connection-owned maintenance handles, and module boundaries.
- Carry correctness proofs through future simplification; avoid line-count-only refactors.

Completion gate:
Historical refactor complete; new scheduling and storage access require their own proof.

Testing plan:
Keep pragma expansion, multi-pragma handle, guard binding, shutdown seal, and suite parity tests.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 18A: Single validation entry point | `be7f6ce`, `aacfff7`; `ValidateIndex`/`LocateIndex` return `IndexVerdict`. Read and mutation policies remain distinct. |
| Complete | Work | 18B: Handle-based execution-time scalars | Same commits; `MaintenanceState`, `PreparedMaintenance`, grouped one-use handles; multi-pragma regression in `ngram_maintenance.test`. Preserve execution-time validation, rather than blindly carrying mutable bind-time metadata. |
| Complete | Work | 18C: Function extraction and overflow handling | Same commits; named helpers and one arithmetic-overflow error. Five reachable decline reasons retained. No new requirement to hit a line-count quota. |
| Complete | Work | 18D: Module layout | Fifteen sources and eleven headers; build, maintenance, identity, and search concerns separated. Prefer a small storage adapter for 19I, not another general abstraction layer. |
| Complete | Decision | 18E: Lazy guard binding | Rejected with host-source analysis: shutdown/DETACH can checkpoint an untouched unbound guard with an old seal. `TestCleanShutdownSeal`; keep callbacks until an alternative proves the same durability behavior. |
| Complete | Work | 18F: Naming and header hygiene | Same commits; `MAX_ROW_ID`, shared helpers, and no-op `VerifyBuffers` override. The host calls this override in DEBUG builds; deleting it is not simplification. |
| Complete | Gate | Behavior-neutral restructuring | Historical 3158 assertions including the new handle test; [Correctness](https://github.com/danthegoodman1/duckdb-ngram/actions/runs/33608194857), [matrix](https://github.com/danthegoodman1/duckdb-ngram/actions/runs/33608194556) on `aacfff7`. |
| Complete | Test | Suite parity and sanitizer evidence | Historical fixed seeds unchanged; focused sanitizers and harness green. Current local test results are recorded in Phase 19. |

## Phase 19: Correct and Bound the New Query Path, Then Cut Its Cost

Goal:
Finish the format-5 work without invalidating the database, exceeding the extension's resource
allowance, or making a small indexed query scale with unrelated data. Then establish latency
improvements on a comparable corpus and machine.

Scope:
- Keep fixed-width gram keys and segment-only storage as the working design; test actual
  collision widening. Compare per-key filtered scans and a combined scan using measured
  visited rows, fragmentation, gram size, and query length; do not mandate `InFilter` by name.
- Repair candidate scheduling. Start with ordered segment production and parallel batch fetch;
  retain parallel decoding only if a bounded ordered publication scheme pays for its complexity.
  Merely sorting the ready queue is insufficient when an earlier segment is still decoding.
- Account and limit query preparation, manifest growth, retained decode capacities, candidate
  publication, and recheck scratch. Poll for cancellation within long-running loops.
- Make candidate ranges and the committed tail physically bounded through a narrow host adapter,
  preserving transaction-local visibility. If no safe API is available, disable the dense-range
  optimization until one is proved; a private DataTable method is not a supported shortcut.
- Filter query shapes before registry discovery; resolve only relevant owners/IDs and preserve
  statement-snapshot validation. Avoid a process-global metadata cache as the first remedy.
- Retain cumulative decode-work admission, add actual visited-row/byte counters, and derive
  profitability from fixed planning cost, locality, projected bytes, compression, tail size,
  and native recheck cost. Avoid evaluating transparent scan filters twice on fallback/tail rows.
- Close every format-5 integration path: old-format cleanup, crash checks, docs, and fixtures.

Out of scope:
A native postings backend, a new codec, API renames, or claiming release performance from the
small diagnostic cases below.

Completion gate:
Both exact paths pass admitted multi-segment CTAS/INSERT/streaming tests at 1/2/8/24 threads,
including unequal decode workloads, cancellations, and local tails. No decreasing or duplicate
batch ownership and no database invalidation. Extension allocation peaks fit the declared
reservation including retained capacity. Bounded scans visit only intersecting row groups or
vectors plus a separately measured local tail. Unrelated numeric filters perform no registry
scan. Crash and real format-4 cleanup tests pass.

After those gates, target enwik9 warm p50 <=3 ms rare, <=12 ms moderate, and dense/fallback
within 10% of the native baseline; tiny-table p50 <0.3 ms. Treat these as targets pending a
fresh comparable baseline, not proof inherited from the prior agent. Record full query latency,
CPU, work counters, RSS, projection, compression, and threads; investigate regressions before
changing a target. A 51k-candidate single-segment case must benefit from parallel fetch.

Testing plan:
- Regressions from `docs/review/2026-09-04/reproduce.py`, converted to deterministic suite tests.
- Actual same-size gram collision injection at an internal test seam; key packing for ASCII,
  Unicode, NUL, and >16-byte grams, including two colliding grams in one row and across refresh.
- Long needles at 8/16/32/64/128 KiB, long haystacks, low budgets, many refresh generations,
  alternating segment densities, concurrent queries, and cancellation before the manifest.
- Fixed candidate count with a 10x larger unrelated prefix/suffix; empty and small tails;
  persistent FSST/uncompressed data; selectivity and projection sweeps.
- An unaccelerated oracle and explicit mode assertions; do not use a count aggregate as the
  only scheduler test. Add Phase 22 regressions as this phase is implemented.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| In Progress | Work | 19A: Integer gram key | `src/gram.cpp` implements byte-packed UHUGEINT/FNV fallback, `ngram_gram_keys`, and key-based build/probe; `gram_key.test` passes locally. Missing: actual same-gram-size collision coverage. Its current `ab` versus `ab`+NUL example compares different gram sizes; the gram-2 index only exercises ordinary shared `ab` postings. |
| In Progress | Work | 19B: Drop the stats table | Build/refresh/compact and `ngram_index_stats` use segments; `ReadGramStats` and the fold are gone. Missing: comparable format-4/5 query/build/refresh/storage measurements, fragmented-generation cases, and 19J closure. Reconsider a compact synopsis only if measurements justify both its query benefit and maintenance cost. |
| In Progress | Work | 19C: Parallel manifest collection | `CollectGramRows` uses one native `ConstantFilter` scan per key via `ParallelForEachUnit`; this differs from the original single-scan prescription. Missing: reserve descriptor growth before allocation, adapt worker count to budget, and compare scan strategies using physical work rather than returned rows alone. |
| In Progress | Work | 19D: Candidate-batch fetch parallelism — P1 correctness | **Reproduced:** `batch_order` at 24 threads fails in both explicit and transparent CTAS with `Pipeline batch index - gotten lower batch index`; a later ordinary SELECT reports database invalidation. One thread returns all 1,065,717 matches. `ClaimCandidateBatch` publishes decode completion order while assigning segment-derived indexes; host `PipelineExecutor::NextBatch` requires monotonicity per worker. Missing: bounded ordered publication or simpler ordered production, deterministic regression, sink/streaming parity, cancellation, and scaling evidence. Current batches are 2,048 rows, not the plan's former 4,096. |
| In Progress | Work | 19E: Decode reuse, order, and resource ownership — P1 invariant gap | `ProbeDecodeScratch` and per-segment smallest-first order exist. **Source-confirmed accounting gap:** `CollectGramRows` fills ordinary vectors before `Grow` reserves only selected descriptors; `per_key` retains unselected rows until planning returns. `SegmentWorkerBytes` counts logical current rows, but `scratch.postings`/`intersection` retain capacities across segments. For counts (300k,300k) then (1,1M), a 300k intersection capacity can coexist with a 1M posting buffer although the modeled row peak is 1M+2. Missing: allocation instrumentation, reserve-before-grow, capacity-aware ownership through queue publication and vector swaps, and concurrent low-memory tests. No process-OOM reproduction is claimed. |
| In Progress | Work | 19F: Bind/init metadata and cheap discovery — P2 performance | `OwnedIndexes` reduces duplicate reads, but `TryRewriteGet` reads the entire registry before checking for a probeable VARCHAR filter. **Measured:** unrelated numeric query with 10,000 registry rows (9,999 synthetic absent owners) costs p50 0.092 ms CPU with acceleration off versus 9.562 ms on. Missing: shape-first eligibility, owner/ID-targeted snapshot reads, no discovery on ineligible scans, and tiny-table/large-registry benchmarks. Do not delete execution-time revalidation to save a read. |
| In Progress | Work | 19G: Candidate/work cost model — P2 design | README/settings describe a 1% threshold, but those prose timings lack a reproducible Phase 19 release artifact. `AdmitSegments` bounds cumulative decoded rowids separately from `SegmentWorkerBytes`; candidate fraction and peak memory do not replace that bound. Missing: retain a work ceiling, account manifest/base visits, compare locality and projected bytes, eliminate duplicate native rechecks where safe, publish counters for both exact APIs, and record a measured default. |
| Incomplete | Work | 19H: Bounded, interruptible needle preparation — P2 performance/resource gap | **Measured:** `long_needle` on one row, gram=6, 16MB memory: 16/32/64/128 KiB needles take about 0.062/0.239/0.965/3.872 s. `DecomposeNeedle`/`MergeKeys` linearly search the accumulated vector for every key, before admission; normalization/decomposition have no interruption checks. Rewrite decomposes needles repeatedly. Missing: bounded deduplication with stable order, admission while emitting keys, reuse of decompositions, periodic interruption checks, and bounded normalization scratch for recheck. |
| Incomplete | Work | 19I: Physical range and tail access — P2 performance | **Measured:** fixed 50k candidates at one thread cost p50 4.524 ms CPU on 1.1M rows versus 36.286 ms on 11M. Native `rowid BETWEEN 1000 AND 2999` profiles 11M visited rows; the equivalent stored `id` filter visits 122,880. `StartRangeScan` initializes an exhaustive scan per batch; rowid segment checks cannot jump because `RowIdColumnData` has no current segment. The comment claiming all other groups are pruned is false. Missing: safe bounded access, empty-tail coverage, physical-work counters, MVCC/delete/checkpoint tests, and correction of inherited pruning claims. These scans still beat native full contains in this fixture; the defect is repeated unrelated work, not a demonstrated overall slowdown versus full scan. |
| Incomplete | Work | 19J: Complete format-5 lifecycle and tooling — P2 correctness/validation | **Reproduced:** `crash_maintenance.py --rows 100 --tail 20 --kills 1 --seed 20261317` exits before any kill because `index_state` still queries `stats_<id>`. `format4_drop` shows the current generic drop leaves the known format-4 stats table and a permanent MALFORMED listing. The latter uses a synthetic cleanup shape, not a real persisted format-4 fixture. Missing: token/ownership-checked cleanup of known old objects, real format-4 and format-5 read-only/reopen fixtures, working crash/digest checks, and README storage-count fixes. |
| Complete | Test | Audit baseline, not phase acceptance | Audited tree: build targets report no pending work; `make test_release` passes the C++ harness and 3188 assertions in 26 cases. `differential_search.py --trials 2 --rows 5000 --seed 20261314`: 1764/0; `--transparent --seed 20261315`: 886/0. `release_evidence.py test --binary build/release/duckdb`, `check`, `scripts/verify_pins.sh`, and `git diff --check` pass. Existing evidence check accepts historical artifacts; this is not a current-source performance claim. The new reproductions fail despite these green checks. |
| Incomplete | Gate | Correctness and bounded-resource targets | Missing: 19D/19E/19H regressions, actual memory/work evidence, format-5 crash and cleanup coverage, and current-head sanitizer/Correctness results. |
| Incomplete | Gate | Latency targets with identical results | Missing: comparable enwik9 before/after artifact and commands, physical-work/projection/locality sweeps, and four-thread-count multiset/sink parity. |

## Phase 20: Maintenance and Build Cost

Goal:
Make maintenance cost its changed data, and reduce build memory where measurements identify
avoidable state. Preserve the meaning of a caller's bound.

Scope:
- Exit no-op refresh/merge without a pointless pack/rewrite, after execution-time validation.
  Exact status counts must not force a whole-table scan on every bounded increment.
- Use 19I's storage-work measurements to separate scans, normalization, pair emission,
  aggregation, sorting, and temporary copies before choosing a build optimization.
- Evaluate 32-bit in-segment offsets and smaller aggregate blocks, with explicit validation of
  the aggregate's accepted rowid range; alternatively keep BIGINT at the public boundary.
  Test fewer packed-temp copies without losing partitioning, spill, or atomicity.
- Preserve `max_rows` as a maximum rowid span. Do not silently round it upward by as much as
  a 2^20-row segment merely to reduce generations. Consider a differently named target only
  if a measured use case warrants an additional API.
- Size partitions from actual emitted distinct keys and aggregate costs. State the one-segment
  lower bound: adding partitions cannot rescue a single oversized segment/row. Evaluate
  spillable partial packing or a clear bounded failure instead of promising arbitrary fit.

Completion gate:
No-op refresh and merge target <20 ms on enwik9 after 19I, with visited work reported. The
bounded loop never exceeds `max_rows`, advances through deleted gaps, and stops correctly.
For build, establish a current baseline first; target substantially lower accounted memory
and RSS with wall time within 5% at identical partitions/settings, exact postings and crash
atomicity. The prior 7GB/8.6s target is not comparable evidence for this dirty implementation.

Testing plan:
Byte/posting identity in both directions; long/skewed rows, Unicode, gram>4, small memory,
partial segments, no-op and populated tails; fixed crash seed 20261317 for every maintenance
shape; wall, CPU, RSS, spill, emitted pairs, and physical visits at fixed settings.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| In Progress | Work | 20A: Delta-only refresh and cheap no-ops | Stats rewrite is gone in the dirty code; refresh still packs an empty delta, computes generation/HWM, and updates the registry; compact materializes all key groups. Missing: execution-time early exits, cheap authoritative progress, no-op work/timings, and tests that the fence/atomic transaction survives the shortcut. Depends on 19I/19J. |
| Incomplete | Work | 20B: Build memory and amplification | `RowidBlock` uses int64 payloads and starts each per-thread group at 16 entries; finalization copies into a vector and blob. `PAIR_STATE_BYTES=32` samples byte-length windows, whereas `GramKeysFunction` now emits distinct keys; ranges cannot be finer than one segment. Missing: current pair/cardinality/peak breakdown, calibrated estimator, measured offset/block/temp alternatives, oversized-segment behavior, and spill/identity proof. |
| Incomplete | Decision | 20C: Bound semantics | Reopened by this audit: the former upward-rounding recommendation conflicts with the useful `max_rows` ceiling and current `BoundedRefreshEnd`. Recommended: retain the hard span ceiling; no new tuning parameter without measured benefit. Missing: implemented/documented contract and boundary tests. |
| Incomplete | Gate | Cheap no-ops and lower build cost | Missing: comparable measurements and work counters; exact identity and fixed-seed crash outcomes. |
| Incomplete | Test | Maintenance and build edge cases | Missing: evidence for small bounds, large deleted gaps, single oversized rows/segments, transaction-local rows, and cancellation/rollback after early exits. |

## Phase 21: Public Surface and Stable Query Semantics

Goal:
Keep search semantics independent of an optional optimization, and make metadata and maintenance
APIs useful without creating a second implementation of exact filtering.

Scope:
- Keep automatic acceleration opt-in until 19D/19E/19F/19H/19I and representative fallback
  performance gates pass. Reassess a default change using eligible and ineligible queries.
- Reading operations become composable table functions, evaluated in the executing statement's
  snapshot: `ngram_indexes()` and `ngram_index_stats(table)`. Provide cheap lifecycle status
  separately from expensive full storage statistics, and expose stranded guards as promised.
- Use `drop_ngram_index(index_ref, catalog=...)` for by-ID cleanup, with unambiguous catalog
  rules for copied attached databases. Retain the table/column form if it remains useful.
- Always return progress from refresh. Prefer an authoritative `has_more` plus rows advanced
  if exact `remaining_tail` requires repeated full counts; define snapshot and local-row meaning.
- Keep raw candidates and storage/codec helpers internal or explicitly diagnostic. A prefix
  does not remove the need for validation, accounting, and documentation of unsafe composition.
- Retain independent decode-work admission; deciding whether the knob is public is separate
  from deleting the protection.
- Give explicit search its own case semantics and an explicit-column form that works without
  an index. A rebuild, different index normalization, or dropping an index must not change
  the selected result set. Prefer one native predicate construction and execution path.
  Prototype bind replacement/shared logical scan before retaining separate custom scan/recheck
  machinery; preserve the distinction between explicit search and the automatic rewrite switch.

Out of scope:
`CREATE INDEX USING NGRAM` as the only front door unless the pinned host can also provide safe
multi-statement build, drop, and transaction lifecycle. Renaming alone does not justify it.

Completion gate:
No-index, CI-index, CS-index, dropped/rebuilt-index, and prepared-query runs of the same explicit
semantic request have identical multisets. Metadata reflects its execution snapshot. Surface
docs, function/setting registrations, examples, and harnesses agree. Default changes meet the
unrelated-query and native fallback performance gates before being enabled.

Testing plan:
Compare native contains/ILIKE semantics, escaped wildcard literals, NULL/empty strings, Unicode
folds, no/multiple indexes, joins, nested projections, prepared plans, and both kill switches.
Test metadata around concurrent refresh/drop and copied attached catalogs. Verify progress
termination and cost; audit all renamed helper callers.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Decision | 21A: Automatic acceleration default | Reopened: 19D can invalidate a database and 19F charges an unrelated query ~9.6 ms. Recommended: remain opt-in until those gates and the native fallback baseline pass. Missing: post-fix workload evidence before a default-true decision. |
| Incomplete | Decision | 21B: Reading operations as table functions | Retain the earlier direction. `NgramIndexesQuery` returns VALUES captured in preprocessing; `NgramIndexStatsQuery` embeds guard/table-size facts while later SQL reads other facts. Missing: one executing snapshot, cheap status versus explicit expensive statistics, and a concurrent metadata test. |
| Incomplete | Decision | 21C: Drop by index_ref overload | Retain the earlier direction, including deleting redundant `ngram_index_status`/`drop_ngram_index_by_id` after replacements exist. Missing: catalog disambiguation, all call-site updates, and safe cleanup from 19J. |
| Incomplete | Decision | 21D: Refresh always returns progress | Retain the direction; settle `has_more` versus exact remaining count on measured cost under 20A. Missing: documented fields, snapshot/local-row behavior, and caller loop updates. |
| Incomplete | Decision | 21E: Raw candidates and low-level helpers | Recommended: internal diagnostic surface (`__ngram_candidates` or equivalent), with exact search as the supported composition path. Missing: minimal helper inventory, tests/benchmarks moved, and public docs made consistent. |
| Incomplete | Decision | 21F: Decode-work limit | Supersedes the earlier removal recommendation: **keep a cumulative work ceiling**. Peak segment memory is bounded while total decoded work grows with segment count; a sparse intersection can also pass candidate fraction despite large posting lists. Missing: measured replacement if the setting itself is removed, with a hard-work regression. |
| Incomplete | Work | 21G: Implement API decisions and update callers | Missing: registrations, metadata implementation, surface docs/test, CLI/Python examples, benchmarks and harness updates. Do semantics first, renames second. Earlier decision rows marked Complete were approvals of intent, not implemented behavior. |
| Incomplete | Work | 21H: Index-independent explicit search — P2 architecture | `BindQueryTarget` currently refuses an absent index and `ngram_search` inherits its case behavior; `RecheckState` separately normalizes strings and uses `std::search`, while rewrite uses the native expression executor. Recommended: explicit semantic options and optional index use, with shared native predicates and one scan design. Missing: prototype, parity of escaped/Unicode literals, projection/dependency/prepared-plan behavior, and measured simplification without disabling explicitly requested acceleration. |
| Incomplete | Gate | Stable semantics and coherent surface | Missing: no-index/index-replacement multiset parity, executing-snapshot metadata evidence, default-policy performance evidence, and matching API docs. |
| Incomplete | Test | API and query-shape matrix | Missing: actual tests and current-head CI evidence, including literal `%`, `_`, and escape characters. |

## Phase 22: Tests That Exercise the Claims

Goal:
Make the suite detect scheduler, multiplicity, resource, persistence, and race defects before
release. Pull the relevant tests forward into each implementation phase.

Scope:
- Race refresh/compact with search, using deterministic barriers where possible and explicit
  allowed conflict errors; compare successful statements against their valid snapshot oracle.
- Replace set-only differential checks with multiset checks, isolate the oracle from rewriting,
  and prove the intended runtime mode. Include ordered sinks and deliberately unequal segments.
- Add long needles/haystacks, Unicode folds, real same-size-key collisions, and persistent
  current/previous-format fixtures opened read-only as well as through WAL/checkpoint paths.
- Split the C++ harness by mechanism and support selection; keep timing report-only and make
  cancellation deterministic. Keep useful semantic checks when deleting obsolete stats checks.
- Share Python process/corpus/oracle infrastructure where semantics truly match. Keep format
  lookup/digest logic in one place; do not force arbitrary file-length targets.
- Run focused query/concurrency/resource tests under ASAN/UBSAN on pull requests; nightly full
  debug/crash coverage and a supported TSan configuration. Add warnings on extension code only,
  maintain host pin checks, and run formatting/tidy checks without treating style as architecture.

Completion gate:
Every 19D/19H/19J reproduction has a regression and relevant mode assertion; multiplicity and
ordered sinks are tested; no correctness assertion depends on machine speed; crash coverage
runs successfully under format 5. Nightly passes once with its exact source/run recorded.

Testing plan:
Demonstrate fail-before/pass-after for targeted defects. Deliberately duplicate an accelerated
result to prove the multiset oracle detects it; force fallback to prove a probe-mode assertion
fails. Use internal barriers for publication/cancellation races, not probabilistic sleeps.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Test | 22A: Maintenance/search races and scheduler contracts | The existing `ngram_parallel.test` has useful EXCEPT ALL and ordering cases but its initial fixture fits one posting segment; the dirty queue fails the new two-segment CTAS. Missing: deterministic out-of-order decoder completion, CTAS/INSERT/streaming, local tails, cancellation, and refresh/compact snapshot races. Implement with 19D, not afterward. |
| Incomplete | Test | 22B: Long inputs, Unicode, and honest oracles | `DecomposeNeedle`'s quadratic behavior is untested. `scripts/differential_search.py` uses EXCEPT; `ngram_search_differential.test` incorrectly says unique IDs make that an exact multiset comparison. `build_scale.test` compares sets of strings and can hide duplicates/fallback. Missing: EXCEPT ALL, independent native oracle, runtime-mode assertions, 4/64 KiB+ haystacks and 128 KiB needles, folds, and real collision injection. |
| Incomplete | Test | 22C: Real format fixtures | Only committed format-3 fixture coverage is recorded. Missing: actual format-4 file for safe drop plus a format-5 file for query/reopen/read-only, fixture source/host identities, and format-5 crash checks. Synthetic audit cleanup is insufficient for this gate. |
| Incomplete | Work | 22D: Selectable, deterministic C++ harness | `ngram_maintenance_checkpoint_gap.cpp` is 2418 lines; registry-scale tests assert elapsed time and cancellation uses sleeps. Missing: mechanism-based selection, barriers, report-only timings, and explicit POSIX/Windows coverage policy. Keep native harnesses for invariants SQL cannot control. |
| Incomplete | Work | 22E: Shared Python harness core | Three drivers duplicate CLI/registry/oracle logic; the crash driver now queries a removed table while differential tests pass. Missing: shared format-aware lookup/digests and multiset oracle, with actual existing seed behavior preserved. |
| Incomplete | Work | 22F: Nightly and focused CI | Current ASAN selector runs lifecycle/search/rewrite/maintenance plus the C++ harness, omitting the new key and SQL concurrency files. Missing: nightly debug/crash/TSan results, focused resource/order tests, extension-only warnings, and appropriate CMake minimum/tidy checks. No unverified claim of current remote CI. |
| Incomplete | Gate | Tests cover the changed mechanisms | Missing: fail-before/pass-after and oracle-sensitivity evidence, working crash lane, deterministic harness, and current-head nightly/Correctness URLs. |

## Phase 23: Documentation, Evidence, and Release

Goal:
Provide a concise, accurate entry point whose guarantees, APIs, installation instructions,
performance claims, and release provenance are all verifiable.

Scope:
- Lead README with supported installation, quick start, semantic/resource contract, and concise
  API/limitations. Link deeper ownership, maintenance, rowid, and recovery explanations in docs;
  do not delete necessary qualifications to meet an arbitrary line quota.
- Treat prior plans as historical evidence. Distill a current design note with explicit
  ownership, snapshot, batch-order, memory/work, and host-dependency invariants; correct the
  old physical-pruning claims rather than copying them into the new design note.
- Rewrite template update/test guidance for this repository; remove unused upload/configuration
  remnants after checking callers. Keep the host source/version definition consistent.
- Give benchmark results one owner and reproducible corpus preparation. Preserve immutable
  old artifact validation by the artifact's recorded format; regenerate current release
  evidence, not old measurements relabeled as the new format.
- Align packaging metadata and docs with the final API and verified release availability.
  Community installation must not be the only apparent working first step before submission.

Completion gate:
Current-source release evidence validates and matches the tagged source and tool identity;
all final API examples run on that build; links and supported installation work; Correctness
and distribution matrix are green on the release candidate/tag source. No unsupported
physical-pruning, unconditional-corruption-recovery, or stale performance claim remains.

Testing plan:
`release_evidence.py validate`, `check --current-source`, executable examples/link checks,
release matrix and current-format reopen/CLI smoke. Full benchmarks are required after the
query/build changes; the audit's synthetic measurements are not a release substitute.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Doc | 23A: README and contract | README still says READY requires both storage tables and asserts physical rowid pruning; top-level exhaustiveness needs its supported-state boundary close by. Missing: accurate format-5 storage, semantics from 21H, work/memory distinction, installation status, and linked detailed docs. |
| Incomplete | Doc | 23B: Plans and current design note | Missing: `docs/design.md` with actual host adapter, guard/fence, snapshot, publication, and accounting proofs. Move plans to `docs/plan/` only with all references fixed; do not duplicate active ledgers. |
| Incomplete | Doc | 23C: Repository-specific maintenance/test docs | Missing: useful `docs/UPDATING.md`, `test/README.md`, verified removal of unused upload tooling/configuration, and commands to select the harness/regressions. |
| Incomplete | Doc | 23D: Benchmark and packaging coherence | Existing `release_evidence.py check` passes historical artifact/pins/ancestry/rendering, not current implementation performance. Missing: single-owner result block, reproducible corpus preparation, stale packaging/source-ID text fixed, format-aware historical validation, and final API metadata. |
| Incomplete | Work | 23E: Fresh evidence and release | Missing: current-format benchmark artifact after 19/20/21, fixed-seed and sanitizer/crash results on the final source, green distribution matrix, and release tag. Tag/publish is not part of this audit's completed work. |
| Incomplete | Gate | Traceable release claims | Missing: strict current-source check, runnable examples, link/install verification, and recorded final-source workflow results. |
