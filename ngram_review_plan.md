# Development Plan: ngram Review Follow-up (Phases 16 to 23)

This is the active continuation of `docs/plan/ngram_index_plan.md`. Independently audited on
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
| Complete | Work | 19A: Integer gram key | `src/gram.cpp` implements byte-packed UHUGEINT/FNV fallback, `ngram_gram_keys`, and key-based build/probe; `gram_key.test` covers packing for ASCII, Unicode, NUL and >16-byte grams. Same-size collisions are forced through an internal seam: every key is ANDed with a process-wide `GramKeyMask` read once per chunk or needle, set only by the C++ harness. `TestGramKeyCollisions` drops the low bit of every packed byte and keeps eight bits of a hashed key, so `abc`/`abb`, `aéx`/`aèx`, chr(0)/chr(1) and 17-grams (9,600 grams into at most 256 keys, 272 rows with two colliding grams) merge their postings; both exact paths return the oracle multiset with index mode asserted, the raw candidates equal the widened set computed from `ngram_gram_key`, the merged key holds exactly the rows of either gram in one row per stage, through build, a refresh generation holding both grams in one row, compaction and purge. Reviewer approved 2026-09-09 after a set-based superset check and per-stage storage checks. |
| Complete | Work | 19B: Drop the stats table | Build/refresh/compact and `ngram_index_stats` use segments; 19J closed the cleanup, crash-check, fixture and README paths. **Measured** (`docs/review/2026-09-09/format_measure.py`, 1M enwik9 lines, 8 threads, format-4 binary from `main` at d3ecccf): build 1.23 s vs 1.31 s and index storage 103.8 vs 105.0 MiB (the same segments rows; file allocation differs); a 10,000-row refresh generation 173 ms vs 105 ms, a refresh with nothing to index 96.5 ms vs 4.0 ms, compaction of 21 generations 2.81 s vs 1.75 s, a compaction with nothing to merge 216 ms vs 23 ms: the statistics table cost format 4 about 90 ms per refresh and 190 ms per compaction whether or not there was work. Fragmented generations: a rare needle's manifest grows from 10 rows at one generation to 177 at twenty and its latency from 4 to 8 ms; compaction merges the rows back to 20 but latency stays at 8 ms because each generation leaves a sorted run whose row groups the manifest scan visits (19C). `ngram_index_stats` aggregates the whole table (11-16 ms per million rows here) while `ngram_indexes` is a registry read; that split feeds 21B. A compact synopsis is not justified: the manifest scan, not a statistics read, is the rare-query cost. |
| Complete | Work | 19C: Parallel manifest collection | **Measured** by physical work (`OPERATOR_ROWS_SCANNED`, `docs/review/2026-09-09/README.md`): on v1.5.5 a `gram_key = ?` scan prunes row groups by zone map but reads every vector of an admitted row group, because `RowGroup::CheckZonemapSegments` skips only the first excluded column segment (108,000 rows per key on a two-row-group table, 1.09M rows for a 10-key needle, 2.83M on the compacted 22-run table), and a `gram_key IN (...)` scan prunes nothing (the whole table). `CollectGramRows` now positions its scans itself: `AdmittedKeySpans` walks the key column's segment tree through the row group's public raw column accessor, keeps the vector-aligned spans of the column segments whose statistics admit the key, and the bounded scan of 19I reads only those spans, then the transaction's local rows. The profile reports the spans as `Ngram Manifest Rows Visited`, recorded in `format_observations.json` (manifest stages): about 14,000 rows per key on the fresh table (7.6x fewer than the host's scan) and 94,000 on the compacted one (3x fewer); the rare needle's probe fell from 4 to 1 ms and from 8 to 3 ms. Descriptors now carry each row's rowid span, which 19G uses. |
| Complete | Work | 19D: Candidate-batch fetch parallelism | Segments publish in ordinal order through `CandidateQueue::pending` (`PublishInOrder`), decode-ahead bounded to max_threads segments, a `failed` flag drains waiters. `ngram_batch_order.test` (CTAS, INSERT and the order-sensitive materialized digest at 1/2/8/24 threads on both paths) fails on the old code at 2 threads; harness `TestParallelCandidateStreams` streams 1,065,717 ascending ids at 24 threads and interrupts at four points. Reviewer approved 2026-09-08. 50k-candidate single-segment search: 11.4 ms at 1 thread, 8.6 ms at 8. |
| Complete | Work | 19E: Decode reuse, order, and resource ownership | `ProbeMemoryReservation` is atomic with `Shrink`; manifest chunks are charged before append and unselected grams released; manifest workers fit the budget; `ReserveExactly` trims postings/intersection to each segment, releasing before it reserves and dropping superseded candidates at once, so the tracked bytes equal the allocation at every point; `ProbeDecodeTracker` records live and peak rowid-buffer bytes (worker scratch plus published vectors through their deleter), exposed as `Ngram Decode Workspace Bytes`/`Ngram Decode Peak Bytes`; the reservation charges M decode peaks plus M-2 candidate vectors, the queue's 2M-2 alive-vector bound. Harness `TestProbeMemoryPeak`: 25.3 MB peak inside a 253.8 MB workspace at 8 workers, 25.17 MB inside 25.43 MB at 1 worker, both paths; DEBUG builds assert peak <= workspace at exhaustion. |
| Complete | Work | 19F: Bind/init metadata and cheap discovery | `TryRewriteGet` checks filter shapes before any registry read and reads per column by owner key; `ReadRegistry` takes a `RegistrySelector` pushed as native filters (owner key, index id) and materializes only selected rows while NULL-checking every identity column; `Verdict` selects by id. **Measured** with 10,000 registry rows at one thread: unrelated numeric query 9.0 ms -> 0.0 ms with acceleration on; tiny-table `ngram_search` 18 ms -> 1 ms; `contains` rewrite 19 ms -> 0 ms. Harness `TestRegistryScale` gates the on/off ratios. Execution-time revalidation is unchanged. |
| Complete | Work | 19G: Candidate/work cost model | The cumulative decode-work ceiling stays (21F). Admission now compares fetch-equivalent rows with the indexed rows (hwm + 1): each segment counts the smaller of its candidate bound and the intersection of its grams' rowid spans over `RANGE_ROWS_PER_FETCH` (4, from the measured fetch-to-span-row ratio of 4.6, the density at which the executor also switches a batch to a range scan), a segment whose spans share no row is left out, and every projected column beyond the recheck's multiplies the charge, since it is one more fetch per kept row. Both exact paths evaluate one native predicate once per row: `ngram_search` binds `contains(lower(col), needle)` (or the case-sensitive form) as a host expression, pushes it into its tail and range scans as a table filter and rechecks only rows fetched by rowid; the rewrite likewise skips the executor for natively filtered chunks. Per-row fetches read the recheck columns first and the rest only for kept rows. `Ngram Admission Rows`, `Ngram Manifest Rows Visited` and the host's rows-scanned metric are published. **Measured** (`docs/review/2026-09-09`): scan 92 ns/row, scattered fetch 0.8-1.5 us, span row 0.29 us, extra column 0.1-11 us per kept row by compression (`probe_observations.json`); the explicit recheck went from 0.47 us/row to the host's, a six-column projection from 484 to 99 ms; with the gate open the index beats the scan up to a 3.2% candidate bound at every thread count and loses at 3.6% on 24 threads, so the default fraction is now 0.02. Tiny tables: about 100 us of fixed cost per accelerated statement. |
| Complete | Work | 19H: Bounded, interruptible needle preparation | `NeedleKeys` deduplicates through a hash set in one pass; `DecomposeNeedle` takes `max_keys` from the budget (`MaxProbeKeys`), pre-checks the byte length, checks for interrupts every 4,096 grams, and the rewrite decomposes each needle once; the recheck is the host's `contains` expression, whose per-chunk scratch is the host's (19G). `ngram_long_needle.test`: an 8 KiB needle probes, 16 KiB and 128 KiB needles decline before any manifest row and stay exact, `ngram_candidates` refuses. **Measured** on the audit's case: 16/32/64/128 KiB from 0.062/0.239/0.965/3.872 s to <= 1 ms each. |
| Complete | Work | 19I: Physical range and tail access | Range batches and tail units run through `InitializeBoundedScan`, which positions the committed scan with the public `RowGroupCollection::InitializeScanWithOffset` and skips row groups the pushed filters' zone maps exclude, and gives every bounded scan a rowid lower bound for the vector before it; the tail is partitioned from hwm+1 in row-group units plus one local-storage unit; batches extend to vector-aligned rowid boundaries so consecutive range scans share no vector. `ngram_physical_access.test` pins equal range spans for 800k and 2.4M rows on both paths, fetch-only sparse batches, tail and local spans, delete/update/checkpoint parity, off-grid row groups after a 100-row append at 1 and 8 threads on both paths, zone-map-excluded units in index and fallback mode, and the short-needle fallback over row groups starting at row 100. **Measured** on the audit's case: 50k candidates from 4.5 ms (1.1M rows) and 36.3 ms (11M) to 0.8 ms on both. |
| Complete | Work | 19J: Complete format-5 lifecycle and tooling | `crash_maintenance.py` digests segment rows and runs through its kills (the audit's `--rows 100 --tail 20` invocation now trips only the script's own vacuity guard); `DropIndexScript` removes a format-4 row's `stats_<id>` table and `ObserveCatalog` attributes that table to its row instead of listing a second MALFORMED object; real fixtures `test/fixtures/format4.duckdb` (written by main at d3ecccf) and `format5.duckdb` with harness `TestFormat4Fixture` (list, fallback, token-checked drop with the statistics table, rebuild) and `TestFormat5Fixture` (reopen READY, both paths exact, refresh, drop), the harness now taking the fixtures directory, and read-only attach blocks in `ngram_lifecycle.test`; README storage wording. |
| Complete | Test | Audit baseline, not phase acceptance | Audited tree: build targets report no pending work; `make test_release` passes the C++ harness and 3188 assertions in 26 cases. `differential_search.py --trials 2 --rows 5000 --seed 20261314`: 1764/0; `--transparent --seed 20261315`: 886/0. `release_evidence.py test --binary build/release/duckdb`, `check`, `scripts/verify_pins.sh`, and `git diff --check` pass. Existing evidence check accepts historical artifacts; this is not a current-source performance claim. The new reproductions fail despite these green checks. |
| Complete | Test | Repair evidence 2026-09-08 | Worktree on `baeccf4`, release build: `make test_release` passes the C++ harness (format-3/4/5 fixtures, streaming and interrupts at 24 threads, decode-peak and registry-discovery gates) and 3450 assertions in 29 sqllogictest cases, the new `ngram_batch_order`, `ngram_long_needle` and `ngram_physical_access` among them (the last carrying the off-grid, zone-map and short-needle fallback regressions from the reviewer's round-one findings). `differential_search.py --trials 2 --rows 5000 --seed 20261314`: 1764/0; `--transparent --seed 20261315`: 886/0. `crash_maintenance.py --seed 20261318` (default sizes): 12 kills through refresh, compact and purge plus a 12-call bounded loop over 11 distinct states, 0 failures. `release_evidence.py check` and `test --binary build/release/duckdb` (the runtime identity check now accepts either abbreviation of the pinned host commit), `scripts/verify_pins.sh` and `git diff --check` pass. Every case of `docs/review/2026-09-04/reproduce.py` has a suite counterpart: `batch_order` and `long_needle` and `range_cost` as sqllogictests, `registry_cost` and `format4_drop` in the harness. Reviewer approved 19D on 2026-09-08, and 19E-19J the same day after one round of fixes: off-grid tail units, zone-map-excluded units, tracker transients, per-needle budget merging in the rewrite, and the needle byte bound. |
| Complete | Test | Remainder evidence 2026-09-09 | Worktree on `48407e6`, release build: the C++ harness (now with `TestGramKeyCollisions`) and 3450 assertions in 29 sqllogictest cases pass; `differential_search.py --trials 2 --rows 5000 --seed 20261314`: 1764/0; `--transparent --seed 20261315`: 886/0; `churn_maintenance.py --rounds 20 --rows 4000` seeds 20261316 and 20261318: 320/0 each; `crash_maintenance.py --seed 20261318`: 12 kills through refresh, compact and purge plus a 10-increment bounded loop resumed after 7 kills, 0 failures; `release_evidence.py check` and `test --binary build/release/duckdb`, `scripts/verify_pins.sh` and `git diff --check` pass. Reviewer approved 19A on 2026-09-09 after a set-based superset check and per-stage storage checks, and 19B/19C/19G the same day after one round of seven fixes: the DEBUG recheck verification, the published-vector charge, one range ratio for gate and executor, its derivation from the recorded observations, decoded postings checked against their row span, segment statistics read through the host under its lock, and numbers reconciled with the JSON. |
| Complete | Gate | Correctness and bounded-resource targets | 19D/19E/19H/19I regressions, memory-peak evidence, format-4/5 fixtures and the crash script landed in `d28ccab` (Linux Correctness run 34285380191, distribution run 34285397084); the same-size collision seam of 19A, the positioned manifest scans, the native recheck and the admission model landed in `464023e`, whose Linux Correctness run [34357778860](https://github.com/danthegoodman1/duckdb-ngram/actions/runs/34357778860) passed (release tests, evidence checks, fixed seeds, ASAN/UBSAN harness). Its distribution run 34357781437 failed on macOS in the harness's sleep-timed query cancellation, which the faster query path now outruns; Phase 20 replaces that test with a streaming interrupt that holds the query's first chunk, and the distribution matrix is re-run on that commit. |
| Complete | Gate | Latency targets with identical results | `docs/review/2026-09-09/latency_rebaseline.py` and `ilike_baseline.py` (observations in `latency_observations.json`): the same enwik9 lines built by `main` at d3ecccf (format 4, the engine before Phase 19) and by the current source, the same needles timed in one warm process with count parity on every pair, 21 runs, 48 GB. Warm p50 at one thread / 24 threads, before -> after: rare (Schwarzschild, 141 rows) 11 / 8 ms -> 1 / 1 ms on both paths; moderate (parliamentary, 3,630) 45 / 12 -> 29 / 6 ms explicit and 49 / 13 -> 40 / 9 ms transparent; moderate (chemistry, 6,350) 24 / 10 -> 13 / 4 ms; declined needles (history 3.2% bound, which 3.6%) run the explicit fallback at the native scan within 1% (1,231 / 82 ms against 1,227 / 82 ms) where it was 49% slower before, and the transparent fallback at the native `ILIKE` within 0.5%. Targets: rare 1 ms (target 3), moderate 4-9 ms (target 12), fallback within 1% (target 10%). Tiny tables (`cost_observations.json`, `tiny_microseconds`): 0.30-0.44 ms per statement end to end, of which the host's own per-statement floor is 0.21-0.29 ms (a plain scan of 1,000 rows) and the accelerated path adds about 0.1 ms, so the 0.3 ms target holds for the extension's share and not for the whole statement. Work counters (`Ngram Fetched/Range/Tail/Local Rows`, `Manifest Rows Scanned/Visited`, `Admission Rows`, the host's rows-scanned metric) and the projection sweep (`cost_observations.json`, `projection`) are recorded; the 51k-candidate single-segment case is the batch-order fixture at 8.6 ms with 8 workers against 11.4 ms with one (19D). Four-thread-count sink parity is `ngram_batch_order.test`. Build 7.20 s -> 6.97 s. |

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
partial segments, no-op and populated tails; fixed crash seed 20261318 for every maintenance
shape; wall, CPU, RSS, spill, emitted pairs, and physical visits at fixed settings.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 20A: Delta-only refresh and cheap no-ops | A refresh whose pragma expansion sees no allocated rowid past the mark (`DataTable::GetTotalRows` counts deleted rows too, so no committed row can lie there) emits only its fence call and progress row; a merge-only compaction consults the segments table's column statistics through `DataTable::GetStatistics` and the transaction's local storage, since one generation cannot hold two rows of a key, and emits only the fence call; compaction leaves every surviving row at generation 0 so that state returns after a checkpoint. A refresh over a tail whose rows were all deleted, or a merge with nothing fragmented before its checkpoint, still packs an empty delta; that delta is now appended at execution by the fence's `__ngram_maintenance_append` call through the host's transaction-local append, which writes nothing when there is nothing, so no empty batch insert can precede a purge in one user transaction (the reviewer's `hazard_gap`, `hazard_refresh` and `hazard_compact` sequences emptied the table in-process before, `docs/upstream/duckdb-empty-batch-insert.md`; `ngram_refresh_noop.test` pins the sequence). **Measured** on enwik9 (`docs/review/2026-09-09/noop_observations.json`, 10.9M rows, 24 threads; each mutating call timed as the first statement of its process): no-op refresh 0 to 1 ms, bounded no-op refresh 1 to 3 ms, no-op compaction of a built index 0 to 1 ms, refresh of one appended 100,000-row generation 529 ms, its merge 719 ms, a compaction right after that merge in the same process 22 to 28 ms (the full check: the merged-away generation's row groups keep their statistics until a checkpoint vacuums them, and a small generation copied into an existing row group keeps them until a purge), the same compaction in a fresh process after the shutdown checkpoint 0 to 1 ms. `ngram_refresh_noop.test` covers rollback of the no-op shortcuts, refusal of a dropped index at expansion (the fence's execution-time refusal is the harness's), the generation renumbering across refresh, merge and purge, and the fingerprint of an untouched segments table. |
| Complete | Work | 20B: Build memory and amplification | The pack aggregate stores 32-bit offsets within the group's segment (every rowid of a group shares segment_no = rowid >> 20 by the pack statement's grouping, enforced with an error), reconstructs absolute rowids at finalize, and so writes byte-identical postings. **Measured** (`docs/review/2026-09-09/build_observations.json`, enwik9, 24 threads, 48 GB, three fresh builds each): peak RSS 8.82 GiB -> 6.88 GiB median (-22%), wall 7.03 s -> 7.00 s, no spill, `postings_bytes` identical; whole-process bytes per posting fell from 13.3 to 10.4 over the recorded 711,434,908 postings. `PAIR_STATE_BYTES` stays 32: a million single-posting groups measure about 33 bytes per pair, where the group entry and first block dominate, and sizing partitions from the emitted distinct keys rather than the pair count remains open. The temp copy of packed rows stays: writing partitions straight into the segments table would leave one sorted run per partition, which 19C showed costs the manifest scan a column segment per run. Oversized segments: a single 2^20-row segment of long rows is the finest partition; the aggregate raises DuckDB's OutOfMemory under the limit rather than spilling, as before. |
| Complete | Decision | 20C: Bound semantics | Decided 2026-09-08: `max_rows` stays a hard maximum rowid span; no upward rounding and no new tuning parameter. Documented in README (Catching up on a large tail): a span, never a byte budget; snapped down to a segment boundary; a deleted gap costs empty increments; one oversized row is one increment; this transaction's uncommitted rows are neither indexed nor counted. `ngram_refresh_bounded.test` pins a 980-row deleted gap crossed in span increments, a 4.5 MB row indexed under `max_rows = 1`, and transaction-local rows excluded until commit. |
| Complete | Gate | Cheap no-ops and lower build cost | No-op refresh and merge at 0-2 ms on enwik9 for a built index and after a checkpoint that vacuumed the merged-away generation's row groups, under the 20 ms target; a compaction in the merge's own session, or of a small generation that shares a row group with older rows, takes the full check at 22 to 28 ms, over the target by about a quarter, because the host keeps a deleted generation's column statistics until a checkpoint vacuum or a purge rewrites those row groups. The bounded loop's span, gap and oversized-row behavior is tested; build RSS -22% at equal wall time with identical `postings_bytes`; `crash_maintenance.py --seed 20261318` (0 failures) and the full maintenance suite pass on the changed aggregate. |
| Complete | Test | Maintenance and build edge cases | `ngram_refresh_bounded.test` (deleted gap, single 4.5 MB row, transaction-local rows, small bounds), `ngram_refresh_noop.test` (rollback after the shortcuts, dropped index, generation renumbering through refresh, merge and purge), `pack_segment.test`, `postings_codec.test`, `unpack_postings.test` and `build_scale.test` (byte identity of postings against brute force across a segment boundary) pass; `crash_maintenance.py --seed 20261318` recorded in the Phase 20 evidence row. |

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
| Decided | Decision | 21A: Automatic acceleration default | Decided 2026-09-08: acceleration stays opt-in until the 19D and 19F gates and the native fallback baseline pass; default-on remains the destination. Missing: post-fix workload evidence before flipping the default. |
| Decided | Decision | 21B: Reading operations as table functions | Decided 2026-09-09: `ngram_indexes()` and `ngram_index_stats(table)` become table functions whose global init observes the catalog in the executing statement's transaction, so a row reflects that snapshot; `ngram_indexes()` is the cheap lifecycle status (registry row, storage objects, guard verdict, stranded guards) and `ngram_index_stats` the full storage aggregation, which the 19B measurement shows costs about 12 ms per million base rows and grows with the index. The pragma spellings stay as one-line aliases that expand to the functions, so scripts and tests keep working; `ngram_index_status` is deleted in favor of `ngram_indexes()` filtered by `index_ref`. Implemented in 21G. |
| Decided | Decision | 21C: Drop by index_ref overload | Decided 2026-09-09: `PRAGMA drop_ngram_index(index_ref, catalog = 'db')` replaces `drop_ngram_index_by_id`; the catalog defaults to the current database and must be named for any other attached one, since copied attached databases may hold the same UUID. The table/column form stays. Implemented in 21G with every caller (README, tests, harness, scripts) updated. |
| Decided | Decision | 21D: Refresh always returns progress | Decided 2026-09-09: every refresh returns one progress row per index (`column_name`, `rows_indexed`, `hwm_rowid`, `remaining_tail`), bounded or not; `remaining_tail` stays an exact count because it is a rowid-pruned scan of the rows past the mark, which 20A measures as cheap, and it counts committed rows in the statement's snapshot, never this transaction's local rows. Implemented in 21G. |
| Decided | Decision | 21E: Raw candidates and low-level helpers | Decided 2026-09-09: `ngram_candidates` keeps its name and is documented as a diagnostic surface: a lossy superset of indexed rows with no recheck and no tail, resource-checked like the exact paths. The helper inventory is `trigrams` (public), `ngram_gram_key`/`ngram_gram_keys` (inspection and the build pipeline), and the codec functions `ngram_pack_segment`, `ngram_unpack_postings`, `ngram_encode_postings`, `ngram_decode_postings` (build pipeline and tests), documented under one diagnostic heading; renaming them buys nothing the documentation does not. |
| Decided | Decision | 21F: Decode-work limit | Decided 2026-09-08: keep a cumulative decode-work ceiling. Peak segment memory bounds one segment while total decoded work grows with segment count, and a sparse intersection can pass the candidate fraction despite large posting lists. `ngram_max_probe_rowids` stays public (documented in the settings table as the hard limit on decoded posting rowids). `ngram_query_bounds.test` pins the regression: a needle whose rarest gram matches 5 of 250,000 rows passes the candidate fraction, decodes the dense gram beside it, and at a ceiling of 1,000 takes the scan fallback with the "decoded-rowid work budget exceeded" mode; at the default ceiling the index answers it with 250,005 decoded rowids. |
| Complete | Work | 21G: Implement API decisions and update callers | `ngram_indexes()` is a table function whose global init observes every attached DuckDB catalog (`ObserveCatalog`) in the executing statement's transaction; `ngram_index_stats(table)` binds to one SELECT (`IndexStatsSelect`, replaced through `bind_replace`) so every column is read by the statement that runs it, the guard verdict through one join with `ngram_indexes()`; `table_max_rowid` and `remaining_tail` count committed rowids only, so this transaction's provisional rowids never appear. The pragma spellings expand to the functions; `ngram_index_status` and `drop_ngram_index_by_id` are gone; `PRAGMA drop_ngram_index(index_ref, catalog = 'db')` drops by reference in the current database unless a catalog is named, and the table/column form resolves to the same path. Every refresh script ends in its progress row. Callers updated: README (surface, stats, diagnostic helpers), `docs/stale-updates.md`, the community-extension description, `docs/review/2026-09-04/reproduce.py`, the C++ harness (`StatusByRef` reads the listing and rejects a missing row), `ngram_lifecycle.test`, `ngram_refresh_bounded.test`, `create_index.test`, `ngram_refresh_noop.test`, `ngram_maintenance_identity.test`; the Python scripts and benchmarks read the pragma spellings, whose columns are unchanged. |
| Deferred | Work | 21H: Index-independent explicit search — P2 architecture | Deferred past v0.1.0 (decided 2026-09-08). `BindQueryTarget` currently refuses an absent index and `ngram_search` inherits its case behavior; `RecheckState` separately normalizes strings and uses `std::search`, while rewrite uses the native expression executor. Recommended: explicit semantic options and optional index use, with shared native predicates and one scan design. Missing: prototype, parity of escaped/Unicode literals, projection/dependency/prepared-plan behavior, and measured simplification without disabling explicitly requested acceleration. |
| Complete | Gate | Stable semantics and coherent surface | `ngram_api.test`: 24 predicate shapes (literal `%`, `_` and escapes, empty and NULL patterns, Unicode folds, conjunctions, disjunctions, negation) compared as multisets in both directions against the extension-disabled plan, with and without an index and, the whole loop again, after the index is rebuilt case-sensitive at gram 2, where `ILIKE` stays native; the shapes that stay native are pinned by EXPLAIN; `ngram_search` against the native `contains(lower(s), lower(needle))` for ten needles; a prepared parameter stays native and exact. Executing-snapshot metadata: a prepared `ngram_index_stats` statement reports a row appended between two executions, this transaction's own uncommitted row is in no column (`table_max_rowid` and `remaining_tail` count committed rowids only), and the listing after a drop is empty in the same connection. Default-policy performance is the Phase 19 latency gate (fraction 0.02, `latency_observations.json`). API docs match: README surface, stats and helper sections, `description.yml`. |
| Complete | Test | API and query-shape matrix | `test/sql/ngram_api.test` (metadata functions, drop by reference with and without a catalog, an unknown catalog and an unknown reference, the one-argument form given a table name, refresh progress, the query-shape matrix on two index configurations, the native shapes, the needle matrix, prepared plans); `ngram_query_bounds.test` (decode-work ceiling on a sparse intersection). Local: the whole SQL suite (30 files) and the C++ harness pass on the Phase 21 build. CI on d83a53b (Phases 20 and 21 together; the Phase 20 push's Correctness run 34366728430 was cancelled by the Phase 21 push under the workflow's concurrency group): distribution matrix [34369526336](https://github.com/danthegoodman1/duckdb-ngram/actions/runs/34369526336) green on every target, macOS harness included; Phase 20's own distribution run [34366742133](https://github.com/danthegoodman1/duckdb-ngram/actions/runs/34366742133) green; Correctness run [34369500941](https://github.com/danthegoodman1/duckdb-ngram/actions/runs/34369500941) green (release suite with the harness, fixed-seed drivers, focused ASAN/UBSAN lane). |

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
| Complete | Test | 22A: Maintenance/search races and scheduler contracts | Publication order is forced rather than left to segment sizes: `src/include/ngram/test_hooks.hpp` declares two process-wide callbacks the extension consults only when set (`before_segment_publish` in the candidate queue's decode path, `before_maintenance_append_chunk` in the fence's append call). The harness's `query/publication-barrier` holds segment 0 at its hook until segment 1 has reached its own, then requires the stream's ids ascending and complete; a second run holds segment 0 at its hook until segment 1's hook has interrupted the query, so the interrupt lands before either segment publishes, and requires the interrupt to surface and the same answer afterwards (a stranded worker would be a query that never returns, which the CI timeout bounds). `query/parallel-candidate-streams` interrupts a 24-thread streamed search after its first, eighth and sixty-fourth chunk; `query/cancellation` interrupts the merge at its second appended chunk through the hook and requires the digest unchanged (the purge reinserts through the host's batch insert, which no hook reaches, so its interrupt stays timed and tolerant of completion, documented in place). CTAS, `LIMIT`, local tails and thread-count parity stay in `ngram_parallel.test` and `ngram_batch_order.test`; refresh/compact snapshot races in `ngram_refresh_concurrent.test` and the harness's `registry/execution-identity-races`. |
| Complete | Test | 22B: Long inputs, Unicode, and honest oracles | Multiset oracles everywhere an accelerated answer meets a brute-force one: `EXCEPT ALL` in both directions in `ngram_search.test`, `ngram_search_concurrent.test`, `ngram_refresh_concurrent.test`, `ngram_maintenance.test`, `ngram_search_visibility.test`, `ngram_search_differential.test`, `build_scale.test`, `ngram_api.test`, `ngram_search_scale.test`, `ngram_refresh_bounded.test`, `ngram_rewrite.test`, `ngram_long_inputs.test`, the three Python drivers (`ngram_harness.multiset_mismatch`; the churn driver's `extra`/`missing` split keeps its two directions) and the crash driver's differential. Runtime modes are pinned by `EXPLAIN` in `ngram_api.test`, `ngram_query_bounds.test`, `ngram_long_inputs.test` and the transparent driver's per-trial rewrite check. `ngram_long_inputs.test`: 4 KiB, 64 KiB and 256 KiB rows with the needle at the start, middle and end, `İstanbul Straße` folds at the end of a 64 KiB row, seven predicate shapes against the extension-disabled answer, a 128 KiB needle of distinct grams (at a 256 MB limit `EXPLAIN ANALYZE` pins the index mode and three rows match; a 70,000-byte substring matches the same rows; a one-byte extension finds none), and the same needle over the key budget at an 8 MB limit: the explicit path reports the "query grams exceed query memory budget" fallback, the transparent plan shows no index scan, both answer exactly, and `ngram_candidates` refuses with that message. `DecomposeNeedle` refuses needles longer than 16 bytes per admitted key before extracting grams and checks the interrupt flag every 4,096 grams, so the work is linear and bounded by the budget. Same-size key collisions: `gram_key.test` and the harness's `query/gram-key-collisions` (19A). |
| Complete | Test | 22C: Real format fixtures | `test/fixtures/README.md` records each file's format, its producing build (identified by the format the file carries) and committing commit (format 3: 4243564, 2026-09-01; formats 4 and 5: d28ccab, 2026-09-08), contents (verified by opening the files read-only: `docs(s)` with 3, 7 and 7 rows; marks 4; guard indexes and storage tables as listed) and what the harness asserts: the format-3 and format-4 files are listed `MALFORMED` with their format, refuse creation and search, fall back exactly on the transparent path, and drop by reference cleanly; the format-5 file reopens `READY`, probes exactly across its persisted tail, refreshes to mark 6 and drops cleanly. Format-5 crash coverage is `scripts/crash_maintenance.py` (seed 20261318, 0 failures on this build), which kills refresh, merge, purge and the bounded loop and reopens. |
| Complete | Work | 22D: Selectable, deterministic C++ harness | The harness's main holds a table of tests grouped by mechanism (`lifecycle`, `registry`, `fixtures`, `query`); `--only PATTERN` (repeatable) keeps the tests whose `mechanism/name` contains the pattern, fixtures are skipped without a directory, and a run reports each test as it starts. `registry/scale` reports its timings and asserts only counts. Cancellation is deterministic through the stream (chunk-counted) and the hooks (22A); the purge's timed interrupt is the documented remainder. Coverage policy in `test/README.md`: child processes through `std::system` with POSIX exit status and the `timeout` command, so Linux and macOS run the harness and Windows the SQL suite. The file stays one translation unit: selection by mechanism replaces a file split, which would have moved 2,900 lines for no new check. |
| Complete | Work | 22E: Shared Python harness core | `scripts/ngram_harness.py`: `Cli` (one process per script, CSV mode, attached or not), `Cli.index_ref` (format-aware lookup through the public listing, status filter per driver), `storage_table`, the index-state and postings digests, and `multiset_mismatch`. The three drivers import it and keep their corpus generation, so every CI seed reproduces: differential 20261314 (1,764 checks) and transparent 20261315 (886 checks), churn 20261316 and 20261318 (320 checks each, 0 detector verdicts), crash 20261318 (0 failures) on this build. |
| Incomplete | Work | 22F: Nightly and focused CI | `Correctness.yml`: the pull-request sanitizer lane runs ten files (lifecycle, search, rewrite, maintenance, api, gram_key, query_bounds, refresh_concurrent, search_concurrent, long_inputs) plus the harness; a format check (`make format-check`, clang-format 11.0.1, black, cmake-format) runs before the release build; the sanitizer build sets `NGRAM_STRICT_WARNINGS=ON`, a CMake option that compiles the extension's own sources with `-Wall -Wextra -Werror` (nine redundant moves in return statements were the only findings). `cmake_minimum_required(VERSION 3.5...3.29)` matches the host. `Nightly.yml` (06:17 UTC, on dispatch, and on a push that changes its definition, since a workflow can be dispatched only once it exists on the default branch): the drivers at full size on a release build, the whole suite and the harness under ASAN/UBSAN, and the concurrency files plus the query harness under ThreadSanitizer in the host's own configuration (clang, RelWithDebInfo, `duckdb/.sanitizer-thread-suppressions.txt`, as `duckdb/.github/workflows/Main.yml` runs it). First runs on the pushed source d0a781b: Correctness [34376945939](https://github.com/danthegoodman1/duckdb-ngram/actions/runs/34376945939) passed its release suite, drivers and format check and timed out in the sanitizer lane on `ngram_parallel.test`, and the rerun [34391482857](https://github.com/danthegoodman1/duckdb-ngram/actions/runs/34391482857) on `ngram_batch_order.test` (each more than fifteen minutes under ASAN on the runner, the run's other files passing), so those two files run only in the nightly lanes and the pull-request lane keeps ten, on which Correctness [34394125570](https://github.com/danthegoodman1/duckdb-ngram/actions/runs/34394125570) is green (release suite with the harness, fixed-seed drivers, format check, and the ten-file ASAN/UBSAN lane with the harness); Nightly [34376946011](https://github.com/danthegoodman1/duckdb-ngram/actions/runs/34376946011): the ThreadSanitizer lane reported a lock-order inversion in `GuardBuildGlobalInit` (the guard build holds the table's append lock for the whole build under the host's pipeline lock, the same shape the host suppresses for its own `InitializeIndexes`), now suppressed in `test/tsan-suppressions.txt` with the reasoning, and the rerun [34391482959](https://github.com/danthegoodman1/duckdb-ngram/actions/runs/34391482959) then reported a data race with no extension frame, the host's progress bar reading a hash join's partition mask while a worker finalizes the join (`JoinHashTable::FinishedPartitionCount`, v1.5.5), suppressed with that attribution; that rerun's driver lane (crash, churn, differential at full size) passed and its ASAN/UBSAN lane passed the whole suite (31 files, 3,958 assertions) and the harness (24 tests) inside the four-hour budget that replaced the first run's two hours, after which the first run had stopped at 27 files with the output reduced to its last three lines. The nightly on that suppression, [34403566888](https://github.com/danthegoodman1/duckdb-ngram/actions/runs/34403566888), passed `ngram_parallel` under ThreadSanitizer and stopped on `ngram_batch_order` with "unlock of an unlocked mutex (or by a wrong thread)": the guard build's append lock (`GuardBuildGlobalState`) is taken by the worker that initializes the sink and released when the client thread destroys the plan, which the C++ standard leaves undefined and glibc's default mutex completes; suppressed with that attribution and recorded as the deferred row below. Missing: the nightly on that suppression, whose ThreadSanitizer lane closes the row. |
| Deferred | Work | 22G: Guard build's append lock crosses threads | `GuardBuildGlobalState` holds `DataTable`'s append lock from the sink's initialization (a worker thread) until the host destroys the create-index plan (the client thread), so that the rowid baseline and `AddIndex` are one window against committed appends. A `std::mutex` released by another thread is undefined by the standard; glibc's default mutex completes it, the host's own `StorageLockKey` held across statements has the same shape, and ThreadSanitizer reports it (suppressed in `test/tsan-suppressions.txt`). A fix needs the host to expose append exclusion whose ownership can move between threads, or to attach the index inside the callback that reads the baseline; both are host changes. |
| Incomplete | Gate | Tests cover the changed mechanisms | Fail-before evidence: the publication barrier holds segment 0 until segment 1 is pending, which the pre-19D queue emitted out of order; the multiset oracle counts a duplicated accelerated row where `EXCEPT` did not (`ngram_search_differential.test` header); the decode-time interrupt surfaces where a stranded worker would hang the harness. No correctness assertion depends on machine speed (registry-scale timings are reports). Crash coverage runs on format 5 with 0 failures. Distribution matrix on 6325062: [34372906707](https://github.com/danthegoodman1/duckdb-ngram/actions/runs/34372906707) green on every target. Missing: the recorded nightly run and the Correctness run on the pushed source; the gate closes with their URLs. |

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
| Complete | Doc | 23A: README and contract | The contract's tail-scan item no longer claims physical row-group pruning: the tail is read by a scan positioned at the first row group past the mark (19C). The README's storage description, lifecycle statuses and metadata surface are the format-5, Phase 21 ones (registry row, segments table, guard; `ngram_indexes()` and `ngram_index_stats()` as table functions; drop by reference); the work/memory distinction is stated in "Querying" (decode work, reserved memory, candidate density as three separate admission checks) and the settings table; installation says the community submission is prepared and the direct load is the working path today; the platform section describes the matrix the workflow builds and points at the recorded runs rather than the Phase 9 numbers; the staleness section links `docs/design.md` and the build section links `docs/UPDATING.md` and `test/README.md`. 21H is deferred, so the semantics section keeps the index-dependent `ngram_search` behavior it documents. |
| Complete | Doc | 23B: Plans and current design note | `docs/design.md`: ownership (the three catalog objects, keys, segments, generations, the guard token as the identity anchor), snapshot and visibility (the three disjoint parts of an answer, the refresh mark rule, the no-op shortcuts and the fence, the append path and the host hazard), the probe and its three admission budgets, publication order and the batch-index contract, memory and work accounting, host dependencies, and the one-line invariants the tests hold. The old physical-pruning claim is corrected rather than copied. `ngram_index_plan.md` moved to `docs/plan/` as historical evidence with its references fixed (the plan header, the settings comment that cites its K sweep, the submission notes); `ngram_review_plan.md` stays at the root while it is the active ledger. |
| Complete | Doc | 23C: Repository-specific maintenance/test docs | `docs/UPDATING.md` rewritten for this repository: where each pin lives (gitlinks, workflow versions, the guard's host identity and the harness test that pins it, the documented identity, the evidence tool's four constants), the seven-step host bump with the host seams to check, and the deployment note; `docs/upstream/duckdb-empty-batch-insert.md` names the test that covers its workaround; `scripts/extension-upload.sh` (template remnant, no caller in this repository) removed. `test/README.md` (Phase 22) describes the four kinds of tests, the harness mechanisms and `--only`, the drivers and their shared core, and the CI lanes. |
| Complete | Doc | 23D: Benchmark and packaging coherence | The release evidence has one owner: the artifact `benchmarks/artifacts/enwik9-current-v1.json`, rendered by `release_evidence.py generate` into the README block and `benchmarks/RESULTS.md`, verified by `check` on every push and by `check --current-source` on tags and dispatches. Corpus preparation is `collect`'s acquisition step (download of the published archive, size and digest checks, normalization to one line per row with recorded digests). The superseded enwik9 artifact (b6a388c8, 2026-08-12) validates with the tool and tree of its own commit, which git history provides; the current tool's frozen protocol (candidate fraction 0.02) rejects it, and the tool's format-3 registry branch, which no artifact in the tree reaches any more, goes with the next collection since the artifact freezes the tool's hash. The ClickHouse artifacts are untouched and `clickhouse_compare.py render` regenerates `CLICKHOUSE.md` from them byte for byte; the benchmark README names their source and date. Packaging: `description.yml` carries the Phase 21 surface and the README's opt-in rationale; `SUBMISSION.md` records the public repository, points at the recorded matrix runs, and holds the current-format installability run. |
| Complete | Work | 23E: Fresh evidence and release | `release_evidence.py collect` on engine commit `6fb01c606165` (build commit `6fb01c606165`; the tool digest and the `src/**` digest are recorded in the artifact): clean release build, enwik9 acquired and normalized, three load/build pairs, the 21-observation query campaign. Index build 7.554 s median (7.477–8.880 s); build-process max RSS 7.920 GiB median. Queries: rare (index, 1 match): search 1 / 2 ms / 0–4 ms against scan 42 / 43 ms / 40–45 ms; moderate (index, 26,068 matches): search 6 / 7 ms / 5–8 ms against scan 28 / 30 ms / 26–31 ms; dense (full-scan-fallback, 1,963,067 matches): search 37 / 40 ms / 36–43 ms against scan 37 / 39 ms / 36–45 ms. `validate --binary build/release/duckdb`, `test --binary` and `check --current-source` pass on the committed source. Installability: the official DuckDB v1.5.5 (Variegata) d8cdaa33fd CLI (SHA-256 `3d33b1df037cb049155c393778df7853fafb23e9d49d7c9cacdde4dd67155788`) installed the artifact (SHA-256 `846d5408fec67f9b041062c2593333ae65151b7eb928a7aa7d322edb4b3fc17b`) from a repository in DuckDB's layout, built and searched an index, reopened the file in a second process with the index `READY`, refreshed after a covered update and a tail insert, dropped by reference leaving no ngram object, and an extension-free third process read the table (`packaging/SUBMISSION.md`; script and transcript in `docs/review/2026-09-09/install_check.sh` and `install_check.log`). The release tag is the maintainer's act and is not part of this ledger. |
| Incomplete | Gate | Traceable release claims | `check --current-source`, `validate --binary` and `test --binary` pass on the evidence commit; every README example ran on the CLI with the documented output (Phase 23 review); links resolve; the stock-CLI install run is recorded (`docs/review/2026-09-09/install_check.log`). Missing: the Correctness run on the pushed evidence commit and the first nightly run; the gate closes with their URLs. |
