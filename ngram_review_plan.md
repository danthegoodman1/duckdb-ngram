# Development Plan: ngram Review Follow-up (Phases 16 to 23)

This plan continues `ngram_index_plan.md`, whose Phases 1 to 15 are complete. It comes from a
2026-09-01 review of the whole codebase for correctness, performance, architecture, and
simplicity. Phase numbering continues from 16 so identifiers stay unique across both files.

## Overarching Goal

Ship a first release of the `ngram` extension that is correct in every process state, half
the size it is today, and fast at the fixed per-query cost, without weakening the guarantee:
every exact query path returns every matching row and no wrong row in every index state.

The review found one defect that breaks every query on an index for the rest of the process
after a documented maintenance call, no green CI run on the current tree, roughly 1,900 lines
of production code that defend against states the design itself creates, and a fixed
per-query cost dominated by string scans of shadow tables rather than by postings work. The
phases below fix the defect first, make CI true, delete before adding, then cut the fixed
cost, then settle the public surface before tagging `v0.1.0`.

Non-goals: a native postings backend, migration paths for the unreleased on-disk formats,
fuzzy or ranked search, and any change to the recheck-everything execution model.

## Review Findings That Drive This Plan

Evidence status: CONFIRMED means reproduced against `build/release/duckdb` in this review or
confirmed by reading the pinned DuckDB v1.5.5 source; MEASURED means a recorded number.

| ID | Finding | Status | Phase item |
| --- | --- | --- | --- |
| F1 | A no-op `ngram_refresh` leaves the in-process stats table empty; every later `ngram_search` and transparent `LIKE` in that process raises "the index is malformed". Root cause is a DuckDB v1.5.5 bug: an empty batch `INSERT ... ORDER BY`, then `DELETE FROM t`, then a batch `INSERT ... ORDER BY` of at least 122,880 rows in one transaction leaves the live table empty until reopen. The refresh script has exactly that shape whenever the packed delta is empty (`src/ngram_maintenance.cpp:783-803`). The README's recommended catch-up loop ends with such a call. | CONFIRMED (plain table and end to end; reopen recovers) | 16A |
| F2 | The guard latches `unsafe_reuse` after any commit that fails in a later index, with no rowid reuse: `Append` advances `max_seen` unconditionally and `TryDelete` is a no-op (`src/rowid_guard.cpp:150-181`); the host appends to indexes before rows land (`duckdb/src/storage/local_storage.cpp:603-620`). One rejected duplicate key on a UNIQUE index created after the guard forces full scans and refuses maintenance until rebuild. | CONFIRMED by code reading | 16B |
| F3 | Reuse of rowids above the high-water mark also latches, although the tail scan already covers those rows. | CONFIRMED by code reading | 16B (decision) |
| F4 | A cached plan errors instead of falling back after drop plus re-create (`src/index_pragmas.cpp:1034-1044` throws on any oid change). Unbound guard replay buffer read without `entry.lock` (`src/rowid_guard.cpp:429`). Extension-owned `InternalException` throws invalidate the whole database. | CONFIRMED by code reading | 16C |
| F5 | No CI run is green on the current tree: both runs on `8667a91` were cancelled; the last success is `08bb554`, before format 3. The repo is public. README, `packaging/SUBMISSION.md`, and `description.yml` describe CI historically. | CONFIRMED (`gh run list`, `gh repo view`) | 16D |
| F6 | Rare-needle latency of 7 ms is 77% stats-table scan: the `InFilter` is only a zone-map hint, so every row of each surviving row group is FSST-decoded and hashed (`src/ngram_search.cpp:683-709`); K serial manifest scans add 12%. A 1,000-row table costs 0.5 ms per query. | MEASURED (gdb sampling, enwik9) | 19A, 19B, 19C, 19F |
| F7 | Fetch parallelism is capped at the number of 2^20-row segments, so a table under 11M rows uses at most 11 threads and one dense segment runs single-threaded (51k candidates: 106 ms at 1 and 24 threads). | MEASURED | 19D |
| F8 | Refresh rewrites the whole stats table every call (no-op refresh 175 ms, 150 ms of it fold; the DB grows about 6 MB per call until checkpoint). No-op compact costs about 400 ms materializing every segment key. | MEASURED | 19B, 20A |
| F9 | Build is the hash aggregate: 9.1 s of 9.5 s, 60% parallel efficiency, 12.3 GB RSS for 963M pairs, 8 bytes per in-segment rowid in state. The codec is not the bottleneck (350M postings/s decode); hybrid bitmap containers would save about 1% of bytes on enwik9. | MEASURED | 20B |
| F10 | Two identity systems overlap: the table fingerprint (oids, instance id, schema fingerprint, column type) and the guard name plus random token. With a guard present DuckDB refuses every ALTER except ADD COLUMN, SET DEFAULT, foreign-key bookkeeping, and comments, and drops indexes with their table (`duckdb/src/catalog/dependency_manager.cpp:642-690, 273-274`), so the guard alone proves identity. | CONFIRMED by reading host source | 17B |
| F11 | Legacy v2 support, per-index opaque schemas, a separate meta table, 8 lifecycle states (3 reachable through the public API), two creation "protector" paths, a 27-argument execution-time scalar, and 7 unreachable overflow decline strings are all removable without touching the guarantee. Nothing has shipped. | CONFIRMED by code reading (line counts in Phase 17 and 18 ledgers) | 17A, 17C, 17D, 18B, 18C |
| F12 | The validation triple (meta, stale reason, guard reason) is hand-inlined at 7 call sites; five functions exceed 150 lines (`PlanIndexProbe` 293, `CreateNgramIndexQuery` 230, `ObserveCatalog` 212, `RefreshNgramIndexQuery` 201, `MaintenanceGuardFunction` 159). | CONFIRMED by grep | 18A, 18C, 18D |
| F13 | Suite gaps: no refresh-or-compact versus search race, no string over 60 bytes, one non-ASCII ILIKE case, no cross-segment search on the partitioned build fixture, two wall-clock assertions in the C++ harness, `crash_maintenance.py` and the concurrency files absent from the sanitizer job. | CONFIRMED by grep | 16A, 22A to 22E |
| F14 | Documentation drift: `RESULTS.md` is a copy of the README block while three documents describe it as the scale results; `SUBMISSION.md` says the repo is private and lists four of five settings; `description.yml` has `ref: 000...`, an outdated `ngram_refresh` signature, and says acceleration stays opt-in pending bounded materialization; `docs/UPDATING.md` and `test/README.md` are template boilerplate; the `vacuum_rebuild_indexes` sentence is wrong (any index blocks moving vacuum by default). | CONFIRMED by reading | 23A to 23D |

Verified and holding, with no plan item: normalization parity between build, needle
decomposition, and DuckDB's `ILIKE` (same per-codepoint `utf8proc_tolower`; Kelvin sign, dotted
I, sharp s, final sigma matched); `LIKE` splitting on `%` is exact because plain `~~` has no
implicit escape; meta, postings, and base rows are read in one transaction and commit order
equals rowid order on v1.5.5; the tail filter is row-exact; vacuum with a non-ART index drops
only trailing fully deleted row groups; UPDATE, ON CONFLICT, INSERT OR REPLACE, and MERGE all
become delete plus insert on covered columns; the codec rejects overflow, zero deltas, and
trailing bytes; `DataTable::Fetch` skips invisible rowids; nested shared locks cannot deadlock.

## Implementation Principles

The principles in `ngram_index_plan.md` remain in force. This plan adds:

- The on-disk format is unreleased. Bump the format version freely, ship no migration code,
  and make a mismatched format drop-only with a rebuild instruction. Exactly one such path
  exists.
- The rowid guard, identified by its name and random token, is the single identity anchor for
  a table and its indexed column. Every read path validates it before touching postings.
- Delete a state before handling it. A new error string or lifecycle status needs a reachable
  scenario written down in the ledger.
- Every performance number in a ledger row comes from a recorded command on the normalized
  enwik9 corpus (`.tmp/clickhouse-phase15/corpus/enwik9.lines.hex.tsv`, 10,920,423 rows) at 24
  threads, or from `benchmarks/release_evidence.py`. Prose numbers without a command are gaps.
- A phase is complete only when `make test_release` passes locally, the Correctness workflow
  is green on the phase's head commit, and `git diff --check` is clean.
- `Decision` rows record user-facing choices. The owner confirmed every recommendation on
  2026-09-01; a new decision row needs the same confirmation before its phase starts.
- Implementers make the smallest change that closes a row. Reviewers reject growth in
  production line count for a phase whose goal is deletion.

## Testing Strategy

- Differential oracle first: every accelerated path is compared with brute-force `LIKE` or
  `contains` in-suite and in the fixed-seed Python harnesses; results must be identical.
- Each defect in the findings table gets a regression test sized to its trigger. F1 needs an
  index with at least 122,880 stats rows, so its test builds one and asserts search and `LIKE`
  in the same process after a no-op refresh and a no-op bounded refresh.
- CI is the gate: the Correctness workflow on every phase head; the distribution matrix on the
  release candidate; a nightly job for the full sanitizer suite, the crash harness, and
  ThreadSanitizer on the concurrency files.
- Benchmark deltas are recorded per performance phase: enwik9 warm p50 for the rare,
  moderate, and dense needles, build wall and peak RSS, no-op refresh and compact wall,
  before and after, with the exact commands.
- Behavior-neutral refactors prove neutrality with the existing fixed seeds (explicit
  `20261314`, transparent `20261315`, churn `20261316`, strict churn `20261318`) producing the
  same pass counts.

## Phase 16: Fix the Refresh Defect and Restore CI Truth

Goal:
No maintenance call can break queries for the rest of the process, the guard latches unsafe
only when rowid reuse is possible, and the current tree has a green CI run.

Scope:
- Remove the empty-batch-insert shape from every generated maintenance script, add the
  regression test, and report the host bug upstream with the plain-table reproduction.
- Latch `unsafe_reuse` only when an append can reuse a rowid that a checkpoint has discarded.
- Turn execution-time identity changes on read paths into fallbacks, take `entry.lock` around
  the unbound replay read, and stop throwing `InternalException` for extension-owned errors.
- Make the Correctness workflow complete on HEAD, restrict the distribution matrix to `main`,
  tags, pull requests, and dispatch, and fix the tracked lock file and ignore rules.

Out of scope:
- Any storage or identity redesign (Phase 17), any latency work (Phase 19).

Completion gate:
The F1 regression test passes; the F2 scenario (guard, later UNIQUE index, rejected duplicate
commit, next insert) leaves `stale_reason` NULL; the Correctness workflow is green on the
phase head; the distribution matrix has been dispatched on that head with its result recorded.

Testing plan:
- New `test/sql/ngram_refresh_noop.test`: at least 150,000 rows of random 90-symbol text (about
  260,000 distinct trigrams), build, no-op `ngram_refresh`, no-op bounded refresh, no-op
  `ngram_compact`, then `ngram_search`, `LIKE` under `ngram_auto_accelerate`, and
  `ngram_index_stats` in the same process; repeat after `CHECKPOINT`.
- Harness case for F2 using two connections and a UNIQUE ART created after the guard.
- Harness case for F4: prepare a `LIKE` statement, drop and re-create the index, execute; expect
  a sequential-scan result rather than an error.
- Full `make test_release`, the four fixed-seed harness commands, and the workflow runs.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 16A: Remove the empty-insert-then-delete shape from refresh and compact | Commit `ac55e30`. The packed delta is folded into the single stats rewrite statement (`src/ngram_maintenance.cpp`), so nothing inserts into `stats` before its `DELETE`; compact audited. `test/sql/ngram_refresh_noop.test` (94 assertions) fails on the old script with `segments and stats descriptor counts disagree` and passes now; `docs/upstream/duckdb-empty-batch-insert.md` holds the verified plain-table reproduction. Open owner action: file the upstream DuckDB issue from that note. |
| Complete | Work | 16B: Latch only on possible reuse | Commit `ac55e30`. Each `max_seen` advance records the header checkpoint iteration; an append at or below `max_seen` latches only when that iteration is unknown or changed (`src/rowid_guard.cpp` `ObservableCheckpointIteration`, `Observe`). Nothing new is persisted; guards bound from disk or WAL and in-memory databases stay conservative; a checkpoint between a rejected commit and its retry still latches. Harness `TestRejectedCommitKeepsGuard` leaves `stale_reason` NULL and keeps `NGRAM_INDEX_SCAN`, and fails on the old code. Reviewer verified every host citation. |
| Complete | Decision | 16B-2: Treat reuse above the high-water mark as safe | Decided 2026-09-01: deferred. Owner confirmed the recommendation; no work in this plan. Revisit if a user reports full scans after deleting and re-inserting rows above the mark. |
| Complete | Work | 16C: Read-path fallbacks and small fixes | Commit `ac55e30`. `IndexLocationAvailable(..., changed_is_absent)` makes `SearchInitGlobal` and `TryProbeIndex` fall back to the exact scan on a changed identity; write paths keep the throw. `entry.lock` around `HasBufferedReplays`; six `InternalException` sites are `InvalidInputException`. F4 coverage: the exact-fallback assertions in harness cases `swapped_part` and `registry_swap`; the `NGRAM_INDEX_SCAN` changed branch has no deterministic test because prepared base-table scans re-bind after catalog changes. |
| Complete | Work | 16D: CI completes on HEAD | Commits `ac55e30`, `cd63a0c`, `20b864a`. Pinned `hendrikmuhs/ccache-action` v1.2.24 keyed on the duckdb gitlink and `src/**`; parallelism 3 release, 2 debug; `OVERRIDE_GIT_DESCRIBE=v1.5.5` in both build jobs because the shallow, tagless submodule checkout otherwise builds a dummy v0.0.1 that the host pin rejects; the pin accepts any 7-to-40 character prefix of commit `d8cdaa33fd...`; fixture-drift step; `scripts/verify_pins.sh` (gitlink match plus clean submodule trees) shared by three jobs and `release_evidence.py`; relaxed `check` on pull requests and `check --current-source` on tags and dispatch; distribution triggers limited to `main`, `v*`, pull requests, dispatch; the POSIX harness hook skipped on Windows; `MinValue<idx_t>` fix for wasm32 and macOS; lock file untracked; `.gitignore` extended; SUBMISSION matrix checkbox unticked pending the release-candidate run. Correctness green on `20b864a`: https://github.com/danthegoodman1/duckdb-ngram/actions/runs/33584571329. Matrix green on all ten default targets: https://github.com/danthegoodman1/duckdb-ngram/actions/runs/33584571470. |
| Complete | Gate | Queries survive maintenance in-process and CI is green | `ngram_refresh_noop.test` and `TestRejectedCommitKeepsGuard` are in the suite and pass on GitHub. Reviewer approved after six rounds (three on the code, three on CI feedback). Correctness run https://github.com/danthegoodman1/duckdb-ngram/actions/runs/33584571329; matrix run https://github.com/danthegoodman1/duckdb-ngram/actions/runs/33584571470, the first full matrix since Phase 9. |
| Complete | Test | Regression and workflow evidence | `make test_release`: 2993 assertions in 25 cases locally and in the Correctness release job. Fixed seeds 20261314 1764/0, 20261315 886/0, 20261316 320/0, 20261318 320/0 locally and in CI. Sanitizer job green. `release_evidence.py check` PASS; `verify_pins.sh` exit 0; `git diff --check` clean. Runs: https://github.com/danthegoodman1/duckdb-ngram/actions/runs/33584571329, https://github.com/danthegoodman1/duckdb-ngram/actions/runs/33584571470. |

Plain-table reproduction of the host bug behind F1 (DuckDB v1.5.5, default threads; the
control without the empty insert returns 200,000 at every stage):

```sql
CREATE TABLE t AS SELECT i, i::VARCHAR AS s FROM range(200000) r(i);
CREATE TABLE src AS SELECT i, i::VARCHAR AS s FROM range(200000) r(i);
BEGIN;
INSERT INTO t SELECT * FROM src WHERE i < 0 ORDER BY s;   -- empty batch insert
CREATE TEMP TABLE folded AS SELECT * FROM t;
DELETE FROM t;
INSERT INTO t SELECT * FROM folded ORDER BY s;            -- 200,000 rows
SELECT count(*) FROM t;                                   -- 0 (expected 200000)
COMMIT;
SELECT count(*) FROM t;                                   -- 0 until the file is reopened
```

## Phase 17: One Identity Anchor, One Storage Layout (Format 4)

Goal:
The guard is the only proof of table identity, each index is one registry row plus one or two
ordinary tables, the lifecycle has four states, and nothing remains of layouts that never
shipped.

Scope:
- Delete legacy v2 support and every pre-registry naming helper.
- Delete the table fingerprint, `CertainStaleReason`, `InstanceId`, the `REPLACED` status, and the
  five meta columns that carried them; keep `total_rows` for the tail estimate.
- Make the registry row the metadata row (index id, owner schema/table/column, gram size, case
  flag, high-water mark, guard name, guard token, format version). Store postings in
  `__ngram.segments_<uuid>` and, until Phase 19 decides, stats in `__ngram.stats_<uuid>`. Delete
  the per-index schema, the meta table, `ReadMetaHeader`, the eight-constraint registry audit,
  and the `UNREGISTERED` and `MISSING_STORAGE` states.
- One rowid guard per table, covering every VARCHAR present at its creation, reference-counted
  through the registry and dropped with the table's last index. Delete both creation
  protector paths and the native-ART lookup. Indexing a VARCHAR added after the guard requires
  dropping and rebuilding the table's ngram indexes; the README states this once.
- Format 4. Any other format is listed as `MALFORMED` with the version in the reason and is
  drop-only through the token-checked generic drop; no other legacy path exists.

Out of scope:
- Changing the gram key type or the stats table's existence (Phase 19).
- Module layout and function splitting (Phase 18).

Completion gate:
Production source shrinks by at least 600 lines; `ngram_indexes` reports only `READY`,
`SCAN_ONLY`, `ORPHAN`, and `MALFORMED`; every read path calls the guard verdict before any
postings read; the lifecycle, identity, and guard suites pass with their legacy and
fingerprint cases deleted rather than adapted.

Testing plan:
- Rewrite `test/sql/ngram_lifecycle.test` for four states; delete
  `ngram_maintenance_identity.test` cases that exist only for the fingerprint, keeping the
  CREATE OR REPLACE, DROP plus CREATE, reopen, and detach/attach scenarios asserted through
  the guard verdict.
- Two indexes on one table share one guard; dropping one keeps the other `READY`; dropping the
  last removes the guard; a VARCHAR added afterward is refused with the documented message.
- A format-3 database fixture opens read-only, lists as `MALFORMED`, and drops by id.
- Fixed-seed harnesses and `make test_release`; Correctness green.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 17A: Delete legacy v2 | Missing: removal of `ShadowSchemaName`, `MetaTableName`, `SegmentsTableName`, `StatsTableName`, `Hex`, `DecodeHex`, `LegacyIndexRef`, `ParseLegacyIndexRef`, `ExistingMetaTables`, the legacy branches of `ObserveCatalog` and `ExistingIndexes`, `LegacyRowIdGuardReason`, `ReadMetaFormatVersion`, the `LEGACY_REBUILD` status and `kind` column; harness `TestLegacyTransition` and `CopyRegisteredToLegacy` deleted; about 230 production lines. |
| Incomplete | Work | 17B: Guard as sole identity | Missing: `TableFingerprint` reduced to `total_rows`; `CertainStaleReason`, `InstanceId`, `ThrowIfCandidatesCertainlyStale`, the "index stale" fallback reason, and the oid re-check in `MaintenanceGuardFunction` deleted; meta columns `schema_fingerprint`, `column_type`, `table_oid`, `catalog_oid`, `instance_id` gone; about 180 lines. |
| Incomplete | Work | 17C: Registry row is the metadata row | Missing: `__ngram.registry` schema with the metadata columns; `__ngram.segments_<uuid>` (and `stats_<uuid>`); create and drop each one transaction; `IndexLocation` reduced to `{index_id, column}`; `IndexLocationAvailable` reduced to "registry row with this id and owner exists"; `ForeignSchemaEntries`, `ParseOpaqueSchema`, `RegisteredStorageSchema`, `ValidateRegistryShape`, `InspectRegistryForCreate` deleted; about 300 lines. |
| Incomplete | Work | 17D: One guard per table | Missing: shared guard name and token recorded per index; refcount by registry rows; `FindNativeUpdateProtector`, `NativeUpdateProtectorReason`, `RowIdGuardProtectionReason`, `CreationProtector`, and the five protector arguments deleted; about 250 lines; README paragraph on later-added VARCHAR columns. |
| Complete | Decision | 17D-2: Guard coverage at first creation | Decided 2026-09-01: keep covering every VARCHAR present at creation. Owner confirmed the recommendation; 17D documents the delete-plus-insert cost once. |
| Incomplete | Work | 17E: Format 4 and drop-only path | Missing: format constant 4; unknown format lists `MALFORMED` with the version; token-checked drop works; the single legacy path is the only one; fixture test. |
| Incomplete | Doc | 17F: Correct the vacuum sentence | Missing: README and `docs/stale-updates.md` say any index blocks moving vacuum by default and the non-ART type matters only under `vacuum_rebuild_indexes`. |
| Incomplete | Gate | Four states, one anchor, 600 fewer lines | Missing: `git diff --stat` line delta; grep proving the four status strings; list of read paths and their guard-verdict call sites. |
| Incomplete | Test | Lifecycle, shared-guard, and fixture suites | Missing: rewritten lifecycle test, shared-guard cases, format-3 fixture test, seed results, run URL. |

## Phase 18: Code Structure

Goal:
Each file owns one thing, each function fits on two screens, and validation has one entry
point, with no behavior change.

Scope:
- `ValidateIndex(context, table, index_ref) -> {meta, hwm, reason}` replaces the seven inlined
  validation triples and owns the read-path fallback policy.
- Replace the 27-argument `__ngram_maintenance_guard` and the 10-argument
  `__ngram_rowid_guard_validate` with one handle: prepared state in a `ClientContextState`
  keyed by a UUID literal in the generated script.
- Split `PlanIndexProbe`, `CreateNgramIndexQuery`, `ObserveCatalog`, `RefreshNgramIndexQuery`,
  and `MaintenanceGuardFunction`; route `CheckedAdd` and `CheckedMultiply` failures to one
  `InvalidInputException` and keep four real decline reasons.
- Adopt the target module layout: `settings`, `gram`, `postings`, `catalog`, `index_state`,
  `rowid_guard`, `fence`, `build_sql`, `pragmas`, `probe`, `search_core`, `search_functions`,
  `rewrite`, plus `ngram_extension.cpp` for registration order.
- Bind guards on the first query or maintenance call that touches them instead of the
  load-time and connection-close callbacks, if a deadlock check against
  `TableIndexList::Bind` passes.
- Comment and naming hygiene: rewrite history-narrating comments as current behavior, reserve
  "guard" for the rowid guard, reference DuckDB's `MAX_ROW_ID` instead of the literal, define
  `ScratchName` and `FETCH_BATCHES_PER_SEGMENT` once, move `MetaInfo` to its own header, and
  stop exporting partition internals from the pragma header.

Out of scope:
- Any change to generated SQL semantics, storage, or public behavior.

Completion gate:
No function over 150 lines and no file over 700 lines in `src/`; identical assertion counts and
identical fixed-seed results before and after; production line count does not grow.

Testing plan:
- Run the full suite and the four seeds before and after; record both.
- Harness case for lazy binding: open, query without any prior bind, `DETACH`, re-attach, query;
  expect `READY` and no rebuild.
- `make tidy-check` and `git diff --check`.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 18A: Single validation entry point | Missing: `ValidateIndex` in `index_state.cpp`; the seven call sites (`ngram_search.cpp:1441-1458, 1651-1659`, `ngram_rewrite.cpp:360-377, 648-661`, `index_pragmas.cpp:405-421, 1377-1396`, `ngram_maintenance.cpp:194-214`) reduced to one call each. |
| Incomplete | Work | 18B: Handle-based execution-time scalars | Missing: `ClientContextState` with the prepared maintenance state; both `MaintenanceGuardCall` builders and the positional decode block deleted; `__ngram_rowid_guard_validate` folded in; about 90 lines. |
| Incomplete | Work | 18C: Split long functions and collapse overflow declines | Missing: the five functions under 150 lines each; decline strings reduced to candidate fraction, decoded-rowid budget, segment memory, manifest memory; overflow paths throw one message. |
| Incomplete | Work | 18D: Module layout | Missing: the fourteen files with one-line responsibility headers; `CMakeLists.txt` source list; no file over 700 lines. |
| Incomplete | Work | 18E: Lazy guard binding | Missing: deadlock analysis against `duckdb/src/storage/table_index_list.cpp` recorded in the ledger; `OnExtensionLoaded` and `OnConnectionClosed` callbacks removed; DETACH caveat removed from README; harness case passes. Blocked by the analysis if it finds a lock-order cycle, in which case this row becomes a documented rejection. |
| Incomplete | Work | 18F: Comment, naming, and header hygiene | Missing: the listed comments rewritten; `MAX_ROW_ID` referenced; duplicates removed; `MetaInfo` header; pragma header exports only what pragmas need. |
| Incomplete | Gate | Behavior-neutral restructuring | Missing: before-and-after assertion counts and seed results; function and file length report from a one-line awk script recorded here. |
| Incomplete | Test | Suite parity and lazy-bind case | Missing: harness case name, run URL. |

## Phase 19: Cut the Fixed Query Cost

Goal:
A rare needle on the 10.9M-row enwik9 index answers in about 2 ms warm, a moderate needle in
about 10 ms, with the same memory bounds and identical results.

Scope:
- Store the gram as a fixed-width integer key: order-preserving byte-packed `UHUGEINT` when the
  normalized gram fits 16 bytes, a 64-bit hash otherwise. Key collisions merge postings and
  only widen candidates. The key is the leading sorted column so a `ConstantFilter` is evaluated
  natively on bit-packed storage.
- Drop the stats table. Select the rarest K grams from segment rows: one parallel scan of the
  segments table with an `InFilter` hint over all needle keys collects descriptors and counts
  for every gram, then the plan keeps K. Delete `ReadGramStats`, the refresh fold, the compact
  stats rebuild, and the stats-versus-segments consistency checks. Bound the manifest as it
  grows against the query budget.
- Hand out candidates in fixed rowid batches through a shared cursor after each segment
  intersection, so fetch parallelism follows candidate count rather than segment count.
- Reuse per-thread decode buffers, resolve shadow column ids once per plan, intersect each
  segment's grams smallest-first by per-segment count.
- Carry `MetaInfo` from bind to init and revalidate with the registry row and guard verdict
  instead of re-resolving and re-reading twice.
- Re-derive the candidate-fraction default from the measured per-candidate fetch cost and the
  new parallelism, and record the model.

Out of scope:
- Postings encoding changes (measured as not the bottleneck).
- Any change to recheck semantics or the tail scan.

Completion gate:
On enwik9 at 24 threads warm: rare p50 at most 3 ms, moderate p50 at most 12 ms, dense
unchanged within 10%; a 1,000-row table costs under 0.3 ms per query; the 51k-candidate
single-segment case scales with threads; `ngram_query_bounds.test` passes with its budgets
re-derived; all differential suites identical.

Testing plan:
- Extend `ngram_query_bounds.test` for the integer key, hash-collision widening (two grams
  forced to one key in a unit test of the key function), and manifest budget declines without
  stats.
- `build_scale.test` gains cross-segment `EXCEPT` identity queries on the partitioned fixture.
- Parity at threads 1, 2, 8, 24 across refresh generations and partial segments.
- Benchmark before and after with `release_evidence.py collect` and the ad hoc `.timer` loops
  recorded in the ledger.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 19A: Integer gram key | Missing: key function with tests for ASCII, 2-, 3-, and 4-byte codepoints, gram sizes 1 to 6, and the hash fallback; segments written sorted by key; probe filters use `ConstantFilter` on the key; `trigrams()` unchanged for users; internal pair emitter for build. |
| Incomplete | Work | 19B: Drop the stats table | Missing: single-scan descriptor collection over all needle keys; rarest-K from collected counts; deletions of `ReadGramStats`, the fold (`ngram_maintenance.cpp:781-804`), stats rebuild (`:1019-1023`), and the six malformed-stats throws; `ngram_index_stats.distinct_grams` computed lazily; measured rare and moderate p50 recorded against 19A alone. Gate for keeping this row: no regression over 10% on rare or moderate p50. |
| Incomplete | Work | 19C: Parallel manifest collection | Missing: `ParallelForEachUnit` over key row groups, or moot after 19B; recorded either way. |
| Incomplete | Work | 19D: Candidate-batch fetch parallelism | Missing: shared cursor over 4,096-rowid batches per intersected segment; batch index scheme preserved; 51k-candidate case time at 1 and 24 threads recorded. |
| Incomplete | Work | 19E: Decode-path allocation and order | Missing: reusable buffers in `SearchCoreLocal`; column ids resolved in `PlanIndexProbe`; per-segment smallest-first order; dense-needle CPU before and after. |
| Incomplete | Work | 19F: Bind-to-init metadata carry | Missing: one `ValidateIndex` per query; tiny-table loop time before and after (baseline 0.11 s per 200 statements). |
| Incomplete | Work | 19G: Candidate gate model | Missing: measured per-candidate fetch cost and per-row scan cost after 19D; default `ngram_max_candidate_fraction` re-derived and justified in the README settings row. |
| Incomplete | Gate | Latency targets with identical results | Missing: the enwik9 numbers with commands; parity suite results at four thread counts; `ngram_query_bounds.test` green. |
| Incomplete | Test | Key, budget, cross-segment, and parity suites | Missing: test names, seed results, run URL. |

## Phase 20: Maintenance and Build Cost

Goal:
A no-op refresh or compact costs milliseconds, a real refresh costs its tail, and build holds
half the memory per pair at the same wall time.

Scope:
- Refresh writes only its delta; compact exits early when no key is fragmented; the bounded
  loop's final no-op call is a guard check plus a progress row.
- Store 32-bit in-segment offsets in the packing aggregate state; recalibrate the per-pair
  byte estimate from measurement; write the segments table directly from the aggregate
  instead of through the `packed` temp table.
- Simplify the bounded-refresh bound: round the bound up to the next segment boundary at or
  beyond `hwm + max_rows` so the loop never issues near-empty increments, and shrink the README
  passage to one paragraph.

Out of scope:
- Postings codec or container changes.
- A background scheduler.

Completion gate:
No-op refresh and no-op compact under 20 ms on the enwik9 index; peak build RSS at most 7 GB
at the same partition count and wall time within 5% of 8.6 s; postings byte-identical to the
previous build of the same corpus.

Testing plan:
- Compare `segments` blobs and counts before and after via `EXCEPT` in both directions.
- Crash harness seed `20261317` across refresh, merge, purge, and bounded catch-up offsets.
- Record build wall, RSS, and spill with `/usr/bin/time` and the sampled temp poll.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 20A: Delta-only refresh and early-exit compact | Missing: no-op refresh and compact timings; early exit when `keys` is empty; bounded loop's final call measured. |
| Incomplete | Work | 20B: Build memory | Missing: 32-bit offsets in state; `PAIR_STATE_BYTES` recalibrated with the measurement; direct `CREATE TABLE ... AS` from the aggregate; RSS and wall recorded at auto partitions. |
| Complete | Decision | 20C: Round the bound up to a segment boundary | Decided 2026-09-01: round up to the next segment boundary. Owner confirmed the recommendation; implemented under 20A. |
| Incomplete | Gate | Cheap no-ops, lighter build, identical postings | Missing: timings, RSS, `EXCEPT` identity result, crash seed result. |
| Incomplete | Test | Identity and crash suites | Missing: command outputs, run URL. |

## Phase 21: Public Surface

Goal:
The API a community-extension reader meets is small, consistent, and defaults to being
useful.

Scope:
- `ngram_auto_accelerate` defaults to true.
- Reading operations become table functions: `ngram_indexes()` (replacing the pragma and
  `ngram_index_status`) and `ngram_index_stats(table)`; mutating operations stay pragmas.
- `drop_ngram_index(index_ref)` overload replaces `drop_ngram_index_by_id`; a named `catalog`
  parameter covers attached databases.
- `ngram_refresh` always returns the progress row.
- Remove `ngram_candidates` from the public surface (delete, or rename to `__ngram_candidates`
  for tests and benchmarks).
- Remove `ngram_max_probe_rowids`; the memory budget and candidate fraction bound work.
- Update `description.yml`, README API and settings tables, tests, and benchmark scripts.

Out of scope:
- `CREATE INDEX ... USING NGRAM` as the front door: the postings build is a multi-statement
  script and `IndexType` has no drop hook, so pragmas remain the only mechanism that runs DDL
  scripts. Rejected.

Completion gate:
A test asserts the exact public surface from `duckdb_functions()` and `duckdb_settings()`; README
and `description.yml` list the same surface; all harnesses green with the new names.

Testing plan:
- Surface test comparing catalog output to a checked list.
- Differential harnesses re-pointed at the renamed or removed functions.
- Plan-shape tests confirm the default rewrite fires and the two kill switches work.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Decision | 21A: `ngram_auto_accelerate` defaults to true | Decided 2026-09-01: default true. Owner confirmed the recommendation; implemented under 21G. |
| Complete | Decision | 21B: Reading operations as table functions | Decided 2026-09-01: yes. Owner confirmed the recommendation; implemented under 21G. |
| Complete | Decision | 21C: Drop by `index_ref` overload; delete `drop_ngram_index_by_id` and `ngram_index_status` | Decided 2026-09-01: yes. Owner confirmed the recommendation; implemented under 21G. |
| Complete | Decision | 21D: `ngram_refresh` always returns progress | Decided 2026-09-01: yes. Owner confirmed the recommendation; implemented under 21G. |
| Complete | Decision | 21E: Remove `ngram_candidates` from the public surface | Decided 2026-09-01: rename to `__ngram_candidates`. Owner confirmed the recommendation; implemented under 21G. |
| Complete | Decision | 21F: Remove `ngram_max_probe_rowids` | Decided 2026-09-01: yes. Owner confirmed the recommendation; implemented under 21G. |
| Incomplete | Work | 21G: Implement confirmed decisions and update surface docs | Missing: code, `description.yml`, README tables, test and benchmark updates, surface test. |
| Incomplete | Gate | Surface test matches docs | Missing: test name, README diff, run URL. |
| Incomplete | Test | Surface, plan-shape, and harness suites | Missing: results. |

## Phase 22: Test Infrastructure

Goal:
The suite covers the races and shapes the review found missing, the C++ harness is
selectable and deterministic, the Python harnesses share one core, and the nightly lane runs
what pull requests cannot afford.

Scope:
- Add refresh and compact racing search in `concurrentloop`, strings over 4 KiB and 64 KiB in
  the differential test and Python corpus, Unicode ILIKE parity cases with byte-length-changing
  folds, and a committed format fixture opened read-only.
- Split the C++ harness per mechanism, move SQL-only cases to sqllogictest, add `--only`, make
  cancellation deterministic, and make the registry-scale timing report-only.
- Extract `scripts/ngramharness/` shared by the three Python drivers.
- Nightly workflow: full `make test_debug`, `crash_maintenance.py`, ThreadSanitizer build of the
  three concurrency files; add those files to the pull-request sanitizer selector.
- `-Wall -Wextra -Werror` behind an option CI enables; `make tidy-check` in Correctness; raise
  `cmake_minimum_required`.

Out of scope:
- New benchmark tooling (Phase 23).

Completion gate:
Nightly job green once; no wall-clock assertion remains in the harness; the three concurrency
files run under ASAN on pull requests; Python harness line count drops by at least a third.

Testing plan:
- The new tests themselves, plus a deliberate revert of 16A to prove `ngram_refresh_noop.test`
  fails on the old script shape.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Test | 22A: Maintenance versus search races | Missing: `concurrentloop` blocks in `ngram_refresh_concurrent.test` and `ngram_search_concurrent.test` with `statement maybe` on conflicts. |
| Incomplete | Test | 22B: Long strings and Unicode ILIKE parity | Missing: rows over 4 KiB and 64 KiB with needles straddling 4 KiB in `ngram_search_differential.test` and `scripts` corpus; Kelvin sign, dotted I, capital sharp s, Cherokee, Deseret needles in `ngram_rewrite.test`. |
| Incomplete | Test | 22C: Committed format fixture | Missing: a small format-4 database under `test/fixtures/` opened read-only and queried. |
| Incomplete | Work | 22D: Harness split and determinism | Missing: per-mechanism files; SQL-only cases moved; `--only`; latch-based cancellation; timing report-only; the harness's POSIX-only child-process code (`<sys/wait.h>`, `WIFEXITED`) either gains a Windows path or the target is documented as Linux-only, since the distribution matrix never compiles it today. |
| Incomplete | Work | 22E: Shared Python harness core | Missing: `scripts/ngramharness/{cli,corpus,oracle}.py`; three drivers under about 120 lines each. |
| Incomplete | Work | 22F: Nightly lane and warnings | Missing: `.github/workflows/Nightly.yml`; ASAN selector adds the concurrency files; `NGRAM_WERROR` option; `tidy-check` step; `cmake_minimum_required` at 3.10 or later. |
| Incomplete | Gate | Nightly green, deterministic harness | Missing: nightly run URL; grep proving no `_ms >` assertions remain; line counts. |

## Phase 23: Documentation, Benchmarks, and Release

Goal:
A reader can install, understand the contract, and find the API in a 250-line README; every
claim traces to a command or artifact; the release is tagged on a green matrix.

Scope:
- README to about 250 lines: install, quick start, contract in five lines, API and settings
  tables, limitations, links. Move staleness and guard mechanics, bounded refresh, lifecycle
  states, and stats columns to `docs/`.
- Move `ngram_index_plan.md` and this file to `docs/plan/`; distill the research facts and
  principles into `docs/design.md`; remove the README reference to plan ledgers.
- Rewrite `docs/UPDATING.md` for this repo (gitlinks, the two workflow SHAs, the source-id pin)
  and `test/README.md` (how to run one file, the harness, seeds, runtimes).
- Delete `scripts/extension-upload.sh`; trim `vcpkg.json`.
- `benchmarks/RESULTS.md` becomes the single owner of the evidence block with README linking to
  it; fix the three documents that call it the scale results, or restore a `SCALE.md` from
  committed JSON; add a `prepare` subcommand to `clickhouse_compare.py` that downloads and
  verifies inputs; trim the render golden hash from `release_evidence.py tests()`.
- Fix `SUBMISSION.md` (public repo, five settings) and `description.yml` (`ref`, `ngram_refresh`
  signature, extended description).
- Re-collect release evidence after Phases 19 and 20; run the distribution matrix; tag
  `v0.1.0`.

Out of scope:
- Community-extensions PR submission itself.

Completion gate:
`release_evidence.py check` passes on the regenerated artifact; README under 300 lines; matrix
green on the tag commit; no document names a CI result older than the tag.

Testing plan:
- `release_evidence.py validate` and `check`; link check over `docs/` and README; matrix run.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Doc | 23A: README diet and `docs/` split | Missing: README line count; new `docs/*.md` files; every moved section linked. |
| Incomplete | Doc | 23B: Plans and design note relocated | Missing: `docs/plan/`, `docs/design.md`, README reference removed. |
| Incomplete | Doc | 23C: Template boilerplate rewritten or deleted | Missing: `docs/UPDATING.md`, `test/README.md`, `scripts/extension-upload.sh` deleted, `vcpkg.json` trimmed. |
| Incomplete | Doc | 23D: Benchmark documents and packaging | Missing: single-owner evidence block; three stale references fixed; `prepare` subcommand; golden trimmed; `SUBMISSION.md` and `description.yml` corrected (including `description.yml:65`, which still lists the two literal source-id forms the guard no longer requires); `release_evidence.py` `runtime_identity` adopts the guard's commit-prefix rule instead of the exact `d8cdaa33` compare so the repo carries one definition of the host pin. |
| Incomplete | Work | 23E: Re-collected evidence and release tag | Missing: new `enwik9-current-v1.json` after Phase 20; matrix run URL on the tag commit; tag `v0.1.0`. |
| Incomplete | Gate | Traceable docs on a green matrix | Missing: check output, README line count, run URL, tag. |
