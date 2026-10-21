# Updating the host pin

The extension links DuckDB's internal C++ API and pins one host: the `duckdb`
submodule at a release tag or release branch, `extension-ci-tools` at the
matching branch, and the source identity the rowid guard accepts. The extension
currently pins the v2.0.0 tag and extension-ci-tools' `v2.0-cyanoptera` branch,
the 2.0 LTS line of both. DuckDB 2.0's CMake derives no release version from
git, even at a tag, so a build that names none reports `v2.0.0-dev<n>` and
stamps its loadable extension with the commit, which a release binary refuses:
the Makefile passes `DUCKDB_EXPLICIT_VERSION`, as do the CI jobs that run CMake
directly. The guard identifies the host by its commit and accepts the release
version or a pre-release label of it, since DuckDB's staged builds, which
extension-ci-tools links for its tests, report one. Every pin lives in one place and
`scripts/verify_pins.sh` checks the submodules against the gitlinks HEAD
records; the Correctness workflow and `benchmarks/release_evidence.py` both run
it.

## Where the pin lives

| Pin | Location | Checked by |
| --- | --- | --- |
| DuckDB sources | the `duckdb` gitlink | `scripts/verify_pins.sh` |
| CI tooling | the `extension-ci-tools` gitlink | `scripts/verify_pins.sh` |
| Build version | `DUCKDB_EXPLICIT_VERSION` in the `Makefile`, and in `Correctness.yml` and `Nightly.yml` for their direct CMake builds | a release binary's `LOAD`, which compares the extension's footer |
| Workflow versions | both jobs' extension-ci-tools commit and the build job's `duckdb_version` in `.github/workflows/MainDistributionPipeline.yml` | the distribution matrix |
| Guard host identity | `DUCKDB_VERSION`, `DUCKDB_SOURCE_COMMIT` and `DUCKDB_SOURCE_ID` in `src/rowid_guard.cpp` | the guard refuses any other host; the harness's `lifecycle/incompatible-guard-quarantine` mutates the recorded source id, and `lifecycle/creation-schedules` requires the pinned host |
| Documented identity | README "Limitations", `packaging/community-extensions/extensions/ngram/description.yml` | reading |
| Release evidence | `DUCKDB_GITLINK`, `CI_GITLINK`, `DUCKDB_VERSION` and `DUCKDB_SOURCE` in `benchmarks/release_evidence.py` | `release_evidence.py check` and `test` |

## Moving to a new DuckDB release

1. Check out the new tag in `duckdb` and the matching branch in
   `extension-ci-tools`; commit the gitlinks. `scripts/verify_pins.sh` must
   print both.
2. Set the workflow versions named above.
3. Build (`make release GEN=ninja`) and fix what the internal API moved. The
   extension reaches into the host at these seams, each of which has changed
   between releases before: `DataTable::InitializeLocalAppend` and the local
   append path (`src/fence.cpp`), `RowGroup::GetRawColumnData` and
   `ColumnData::CheckZonemap` for the positioned manifest scans
   (`src/probe.cpp`), `TableScanState` and the bounded scans
   (`src/search_core.cpp`), the optimizer extension and `TableFilter` pushdown
   (`src/rewrite.cpp`), the custom index type and its checkpoint seal
   (`src/rowid_guard.cpp`), and the pragma preprocessor that expands a
   maintenance pragma into a transaction (`src/pragmas.cpp`).

   These compile clean and fail only at run time, so the suite is what catches
   them:

   - **A pushed filter is keyed by its position in the scan's projection**, not
     by the table's column index. Reading it as a column index silently stops
     the transparent rewrite from firing on any table with more than one
     column, and every result stays correct, so only a plan assertion notices.
   - **Table filters are expressions.** `TableFilterSet::PushFilter` takes an
     `ExpressionFilter`; the comparison classes are `Legacy*` and only
     deserialize. Several filters on one column arrive ANDed into a single
     expression, so a needle collector has to walk conjunctions.
   - **The row count is not the end of the rowid space.** A checkpoint that
     vacuums whole row groups leaves the survivors at their original row
     numbers, so the numbering has holes: seek to a row group rather than
     looking a row number up, and bound the tail with `GetNextRowId()`.
   - **Sentinels that became `optional_idx`.** A host field that used
     `MAX_TRANSACTION_ID` for "none" is an `optional_idx`, so test it with
     `IsValid()`. `MAX_TRANSACTION_ID` equals `INVALID_INDEX`, which
     `optional_idx` holds as an ordinary value: comparing against it reads
     "active" where the old `transaction_t` comparison read "no active
     checkpoint".
   - **A positioned scan carries its own row-group tree.**
     `RowGroupCollection::InitializeScanInRowGroup` leaves
     `CollectionScanState::row_groups` alone, so the bounded scan sets it; a
     scan that crosses into a second row group otherwise dereferences a null
     tree. The suite reaches that through a checkpoint between an index's
     generations.
   - **A partition subset belongs to the scan.** The swapped scan declines a
     `LogicalGet` whose `scan_partition_indices` is set: the optimizer answered
     the other partitions from statistics, and scanning all of them would count
     those rows twice.

   The suite leans on host behavior of its own, which 2.0 also changed:

   - **Statistics answer substring filters.** `contains` and `suffix` fold to
     true or false when a column's min and max prefixes are equal, `prefix`
     from where the needle falls in that range, and `count(*)` then comes from
     metadata. A transparent-path test over a column holding one repeated
     string never reaches the rewrite, so such tables mix two strings. To find tests the host bypasses, log each query where the
     rewrite fires and diff runs with the functions' prune callbacks on and off.
   - **Vacuum moves rows only on all-ART tables, in two regimes.** Storage
     v2.0.0 drops empty row groups in place and remaps rowids when it merges
     partially deleted ones; older storage renumbers densely once
     `vacuum_rebuild_indexes` allows a rebuild. A control that proves a moving
     vacuum needs partially deleted row groups on both sides of an empty one.
   - **`Connection::Query` runs the query to completion.** A test that
     interrupts or observes a query mid-stream opens a `QueryResultStream` on
     `Connection::Submit`.
   - **`EXPLAIN` names operators in title case**, so the transparent scan reads
     `Ngram Index Scan`.
   - **A statement that expands into several runs as one query.** The host
     expands a pragma in a transaction of its own, then runs the generated
     statements in one fresh transaction, refuses transaction statements among
     them, and serves only the last statement's rows. The statement iterator
     no longer expands pragmas, so the harness expands them through
     `StatementPreprocessor::ExpandStatement` and wraps the result in
     `BEGIN`/`COMMIT` to act between statements.
   - **The CLI has an agent mode.** It changes the output format when a
     coding agent's environment variables (`CLAUDECODE`, `AGENT`, `CODEX_*`,
     ...) are set and stdout is not a terminal, so every script that runs the
     CLI passes `DUCKDB_AGENT_MODE=0`; `release_evidence.py` runs it in a
     scrubbed environment.
   - **Static extensions link only on request.** DuckDB links into its shell
     and `unittest` only the extensions a config names with
     `duckdb_extension_statically_link`, so `extension_config.cmake` names
     `ngram`, and the harness links `core_functions_extension` itself.

   The guard's create-index operator (`PhysicalCreateRowIdGuard` in
   `src/rowid_guard.cpp`) repeats the catalog steps of
   `PhysicalCreateIndex::Finalize`, so that one thread holds the table's
   append lock from the rowid baseline through `DataTable::AddIndex`. A change
   to that function compiles clean and can pass the suite, so diff it between
   the old and new pins and carry any change over.
4. Update `DUCKDB_VERSION`, `DUCKDB_SOURCE_COMMIT` and `DUCKDB_SOURCE_ID` to
   the new tag, then the build version, the commit the harness's
   `lifecycle/creation-schedules` names, and the two documented identities.
   The guard fails closed on any other host, so a wrong pin refuses
   `create_ngram_index` on a fresh database and lists existing indexes
   `SCAN_ONLY` in the first test run.
5. Run the whole suite and the harness (`make test`), the fixed-seed drivers
   in `Correctness.yml`, and the host hazards recorded under `docs/upstream/`:
   each note names the DuckDB behavior the extension works around and the
   test that covers the workaround.
6. If the storage format changes, bump `NGRAM_FORMAT_VERSION`, add a fixture
   for the previous format under `test/fixtures` (its README says how), and
   extend the harness's fixture tests.
7. Collect fresh release evidence on the final source
   (`benchmarks/release_evidence.py collect`), update `ENGINE_COMMIT` and the
   gitlink constants in the tool, and render the blocks (`generate`).

## Deployment

The extension is distributed through community-extensions only
(`packaging/SUBMISSION.md`); the repository carries no upload tooling.
