# Updating the host pin

The extension links DuckDB's internal C++ API and pins one host: the `duckdb`
submodule at a release tag or release branch, `extension-ci-tools` at the
matching branch, and the source identity the rowid guard accepts. The extension
currently tracks the unreleased `v2.0-cyanoptera` branch, which carries no tag,
so the workflows name the version through `OVERRIDE_GIT_DESCRIBE` and a local
build needs `OVERRIDE_GIT_DESCRIBE=v2.0.0` for the guard to accept the host. Every pin lives in one place and
`scripts/verify_pins.sh` checks the submodules against the gitlinks HEAD
records; the Correctness workflow and `benchmarks/release_evidence.py` both
run it.

## Where the pin lives

| Pin | Location | Checked by |
| --- | --- | --- |
| DuckDB sources | the `duckdb` gitlink | `scripts/verify_pins.sh` |
| CI tooling | the `extension-ci-tools` gitlink | `scripts/verify_pins.sh` |
| Workflow versions | `duckdb_version` and `ci_tools_version` in `.github/workflows/MainDistributionPipeline.yml`; `OVERRIDE_GIT_DESCRIBE` in `Correctness.yml` and `Nightly.yml` | the distribution matrix |
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

   Four of these compile clean and fail only at run time, so the suite is what
   catches them:

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
   - **Sentinels that became `optional_idx`.** `MAX_TRANSACTION_ID` equals
     `INVALID_INDEX`, so comparing an `optional_idx` against it throws where
     the old `transaction_t` comparison read "no active checkpoint".
4. Update `DUCKDB_VERSION`, `DUCKDB_SOURCE_COMMIT` and `DUCKDB_SOURCE_ID` to
   the new tag, and the two documented identities. The guard fails closed on
   any other host, so a wrong pin refuses `create_ngram_index` on a fresh
   database and lists existing indexes `SCAN_ONLY` in the first test run.
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
