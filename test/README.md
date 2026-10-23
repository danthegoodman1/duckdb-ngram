# Testing this extension

Four kinds of tests cover the extension. Each one exists because the others
cannot control what it checks.

## SQLLogicTests: `test/sql`

Every semantic claim that SQL can state lives here, one file per subject
(`ngram_search.test`, `ngram_api.test`, `ngram_maintenance.test`, ...). A
query-shape test compares an accelerated answer with the same query run under
`SET disabled_optimizers = 'extension'` as a multiset in both directions, so a
duplicated or dropped row fails; `EXPLAIN` pins which shapes accelerate and
which stay native. Run the whole suite or one file:

```bash
make release GEN=ninja                      # build first
make test                                   # the C++ harness, then the whole suite
build/release/test/unittest 'test/sql/*'    # the suite alone
build/release/test/unittest test/sql/ngram_api.test
```

The files in `test/sql/lake` index DuckLake tables (`docs/ducklake.md`).
They need DuckLake, Parquet and the SQLite scanner, which a build links when
it is configured with `NGRAM_DUCKLAKE=1`; other builds skip them. DuckLake links CRoaring, which
`scripts/build_croaring.sh` installs under `build/deps`:

```bash
scripts/build_croaring.sh
NGRAM_DUCKLAKE=1 GEN=ninja make release
build/release/test/unittest 'test/sql/lake/*'
```

## The C++ harness: `test/cpp/ngram_maintenance_checkpoint_gap.cpp`

Invariants SQL cannot control: a pause between a pragma's expansion and its
transaction, a checkpoint or a process kill at a chosen point, a second
process on the same file, a forced publication order inside a parallel probe,
an interrupt at a chosen decode point. Tests are grouped by mechanism and
selectable:

| Mechanism | What it exercises |
| --- | --- |
| `lifecycle` | index creation schedules, the shared rowid guard, drop, vacuum, WAL replay and a stock host, checkpoint seals, quarantine, storage corruption |
| `registry` | registry rows, bootstrap, catalog identity across copies, corruption, execution-time identity races, a 10,000-row registry |
| `fixtures` | the persisted format-3, format-4 and format-5 databases in `test/fixtures` (see its README), each refused until rebuilt |
| `query` | cancellation through a streamed result and through the maintenance append hook, ordered candidate streams, the publication barrier, the probe's memory peak, same-size key collisions |
| `lake` | a statement prepared through the client API over a DuckLake table, executed after other connections' writes and refresh; it loads the build's DuckLake extension and is skipped when the build has none |

```bash
cmake --build build/release --target ngram_checkpoint_gap_test
build/release/extension/ngram/ngram_checkpoint_gap_test /tmp/harness.db test/fixtures
build/release/extension/ngram/ngram_checkpoint_gap_test /tmp/harness.db test/fixtures --only query
build/release/extension/ngram/ngram_checkpoint_gap_test /tmp/harness.db --only registry/scale --only lifecycle/wal
```

`--only PATTERN` keeps the tests whose `mechanism/name` contains the pattern.
Timings the harness prints (the registry-scale lines) are reports, never
assertions: no correctness result depends on the machine's speed. Publication
order and the merge's cancellation are forced through
`src/include/ngram/test_hooks.hpp`, two process-wide callbacks the extension
consults only when a harness has set them. The harness runs child processes
through `std::system` and reads their POSIX exit status (`WIFEXITED`,
`WEXITSTATUS`), bounding one with the `timeout` command, so it runs on Linux
and macOS; Windows runs the SQL suite only.

## Python drivers: `scripts/`

Randomized, seeded runs that take minutes rather than seconds. The
differential driver picks `ngram` or `sparse` per trial unless `--tokenizer`
fixes it; `sparse_grams_check.py` compares the sparse extractor with a
brute-force reading of the definition.

```bash
python3 scripts/differential_search.py --trials 8 --seed 12345            # explicit functions against brute force
python3 scripts/differential_search.py --transparent --trials 8 --seed 12345
python3 scripts/differential_search.py --tokenizer sparse --trials 8 --seed 12345
python3 scripts/sparse_grams_check.py --cases 3000 --seed 1               # sparse_grams() against the definition
python3 scripts/churn_maintenance.py --rounds 40 --seed 12345             # interleaved DML, checkpoints and maintenance
python3 scripts/crash_maintenance.py --seed 12345                         # SIGKILL during maintenance, then reopen
```

Two drivers cover DuckLake tables and need an `NGRAM_DUCKLAKE=1` build. The
differential applies random writes, ngram maintenance, DuckLake's file merges,
rewrites, inline flushes and snapshot expiry, DDL and ALTERs to a lake table,
and after every step compares the index's answers with a scan. The concurrency
driver runs writer, maintainer, reader and DDL processes against one lake on a
SQLite catalog; each reader compares the index with a scan inside one
transaction, and every ngram maintenance call that loses a conflict must
succeed when run again. Both keep their lakes under `build/lake_runs` only
with `--keep`.

```bash
python3 scripts/lake_differential.py --trials 6 --seed 101                # both tokenizers, a DuckDB catalog
python3 scripts/lake_differential.py --trials 4 --seed 202 --catalog sqlite
python3 scripts/lake_concurrency.py --seconds 300 --seed 303              # add --tokenizer sparse for a sparse index
```

`scripts/lake_bench.py` times the index against a scan on a lake of your own.

`scripts/ngram_harness.py` holds what they share: the CLI runner, the
format-aware registry lookup, the storage digests and the multiset oracle.
Each driver keeps its own corpus generation, so a seed reproduces the same
run across refactors. The CI seeds are in `.github/workflows/Correctness.yml`.

## Continuous integration

`Correctness.yml` runs on every push: the release suite with the harness, the
fixed-seed drivers, a format check of `src` and `test`, an `NGRAM_DUCKLAKE=1`
build that runs the lake suite, the harness's lake test and the fixed-seed
lake drivers, and a focused ASAN/UBSAN debug build (warnings are errors for
the extension's sources) of the query, concurrency and resource files plus the
harness. `Nightly.yml` runs the drivers at full size, the lake drivers among
them, the whole suite and the harness under ASAN/UBSAN, and the concurrency
tests and the query harness under ThreadSanitizer with the host's suppressions
plus `test/tsan-suppressions.txt` (each entry explained in the file). The
ThreadSanitizer lane is advisory, as the host's own is: its log is reviewed
and its exit code does not fail the nightly, because which reports appear
depends on thread interleaving. `MainDistributionPipeline.yml` builds the
distributed binaries and runs the suite on each platform.
