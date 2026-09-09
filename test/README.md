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
| `fixtures` | the persisted format-3, format-4 and format-5 databases in `test/fixtures` (see its README) |
| `query` | cancellation through a streamed result and through the maintenance append hook, ordered candidate streams, the publication barrier, the probe's memory peak, same-size key collisions |

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

Randomized, seeded runs that take minutes rather than seconds:

```bash
python3 scripts/differential_search.py --trials 8 --seed 12345            # explicit functions against brute force
python3 scripts/differential_search.py --transparent --trials 8 --seed 12345
python3 scripts/churn_maintenance.py --rounds 40 --seed 12345             # interleaved DML, checkpoints and maintenance
python3 scripts/crash_maintenance.py --seed 12345                         # SIGKILL during maintenance, then reopen
```

`scripts/ngram_harness.py` holds what they share: the CLI runner, the
format-aware registry lookup, the storage digests and the multiset oracle.
Each driver keeps its own corpus generation, so a seed reproduces the same
run across refactors. The CI seeds are in `.github/workflows/Correctness.yml`.

## Continuous integration

`Correctness.yml` runs on every push: the release suite with the harness, the
fixed-seed drivers, a format check of `src` and `test`, and a focused
ASAN/UBSAN debug build (warnings are errors for the extension's sources) of
the query, concurrency and resource files plus the harness. `Nightly.yml`
runs the drivers at full size, the whole suite and the harness under
ASAN/UBSAN, and the concurrency tests and the query harness under
ThreadSanitizer. `MainDistributionPipeline.yml` builds the distributed
binaries and runs the suite on each platform.
