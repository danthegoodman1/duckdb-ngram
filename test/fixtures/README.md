# Persisted format fixtures

Each file is a DuckDB database written by an earlier build of this extension,
kept so the current build's behavior on an old index is tested on the real
thing rather than on a synthetic imitation. The C++ harness
(`test/cpp/ngram_maintenance_checkpoint_gap.cpp`, `fixtures/format3`,
`fixtures/format4`, `fixtures/format5`) copies each file before touching it;
the files themselves are never written.

| File | Index format | Producing build and committing commit | Contents | What the harness asserts |
| --- | --- | --- | --- | --- |
| `format3.duckdb` | 3 (schema per index: `__ngram_idx_<uuid>.meta/segments/stats`, guard index `__ngram_rowid_guard_<uuid>`) | written by a format-3 build; committed in 4243564, 2026-09-01 (Phase 17) | `docs(s)` with 3 rows, one index on `s` | listed `MALFORMED` with "format 3" in the reason; `create_ngram_index` on the table refuses with "predates format 5"; `drop_ngram_index` by reference removes the guard, the row, the old schema and the old registry, leaves the 3 rows, and a fresh index is `READY` and exact |
| `format4.duckdb` | 4 (`__ngram.segments_<hex>`, `__ngram.stats_<hex>`, guard `__ngram_guard_<hex>`) | written by a format-4 build (the Phase 17 and 18 source); committed in d28ccab, 2026-09-08 | `docs(s)` with 7 rows, one index on `s` (gram 3, case-insensitive, mark 4) | listed `MALFORMED` with "index format 4"; the statistics table is present; the transparent path falls back and answers 3 rows for `%tent%`; `ngram_search` and `create_ngram_index` refuse and name the drop; the drop by reference removes every ngram object and leaves the 7 rows; the rebuild is `READY` and exact |
| `format5.duckdb` | 5 (`__ngram.segments_<hex>` with `generation`, guard `__ngram_guard_<hex>`) | written by the format-5 build of d28ccab, 2026-09-08 (Phase 19 repair), which committed it | `docs(s)` with 7 rows (rowids 0 to 6), one index on `s` (gram 3, case-insensitive, mark 4, so rows 5 and 6 are a persisted tail) | listed `SCAN_ONLY` because its guard names DuckDB v1.5.5; explicit and transparent searches scan and match the oracle across the persisted tail; a refresh refuses and names the rebuild; the drop by reference removes every ngram object and leaves the 7 rows; the rebuild is `READY`, covers the tail (mark 6), and probes exactly on both paths |

The harness also opens `format5.duckdb` through the ordinary attach path,
which replays no WAL because each fixture was checkpointed before it was
committed. The crash driver (`scripts/crash_maintenance.py`) covers format 5
under interrupted maintenance on databases it builds itself.

Every fixture was written by a build of this extension against DuckDB v1.5.5
at storage version 64 (the files' storage tag reads `v1.0.0+`). The extension
now pins DuckDB 2.0, so `format5.duckdb` is also the real artifact of a host
upgrade: its guard records v1.5.5, which the current build refuses to trust
until the index is rebuilt. Indexes the current host writes are covered by the
suite's own restart tests instead, which build on whichever host is pinned. Git records the committing commit; the producing
build's own commit is identified by the format the file carries (the
registry's `format_version` and the objects listed above), which is what the
harness checks.

To regenerate a fixture for a future format, build the extension at the commit
that writes that format, create a `docs(s)` table with the rows above, create
the index, refresh it to the mark shown, run `CHECKPOINT`, close the database,
and record the producing commit in this table.
