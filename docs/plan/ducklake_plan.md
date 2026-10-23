# DuckLake indexes: plan

Goal: ngram and sparse indexes on DuckLake tables with the guarantees the native
index has, exact answers for every reader and any number of writers, and a
query path that beats a scan on large tables. The design is the
lake-native one in the design doc (index tables in the lake, keyed by DuckLake
row ids, the change feed for rows past the covered snapshot); the 20x spike
measured the query path it uses.

Each phase ends at a gate. A phase is committed only after its reviewer
approves it.

## Phase 1: maintenance and queries

- `create_ngram_index`, `ngram_refresh`, `ngram_compact`, `drop_ngram_index`,
  `ngram_index_stats` and `ngram_indexes` on DuckLake tables.
- `ngram_search`, `ngram_candidates` and the LIKE/ILIKE/contains rewrite on
  DuckLake tables, through a pushed `rowid` filter whose statistics callback
  lets DuckLake's scan skip row groups.
- Prepared statements that read an indexed lake are planned again for every
  execution: DuckDB 2.0 replans every prepared statement that reads a
  database, and the tests hold it to that.
- DuckLake and the SQLite scanner link into test builds configured with
  `NGRAM_DUCKLAKE=1`.

Gate: `test/sql/lake/*.test` pass and cover every maintenance call, both
tokenizers, inserts, updates and deletes after the covered snapshot, merges,
rewrites and inline flushes, ALTER staleness, transactions that wrote the
lake, time travel, and prepared statements; the native suite still passes.

## Phase 2: differential and concurrency runs

- `scripts/lake_differential.py`: random writes and maintenance on a lake
  table, each step compared with a scan for many needles, both tokenizers.
- `scripts/lake_concurrency.py`: writer, maintainer and reader processes on a
  SQLite-backed lake; a reader compares the index's answer with a scan inside
  one transaction.

Gate: no mismatch over the runs; every maintenance call that loses a conflict
fails with DuckLake's conflict error and succeeds when rerun.

## Phase 3: performance

- Rerun the 20x spike's queries through the extension on simulated S3
  (s3proxy behind toxiproxy): index against scan, per needle, at both data
  row-group sizes; build time and memory.
- The native suite's timings show no regression.

Gate: the extension matches the spike's index-path times within the planning
overhead it adds, and the row-group gate picks the faster plan.

## Phase 4: documentation and release

- README section and `docs/ducklake.md`; a CI job that builds with DuckLake and
  runs the lake tests.

Gate: CI green; docs describe the guarantees, the costs and the limits.
