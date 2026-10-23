# Indexes on DuckLake tables

`ngram` indexes DuckLake tables as well as DuckDB tables. The same pragmas
create and maintain the index, and the same functions and rewrite query it:

```sql
ATTACH 'ducklake:metadata.ducklake' AS lk (DATA_PATH 's3://bucket/lake/');

PRAGMA create_ngram_index('lk.logs', 'message');
SELECT * FROM ngram_search('lk.logs', 'connection reset');

SET ngram_auto_accelerate = true;
SELECT count(*) FROM lk.logs WHERE message LIKE '%connection reset%';
```

Every query returns every matching row, at the snapshot its transaction reads,
whatever other clients write meanwhile. Clients that write the table need no
extension; only the clients that query through the index or maintain it load
`ngram`. This note describes how the index works on a lake, what it
guarantees, what it costs, and where it stops.

## Storage

Indexes live in the lake's `__ngram` schema. The first `create_ngram_index` in
a lake creates the schema and its registry; each index adds a row to the
registry and a postings table of its own:

- `__ngram.registry` holds one row per index and one lock row. An index row
  names the table and column, the tokenizer and its gram lengths, DuckLake's
  table id, the **covered snapshot** (every row the table held at that
  snapshot is indexed), and the lake's schema version when the index was last
  maintained.
- `__ngram.segments_<index id>` holds the postings, laid out as on a DuckDB
  table: one row per gram, segment of 2^20 row ids and generation, with the
  row ids delta-encoded in a blob (`docs/design.md`). The 128-bit gram key is stored as
  two `UBIGINT` halves, because DuckDB's Parquet writer stores `UHUGEINT` as
  `DOUBLE`. The table is sorted by key and capped at 1 MB per row group, so a
  rare gram's lookup reads one small row group.

Postings hold DuckLake **row ids**. DuckLake never reuses a row id, and keeps
it through `UPDATE`, inline flushes, `ducklake_merge_adjacent_files` and
`ducklake_rewrite_data_files`; a posting therefore never points at another
row. Rows inserted after the covered snapshot, an update's new version among
them, come from DuckLake's change feed, `ducklake_table_insertions`.

## Queries

A query plans with the index when `ngram_auto_accelerate` is on and its filter
holds a needle the index can probe (`LIKE '%x%'`, `ILIKE` on a
case-insensitive index, `contains`), or when it calls `ngram_search`. While
planning, the extension reads the lake at the snapshot of the query's own
transaction, through a connection of its own:

1. the registry row, and whether the table changed shape since (Staleness);
2. the postings of up to `ngram_lake_max_grams_per_query` of the needles'
   grams, which it intersects segment by segment;
3. the change feed from the covered snapshot to the query's, filtered by the
   needles once case is folded.

The union of the two is the candidate set. The plan keeps DuckLake's own scan
of the table and adds a filter on the row id, `__ngram_lake_rowids(rowid)`,
whose statistics callback tells the scan to skip every row group whose row-id
range holds no candidate. The query's own filter still runs on every row the
scan reads, so a candidate that does not match costs only its read. `EXPLAIN`
shows the filter on the scan, here for
`SELECT count(*) FROM lk.logs WHERE message LIKE '%reset%'`:

```
╭─ Ungrouped Aggregate ────────────────╮
│ Aggregates: count_star()             │
│ ~1 row                               │
╰───────────────────┬──────────────────╯
╭─ Ducklake Scan ───┴──────────────────╮
│ Table: lk.main.logs                  │
│ Projections: message                 │
│ Filters: contains(message, 'reset'), │
│          __ngram_lake_rowids(rowid)  │
│ ~2,000 rows                          │
╰──────────────────────────────────────╯
```

A scan reads each row group holding a candidate in full, so once the
candidates spread over most row groups the query reads about as much as a
plain scan; measured, it took at most 7% longer (Performance). Set
`ngram_lake_max_row_group_fraction` below its default of 1.0 to send a query
whose candidates fall in more than that fraction of the table's row groups to
a plain scan instead. The decision follows the postings reads, so it saves
only the row-id filter; lower it if `scripts/lake_bench.py` shows the index
losing to a scan on your store. `ngram_search` follows the setting;
`ngram_candidates` returns every candidate.

With logging on, DuckDB records each decision:

```sql
CALL enable_logging(level = 'info');
SELECT count(*) FROM lk.logs WHERE message LIKE '%reset%';
SELECT message FROM duckdb_logs WHERE message LIKE 'ngram:%';
-- ngram: lk.main.logs.message reads 9 candidates (9 from 4 grams' postings, 0 from the change feed) in 0.2% of its row groups
```

Some queries always scan the table, and stay exact:

- a query in a transaction that has written the lake: an uncommitted update
  keeps its row's id and is in no change feed;
- time travel (`AT (VERSION => ...)`) and DuckLake's change-feed functions,
  which read other snapshots than the transaction's;
- a sample (`USING SAMPLE`);
- a query whose snapshot the index can no longer be read at, for instance
  because `ducklake_expire_snapshots` removed it while the transaction ran;
- a query on a stale index.

DuckDB plans a prepared statement that reads a database again for every
execution, so a prepared query's candidates always belong to the snapshot it
runs at.

## Maintenance

| Call | What it does on a lake |
| --- | --- |
| `create_ngram_index(t, col, ...)` | Builds the postings from the table at the call's snapshot and records that snapshot as covered. The first create in a lake also creates `__ngram`. |
| `ngram_refresh(t)` | Indexes the change feed since the covered snapshot as a new generation and advances the covered snapshot. `max_rows` applies to DuckDB tables only. |
| `ngram_compact(t, purge => ...)` | Merges the generations into one. With `purge`, drops postings of rows the table no longer holds. It keeps the covered snapshot. |
| `drop_ngram_index(t, col)` | Drops the index's registry row and segments table. By reference, name the lake: `drop_ngram_index(ref, catalog = 'lk')`. |
| `ngram_index_stats(t)`, `ngram_indexes()` | Report on lake indexes as on DuckDB ones; stats add `covered_snapshot`, `current_snapshot` and `remaining_tail`, the live rows a query reads from the change feed. |

Each call runs as one transaction and commits as one DuckLake snapshot.
Every call that changes an index deletes and re-inserts the registry's lock
row, and DuckLake refuses to commit the second of two transactions that
delete the same row. Of two concurrent calls on one lake, one commits. The
other fails with DuckLake's conflict error, or with "the ngram registry of lk
changed since this call was planned; run it again", and succeeds when run
again. Maintenance holds no lock of its own while it runs, and a query reads
the index as of its own snapshot whatever maintenance commits meanwhile.

Maintenance calls and `ngram_index_stats` refuse a transaction that has
already written the lake, since they read the lake's committed state; run
them in a transaction of their own. An index created inside a transaction
serves queries once the transaction commits.

Refresh regularly, and before expiring snapshots. A query reads the change
feed from the covered snapshot on, so its cost grows with the rows written
since; and the staleness check reads the snapshot history after the covered
snapshot, which `ducklake_expire_snapshots` removes. Once expiry has removed
part of the feed, a query reads it by filtering the table on each row's
snapshot, which can read the whole table, until the next refresh. Refresh
after `ducklake_rewrite_data_files` too: it stamps the rows it rewrites with a
new snapshot, so they return to the change feed, and `remaining_tail` counts
them. Compact when `generations` grows, and purge after many deletes.

## Staleness

An index describes its table while the table keeps its id and shape. A
`DROP TABLE` (even one followed by a `CREATE TABLE` of the same name) or any
`ALTER TABLE` of the indexed table after the covered snapshot makes the index
stale: dropping a column and adding it back with a default, for instance,
gives every old row a new value with no change-feed entry. A stale index
reports `SCAN_ONLY` with its reason, its queries scan, and maintenance refuses
it; drop it and create it again.

The check costs one comparison while the lake's schema version equals the one
the index recorded. A schema change anywhere in the lake moves the version;
the check then reads DuckLake's snapshot list from the covered snapshot on,
looking for the indexed table among the altered and dropped ones, until the
next refresh records the new version. If snapshot expiry removed part of that
list, the check cannot rule out an `ALTER` and calls the index stale.

## Concurrency

Any number of clients may read and write the lake while indexes are in use,
and any of them may run maintenance:

- **Writers** need no extension. Their rows reach queries through the change
  feed until a refresh indexes them.
- **Readers** read the index and the change feed at their own snapshot, so
  their answers are those of a scan at that snapshot.
- **Maintenance** commits one call at a time per lake; a call that loses
  fails with a conflict and can be rerun.

A lake whose catalog is a SQLite database should be attached in WAL mode
(`META_JOURNAL_MODE 'wal'`): in SQLite's default journal mode a transaction
that has read the catalog blocks every other client's commit until it ends.

## Performance

Measured on a 24-core machine against a lake whose files sit behind s3proxy
and toxiproxy, which add 20 ms to every request and cap each connection at
80 MB/s. The table holds 218,408,460 lines: enwik9 and 19 letter-rotated
copies of it, interleaved in blocks of 1,024 rows, 11.7 GB of Parquet. Its
case-sensitive trigram index has 2,092,233 keys in 60,640,255 segment rows,
16 GB of Parquet. Times are medians of 3 processes: a cold query is a
process's first, a warm one its second, when DuckDB has cached the files.

With the table written in row groups of 16,384 rows:

| needle | matches | row groups hit | index, cold | scan, cold | index, warm | scan, warm |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| quokka | 5 | 0.0% | 0.37 s | 5.2 s | 82 ms | 1.6 s |
| Kierkegaardian | 6 | 0.0% | 0.51 s | 5.1 s | 94 ms | 1.4 s |
| Kepler | 536 | 1.6% | 0.63 s | 5.2 s | 115 ms | 1.4 s |
| Beethoven | 1,352 | 3.7% | 1.0 s | 5.2 s | 143 ms | 1.4 s |
| Shakespeare | 3,211 | 11% | 2.0 s | 5.2 s | 260 ms | 1.4 s |
| Netherlands | 10,690 | 27% | 3.2 s | 5.2 s | 495 ms | 1.4 s |
| Germany | 28,379 | 54% | 4.3 s | 5.2 s | 812 ms | 1.4 s |
| States | 146,423 | 76% | 4.9 s | 5.2 s | 1.1 s | 1.4 s |
| `[[Category:` | 394,868 | 78% | 5.1 s | 5.2 s | 1.2 s | 1.6 s |

- Planning takes 0.23 to 0.44 s cold and 37 to 130 ms warm: the registry,
  the snapshot, the postings and the change feed, read in turn. Planning a rare
  needle reads 8 MB in 13 requests.
- The scan reads every data file's footer (84 here) and each row group that
  holds a candidate in full; a warm process reads both from its cache.
- At DuckDB's default row group of 122,880 rows, the scan costs about the same
  (4.9 s cold) and the index reads more per candidate: Kepler 0.93 s,
  Shakespeare 3.2 s, Netherlands 4.5 s. Needles whose candidates hit 96%
  of the row groups or more took 1.01 to 1.07 times a plain scan's time.
- Writing indexed tables in smaller row groups (`CALL
  lk.set_option('parquet_row_group_size', 16384, table_name => 'logs')`)
  therefore speeds up their index reads by up to 1.6x and slows a scan by
  about 6%.
- `create_ngram_index` took 184 s on the 122,880-row table and 215 s on the
  16,384-row one, peaking at 24.3 and 24.7 GB of memory with `memory_limit`
  at 24 GB.

The simulated store serves a scan at over 2 GB/s, which favors the scan;
a real object store adds more latency to each request and charges for it.
Data clustered by time or source, as logs usually are, puts a needle's matches
in fewer row groups than this interleaved corpus does. `scripts/lake_bench.py`
runs these measurements on any lake.

## Limits

- DuckLake lists tables by name alone, so an index needs a table name that no
  other schema of the lake uses.
- Every query that plans with the index makes several reads of the lake's
  catalog and of the `__ngram` tables. On a remote catalog each costs a round
  trip.
- The scan opens every data file of the table to read its row-group
  statistics: DuckLake prunes files by column statistics, which hold no row-id
  ranges, so a table of many files pays a footer read per file.
- The gate estimates the row groups a query reads from row ids and the
  table's row-group size. Small inserts that each write a file of their own
  make more row groups than it counts until DuckLake merges them; deleted rows
  leave gaps in the row ids, so after `ducklake_rewrite_data_files` drops them
  it counts fewer. Near a gate set below 1.0, either plan costs about the
  same.
