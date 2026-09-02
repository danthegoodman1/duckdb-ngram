# DuckDB v1.5.5: empty batch insert, delete, reinsert leaves a table empty in-process

Status: reproduced 2026-09-01 against the pinned host; not yet filed upstream.

## Summary

Inside one transaction, an empty batch `INSERT ... ORDER BY` into table `t`,
then `DELETE FROM t`, then a batch `INSERT` of at least 122,880 rows into `t`
leaves `t` reading zero rows for the rest of the process. The rows are in the
file: reopening it returns them all. The same statements with `threads=1`, or
without the empty insert, return every row at every stage.

The `ngram` extension hit this through `PRAGMA ngram_refresh` on an index whose
tail was empty: the generated script appended an empty delta to the stats table,
deleted the table, and reinserted the folded rows. Every later query on that
index in the process failed with "the index is malformed" until the file was
reopened. The refresh script no longer inserts into the stats table before the
delete.

## Environment

```
SELECT version(), source_id FROM pragma_version();
-- v1.5.5, d8cdaa33   (also the official binary form d8cdaa33fd)
SELECT current_setting('threads');
-- 24
```

Linux x86_64, release build from the `duckdb` submodule pinned at
`d8cdaa33fda8df955cc76ef58a280f68f4cd43fa`. Reproduces in-memory and on a file
database with the default `checkpoint_threshold`; the extension-level failure
also reproduces under DuckDB's sqllogictest runner, which sets
`checkpoint_wal_size = 0`.

## Reproduction

Run one statement at a time (for example from a file on stdin to the CLI):

```sql
CREATE TABLE t AS SELECT i, i::VARCHAR AS s FROM range(200000) r(i);
CREATE TABLE src AS SELECT i, i::VARCHAR AS s FROM range(200000) r(i);
SELECT count(*) AS before FROM t;                          -- 200000
BEGIN;
INSERT INTO t SELECT * FROM src WHERE i < 0 ORDER BY s;    -- empty batch insert
CREATE TEMP TABLE folded AS SELECT * FROM t;
DELETE FROM t;
INSERT INTO t SELECT * FROM folded ORDER BY s;             -- 200,000 rows
SELECT count(*) AS in_transaction FROM t;                  -- 0, expected 200000
COMMIT;
SELECT count(*) AS after_commit FROM t;                    -- 0, expected 200000
```

Reopen the file in a new process:

```sql
SELECT count(*) AS after_reopen FROM t;                    -- 200000
```

## Observed versus expected

| Stage | Observed (24 threads) | Observed (`SET threads=1`) | Expected |
| --- | --- | --- | --- |
| before | 200000 | 200000 | 200000 |
| in transaction | 0 | 200000 | 200000 |
| after commit | 0 | 200000 | 200000 |
| after reopen | 200000 | 200000 | 200000 |

Variants, each run as above with 24 threads (in transaction / after commit):

| Variant | Result |
| --- | --- |
| control: no empty insert | 200000 / 200000 |
| empty insert without `ORDER BY` | 200000 / 200000 |
| empty insert as `INSERT INTO t SELECT * FROM src LIMIT 0` | 200000 / 200000 |
| big insert without `ORDER BY` (empty insert keeps `ORDER BY`) | 0 / 0 |
| 122,879 rows reinserted | 0 / 122879 |
| 122,880 rows reinserted | 0 / 0 |

The empty insert has to take the batch (`ORDER BY`) insert path. Below 122,880
reinserted rows the in-transaction read is still wrong but the commit repairs
it; from 122,880 rows the table stays empty in-process.
