# Benchmarks

This directory contains reproducible performance evidence for `duckdb-ngram`.
The comparison below is a bounded same-machine point of reference against
ClickHouse 26.7.3.19's GA `text(tokenizer = ngrams(3))` index, not a universal
database ranking.

Both campaigns used:

- The same 1 GB enwik9-derived line corpus: 10,920,423 rows and 986,852,975
  text bytes.
- DuckDB 1.5.5 and ClickHouse 26.7.3.19 on the same 24-core machine.
- 24 threads and a 48 GiB engine memory limit.
- Three fresh load/index-build repetitions, alternating engine order.
- Three file-data-cold queries per engine, needle, and mode. Before each query,
  the engine was stopped, files were synced and evicted with
  `POSIX_FADV_DONTNEED`, and a fresh process executed once without warm-up.
- One warm-up and ten measured warm queries per engine, needle, and mode.
- A normal indexed search and an index-disabled scan control in each engine.
- A SHA-256 over sorted matching row IDs. The indexed and scan results from
  both engines returned the same document set for every needle.

The source file was already in the host page cache, so load figures are
warm-source ingest times. Storage compares DuckDB's database file with
ClickHouse's active-table on-disk bytes; ClickHouse logs and temporarily
retained inactive parts are excluded.

“File-data-cold” is narrower than a reboot or globally empty kernel cache: all
database and index file pages were explicitly evicted, but filesystem metadata
may remain cached. This models a working set larger than available memory much
more closely than the warm measurements do.

## Case-sensitive trigrams

DuckDB used `gram=3, case_insensitive=false`. ClickHouse used
`TYPE text(tokenizer = ngrams(3))` with an exact `LIKE` recheck.

### Load, build, and storage

| Engine | Load median (range) | Index build median (range) | Base | Indexed | Index delta |
| --- | ---: | ---: | ---: | ---: | ---: |
| DuckDB | 2.102 s (1.471–2.114) | 7.541 s (7.537–7.606) | 0.690 GiB | 1.685 GiB | 0.995 GiB |
| ClickHouse | 1.348 s (1.301–1.386) | 18.836 s (16.814–20.048) | 0.538 GiB | 1.853 GiB | 1.316 GiB |

- ClickHouse loaded 1.56x faster.
- DuckDB built the index 2.50x faster.
- DuckDB used 1.32x less incremental index storage.

### File-data-cold substring queries

Values are medians with the three-sample range in parentheses.

| Needle | Matches | DuckDB search | DuckDB scan | ClickHouse search | ClickHouse scan | Faster search |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Rare | 1 | 17.0 ms (15.0–17.0) | 171.0 ms (167.0–171.0) | 37.0 ms (35.7–43.6) | 129.4 ms (116.4–131.2) | DuckDB by 2.18x |
| Moderate | 26,068 | 142.0 ms (136.0–160.0) | 163.0 ms (163.0–164.0) | 138.5 ms (138.2–139.1) | 120.6 ms (119.9–121.0) | Roughly tied |
| Common | 1,963,067 | 175.0 ms (164.0–177.0) | 165.0 ms (162.0–167.0) | 162.8 ms (158.6–164.5) | 125.0 ms (116.1–127.2) | Roughly tied |

### Warm substring queries

| Needle | Matches | DuckDB search | DuckDB scan | ClickHouse search | ClickHouse scan | Faster search |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Rare | 1 | 7.0 ms | 40.0 ms | 11.9 ms | 43.7 ms | DuckDB by 1.69x |
| Moderate | 26,068 | 15.0 ms | 27.0 ms | 68.7 ms | 43.4 ms | DuckDB by 4.58x |
| Common | 1,963,067 | 41.5 ms | 36.0 ms | 98.3 ms | 44.9 ms | DuckDB by 2.37x |

See the [detailed case-sensitive report](CLICKHOUSE.md) and
[raw case-sensitive artifact](artifacts/enwik9-clickhouse-text-vs-ngram-v1.json).

## Case-insensitive trigrams

The original mixed-case corpus was stored unchanged in both engines. DuckDB
used its native `case_insensitive=true` index. ClickHouse used `ngrams(3)` with
`lowerUTF8(text)` index preprocessing, `hasAllTokens` candidate lookup, and an
exact `ILIKE` recheck.

### Load, build, and storage

| Engine | Load median (range) | Index build median (range) | Base | Indexed | Index delta |
| --- | ---: | ---: | ---: | ---: | ---: |
| DuckDB | 1.820 s (1.620–2.190) | 7.014 s (6.969–7.028) | 0.683 GiB | 1.614 GiB | 0.931 GiB |
| ClickHouse | 1.309 s (1.286–1.332) | 17.729 s (16.598–17.879) | 0.538 GiB | 1.808 GiB | 1.270 GiB |

- ClickHouse loaded 1.39x faster.
- DuckDB built the index 2.53x faster.
- DuckDB used 1.36x less incremental index storage.

### File-data-cold substring queries

Values are medians with the three-sample range in parentheses.

| Needle | Matches | DuckDB search | DuckDB scan | ClickHouse search | ClickHouse scan | Faster search |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Rare | 12 | 18.0 ms (17.0–19.0) | 286.0 ms (284.0–289.0) | 49.2 ms (48.1–54.5) | 137.8 ms (127.1–138.1) | DuckDB by 2.73x |
| Moderate | 26,540 | 143.0 ms (140.0–152.0) | 266.0 ms (263.0–295.0) | 162.1 ms (154.7–166.3) | 125.9 ms (120.3–131.0) | DuckDB by 1.13x |
| Common | 2,203,902 | 221.0 ms (219.0–229.0) | 204.0 ms (198.0–219.0) | 203.4 ms (196.3–203.9) | 127.4 ms (122.7–134.4) | Roughly tied |

### Warm substring queries

| Needle | Matches | DuckDB search | DuckDB scan | ClickHouse search | ClickHouse scan | Faster search |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Rare | 12 | 6.0 ms | 209.0 ms | 13.3 ms | 59.5 ms | DuckDB by 2.21x |
| Moderate | 26,540 | 16.0 ms | 190.5 ms | 86.2 ms | 49.5 ms | DuckDB by 5.39x |
| Common | 2,203,902 | 117.0 ms | 132.0 ms | 147.3 ms | 53.6 ms | DuckDB by 1.26x |

See the [detailed case-insensitive report](CLICKHOUSE_CASE_INSENSITIVE.md) and
[raw case-insensitive artifact](artifacts/enwik9-clickhouse-text-ci-vs-ngram-v1.json).

## Reproducing the comparison

The compact standard-library harness is
[`clickhouse_compare.py`](clickhouse_compare.py). With the pinned binaries and
normalized corpus in the default scratch paths, run:

```sh
python3 benchmarks/clickhouse_compare.py collect
python3 benchmarks/clickhouse_compare.py render

python3 benchmarks/clickhouse_compare.py collect --case-insensitive \
  --work .tmp/clickhouse-quick-ci-run \
  --output benchmarks/artifacts/enwik9-clickhouse-text-ci-vs-ngram-v1.json
python3 benchmarks/clickhouse_compare.py render \
  --artifact benchmarks/artifacts/enwik9-clickhouse-text-ci-vs-ngram-v1.json \
  --output benchmarks/CLICKHOUSE_CASE_INSENSITIVE.md
```

The broader checked DuckDB-only benchmark and its provenance are documented in
[`RESULTS.md`](RESULTS.md).
