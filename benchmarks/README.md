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
- One warm-up and ten measured warm queries per engine, needle, and mode.
- A normal indexed search and an index-disabled scan control in each engine.
- A SHA-256 over sorted matching row IDs. The indexed and scan results from
  both engines returned the same document set for every needle.

The source file was already in the host page cache, so load figures are
warm-source ingest times. Storage compares DuckDB's database file with
ClickHouse's active-table on-disk bytes; ClickHouse logs and temporarily
retained inactive parts are excluded.

## Case-sensitive trigrams

DuckDB used `gram=3, case_insensitive=false`. ClickHouse used
`TYPE text(tokenizer = ngrams(3))` with an exact `LIKE` recheck.

### Load, build, and storage

| Engine | Load median (range) | Index build median (range) | Base | Indexed | Index delta |
| --- | ---: | ---: | ---: | ---: | ---: |
| DuckDB | 1.869 s (1.554–2.395) | 7.602 s (7.575–7.696) | 0.692 GiB | 1.687 GiB | 0.995 GiB |
| ClickHouse | 1.383 s (1.320–1.412) | 18.115 s (17.152–18.484) | 0.538 GiB | 1.853 GiB | 1.315 GiB |

- ClickHouse loaded 1.35x faster.
- DuckDB built the index 2.38x faster.
- DuckDB used 1.32x less incremental index storage.

### Warm substring queries

| Needle | Matches | DuckDB search | DuckDB scan | ClickHouse search | ClickHouse scan | Faster search |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Rare | 1 | 6.0 ms | 41.0 ms | 12.1 ms | 41.4 ms | DuckDB by 2.02x |
| Moderate | 26,068 | 15.0 ms | 27.5 ms | 66.1 ms | 43.6 ms | DuckDB by 4.41x |
| Common | 1,963,067 | 43.0 ms | 37.0 ms | 94.3 ms | 43.0 ms | DuckDB by 2.19x |

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
| DuckDB | 1.890 s (1.776–2.010) | 7.089 s (7.012–7.126) | 0.683 GiB | 1.613 GiB | 0.930 GiB |
| ClickHouse | 1.328 s (1.325–1.348) | 17.471 s (16.242–19.425) | 0.538 GiB | 1.808 GiB | 1.270 GiB |

- ClickHouse loaded 1.42x faster.
- DuckDB built the index 2.46x faster.
- DuckDB used 1.37x less incremental index storage.

### Warm substring queries

| Needle | Matches | DuckDB search | DuckDB scan | ClickHouse search | ClickHouse scan | Faster search |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Rare | 12 | 8.0 ms | 211.0 ms | 12.9 ms | 62.4 ms | DuckDB by 1.61x |
| Moderate | 26,540 | 16.5 ms | 190.0 ms | 84.4 ms | 54.2 ms | DuckDB by 5.12x |
| Common | 2,203,902 | 119.5 ms | 135.0 ms | 149.3 ms | 56.1 ms | DuckDB by 1.25x |

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
