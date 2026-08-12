# DuckDB ngram vs ClickHouse text index

This is a bounded same-machine point of reference, not an exhaustive tuning study.
Both engines used 24 threads, a 48 GiB memory setting, the same 1 GB enwik9-derived
line corpus, case-sensitive 3-grams, and exact substring rechecks.

ClickHouse used `TYPE text(tokenizer = ngrams(3))`; DuckDB used this extension's
default production query policy, including its adaptive scan fallback for dense terms.

## Load, build, and storage

The input file was already in the host page cache; these are warm-source ingest times.
Storage is paired DuckDB database-file and ClickHouse active-table on-disk size;
ClickHouse logs and temporarily retained inactive parts are excluded.

| Engine | Load median (range) | Index build median (range) | Base | Indexed | Index delta |
| --- | ---: | ---: | ---: | ---: | ---: |
| duckdb | 2.744 s (1.641–3.309) | 7.598 s (7.594–7.744) | 0.684 GiB | 1.680 GiB | 0.995 GiB |
| clickhouse | 1.299 s (1.282–1.304) | 16.761 s (16.712–17.986) | 0.538 GiB | 1.854 GiB | 1.316 GiB |

At a glance:

- Load: ClickHouse by 2.11x.
- Index build: DuckDB by 2.21x.
- Incremental index storage: DuckDB by 1.32x.

## Warm query latency

One warm-up preceded 10 measured samples per cell. Counts matched across both engines
and their scan controls.

| Needle | Matches | DuckDB search | DuckDB scan | ClickHouse search | ClickHouse scan | Faster search |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| rare | 1 | 7.5 ms | 40.5 ms | 11.8 ms | 40.8 ms | DuckDB by 1.58x |
| moderate | 26068 | 15.0 ms | 27.0 ms | 65.3 ms | 42.4 ms | DuckDB by 4.35x |
| common | 1963067 | 42.0 ms | 37.0 ms | 93.4 ms | 41.9 ms | DuckDB by 2.22x |

Raw samples and exact versions are in
[`artifacts/enwik9-clickhouse-text-vs-ngram-v1.json`](artifacts/enwik9-clickhouse-text-vs-ngram-v1.json).

The result compares different architectures: ClickHouse's native text inverted index
and this extension's postings index plus exact base-table recheck. It should be read as a
practical reference for this corpus and machine, not a universal engine ranking.
