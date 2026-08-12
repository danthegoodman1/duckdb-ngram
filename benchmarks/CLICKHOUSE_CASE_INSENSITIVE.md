# DuckDB ngram vs ClickHouse case-insensitive text index

This is a bounded same-machine point of reference, not an exhaustive tuning study.
Both engines used 24 threads, a 48 GiB memory setting, the same 1 GB enwik9-derived
line corpus, case-insensitive 3-grams, and exact substring rechecks.

ClickHouse used `ngrams(3)` with `lowerUTF8(text)` preprocessing;
DuckDB used its native case-insensitive trigram index.
DuckDB retained its production adaptive scan fallback for dense terms.

## Load, build, and storage

The input file was already in the host page cache; these are warm-source ingest times.
Storage is paired DuckDB database-file and ClickHouse active-table on-disk size;
ClickHouse logs and temporarily retained inactive parts are excluded.

| Engine | Load median (range) | Index build median (range) | Base | Indexed | Index delta |
| --- | ---: | ---: | ---: | ---: | ---: |
| duckdb | 1.890 s (1.776–2.010) | 7.089 s (7.012–7.126) | 0.683 GiB | 1.613 GiB | 0.930 GiB |
| clickhouse | 1.328 s (1.325–1.348) | 17.471 s (16.242–19.425) | 0.538 GiB | 1.808 GiB | 1.270 GiB |

At a glance:

- Load: ClickHouse by 1.42x.
- Index build: DuckDB by 2.46x.
- Incremental index storage: DuckDB by 1.37x.

## Warm query latency

One warm-up preceded 10 measured samples per cell. Sorted matching-row digests matched
across both engines and their scan controls.

| Needle | Matches | DuckDB search | DuckDB scan | ClickHouse search | ClickHouse scan | Faster search |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| rare | 12 | 8.0 ms | 211.0 ms | 12.9 ms | 62.4 ms | DuckDB by 1.61x |
| moderate | 26540 | 16.5 ms | 190.0 ms | 84.4 ms | 54.2 ms | DuckDB by 5.12x |
| common | 2203902 | 119.5 ms | 135.0 ms | 149.3 ms | 56.1 ms | DuckDB by 1.25x |

The original mixed-case text was stored unchanged in both engines. DuckDB folded
its index keys; ClickHouse used `lowerUTF8` preprocessing for index tokens.

Raw samples and exact versions are in
[`artifacts/enwik9-clickhouse-text-ci-vs-ngram-v1.json`](artifacts/enwik9-clickhouse-text-ci-vs-ngram-v1.json).

The result compares different architectures: ClickHouse's native text inverted index
and this extension's postings index plus exact base-table recheck. It should be read as a
practical reference for this corpus and machine, not a universal engine ranking.
