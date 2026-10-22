# DuckDB sparse-gram index vs ClickHouse sparseGrams text index

This is a bounded same-machine point of reference, not an exhaustive tuning study.
Both engines used 24 threads, a 48 GiB memory setting, the same 1 GB enwik9-derived
line corpus, sparse grams of 3 to 8 characters, and exact substring rechecks.

ClickHouse used `TYPE text(tokenizer = sparseGrams(3, 8))`; DuckDB used
`create_ngram_index(..., tokenizer='sparse', min_gram=3, max_gram=8, case_insensitive=false)`.
DuckDB retained its production adaptive scan fallback for dense terms.

## Load, build, and storage

The input file was already in the host page cache; these are warm-source ingest times.
Storage is paired DuckDB database-file and ClickHouse active-table on-disk size;
ClickHouse logs and temporarily retained inactive parts are excluded.

| Engine | Load median (range) | Index build median (range) | Base | Indexed | Index delta |
| --- | ---: | ---: | ---: | ---: | ---: |
| duckdb | 4.126 s (3.485–4.584) | 16.712 s (16.584–16.899) | 0.550 GiB | 2.960 GiB | 2.411 GiB |
| clickhouse | 1.521 s (1.512–2.357) | 104.377 s (104.271–105.370) | 0.538 GiB | 3.391 GiB | 2.853 GiB |

At a glance:

- Load: ClickHouse by 2.71x.
- Index build: DuckDB by 6.25x.
- Incremental index storage: DuckDB by 1.18x.

## File-data-cold query latency

Each observation used a stopped engine, `sync`, and `POSIX_FADV_DONTNEED` on every
database/index file before a fresh process and one query with no warm-up. These three
samples model file-data-cold access; filesystem metadata may remain cached.

| Needle | Matches | DuckDB search | DuckDB scan | ClickHouse search | ClickHouse scan | Faster search |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| rare | 1 | 9.0 ms (9.0–10.0) | 129.0 ms (127.0–133.0) | 25.1 ms (19.8–29.2) | 125.9 ms (123.7–133.9) | DuckDB by 2.79x |
| moderate | 26068 | 121.0 ms (112.0–137.0) | 121.0 ms (113.0–174.0) | 143.5 ms (142.3–151.3) | 144.8 ms (117.0–161.2) | DuckDB by 1.19x |
| common | 1963067 | 128.0 ms (124.0–136.0) | 128.0 ms (127.0–172.0) | 248.7 ms (234.6–350.0) | 126.7 ms (125.9–148.7) | DuckDB by 1.94x |

## Warm query latency

One warm-up preceded 10 measured samples per cell. Sorted matching-row digests matched
across both engines and their scan controls.

| Needle | Matches | DuckDB search | DuckDB scan | ClickHouse search | ClickHouse scan | Faster search |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| rare | 1 | 2.0 ms | 56.5 ms | 5.9 ms | 45.6 ms | DuckDB by 2.93x |
| moderate | 26068 | 6.0 ms | 37.5 ms | 71.4 ms | 49.0 ms | DuckDB by 11.90x |
| common | 1963067 | 55.0 ms | 51.0 ms | 104.6 ms | 47.0 ms | DuckDB by 1.90x |

Raw samples and exact versions are in
[`artifacts/enwik9-clickhouse-sparse-vs-sparse-v1.json`](artifacts/enwik9-clickhouse-sparse-vs-sparse-v1.json).

The result compares different architectures: ClickHouse's native text inverted index
and this extension's postings index plus exact base-table recheck. It should be read as a
practical reference for this corpus and machine, not a universal engine ranking.
