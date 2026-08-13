# DuckDB ngram vs ClickHouse text index

This is a bounded same-machine point of reference, not an exhaustive tuning study.
Both engines used 24 threads, a 48 GiB memory setting, the same 1 GB enwik9-derived
line corpus, 3-grams, and exact substring rechecks.

ClickHouse used `TYPE text(tokenizer = ngrams(3))`; DuckDB used its
native case-sensitive trigram index.
DuckDB retained its production adaptive scan fallback for dense terms.

## Load, build, and storage

The input file was already in the host page cache; these are warm-source ingest times.
Storage is paired DuckDB database-file and ClickHouse active-table on-disk size;
ClickHouse logs and temporarily retained inactive parts are excluded.

| Engine | Load median (range) | Index build median (range) | Base | Indexed | Index delta |
| --- | ---: | ---: | ---: | ---: | ---: |
| duckdb | 2.102 s (1.471–2.114) | 7.541 s (7.537–7.606) | 0.690 GiB | 1.685 GiB | 0.995 GiB |
| clickhouse | 1.348 s (1.301–1.386) | 18.836 s (16.814–20.048) | 0.538 GiB | 1.853 GiB | 1.316 GiB |

At a glance:

- Load: ClickHouse by 1.56x.
- Index build: DuckDB by 2.50x.
- Incremental index storage: DuckDB by 1.32x.

## File-data-cold query latency

Each observation used a stopped engine, `sync`, and `POSIX_FADV_DONTNEED` on every
database/index file before a fresh process and one query with no warm-up. These three
samples model file-data-cold access; filesystem metadata may remain cached.

| Needle | Matches | DuckDB search | DuckDB scan | ClickHouse search | ClickHouse scan | Faster search |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| rare | 1 | 17.0 ms (15.0–17.0) | 171.0 ms (167.0–171.0) | 37.0 ms (35.7–43.6) | 129.4 ms (116.4–131.2) | DuckDB by 2.18x |
| moderate | 26068 | 142.0 ms (136.0–160.0) | 163.0 ms (163.0–164.0) | 138.5 ms (138.2–139.1) | 120.6 ms (119.9–121.0) | roughly tied (within 10%) |
| common | 1963067 | 175.0 ms (164.0–177.0) | 165.0 ms (162.0–167.0) | 162.8 ms (158.6–164.5) | 125.0 ms (116.1–127.2) | roughly tied (within 10%) |

## Warm query latency

One warm-up preceded 10 measured samples per cell. Sorted matching-row digests matched
across both engines and their scan controls.

| Needle | Matches | DuckDB search | DuckDB scan | ClickHouse search | ClickHouse scan | Faster search |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| rare | 1 | 7.0 ms | 40.0 ms | 11.9 ms | 43.7 ms | DuckDB by 1.69x |
| moderate | 26068 | 15.0 ms | 27.0 ms | 68.7 ms | 43.4 ms | DuckDB by 4.58x |
| common | 1963067 | 41.5 ms | 36.0 ms | 98.3 ms | 44.9 ms | DuckDB by 2.37x |

Raw samples and exact versions are in
[`artifacts/enwik9-clickhouse-text-vs-ngram-v1.json`](artifacts/enwik9-clickhouse-text-vs-ngram-v1.json).

The result compares different architectures: ClickHouse's native text inverted index
and this extension's postings index plus exact base-table recheck. It should be read as a
practical reference for this corpus and machine, not a universal engine ranking.
