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
| duckdb | 1.820 s (1.620–2.190) | 7.014 s (6.969–7.028) | 0.683 GiB | 1.614 GiB | 0.931 GiB |
| clickhouse | 1.309 s (1.286–1.332) | 17.729 s (16.598–17.879) | 0.538 GiB | 1.808 GiB | 1.270 GiB |

At a glance:

- Load: ClickHouse by 1.39x.
- Index build: DuckDB by 2.53x.
- Incremental index storage: DuckDB by 1.36x.

## File-data-cold query latency

Each observation used a stopped engine, `sync`, and `POSIX_FADV_DONTNEED` on every
database/index file before a fresh process and one query with no warm-up. These three
samples model file-data-cold access; filesystem metadata may remain cached.

| Needle | Matches | DuckDB search | DuckDB scan | ClickHouse search | ClickHouse scan | Faster search |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| rare | 12 | 18.0 ms (17.0–19.0) | 286.0 ms (284.0–289.0) | 49.2 ms (48.1–54.5) | 137.8 ms (127.1–138.1) | DuckDB by 2.73x |
| moderate | 26540 | 143.0 ms (140.0–152.0) | 266.0 ms (263.0–295.0) | 162.1 ms (154.7–166.3) | 125.9 ms (120.3–131.0) | DuckDB by 1.13x |
| common | 2203902 | 221.0 ms (219.0–229.0) | 204.0 ms (198.0–219.0) | 203.4 ms (196.3–203.9) | 127.4 ms (122.7–134.4) | roughly tied (within 10%) |

## Warm query latency

One warm-up preceded 10 measured samples per cell. Sorted matching-row digests matched
across both engines and their scan controls.

| Needle | Matches | DuckDB search | DuckDB scan | ClickHouse search | ClickHouse scan | Faster search |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| rare | 12 | 6.0 ms | 209.0 ms | 13.3 ms | 59.5 ms | DuckDB by 2.21x |
| moderate | 26540 | 16.0 ms | 190.5 ms | 86.2 ms | 49.5 ms | DuckDB by 5.39x |
| common | 2203902 | 117.0 ms | 132.0 ms | 147.3 ms | 53.6 ms | DuckDB by 1.26x |

The original mixed-case text was stored unchanged in both engines. DuckDB folded
its index keys; ClickHouse used `lowerUTF8` preprocessing for index tokens.

Raw samples and exact versions are in
[`artifacts/enwik9-clickhouse-text-ci-vs-ngram-v1.json`](artifacts/enwik9-clickhouse-text-ci-vs-ngram-v1.json).

The result compares different architectures: ClickHouse's native text inverted index
and this extension's postings index plus exact base-table recheck. It should be read as a
practical reference for this corpus and machine, not a universal engine ranking.
