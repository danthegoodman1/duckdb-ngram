# ngram benchmark results

<!-- BEGIN NGRAM RELEASE EVIDENCE -->
## Checked bounded benchmark

Same-machine observations; queries are warm-cache for a **non-default case-sensitive trigram
index** over nonempty line-per-row `enwik9`. These are not cold-cache, large-scale, or
shipped-default claims. Raw evidence: [`benchmarks/artifacts/enwik9-current-v1.json`](artifacts/enwik9-current-v1.json).

- Engine commit: `b6a388c8c39f`; build commit: `b6a388c8c39f`; DuckDB v1.5.5 / source d8cdaa33;
  static-extension release CLI. The numbers describe the engine commit's `src/**` and are
  re-collected on release; later commits keep this block until the next collection.
- Corpus: 10,920,423 rows, 0.919 GiB of UTF-8 text; three fresh load/build pairs.
- Timed load—fresh CLI and absent DB through create, hex decode, insert, CHECKPOINT—was
  2.299 s median (2.170–2.401 s). Timed index build—fresh CLI through create-index and
  CHECKPOINT—was 7.834 s median (7.781–7.941 s), 120.14 MiB/s of source text.
- Paired whole-database size increase: 0.996 GiB apparent, 0.996 GiB allocated
  (median); 1.083× source bytes. This whole-DB effect includes allocator/checkpoint effects.
- Build-process max RSS: 11.752 GiB median. Sampled peak temp apparent file bytes: 0.000 GiB
  median, polled every 100 ms; zero means none observed, not proof that no brief spill occurred.
  Acquisition, normalization, relation/stat checks, EXPLAIN, and parity are untimed. Loads may
  read cached transport pages; builds follow relation identity and may read cached source pages.

| needle class | ngram_search mode | exact matches | candidates | ngram_search p50 / p95 / range | scan p50 / p95 / range | scan ÷ search p50 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| rare | index | 1 | 12 | 7 / 7 ms / 6–7 ms | 43 / 45 ms / 42–47 ms | 6.14× |
| moderate | index | 26,068 | 26,381 | 15 / 16 ms / 14–17 ms | 29 / 30 ms / 27–31 ms | 1.93× |
| dense | full-scan-fallback | 1,963,067 | 1,963,067 | 44 / 45 ms / 43–46 ms | 39 / 40 ms / 37–40 ms | 0.89× |

The timed campaign adds one warmup per variant after untimed parity/EXPLAIN executions, then
twenty-one measured observations per variant using a fixed-seed interleaving on one connection.
Timer resolution is one millisecond. Exact ngram_search and scan counts match every observation;
candidate counts are a separately measured lossy superset.
`check` verifies the artifact, the pinned gitlinks, commit ancestry, and this rendered block on
any commit; `--current-source` also requires `src/**` and the tool to hash to the artifact's
records, which holds at the engine commit and on release tags. `validate` applies the strict
check to any artifact path:

```sh
python3 benchmarks/release_evidence.py check
python3 benchmarks/release_evidence.py check --current-source
python3 benchmarks/release_evidence.py validate --artifact benchmarks/artifacts/enwik9-current-v1.json
```

Optional `--binary` validation requires the exact CLI/SHA recorded for the artifact build commit;
an arbitrary later rebuild is rejected.

For a fresh independent run:

```sh
python3 benchmarks/release_evidence.py collect --output-root ./release-evidence-work \
  --artifact ./release-evidence-rerun.json
python3 benchmarks/release_evidence.py validate --artifact ./release-evidence-rerun.json \
  --binary build/release/duckdb
```

`enwik9` is the first billion bytes of the March 2006 English Wikipedia dump, obtained from
[Matt Mahoney's canonical benchmark page](https://www.mattmahoney.net/dc/textdata.html). It is attributable to English Wikipedia
contributors. SHA-256 values were freshly observed; raw MD5/SHA-1 are published there. The
corpus is not redistributed or licensed by this repository's MIT license; do not assume
public-domain status. Review [Wikimedia reuse guidance](https://dumps.wikimedia.org/legal.html) and the
[Wikimedia Terms of Use](https://foundation.wikimedia.org/wiki/Policy:Terms_of_Use).
<!-- END NGRAM RELEASE EVIDENCE -->
