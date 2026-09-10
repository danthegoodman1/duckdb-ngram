# Phase 19 measurements, 2026-09-09

Measurements behind the 19B, 19C and 19G ledger rows of `ngram_review_plan.md`.
Every number comes from one machine (24 logical CPUs, 125 GiB, NVMe) and the
scripts beside this file; raw observations are in the JSON files they write.

| Script | Writes | Question |
| --- | --- | --- |
| `format_measure.py` | `format_observations.json` | Format 4 against format 5; refresh generations; manifest scan work |
| `cost_measure.py` | `cost_observations.json` | Per-row costs of scan, fetch and range access; projection; tiny tables; the index/scan crossover |
| `zonemap_probe.py` | stdout | Rows the host's filtered scan reads for one key of the segments table |
| `probe_measure.py` | `probe_observations.json` | Runs `column_probe.sql` (per-column fetch cost by compression) and `recheck_probe.sql` (range against scattered access on both paths) and records their timings |

## Format 4 against format 5 (19B)

Corpus: the first 1,000,000 enwik9 lines (107 MB of text) loaded as `docs(id, s)`,
case-insensitive trigram index, 8 threads, 16 GB memory limit. Twenty refresh
generations of 10,000 appended rows each, then one compaction. The format-4
binary is `main` at d3ecccf, whose query engine predates Phase 19, so the query
columns compare engines as much as formats; build, refresh, compaction and
storage compare the formats directly, because the segments table is identical
and format 4 adds only the statistics table.

| | Format 4 | Format 5 |
| --- | ---: | ---: |
| Index build (create plus checkpoint) | 1.23 s | 1.31 s |
| Index storage after build | 103.8 MiB | 105.0 MiB |
| Refresh of one 10,000-row generation, p50 (min–max) | 173 ms (142–195) | 105 ms (72–123) |
| Refresh with nothing to index, p50 | 96.5 ms | 4.0 ms |
| Storage after 21 generations | 261.0 MiB | 264.3 MiB |
| Compaction of 21 generations | 2.81 s | 1.75 s |
| Compaction with nothing to merge | 216 ms | 23 ms |
| Storage after compaction | 333.0 MiB | 344.8 MiB |
| `ngram_index_stats`, one generation / 21 / compacted | 13 / 16 / 12 ms | 11 / 16 / 13 ms |
| `ngram_indexes` | < 1 ms | < 1 ms |

Reading:

- The statistics table cost format 4 about 90 ms on every refresh and 190 ms on
  every compaction, whether or not there was work, because both rewrote it.
  Format 5 removed that fixed cost; a no-op refresh is now the execution-time
  validation plus an empty pack (20A takes the empty pack out).
- Storage differs by the block allocation of the file, not by the table: both
  segments tables hold the same 238,546 rows after build. Compaction grows the
  file in both formats because it appends the merged rows and DuckDB keeps the
  deleted rows' blocks until a later checkpoint reuses them.
- `ngram_index_stats` aggregates the whole segments table (count distinct,
  fragmentation grouping, posting bytes), so it costs about 12 ms per million
  base rows here and grows with the index; `ngram_indexes` is a registry read.
  This is the cheap-status versus expensive-statistics split 21B asks for.

Warm query latency (7 measured runs after one warmup; the scan column is the
unaccelerated `contains(lower(s), ...)` count):

| Needle | Matches | Format 4 search / scan | Format 5 search / scan | Format 5 after 21 generations | After compaction |
| --- | ---: | ---: | ---: | ---: | ---: |
| Schwarzschild | 23 | 8 / 23 ms | 4 / 23 ms | 8 / 36 ms | 8 / 39 ms |
| photosynthesis | 63 | 8 / 21 ms | 4 / 21 ms | 8 / 35 ms | 9 / 35 ms |
| parliamentary | 577 | 13 / 22 ms | 8 / 20 ms | 15 / 35 ms | 13 / 35 ms |
| chemistry | 1,285 | fallback 37 / 23 ms | fallback 32 / 23 ms | fallback | fallback |

The scan column slows after the appends because the 20 appended row groups of
10,000 rows each are scanned as separate units. The search column slows with
the generations (manifest rows for the rare needle: 10, 43, 91, 177 at 1, 5,
10, 20 generations) and does not recover after compaction even though the
manifest is back to 20 rows: the reason is the physical layout measured next.

## Manifest scans (19C)

`CollectGramRows` ran one filtered scan per needle key, `gram_key = ?`, over the
key-ordered segments table. The profiler's `OPERATOR_ROWS_SCANNED` for the same
scans issued as SQL, on the one-generation table (238,546 rows, 2 row groups)
and on the compacted table (334,477 rows, 3 row groups, 22 sorted runs):

| Needle | Keys | Rows scanned, one scan per key | Rows scanned, one `IN` scan | Probe (`ngram_candidates`) |
| --- | ---: | ---: | ---: | ---: |
| Schwarzschild, fresh build | 10 | 1,085,256 | 238,546 | 4 ms |
| which, fresh build | 3 | 318,418 | 238,546 | 3 ms |
| Schwarzschild, compacted | 10 | 2,832,770 | 334,477 | 8 ms |
| which, compacted | 3 | 849,831 | 334,477 | 7 ms |

Two host facts follow from the numbers and were confirmed in
`duckdb/src/storage/table/row_group.cpp`:

- A constant filter prunes whole row groups by zone map but reads every vector
  of an admitted row group: `RowGroup::CheckZonemapSegments` derives the vector
  to skip to from a column segment's row-group-relative start, which only skips
  the first excluded segment. A key that lives in one 14,336-row column segment
  costs a 115,666-row read (`zonemap_probe.py`).
- A constant `IN` list does not prune at all on v1.5.5: the combined scan reads
  the whole table.

So per-key scans cost about one row group per key per sorted run, and a
combined scan costs the whole table; neither is the right shape. The remedy
is in `src/probe.cpp`: `AdmittedKeySpans` walks the key column's segment tree
through the row group's public raw column accessor, keeps the vector-aligned
spans of the column segments whose statistics admit the key, and
`CollectGramRows` scans only those spans with the same bounded scan the range
and tail paths use, then the transaction's local rows. The profile reports the
spans as `Ngram Manifest Rows Visited`; the manifest stages of
`format_observations.json`, re-run on the changed binary, record them beside
the probe time:

| Needle | Keys | Rows the host's scans read | Rows the positioned scans visit | Probe before / after |
| --- | ---: | ---: | ---: | ---: |
| Schwarzschild, fresh build | 10 | 1,085,256 | 143,360 | 4 / 1 ms |
| which, fresh build | 3 | 318,418 | 43,008 | 3 / 2 ms |
| Schwarzschild, compacted | 10 | 2,832,770 | 938,224 | 8 / 3 ms |
| which, compacted | 3 | 849,831 | 283,720 | 7 / 3 ms |

## Query cost model (19G)

`cost_measure.py` on the full corpus (10,920,423 rows, case-insensitive trigram
index, 48 GB memory limit, warm); `cost_observations.json` holds the run after
the two execution changes below, and the figures quoted for the run before
them came from the same script on the source before those changes. Costs are CPU seconds at one
thread unless stated, from seven measured runs after a warmup, with the
probe-only time (`ngram_candidates`) subtracted where a per-row figure is given.

Per-row costs:

| Access | Cost per row | Source |
| --- | ---: | --- |
| Full scan, `contains(lower(s), needle)`, one thread | 92 ns CPU | `scan` |
| Full scan, 24 threads | 6.3 ns wall (134 ns CPU) | `scan` |
| Scattered candidate fetch by rowid, one thread | 0.8 to 1.5 µs | `fetch_threads_1` |
| Scattered candidate fetch, 24 threads | 0.22 to 0.29 µs wall | `fetch_threads_24` |
| Row of a range scan, one thread, filters included | 0.29 µs | `range_threads_1.history`: (58.18 − 6.35 ms − 18,817 × 1.35 µs) / 90,112 rows |
| Extra fetched column per kept row: bit-packed BIGINT / ALP DOUBLE / short FSST VARCHAR | none measurable / 1.1 / 11 µs | `probe_observations.json`, `column_probe.sql` |
| Probe (manifest plus decode), rare to 3% needles | 1 to 7 ms | `probe_real_ms` |

Two execution changes came out of the phase timing:

- The explicit path's recheck folded and searched every row itself at 0.47 µs
  per row, five times the host's `contains(lower(s), needle)`, and the tail
  scan then repeated the work. `ngram_search` now binds that host expression
  once, pushes it as a native table filter into its tail and range scans, and
  runs it through an executor only on rows fetched by rowid. Both exact paths
  therefore evaluate one native predicate, once per row. The clustered
  `history` query fell from 81 to 58 ms and the scattered one from 178 to
  154 ms at one thread.
- Per-row fetches read the recheck columns first and the remaining projected
  columns only for the rows the recheck keeps. Fetching a short FSST string
  by rowid costs about 11 µs on this corpus because the host decompresses
  from the segment start, so a six-column projection cost 484 ms against a
  503 ms scan before the change and 99 ms after it.

Locality: the same `history` query on a copy of the corpus with its matches
contiguous runs 90,112 of its 108,929 candidates through range scans and
takes 58 ms against 154 ms scattered. A span row costs 0.29 µs against 1.35 µs
for a scattered fetch, a ratio of 4.6, so `RANGE_ROWS_PER_FETCH` is 4: the
executor reads a batch as a range scan when its candidates fill at least a
quarter of their span, and the admission gate prices a segment at the smaller
of its candidate bound and its rowid span over four. The bound of this needle
is 351,383 rows (3.2%) on both layouts because its grams' postings span whole
segments either way; the discount applies where the postings themselves are
clustered.

Crossover with the gate open (index against scan, wall time):

| Needle | Candidate bound | One thread | 24 threads |
| --- | ---: | ---: | ---: |
| government | 0.56% | 88 vs 1,123 ms | 18 vs 77 ms |
| parliamentary | 0.67% | 28 vs 1,157 ms | 9 vs 78 ms |
| chemistry | 0.98% | 13 vs 1,218 ms | 5 vs 81 ms |
| history | 3.2% | 154 vs 1,228 ms | 33 vs 81 ms |
| which | 3.6% | 446 vs 1,103 ms | 85 vs 74 ms |
| the | 20% | 2,660 vs 1,149 ms | 623 vs 80 ms |

The default `ngram_max_candidate_fraction` moves from 0.01 to 0.02: below the
24-thread crossover, rounded toward scanning. The previous default came from a
3.8 µs fetch measured before the Phase 19 repairs.

Tiny tables (per statement, warm, in one process, the CLI's own overhead
included; `tiny_microseconds`):

| Rows | Thread count | Transparent rewrite | Plain scan | `ngram_search` |
| ---: | ---: | ---: | ---: | ---: |
| 1,000 | 1 | 315 µs | 210 µs | 327 µs |
| 1,000 | 24 | 368 µs | 292 µs | 365 µs |
| 100,000 | 1 | 515 µs | 4,815 µs | 440 µs |
| 100,000 | 24 | 365 µs | 5,023 µs | 296 µs |

The accelerated paths carry about 100 µs of fixed cost (registry read, guard
verdict, manifest scan, and on the 1,000-row table a declined probe), which a
scan of a thousand rows undercuts; at a hundred thousand rows they win nine
times over. Acceleration stays opt-in, so no row-count floor is imposed.

## Latency before and after Phase 19 (gate)

`latency_rebaseline.py` builds one database per binary from the same staged
enwik9 lines (`main` at d3ecccf writes format 4 with the query engine before
Phase 19; the current build writes format 5) and times the same needles in one
warm process: twenty-one measured runs after a warmup, count parity checked on
every pair, 48 GB memory limit. `latency_observations.json` holds every sample;
the table gives p50 wall time at one thread / 24 threads.

| Needle | Matches | `ngram_search` before | after | Transparent rewrite before | after | Unaccelerated scan | Probe before / after |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Schwarzschild (rare) | 141 | 11 / 8 ms | 1 / 1 ms | 11 / 8 ms | 1 / 1 ms | 1,291 / 85 ms | 9 / 1 ms |
| parliamentary (moderate) | 3,630 | 45 / 12 ms | 29 / 6 ms | 49 / 13 ms | 40 / 9 ms | 1,159 / 78 ms | 10 / 3 ms |
| chemistry (moderate) | 6,350 | 24 / 10 ms | 13 / 4 ms | 25 / 11 ms | 15 / 4 ms | 1,218 / 81 ms | 11 / 3 ms |
| history (declined, 3.2% bound) | 90,665 | 1,835 / 125 ms | 1,231 / 82 ms | 2,678 / 206 ms | 2,641 / 198 ms | 1,227 / 82 ms | 14 / 6 ms |
| which (declined, 3.6% bound) | 297,687 | 1,697 / 117 ms | 1,098 / 75 ms | 2,585 / 200 ms | 2,347 / 181 ms | 1,096 / 74 ms | 14 / 5 ms |

Against the Phase 19 targets (warm p50 at 24 threads): rare 1 ms against 3 ms,
moderate 4 to 9 ms against 12 ms. The explicit fallback is now the native scan
within 1% at both thread counts, where it was 49% slower before: its tail scan
runs the host's predicate as a pushed filter instead of rechecking every row
itself. The transparent fallback runs the user's `ILIKE` natively; its
baseline is the same `ILIKE` with the extension's optimizer disabled, which
`ilike_baseline.py` records in the same JSON: after the change the declined
`history` query runs 2,633 / 193 ms through the extension against 2,625 /
193 ms natively, and `which` 2,339 / 175 ms against 2,328 / 175 ms, within
0.5%; before it, `which` ran 8% slower than native at one thread. Index build
time went from 7.20 s to 6.97 s on this corpus. Tiny tables are covered in the cost section: the
accelerated paths cost about 100 µs of fixed work per statement on top of the
host's own 0.2 ms per-statement floor, so the 0.3 ms target reads as 0.3 to
0.4 ms per statement measured end to end, of which the extension's share is a
third.

## Build memory and no-op maintenance (Phase 20)

`build_measure.py` loads the staged enwik9 lines into a fresh database and
meters `PRAGMA create_ngram_index` in its own process with `/usr/bin/time -v`
and a spill-directory sampler, three times per label; `noop_measure.py` times
the maintenance calls on the benchmark database at 24 threads.

| Build | Wall, median | Peak RSS, median | Spill | Index bytes |
| --- | ---: | ---: | ---: | ---: |
| baseline (464023e, 64-bit rowids in the pack aggregate) | 7.03 s | 8.82 GiB | none | 0.929 GiB |
| 32-bit in-segment offsets | 7.00 s | 6.88 GiB | none | 0.929 GiB |

The aggregate's payload is the build's largest allocation: 711 M postings at
eight bytes each held until finalization. Storing each rowid as its offset
within the group's segment halves that and leaves the postings byte-identical
(`build_scale.test` compares them with brute force). Whole-process peak bytes
per posting, the base table's buffers included, went from 13.3 to 10.4.
`PAIR_STATE_BYTES` stays 32: a million single-posting groups measure about
33 bytes per pair, where the group entry and the first block dominate.

| Maintenance call | time |
| --- | ---: |
| `ngram_refresh('docs')`, nothing past the mark | 0 to 1 ms |
| `ngram_refresh('docs', 1000000)`, nothing past the mark | 1 to 3 ms |
| `ngram_compact('docs')`, one generation | 0 to 1 ms |
| `ngram_refresh('docs')` after appending 100,000 rows | 529 ms |
| `ngram_refresh('docs')` again in that process | 0 to 1 ms |
| `ngram_refresh('docs')` in a fresh process | 0 to 1 ms |
| `ngram_compact('docs')` merging that generation | 719 ms |
| `ngram_compact('docs')` again in that process, before a checkpoint | 22 to 28 ms |
| `ngram_compact('docs')` in a fresh process, after the shutdown checkpoint | 0 to 1 ms |

A no-op refresh is decided at pragma expansion, where the mark and the table's
allocated rowids are known, and still runs its fence call. A no-op merge is
decided from the generation column's row-group statistics, and compaction
leaves every row at generation 0, so the shortcut applies to a built index,
after a purge, and after a checkpoint whose vacuum dropped the row groups that
held the merged-away generation, which a 100,000-row generation on enwik9
forms on its own. In the merge's own transaction and session, and for a small
generation that shares a row group with older rows until a purge rewrites the
table, the statistics keep the old maximum and the full check runs: about
24 ms per two million segment rows here, over the 20 ms target. A refresh over
a tail whose rows were all deleted still packs an empty delta; the delta is
appended at execution by the fence's append call, which writes nothing when
there is nothing, instead of an empty batch insert that the host's
empty-insert hazard turns into an empty table when a purge follows in the same
transaction.
