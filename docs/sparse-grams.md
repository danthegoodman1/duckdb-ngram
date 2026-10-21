# Sparse grams

A sparse index (`tokenizer = 'sparse'`) stores every trigram of a row plus
longer grams that a fixed hash picks out, so a word- or phrase-length needle
can probe a few long, rare grams in place of many common trigrams. This note
defines the grams, explains how build and query use them, and records the
measurements behind the defaults. The README's "Choosing a tokenizer"
section is the user-facing summary.

## The grams

Give every (`min_gram` − 1)-character window of the folded text a weight. A
substring of `min_gram` to `max_gram` characters is a **sparse gram** when
every window strictly inside it weighs less than both windows at its borders.
With the default `min_gram = 3` the windows are character pairs:

```sql
SELECT sparse_grams('Hello World');
-- [hel, ell, llo, 'lo ', o w, o wo, ' wo', wor, orl, orld, rld]
```

A trigram has no inner window, so every trigram is a sparse gram, and a sparse
index holds everything an `ngram(min_gram)` index holds. Longer grams appear
where the pair weights dip: `orld` qualifies because `rl` weighs less than
both `or` and `ld`.

Whether a substring is a sparse gram depends only on its own characters. If a
row contains the needle, each sparse gram of the needle is therefore a sparse
gram of the row too, and intersecting the postings of any subset of the
needle's grams yields a superset of the matching rows. That is the same
argument that makes a trigram index exact, and the recheck narrows the
superset to the answer as before.

## Extraction

`ExtractGramRanges` (`src/include/ngram/gram.hpp`) enumerates a string's
sparse grams in one pass over a stack of windows whose weights strictly
decrease. Each new window pops and pairs with every lighter window, pairs with
the heavier window left on top, retires windows of equal weight, and pushes
itself. Every window enters and leaves the stack once, so the pass is linear
and yields at most two grams per character; with `max_gram = 8` it yields
about 1.7. A border more than `max_gram` characters back clears the stack,
since every window beneath it lies further still. Build and query share this
function, and `sparse_grams()` exposes it.

`scripts/sparse_grams_check.py` compares `sparse_grams()` with a brute-force
reading of the definition on random strings over small alphabets, where
repeated windows exercise the tie rule.

## Weights

A window's weight is MurmurHash3's 64-bit finalizer over its codepoints, each
entering as its UTF-8 bytes packed big-endian (`SparseWindowWeight`). For a
two-character window the packed pair goes through one bijection, so distinct
pairs never tie. The function uses integer arithmetic only, so every platform
cuts the same grams. It is part of the storage format: changing it would
change every sparse index's keys.

Other implementations chose differently. ClickHouse weighs each window with
one CRC32-C step of its `updateWeakHash32`, which falls back to a different
hash on CPUs without a CRC instruction, so those machines cut different
grams; its gram sets also changed twice across releases. Danlark1's reference
implementation hashes signed `char` bytes, so bytes above 0x7F weigh
differently on x86 and ARM. GitHub's code search does not publish its weight
function. Cursor and ami-gin weigh pairs by corpus frequency, which an
extension that must agree with every past build cannot do without freezing a
table into the format.

## Probing: covering grams

Two sparse grams of one string either nest or share at most one border
window, so the grams that no longer gram contains form a chain across the
needle. A row holding a gram holds every gram inside it, so a contained
gram's postings include the container's and add nothing to an intersection.
`DecomposeNeedle` keys only that chain, the **covering grams**, in one pass,
and the planner probes the rarest `ngram_max_grams_per_query` of them, as it
does an `ngram` index's grams. `EXPLAIN ANALYZE` lists the grams it probed:

```
Ngram Tokenizer: sparse(3, 8)
Ngram Mode: index (<= 16 candidates, 302 decoded rowids)
Ngram Probe Grams: 'world 1', 'hel', 'ell'
```

ClickHouse probes the same chain (its `compactTokens`) but requires every
covering gram. GitHub's write-up also keeps the covering grams.

## Storage: bucketed long grams

The segments table holds one row per (gram key, segment of 2^20 rowids).
Trigram keys are few and dense; long sparse grams are many and rare, and a
row per distinct long gram swamps the table. On enwik9 an exact-key
sparse(3, 16) index held 84 million rows, 60 million of them with a single
posting, and took 4.6 GB.

A sparse index therefore keys each gram longer than `min_gram` by one of
2^20 buckets: FNV-1a over the gram's bytes, mixed by the same finalizer, its
top 20 bits under the reserved upper half `0xFFFFFFFFFFFFFFFE`. `min_gram`
grams keep their exact keys. A bucket's postings are the union of its grams',
a superset that the intersection with the needle's other grams and the
recheck narrow. On enwik9 the candidate counts stayed within 1% of exact keys
while the sparse(3, 16) index shrank from 4.6 to 2.9 GB.

## Measurements

Same machine, enwik9 split into 10.9 million nonempty lines (0.92 GiB of
text), 24 threads, case-insensitive indexes, three alternating builds per
variant:

| Index | Build | Build RSS | Index size |
| --- | ---: | ---: | ---: |
| `ngram(3)` | 5.2 s | 5.4 GiB | 0.93 GiB |
| `sparse(3, 8)` | 16.0 s | 10.5 GiB | 2.30 GiB |
| `sparse(3, 16)` | 20.1 s | 12.6 GiB | 2.72 GiB |

Warm `ngram_search` medians over fourteen interleaved runs per cell, with the
rows each probe fetched (`scan` marks a candidate-fraction fallback). Other
work loaded the machine during the run, which inflates the scans; read the
columns against each other:

| Needle | `ngram(3)` | `sparse(3, 8)` | `sparse(3, 16)` |
| --- | ---: | ---: | ---: |
| `history` | 117 ms, scan | 14 ms, 90,703 | 14 ms, 90,715 |
| `the United States` | 130 ms, scan | 10 ms, 42,709 | 10 ms, 42,713 |
| `United States` | 116 ms, scan | 23 ms, 147,048 | 22 ms, 147,052 |
| `References` | 17 ms, 115,538 | 9 ms, 50,299 | 9 ms, 50,299 |
| `New York City` | 3 ms, 9,602 | 4 ms, 8,408 | 4 ms, 8,408 |
| `photosynthesis` | 1 ms, 464 | 2 ms, 347 | 2 ms, 347 |
| `chemistry` | 3 ms, 7,334 | 4 ms, 6,351 | 4 ms, 6,351 |
| `Wikipedia:` | 5 ms, 26,575 | 6 ms, 26,565 | 6 ms, 26,565 |
| `supercalifragilistic` | 1 ms, 12 | 2 ms, 12 | 2 ms, 12 |
| `of the` | 121 ms, scan | 123 ms, scan | 122 ms, scan |

Sparse grams pay off where a needle's trigrams are all common but the needle
is not: `history`'s trigrams are common enough that the trigram probe's
candidate bound exceeds the candidate fraction and the query scans, while the
sparse probe reads `istory`, a gram the trigram index lacks. Where three
trigrams already isolate the answer, the sparse index fetches the same rows
about a millisecond slower. Needles made of very common grams (`of the`) scan
under either index.

## Choosing the lengths

`min_gram` is the shortest needle the index answers, and every `min_gram`
window is a gram, so it plays the part `gram` plays for an `ngram` index;
three suits most text. `max_gram` caps the long grams: raising it from 8 to 16
made the enwik9 index 18% larger and its build 26% slower and changed no
needle's fetches by more than a few rows. Code or identifiers with long
distinctive runs may reward a larger cap.

## Compared with other implementations

| | Weight | Unit | Lengths | Query side |
| --- | --- | --- | --- | --- |
| this extension | fmix64 of packed codepoints | codepoint | 3–8 by default, up to 64 | rarest K covering grams, then recheck |
| ClickHouse text index | CRC32-C step of `updateWeakHash32` (CPU-dependent fallback) | codepoint | 3–100 by default | every covering gram (`compactTokens`), then recheck |
| GitHub code search | unpublished | unpublished | unpublished | covering grams |
| Danlark1/sparse_ngrams | multiply-shift over signed `char` | byte | uncapped; covering capped at 16 | covering grams |
| SereneDB | Danlark1's hash over codepoints | codepoint | up to 16 | covering grams |
| Cursor, ami-gin | corpus pair frequency | — | ami-gin: knee at 5–6 | covering grams |

Against ClickHouse 26.7's `text(tokenizer = sparseGrams(3, 8))` on the same
corpus and machine ([`benchmarks/CLICKHOUSE_SPARSE.md`](../benchmarks/CLICKHOUSE_SPARSE.md),
case-sensitive, three fresh builds), DuckDB built its `sparse(3, 8)` index in
16.7 s against 104.4 s, stored 2.41 GiB against 2.85 GiB, and answered the
warm rare, moderate and common needles in 2, 6 and 55 ms against 5.9, 71.4 and
104.6 ms. ClickHouse requires every covering gram; this extension probes the
rarest few and rechecks the rows it fetches. The collection ran this branch's build before it was
committed, so the artifact's `source_commit` names its base.
