# ngram — exhaustive substring search for DuckDB

`ngram` gives DuckDB a durable trigram index that accelerates substring search
over large text columns:

```sql
INSTALL ngram FROM community;
LOAD ngram;

PRAGMA create_ngram_index('logs', 'message');

SELECT * FROM ngram_search('logs', 'connection reset');
```

Every query returns **every** matching row. The index proposes candidates; the
original predicate is then re-evaluated against the real string, so an index
that is imprecise, lagging, or simply unusable can cost you time — never a row,
and never a wrong row.

The index lives in ordinary DuckDB tables. It is written by the WAL, recovered
on restart, buffer-managed like any other data, and completely inert when the
extension is not loaded: a database with an ngram index opens, reads, and
writes normally in a stock DuckDB build.

---

## Contents

- [Installing](#installing)
- [Quick start](#quick-start)
- [The correctness contract](#the-correctness-contract)
- [Staleness: what is detected and what is not](#staleness-what-is-detected-and-what-is-not)
- [Maintenance](#maintenance)
- [API reference](#api-reference)
- [Settings](#settings)
- [Performance expectations](#performance-expectations)
- [Limitations](#limitations)
- [Platform support](#platform-support)
- [Building from source](#building-from-source)

---

## Installing

```sql
INSTALL ngram FROM community;
LOAD ngram;
```

The extension is built against DuckDB **v1.5.5** and links its internal C++
API, so it must be loaded into a matching DuckDB version.

> The community-extensions submission is prepared but not yet merged (see
> [`packaging/SUBMISSION.md`](packaging/SUBMISSION.md)). Until it is, load the
> binary directly:
>
> ```sh
> duckdb -unsigned -c "LOAD '/path/to/ngram.duckdb_extension';"
> ```

---

## Quick start

```sql
LOAD ngram;

CREATE TABLE logs AS SELECT * FROM read_csv('logs/*.csv');

-- Build the index. This is a normal DuckDB query internally: parallel,
-- spillable, and it can take a while on a large table.
PRAGMA create_ngram_index('logs', 'message');

-- Explicit search: exact, complete, rechecked.
SELECT * FROM ngram_search('logs', 'connection reset');

-- Or let plain SQL use the index (opt-in, see Settings).
SET ngram_auto_accelerate = true;
SELECT * FROM logs WHERE message LIKE '%connection reset%';

-- After appending rows, fold them into the index.
INSERT INTO logs SELECT * FROM read_csv('logs/2026-08-*.csv');
PRAGMA ngram_refresh('logs');

-- Check on the index.
PRAGMA ngram_index_stats('logs');
```

> **One statement at a time.** DuckDB expands a pragma before the surrounding
> batch executes, so `PRAGMA create_ngram_index('t', 'c')` cannot appear in the
> same multi-statement batch as the `CREATE TABLE t` it refers to. Run it as its
> own statement.

---

## The correctness contract

A row that contains `needle` necessarily contains every trigram of `needle`.
Intersecting the posting lists of those trigrams therefore yields a **superset**
of the true matches. Everything the index does to go faster — probing only the
rarest few trigrams, skipping segments, letting the index lag behind the table —
can only make that superset larger, never smaller.

The answer is then narrowed to exactly the truth by three mechanisms:

1. **Recheck.** Every candidate row is fetched and the original predicate is
   evaluated against the real string. For the transparent path this is the
   query's own pushed-down filter, evaluated exactly, so query semantics never
   depend on how the index normalizes text. This is why **false positives are
   impossible in every state the index can be in**.
2. **The tail scan.** Rows appended since the last `create`/`refresh` are past
   the index's high-water mark. Every query brute-force scans that tail and
   unions the matches in. A rowid zone-map filter means the scan skips the
   indexed row groups entirely, so it costs about what the unindexed tail costs.
3. **The transaction-local phase.** Rows your own open transaction has inserted
   but not committed have no permanent rowids yet and are never indexed. They
   are covered by the same tail scan, so a search inside a writing transaction
   sees your own uncommitted rows.

Short needles (fewer characters than the gram size, 3 by default) cannot be
probed at all. Those queries fall back to a full scan — slower, still exact.

### Case sensitivity

An index is case-insensitive by default (`case_insensitive := true`), folding
text with a simple per-codepoint lowercase that index build and needle
decomposition share.

- `ngram_search` matches with the index's own semantics: case-insensitively
  over a case-insensitive index, byte-exact over a case-sensitive one.
- The transparent path always applies **your** predicate. `LIKE` and
  `contains()` stay case-sensitive even over a case-insensitive index (the
  folded index just proposes a few extra candidates that recheck discards);
  `ILIKE` only ever probes a case-insensitive index, because a case-sensitive
  one could not propose the case-variant matches it must find.

---

## Staleness: what is detected and what is not

The index tracks appends through `PRAGMA ngram_refresh`. Other kinds of change
can leave it describing a table that no longer exists in that shape. DuckDB
v1.5.5 offers no triggers and no change feed, so the extension cannot always
know. What it can prove, it acts on.

**Detected — and therefore refused, not silently answered:**

| State | What happens |
| --- | --- |
| The table is shorter than the index's high-water mark (a checkpoint vacuumed rows away) | `ngram_search` / `ngram_candidates` raise a rebuild-required error; the transparent path silently falls back to a plain scan; the maintenance pragmas refuse |
| The table was dropped and re-created under the same name, in the running instance | same |
| The indexed column was renamed or retyped | same |
| The table's column list is no longer a prefix of the recorded one (when the table's identity cannot be proven — another session, or after a `DETACH` + re-`ATTACH`) | same |
| A recorded row witness no longer holds the value the index was built from | `ngram_refresh` / `ngram_compact` / `ngram_index_stats` report it and refuse to maintain |

The query-path checks cost one O(1) read of the table's row count plus its
column list — cheap enough to run on every query. The row-witness check re-reads
a handful of rows and therefore runs only in the maintenance pragmas.

**Not detected — misses only, never wrong rows:**

| State | Why | What to do |
| --- | --- | --- |
| An `UPDATE` that rewrote a row in place, below the high-water mark | v1.5.5 updates in place and offers no way to enumerate the rows an `UPDATE` touched | rebuild: `drop_ngram_index` then `create_ngram_index` |
| A checkpoint vacuum whose row motion happens to leave every recorded witness holding an equal value | the surviving evidence is indistinguishable from a healthy index | rebuild |
| The indexed column dropped and re-added with the same name and type | the query paths see a healthy shape; `ngram_index_stats` still catches it via the witnesses | rebuild |
| A same-shape re-creation of the table performed in an **earlier** session | v1.5.5 persists no table identity token, and catalog OIDs are per-process | rebuild |

**Never affected:** false positives. Recheck makes a wrong row impossible in
every one of the states above.

This is exactly why `ngram_auto_accelerate` is **off by default**: a plain
`LIKE` silently rewritten into a path that can miss rows is not an acceptable
default. `ngram_search` is explicit — you asked for the index — and errors
rather than under-reporting whenever it can prove the index is stale.

---

## Maintenance

### When to refresh

`PRAGMA ngram_refresh('logs')` indexes the rows appended since the last build or
refresh. Its cost is proportional to the size of that tail, not to the size of
the table, so refreshing often is cheap. Run it after a batch of `INSERT`s.

Skipping it is safe: unindexed rows are found by the tail scan. That scan is a
brute-force read of the tail, so queries get gradually slower as the tail grows.
Refresh when the tail is a meaningful fraction of the table.

### When to compact

`PRAGMA ngram_compact('logs')` merges the posting-list rows that share a key.
Parallel builds and each refresh generation add rows rather than rewriting
blobs, so a table that has been refreshed many times reads more rows per probe
than it needs to. `PRAGMA ngram_compact('logs', purge := true)` additionally
drops postings that point at rows that no longer exist.

**On an append-only table, compaction is rarely worth it.** Each refresh
generation lands in a fresh range of rowids, so successive generations barely
share `(gram, segment_no)` keys and there is very little to merge. Measured on
a 92 GiB corpus built in 100 refreshes: compaction ran for 2 h 37 m and merged
4 % of segment rows, shrinking the encoded postings by 0.05 %. What it did buy
was a stats table rebuilt from 78.8 million rows (one per gram per generation)
to 4.5 million (one per gram), which is read on every probe — so if you have
refreshed dozens of times, that is the reason to do it, and once is enough.

Compaction is really for **delete-heavy or interleaved workloads**, not routine
post-ingest hygiene. Check `fragmented_keys` and `generations` in
`ngram_index_stats` before running it, budget disk for the rewrite (the file
grows before it shrinks), and reserve `purge := true` for after a large
`DELETE` — it rewrites every key rather than just the fragmented ones and cost
228× the plain variant on a corpus with nothing to purge.

### When to rebuild

After `UPDATE`s to the indexed column, and after a checkpoint has vacuumed
deleted rows. `DELETE` on its own does not need a rebuild: deleted rows stay
where they are until a checkpoint vacuums them, transaction visibility hides
them, and recheck discards the stale postings.

```sql
PRAGMA drop_ngram_index('logs', 'message');
PRAGMA create_ngram_index('logs', 'message');
```

### Reading `ngram_index_stats`

```sql
PRAGMA ngram_index_stats('logs');
```

| Column | Meaning |
| --- | --- |
| `column_name`, `gram_size`, `case_insensitive` | the options the index was built with |
| `hwm_rowid` | the highest rowid the index covers |
| `table_max_rowid` | the table's current highest rowid — the gap is what the tail scan reads on every query |
| `distinct_grams` | size of the index's gram dictionary |
| `segments` | posting-list rows |
| `fragmented_keys` | keys stored as more than one row; the compaction target |
| `generations` | build plus refresh generations present |
| `posting_entries`, `postings_bytes` | total postings and their encoded size |
| `stale_reason` | `NULL` when the index is healthy; otherwise what is provably wrong with it |

---

## API reference

### Index lifecycle

```sql
PRAGMA create_ngram_index('table', 'column');
PRAGMA create_ngram_index('table', 'column', gram := 3, case_insensitive := true);
PRAGMA drop_ngram_index('table', 'column');
```

`gram` is the number of characters per gram (default 3). Larger grams are more
selective but cannot answer needles shorter than themselves; smaller grams
accept shorter needles but propose far more candidates.

Indexes are supported on `VARCHAR` columns of DuckDB base tables. Views,
temporary tables, tables in foreign catalogs (SQLite, Postgres, …), tables with
generated columns, and tables with a user column named `rowid` are rejected.

### Querying

```sql
SELECT * FROM ngram_search('table', 'needle');
SELECT * FROM ngram_search('table', 'needle', col := 'column');
SELECT rowid FROM ngram_candidates('table', 'column', 'needle');
```

`ngram_search` returns whole rows, exactly and completely. `col` is only needed
when a table has indexes on more than one column. (The parameter is `col`, not
`column`, because `column` is a reserved word.)

`ngram_candidates` is the raw, lossy candidate set: a superset of the true
matches **among indexed rows only**. It does not recheck and it does not cover
the tail. It exists for inspection and for building your own pipelines; if you
use it, you own the recheck and the tail scan.

### Transparent acceleration

With `SET ngram_auto_accelerate = true`, an optimizer pass rewrites qualifying
scans into `NGRAM_INDEX_SCAN`. It fires for `contains(col, 'lit')`,
`col LIKE '%lit%'`, `col ILIKE '%lit%'` (case-insensitive indexes only),
`regexp_matches(col, 'literal')`, multi-segment patterns like
`col LIKE '%a%b%'`, and those combined with other filters via `AND`.

It declines — leaving an ordinary sequential scan — for `_` wildcards, `ESCAPE`
clauses, anchored/prefix/suffix patterns, `NOT LIKE`, `OR`-ed predicates,
expressions over the column (`lower(col) LIKE …`), needles shorter than the gram
size, tables with no index on that column, and any state where a detector proves
the index stale.

`EXPLAIN` shows which happened:

```sql
EXPLAIN SELECT * FROM logs WHERE message LIKE '%reset%';
-- ... NGRAM_INDEX_SCAN  Table: logs  Ngram Column: message  Ngram Needles: reset

EXPLAIN ANALYZE SELECT * FROM logs WHERE message LIKE '%reset%';
-- ... Ngram Mode: index (1423 candidates)
-- or   Ngram Mode: full scan fallback: candidate fraction exceeded
```

Two kill switches: `SET ngram_auto_accelerate = false`, and DuckDB's own
`SET disabled_optimizers = 'extension'`.

### Helper functions

```sql
SELECT trigrams('hello');                     -- ['hel', 'ell', 'llo']
SELECT trigrams('Hello', 4, false);           -- gram size 4, case-sensitive
SELECT ngram_encode_postings([1, 2, 5]);      -- posting blob codec
SELECT ngram_decode_postings(blob);
```

---

## Settings

| Setting | Default | What it does |
| --- | --- | --- |
| `ngram_auto_accelerate` | `false` | Whether plain `LIKE`/`contains`/`ILIKE` may be rewritten to use the index. Off by default because the index can miss rows changed by in-place `UPDATE`s, and a plain `LIKE` must not quietly start missing rows. Turn it on when your table is append-only, or when you refresh and rebuild deliberately. |
| `ngram_max_candidate_fraction` | `0.01` | An accelerated scan that would fetch more than this fraction of the table as candidates abandons the index and scans instead. Fetching costs ~250–300 ns per candidate at every scale measured, and a parallel scan of the whole table costs ~0.04 s at 1 GB, ~0.35 s at 10 GB and ~3.5 s at 100 GB — putting the break-even at 1.6 %, 1.3 % and 1.1 % of rows. One percent is that crossover, rounded toward scanning. |
| `ngram_max_grams_per_query` | `3` | How many of the needle's rarest grams to probe. Each extra gram costs another posting-list decode, and rarest-first means each one is denser than the last, so total query time is a shallow U with a steep right arm: at 100 GB a rare needle costs 0.47 s at 2, 0.64 s at 3 and 1.75 s at 8. Three sits at the floor while still giving a genuine three-way intersection. |

All three are session settings; set them per connection.

**Raising or lowering these never changes your results**, only the time taken.
Probing fewer grams can only widen the candidate set, and every candidate is
rechecked; the selectivity gate only chooses between two exhaustive strategies.

Worth tuning by hand if your data is unusual:

- **A small alphabet** (hex dumps, base64, DNA) makes every gram dense, so the
  intersection needs more grams to bite. Try `ngram_max_grams_per_query = 4`
  or higher, and consider `gram := 4` at build time.
- **Long, highly distinctive needles** get nothing from extra grams — the first
  two already isolate the answer. `ngram_max_grams_per_query = 2` is measurably
  the fastest setting on natural-language corpora.
- **A table that is mostly matches** for your typical needle should not use the
  index at all; that is what the gate is for, and lowering
  `ngram_max_candidate_fraction` further makes it give up sooner.

The measured rationale for the defaults, and the sweeps behind them, are in
[`benchmarks/RESULTS.md`](benchmarks/RESULTS.md).

---

## Performance expectations

Substring search is not an interactive-latency feature. The design target is
*hundreds of milliseconds for a selective needle over a very large corpus*, and
the shape of the result is: **the rarer the needle, the bigger the win.**

| Needle class | Share of rows matched | What to expect |
| --- | --- | --- |
| Rare | ≲ 1 in 10⁵ | The win, and it holds as the corpus grows: measured 1.6× at 1 GB, 6.5× at 10 GB, 6.7× at 100 GB against a 24-thread scan of the same query. |
| Moderate | ~0.1 % – 1 % | A modest win — 2.0× at 10 GB, 1.6× at 100 GB — and roughly a wash on a small table where a parallel scan is already fast. |
| Dense | ≳ 1 % | No win. Fetching millions of individual rows costs more than streaming the column. This is what `ngram_max_candidate_fraction` exists to prevent; leave it on. |

Concretely, on a 92 GiB corpus of English text (24 cores) a rare-needle query
is **0.64 s warm / 3.2 s cold** at the default settings, against 4.3 s / 17.6 s
for the parallel scan it replaces.

**The probe is the floor, and it grows with the corpus.** A query costs the
index probe plus the fetch-and-recheck of whatever it finds. The probe reads
and decodes one posting list per gram it examines, and those lists grow with
the corpus however selective your needle is — at 100 GB a needle matching 88
rows out of 1.09 billion still spends 0.56 s of its 0.64 s in the probe, on a
single thread. `ngram_max_grams_per_query` is the lever you have over it, and
it is a strong one: the same query costs 1.75 s at 8 grams and 0.64 s at the
default 3.

Two consequences worth planning around:

- **A needle's cost tracks its densest gram, not its rarity.** `Ethelred` is
  eight characters, so all of its grams get probed including `the`; it matches
  1 row in 67,000 and still costs 3× a full scan. A longer needle is cheaper,
  because rarest-first has dense grams it can drop.
- **Cold is 5× warm once the index outgrows RAM**, and that is where large
  deployments live. Size RAM against the working set, not the corpus.

Full numbers — build cost, index size, p50/p95 per class warm and cold at 1,
10 and 100 GB, plus a terabyte extrapolation with its assumptions — are in
[`benchmarks/RESULTS.md`](benchmarks/RESULTS.md), together with the scripts
that produce them.

**Index size.** Expect the index to be roughly the size of the text it indexes
(0.7×–1.1× on disk for real text; more for high-entropy content like hex dumps).
This is normal for an exhaustive trigram index — PostgreSQL's `pg_trgm` GIN
indexes land in the same range. Plan disk accordingly: a 100 GB corpus wants
about 100 GB for the index.

**Build cost.** The build runs through DuckDB's own engine, so it is parallel and
spills to disk under a `memory_limit`. Sorting the (gram, rowid) pairs is the
expensive part, and its scratch space is several times the corpus size. On a
large corpus, build incrementally — load a chunk, `create_ngram_index`, then
append and `ngram_refresh` per chunk — rather than indexing everything at once;
that keeps each sort's spill bounded. `benchmarks/bench_build.py` does exactly
this and is the reference for how.

---

## Limitations

- Needles shorter than the gram size cannot use the index (they fall back to a
  full scan, still exact).
- No fuzzy or similarity ranking, and no general regular-expression support —
  only regexes that are a plain literal reduce to an indexable substring.
- `UPDATE`s to the indexed column require a rebuild (see
  [Staleness](#staleness-what-is-detected-and-what-is-not)).
- One index per (table, column). Multi-column indexes do not exist; build one
  index per column you search.
- Temporary tables and non-DuckDB catalogs are not supported.
- Inside an explicit transaction, DuckDB's pragma preprocessor sets
  `current_transaction_invalidation_policy` for the duration of a multi-statement
  pragma expansion and restores DuckDB's default afterwards, not whatever you had
  configured. If you set that policy yourself, re-set it after running an ngram
  pragma inside a transaction.

---

## Platform support

Built and tested on **Linux x86_64** against DuckDB v1.5.5. The community
extension build produces binaries for the platforms DuckDB's own distribution
matrix covers; macOS, Windows, and Wasm coverage in this repository's CI is
being restored.

---

## Building from source

```sh
git clone --recurse-submodules <repo>
cd duckdb-ngram
make                # release build; ./build/release/duckdb has the extension linked in
make test           # sqllogictest suite
GEN=ninja make debug        # DEBUG + AddressSanitizer build
```

The loadable binary is
`build/release/extension/ngram/ngram.duckdb_extension`; load it into a stock
DuckDB v1.5.5 with:

```sh
duckdb -unsigned -c "LOAD '/path/to/ngram.duckdb_extension';"
```

Property-based and long-running harnesses live in `scripts/`:

```sh
python3 scripts/differential_search.py --trials 8 --seed 12345
python3 scripts/differential_search.py --transparent --trials 8 --seed 12345
python3 scripts/churn_maintenance.py --rounds 40 --seed 12345
python3 scripts/crash_maintenance.py --seed 12345
```

Benchmarks and corpus generation live in `benchmarks/`; see
[`benchmarks/RESULTS.md`](benchmarks/RESULTS.md).

## License

MIT. See [LICENSE](LICENSE).
