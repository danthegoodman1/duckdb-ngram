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

An index is case-insensitive by default (`case_insensitive = true`), folding
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

### Catching up on a large tail, one bounded step at a time

A refresh is one transaction. After a very large ingest that means one very long
transaction, and a crash partway through it rolls the whole catch-up back to
where it started. Pass a row bound to break the catch-up into committed steps:

```sql
-- indexes at most ~1,000,000 rows of tail, commits, and reports progress
PRAGMA ngram_refresh('logs', 1000000);
-- ┌─────────────┬──────────────┬───────────┬────────────────┐
-- │ column_name │ rows_indexed │ hwm_rowid │ remaining_tail │
-- ├─────────────┼──────────────┼───────────┼────────────────┤
-- │ message     │      1000000 │   1000099 │        2100000 │
-- └─────────────┴──────────────┴───────────┴────────────────┘
```

Each call commits durably before returning, so a crash costs the increment in
flight and nothing already committed — the next call picks up from the last
committed high-water mark. Queries stay exact at every step in between: rows
past the mark are simply still tail.

The loop lives in your code, and it ends when `remaining_tail` reaches 0:

```python
while True:
    row = con.execute("PRAGMA ngram_refresh('logs', 1000000)").fetchone()
    print(f"indexed {row[1]} rows, mark now {row[2]}, {row[3]} to go")
    if row[3] == 0:
        break
```

One row comes back per indexed column of the table — one row for the usual
single-index table, and `col = 'message'` narrows it to one index as always.
Calling it again on a caught-up index is a cheap no-op that reports
`remaining_tail = 0`, so the loop is safe to run one extra time. A bound wider
than the tail does exactly what the unbounded form does, in one call, and still
reports progress — which is the way to get a summary out of a full catch-up.

The bound is approximate in one direction only: it is spent as a span of rowids,
so deletes and gaps mean a call may index **fewer** rows than asked, never more.
A bound of 100,000 rows on a tail whose rowids are half deleted indexes ~50,000
rows and advances the mark 100,000 rowids. Bounds large enough to cross one of
the index's 2²⁰-rowid segment boundaries are snapped back to it, because an
increment that ends on a boundary writes exactly the segment rows one unbounded
refresh would have written; smaller bounds are honoured as given and leave one
segment split across two generations, which the next `ngram_compact` merges.

**Pick a bound of a whole number of 2²⁰-rowid segments if you can** — 1048576,
2097152, and so on. Because the snap rounds down, a bound just short of a
segment boundary alternates between a full increment and a tiny one: at
`max_rows = 1048575` the loop reports 1,048,575 rows, then 1 row, then
1,048,575, then 1. Every call is correct and the loop still terminates in the
same number of rows, but half of them are near-empty transactions. A mark that
does not start on a boundary has the same effect once, on the first call.

A long run of deleted rowids costs one no-op call per bound: crossing a gap of
1,000 deleted rowids at `max_rows = 100` takes ten calls that each report
`rows_indexed = 0` before the loop reaches live rows again. They are cheap —
there is nothing to index, so the call is a mark update — but if a catch-up
crawls, a wider bound walks the gap in fewer steps.

Without a bound, `ngram_refresh` behaves exactly as it always has and returns no
rows.

### When to compact

`PRAGMA ngram_compact('logs')` merges the posting-list rows that share a key.
Each refresh appends a new generation of rows rather than rewriting existing
blobs, so a table that has been refreshed many times reads more rows per probe
than it needs to. `PRAGMA ngram_compact('logs', purge = true)` additionally
drops postings that point at rows that no longer exist.

**On an append-only table, compaction has little to merge.** Each refresh
generation lands in a fresh range of rowids, so successive generations barely
share `(gram, segment_no)` keys. Measured on a 92 GiB corpus built in 100
refreshes, compaction merged 4 % of segment rows and shrank the encoded
postings by 0.05 %. What it did buy was a stats table rebuilt from 78.8 million
rows (one per gram per generation) to 4.5 million (one per gram), which is read
on every probe — so if you have refreshed dozens of times, that is the reason
to do it, and once is enough.

Compaction is mainly for **delete-heavy or interleaved workloads**. Check
`fragmented_keys` and `generations` in `ngram_index_stats` before running it,
and budget disk for the rewrite (the file grows before it shrinks). Reserve
`purge = true` for after a large `DELETE`: it rewrites every key rather than
just the fragmented ones, and on a 9 GiB index with nothing to purge that is
413 s against 9 s for the plain variant.

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
| `remaining_tail` | committed rows past `hwm_rowid`: what the tail scan actually reads, and what a refresh would index (the rowid gap counts deleted rows, this does not) |
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
PRAGMA create_ngram_index('table', 'column', gram = 3, case_insensitive = true);
PRAGMA drop_ngram_index('table', 'column');
```

`gram` is the number of characters per gram (default 3). Larger grams are more
selective but cannot answer needles shorter than themselves; smaller grams
accept shorter needles but propose far more candidates.

Indexes are supported on `VARCHAR` columns of DuckDB base tables. Views,
temporary tables, tables in foreign catalogs (SQLite, Postgres, …), tables with
generated columns, and tables with a user column named `rowid` are rejected.

### Maintenance

```sql
PRAGMA ngram_refresh('table');                        -- index the whole tail, returns nothing
PRAGMA ngram_refresh('table', 1000000);               -- at most ~1e6 rows, returns a progress row
PRAGMA ngram_refresh('table', max_rows = 1000000);    -- same, named
PRAGMA ngram_refresh('table', 1000000, col = 'c');    -- one index of a multi-index table
PRAGMA ngram_compact('table');
PRAGMA ngram_compact('table', col = 'c', purge = true);
PRAGMA ngram_index_stats('table');
```

Pragma named parameters take `=`, not `:=`. Each call is one transaction,
whether or not it is bounded; `max_rows` must be at least 1.

The bounded form returns one row per index it advanced:

| Column | Meaning |
| --- | --- |
| `column_name` | the indexed column this row is about |
| `rows_indexed` | committed rows this call brought under the mark (rows whose value is `NULL` included: they are covered, they just hold no grams) |
| `hwm_rowid` | the high-water mark the call committed |
| `remaining_tail` | committed rows still past it — loop until this is 0 |

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
| `ngram_build_partitions` | `0` | How many rowid-range partitions `create_ngram_index`, `ngram_refresh` and `ngram_compact` split their packing pass into. Zero sizes it from `memory_limit`, using a sample of the rows being indexed; raise it if a build runs out of memory on unusually long rows, lower it (to 1) to pack in a single pass. The index it produces is identical whatever you set. |

All four are session settings; set them per connection.

**Raising or lowering these never changes your results**, only the time taken.
Probing fewer grams can only widen the candidate set, and every candidate is
rechecked; the selectivity gate only chooses between two exhaustive strategies.

Worth tuning by hand if your data is unusual:

- **A small alphabet** (hex dumps, base64, DNA) makes every gram dense, so the
  intersection needs more grams to bite. Try `ngram_max_grams_per_query = 4`
  or higher, and consider `gram = 4` at build time.
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
| Rare | ≲ 1 in 10⁵ | The win, and it widens as the corpus grows: measured 3.9× at 1 GB, 18× at 10 GB, 33× at 100 GB against a 24-thread scan of the same query. |
| Moderate | ~0.1 % – 1 % | A modest win — 2.7× at 10 GB, 2.8× at 100 GB — and roughly a wash on a small table where a parallel scan is already fast. |
| Dense | ≳ 1 % | No win. Fetching millions of individual rows costs more than streaming the column. This is what `ngram_max_candidate_fraction` exists to prevent; leave it on. |

Concretely, on a 92 GiB corpus of English text (24 cores) a rare-needle query
is **0.13 s warm / 0.71 s cold** at the default settings, against 4.2 s / 16.4 s
for the parallel scan it replaces.

**The fetch is the floor, and it tracks how many candidates you have.** A query
costs the index probe plus the fetch-and-recheck of whatever it finds. The
probe reads and decodes one posting list per gram, and those lists grow with
the corpus however selective your needle is — but the probe runs across all
your cores, so at 100 GB a needle matching 88 rows in 1.09 billion spends
0.045 s of its 0.125 s there. The rest is fetching rows.
`ngram_max_grams_per_query` is still the lever over the probe: the same query
costs more at 8 grams than at the default 3.

Two consequences worth planning around:

- **A needle's cost tracks its densest gram, not its rarity.** `Ethelred` is
  eight characters, so all of its grams get probed including `the`. A longer
  needle is cheaper, because rarest-first has dense grams it can drop.
- **Cold costs about 6× warm once the index outgrows RAM**, and that is where
  large deployments live: at 100 GB, 0.71 s cold against 0.13 s warm, of which
  0.30 s is reading posting blobs and ~0.46 s is a thousand random row reads.
  Both are disk. Size RAM against the working set, not the corpus.

Full numbers — build cost, index size, p50/p95 per class warm and cold at 1,
10 and 100 GB, plus a terabyte extrapolation with its assumptions — are in
[`benchmarks/RESULTS.md`](benchmarks/RESULTS.md), together with the scripts
that produce them.

**Index size.** Expect the index to be roughly the size of the text it indexes
(0.7×–1.1× on disk for real text; more for high-entropy content like hex dumps).
This is normal for an exhaustive trigram index — PostgreSQL's `pg_trgm` GIN
indexes land in the same range. Plan disk accordingly: a 100 GB corpus wants
about 100 GB for the index.

**Build cost.** The build runs through DuckDB's own engine, so it is parallel.
Expect roughly **1 GB of text per second** on 24 cores: measured 8.1 s to index
a 0.92 GiB corpus and 38.7 s for a 4.6 GiB one, with no spill at any memory
limit that completes. Refresh costs the same per byte of *tail*, independently
of how large the index already is.

Memory is the constraint worth planning for, not disk. The packing pass holds
one partition of the pair stream in memory at a time and sizes those partitions
against `memory_limit`, so a tighter limit means more partitions rather than
spilling — 9.2 GiB of text indexes in one statement in 78 s at
`memory_limit='48GB'` and 137 s at `'8GB'`. The floor is structural: postings
are bucketed into 2²⁰-rowid segments and a segment cannot be split, so about
100 MB of text is the smallest unit the packer can group, and a limit under
~6 GB will fail with an out-of-memory error rather than run slowly. That
failure is clean — the pragma's transaction rolls back and leaves no index
behind. `SET ngram_build_partitions = N` overrides the sizing if you need to.

Building incrementally — load a chunk, `create_ngram_index`, then append and
`ngram_refresh` per chunk — is still the recommendation for a very large
corpus, because a single statement has to hold its whole packed output before
writing it. `benchmarks/bench_build.py` does exactly this and is the reference
for how. When the rows arrive faster than that, or already have,
`PRAGMA ngram_refresh('t', max_rows)` in a loop bounds the same work from the
other end: each call is its own transaction and its own bounded packing pass,
so peak memory and crash exposure both follow the bound rather than the tail.

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
