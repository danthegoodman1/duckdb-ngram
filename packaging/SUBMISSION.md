# Submitting `ngram` to duckdb/community-extensions

Everything here is prepared and locally validated. **The pull request itself is
not opened by this repository's tooling** — submitting is a deliberate, outward
facing act and belongs to the maintainer.

## What a submission is

A single file, `extensions/ngram/description.yml`, in a PR against
[duckdb/community-extensions](https://github.com/duckdb/community-extensions).
No CMake, no manifest, nothing else. Two hard rules enforced by that repo's
`scripts/build.py`:

* exactly one descriptor may change per PR;
* the directory name must equal `extension.name`.

The ready-to-copy file is
[`community-extensions/extensions/ngram/description.yml`](community-extensions/extensions/ngram/description.yml).

## DuckDB version

community-extensions builds every submission against a **centrally pinned**
DuckDB version — there is no `duckdb_version` field in the descriptor. As of
this writing that pin is `v1.5.5` (`.github/workflows/build.yml`:
`DUCKDB_LATEST_STABLE: 'v1.5.5'`, `ci_tools_version: v1.5-variegata`), and the
default vcpkg commit for v1.5.5 is
`84bab45d415d22042bd0b9081aea57f362da3f35`.

This extension pins `duckdb` and `extension-ci-tools` to `v1.5.5`, so it builds
against exactly what community CI uses. The descriptor therefore sets no
`vcpkg_commit` and no `requires_toolchains` (this extension has no external
dependencies: `vcpkg.json` declares none).

## Name availability

`ngram` is unclaimed: no extension of that name exists in the registry, and no
core DuckDB extension uses it. There is also no existing community extension
providing substring/trigram/n-gram indexing — the nearest neighbours
(`fuzzycomplete`, `rapidfuzz`, `splink_udfs`, `marisa`) are scalar matching
functions or prefix tries, none of them an index over substrings.

Scope overlap with `duckdb-fts` was rechecked: its `main` branch gained a
trigram *sidecar* (PR #52, merged 2026-08-04, unreleased) which indexes the
**term dictionary**, not document text — whole-token wildcard/regex matching
against normalized dictionary entries, with sidecars that "grow with the term
dictionary rather than with the document corpus". That is a different thing
from a character-trigram index over raw strings that matches across token
boundaries. No collision.

## Blocking checklist

- [ ] **The repository is public.** Community extensions "must be public,
      open-source, and hosted on GitHub". This repo is private today.
- [x] **The full platform matrix is green in this repo's CI.** Done in Phase 9:
      `exclude_archs` is gone from `.github/workflows/MainDistributionPipeline.yml`,
      and run
      [31415223698](https://github.com/danthegoodman1/duckdb-ngram/actions/runs/31415223698)
      is green at `duckdb_version: v1.5.5` / `ci_tools_version: v1.5.5` across
      all ten default archs — Linux amd64/arm64, macOS amd64/arm64, Windows
      amd64/arm64/mingw, and wasm mvp/eh/threads. That workflow *is* the one
      community CI calls, so this is the real predictor. Re-run it against the
      exact commit being submitted, since it is a mutable branch ref on the
      tooling side (`extension-ci-tools` `v1.5.5` is `refs/heads/v1.5.5`, not a
      tag) and upstream can move it.
- [x] **Set `excluded_platforms`** in the descriptor from whatever that run
      shows failing, using the `;`-separated syntax
      (e.g. `"wasm_mvp;wasm_eh;wasm_threads"`). Nothing failed, so the key stays
      out entirely — `description.yml` already keeps it commented out, which
      asks community CI to build every platform. Leave it that way unless a
      re-run against the submitted commit shows a failure.
- [ ] **Pin `repo.ref`** to the full 40-character commit SHA of the release
      commit on the public repo, and make `extension.version` match the tag you
      cut. (Both a SHA and a tag are accepted; a SHA is the strong convention.)
- [ ] **Confirm the maintainer handle** in `extension.maintainers`.
- [ ] Re-run the local validation below against the exact commit being pinned.

## Local validation of the descriptor

Run the same script community CI runs:

```sh
git clone --depth 1 https://github.com/duckdb/community-extensions.git
cd community-extensions
python3 -m pip install pyyaml
mkdir -p extensions/ngram
cp /path/to/duckdb-ngram/packaging/community-extensions/extensions/ngram/description.yml \
   extensions/ngram/description.yml
ALL_CHANGED_FILES="extensions/ngram/description.yml" \
DUCKDB_VERSION=v1.5.5 DUCKDB_LATEST_STABLE=v1.5.5 \
  python3 scripts/build.py && cat env.sh
```

`env.sh` should name this extension, its repository and its ref.

## Local validation of installability (recorded)

The point of this test is that a **stock** DuckDB binary — not this repo's
build, which links the extension statically — can install the distributable
artifact from a repository and use it.

Revalidated on 2026-08-11 with the current format-3 artifact and the official
v1.5.5 CLI (`d8cdaa33fd`, SHA-256
`3d33b1df037cb049155c393778df7853fafb23e9d49d7c9cacdde4dd67155788`).
The run used a fresh temporary `extension_directory`, `FORCE INSTALL ngram`
from the repository layout below, and `LOAD ngram`; it reported
`install_mode=REPOSITORY`. A guard-backed create survived reopen; a covered
`UPDATE` plus tail `INSERT` then produced exactly two search matches, and a
second loaded reopen ran public drop with two base rows and zero remaining
meta tables. A separate checkpoint/reopen pass reported `stale_reason=NULL`.

```sh
# 1. a stock v1.5.5 CLI
curl -sSL -o duckdb.zip \
  https://github.com/duckdb/duckdb/releases/download/v1.5.5/duckdb_cli-linux-amd64.zip
unzip -q duckdb.zip           # ./duckdb  ->  v1.5.5 (Variegata) d8cdaa33fd

# 2. a local extension repository in DuckDB's layout:
#    <repo>/<duckdb version>/<platform>/<name>.duckdb_extension.gz
mkdir -p /tmp/local-extension-repo/v1.5.5/linux_amd64
gzip -c build/release/extension/ngram/ngram.duckdb_extension \
      > /tmp/local-extension-repo/v1.5.5/linux_amd64/ngram.duckdb_extension.gz

# 3. install and use it from the stock binary
./duckdb -unsigned /tmp/install-test.db
```

```sql
SET custom_extension_repository='/tmp/local-extension-repo';
INSTALL ngram;
LOAD ngram;
SELECT extension_name, installed, loaded, install_mode
  FROM duckdb_extensions() WHERE extension_name='ngram';
```

Recorded output:

```
┌────────────────┬───────────┬─────────┬──────────────┐
│ extension_name │ installed │ loaded  │ install_mode │
├────────────────┼───────────┼─────────┼──────────────┤
│ ngram          │ true      │ true    │ REPOSITORY   │
└────────────────┴───────────┴─────────┴──────────────┘
```

The shipped defaults come across intact:

```sql
SELECT name, value FROM duckdb_settings() WHERE name LIKE 'ngram%' ORDER BY name;
```

```
┌──────────────────────────────┬─────────┐
│             name             │  value  │
├──────────────────────────────┼─────────┤
│ ngram_auto_accelerate        │ false   │
│ ngram_build_partitions       │ 0       │
│ ngram_max_candidate_fraction │ 0.01    │
│ ngram_max_grams_per_query    │ 3       │
└──────────────────────────────┴─────────┘
```

```sql
CREATE TABLE logs AS
  SELECT i AS id, 'connection reset by peer #' || i AS message FROM range(5000) t(i);
PRAGMA create_ngram_index('logs','message');
SELECT * FROM ngram_search('logs','reset by peer #4242');
```

```
┌───────┬────────────────────────────────┐
│  id   │            message             │
├───────┼────────────────────────────────┤
│  4242 │ connection reset by peer #4242 │
└───────┴────────────────────────────────┘
```

Reopened by the same stock binary **without** loading the extension, the
database remains readable, but the guarded base table is read-only until
`ngram` is loaded. On pinned v1.5.5, INSERT/UPDATE fail binding and DELETE may
busy-spin in the unknown-index binder, so do not attempt extension-free DML:

```
SELECT count(*) FROM logs;                              -- 5000
SELECT count(*) FROM ngram_main_logs.segments_message;  -- 1124
INSERT INTO logs VALUES (99999, 'requires the extension');
-- Binder Error mentioning unknown index type NGRAM_ROWID_GUARD
SELECT * FROM ngram_search('logs','reset');
-- Catalog Error: Table Function with name ngram_search does not exist!
```

Reopened again with the extension loaded, the index is intact and ordinary
writes resume; the new row is found by the tail scan:

```
INSERT INTO logs VALUES (99999, 'written after the extension loaded');
SELECT count(*) FROM ngram_search('logs','reset by peer #4242');   -- 1
SELECT count(*) FROM ngram_search('logs','after the extension');   -- 1
```

## Draft PR

**Title**

```
Add ngram: durable trigram index for exhaustive substring search
```

**Body**

```markdown
Adds `ngram`, a DuckDB extension providing a disk-persisted trigram index that
accelerates substring search (`LIKE '%needle%'`, `contains()`, `ILIKE`, literal
`regexp_matches`) over large text columns while returning every matching row.

- Repository: https://github.com/danthegoodman1/duckdb-ngram
- Pinned ref: `<40-char SHA>` (tag `v0.1.0`)
- Built against DuckDB v1.5.5; `duckdb` and `extension-ci-tools` submodules are
  pinned to v1.5.5. No external dependencies (empty `vcpkg.json`).
- Full distribution matrix green on the extension repo's own
  `MainDistributionPipeline` run: `<link>`

Design: postings live in ordinary DuckDB tables (the duckdb-fts storage model),
so they are durable, WAL-recovered, and buffer-managed. A zero-posting native
rowid guard makes covered updates delete+append, prevents moving vacuum, and
latches trailing-rowid reuse. Candidate rowids remain supersets only; every
candidate is rechecked, the tail is scanned, and uncertain guard state selects
one exhaustive scan. A stock build can read the database, but guarded-table DML
requires `ngram` to be loaded; on pinned v1.5.5 extension-free DELETE may
busy-spin, so guarded tables must be treated as read-only before `LOAD ngram`.

Transparent rewriting of plain `LIKE` is opt-in
(`SET ngram_auto_accelerate = true`) until the next phase bounds candidate
materialization and consolidates the explicit and transparent engines; both
paths are exhaustive today.

Testing: local release and DEBUG + AddressSanitizer gates, property-based
differential harnesses (explicit and transparent paths), churn over
insert/delete/update/refresh/compact/checkpoint/reopen cycles, deterministic
transaction/checkpoint schedules, and crash interruption. The full distribution
matrix must be rerun against the exact submitted commit; earlier cross-platform
runs are historical, not evidence for this final tree. Scale benchmarks up to a
100 GB corpus are recorded in `benchmarks/RESULTS.md`.

No scope collision with existing extensions: nothing in the registry provides
substring/trigram indexing, and duckdb-fts's (unreleased) trigram sidecar
indexes the term dictionary for whole-token wildcard matching, not raw-string
substrings.
```

## After merge

Only `main` builds deploy; a PR build never publishes. Once merged,
`INSTALL ngram FROM community; LOAD ngram;` works on DuckDB v1.5.5.

Updating later means changing `repo.ref` (and `extension.version`) in the same
descriptor and opening another PR: the registry serves exactly one source ref
per extension, with no way for users to install an older one by version.
