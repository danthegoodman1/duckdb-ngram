#!/usr/bin/env python3
"""Reproducible corpus generation for the ngram scale benchmarks.

Two real-text sources, no synthetic alphabets:

  enwik9  the first 10^9 bytes of an English Wikipedia XML dump
          (mattmahoney.net/dc/enwik9.zip), one line per row. Scaled past its
          own 1 GB by REPLICATION WITH PERTURBATION: replica r of source line
          i keeps the line but rewrites three characters at a seeded position
          with seeded lowercase letters. Every replica therefore draws its
          grams from enwik9's own distribution (the point of using real text)
          while no two rows are byte-identical and no posting list degenerates
          into N copies of one pattern.

  code    every .cpp/.hpp line of the pinned duckdb checkout, one line per
          row. Used for the size-ratio table at small scale; not replicated.

Everything is seeded and derived from SQL, so a reviewer reproduces a corpus
by re-running with the same --seed at whatever --replicas they have room for.

    python3 benchmarks/gen_corpus.py source          # fetch + stage enwik9
    python3 benchmarks/gen_corpus.py code            # stage the code corpus
"""

import argparse
import os
import subprocess
import sys
import zipfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import (BENCH_DIR, REPO, bench_path, gb, log, require_duckdb,  # noqa: E402
                    run_sql)

ENWIK9_URL = "https://mattmahoney.net/dc/enwik9.zip"
DEFAULT_SEED = 20260809

#! Source line tables live in their own database so a corpus build reads them
#! over ATTACH and never re-parses the raw file.
SOURCE_DB = "corpus_source.db"


def ensure_enwik9():
    raw = bench_path("enwik9")
    if os.path.exists(raw) and os.path.getsize(raw) > 900_000_000:
        return raw
    archive = bench_path("enwik9.zip")
    if not os.path.exists(archive) or os.path.getsize(archive) < 322_000_000:
        log("downloading", ENWIK9_URL)
        subprocess.run(["curl", "-sSL", "-o", archive, ENWIK9_URL], check=True, timeout=3600)
    log("unzipping", archive)
    with zipfile.ZipFile(archive) as zf:
        with zf.open("enwik9") as src, open(raw + ".part", "wb") as dst:
            while True:
                block = src.read(1 << 22)
                if not block:
                    break
                dst.write(block)
    os.replace(raw + ".part", raw)
    return raw


#! The CSV reader is the only line splitter that streams a 1 GB file without
#! materializing it: quoting and escaping are disabled so no byte of the corpus
#! is reinterpreted, and the delimiter is a control byte the text never uses.
def read_lines_sql(glob_pattern):
    return (
        "SELECT * FROM read_csv(%s, delim='\\x01', quote='', escape='', header=false, "
        "columns={'line': 'VARCHAR'}, ignore_errors=true, strict_mode=false, "
        "new_line='\\n', all_varchar=true)" % _lit(glob_pattern)
    )


def _lit(value):
    return "'" + str(value).replace("'", "''") + "'"


def stage_source(force=False):
    """Materialize enwik9 as (id, line) in the shared source database."""
    require_duckdb()
    db = bench_path(SOURCE_DB)
    exists = run_sql(db, "SELECT count(*) FROM duckdb_tables() WHERE table_name='enwik9_lines';").scalar()
    if exists != "0" and not force:
        stats = run_sql(db, "SELECT count(*), sum(strlen(line)) FROM enwik9_lines;").rows[0]
        log("enwik9_lines already staged: %s rows, %.3f GB" % (stats[0], gb(int(stats[1]))))
        return db
    raw = ensure_enwik9()
    log("staging enwik9 lines into", db)
    sql = (
        "DROP TABLE IF EXISTS enwik9_lines;\n"
        "CREATE TABLE enwik9_lines AS SELECT (row_number() OVER () - 1)::BIGINT AS id, line FROM (%s) "
        "WHERE line IS NOT NULL AND length(line) > 0;\n"
        "CHECKPOINT;\n"
        "SELECT count(*), sum(strlen(line)), avg(strlen(line)) FROM enwik9_lines;" % read_lines_sql(raw)
    )
    result = run_sql(db, sql, timeout=7200)
    rows, total, avg = result.rows[0]
    log("enwik9_lines: %s rows, %.3f GB, %.1f bytes/row (%.1f s)" % (rows, gb(int(total)), float(avg), result.seconds))
    return db


def stage_code(force=False):
    """Materialize the duckdb C++ sources as (id, line)."""
    require_duckdb()
    db = bench_path(SOURCE_DB)
    exists = run_sql(db, "SELECT count(*) FROM duckdb_tables() WHERE table_name='code_lines';").scalar()
    if exists != "0" and not force:
        stats = run_sql(db, "SELECT count(*), sum(strlen(line)) FROM code_lines;").rows[0]
        log("code_lines already staged: %s rows, %.3f GB" % (stats[0], gb(int(stats[1]))))
        return db
    patterns = [os.path.join(REPO, "duckdb", "**", "*.cpp"),
                os.path.join(REPO, "duckdb", "**", "*.hpp")]
    unions = " UNION ALL ".join("(%s)" % read_lines_sql(p) for p in patterns)
    sql = (
        "DROP TABLE IF EXISTS code_lines;\n"
        "CREATE TABLE code_lines AS SELECT (row_number() OVER () - 1)::BIGINT AS id, line FROM (%s) "
        "WHERE line IS NOT NULL AND length(line) > 0;\n"
        "CHECKPOINT;\n"
        "SELECT count(*), sum(strlen(line)), avg(strlen(line)) FROM code_lines;" % unions
    )
    result = run_sql(db, sql, timeout=3600)
    rows, total, avg = result.rows[0]
    log("code_lines: %s rows, %.3f GB, %.1f bytes/row (%.1f s)" % (rows, gb(int(total)), float(avg), result.seconds))
    return db


#! One replica of the source table, perturbed. `pos` and `salt` are pure
#! functions of (source id, replica, seed), so the corpus is byte-reproducible
#! from the seed alone and independent of thread scheduling.
def replica_select(source_table, replica, seed, id_stride=1_000_000_000):
    r = int(replica)
    h = "(hash(id * 1000003 + %d * 7919 + %d) %% 1000000007)::BIGINT" % (r, int(seed))
    return """
SELECT {stride}::BIGINT * {r} + id AS id,
       CASE WHEN n < 12 THEN line || salt
            ELSE substr(line, 1, pos) || salt || substr(line, pos + 4) END AS s
FROM (
  SELECT id, line, n,
         1 + (h % greatest(n - 6, 1)) AS pos,
         chr(97 + (h // 7 % 26)::INT) || chr(97 + (h // 199 % 26)::INT)
           || chr(97 + (h // 5209 % 26)::INT) AS salt
  FROM (SELECT id, line, length(line) AS n, {h} AS h FROM {src})
)""".format(stride=id_stride, r=r, h=h, src=source_table)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("what", choices=["source", "code", "both"])
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    log("bench dir", BENCH_DIR)
    if args.what in ("source", "both"):
        stage_source(args.force)
    if args.what in ("code", "both"):
        stage_code(args.force)


if __name__ == "__main__":
    main()
