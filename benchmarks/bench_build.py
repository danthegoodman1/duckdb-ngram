#!/usr/bin/env python3
"""Metered, incrementally-built ngram index over a real-text corpus.

Builds the corpus and its index the way a user with a growing table would:
the first chunk is loaded and indexed with PRAGMA create_ngram_index, every
later chunk is appended and folded in with PRAGMA ngram_refresh. That keeps
each sort inside a bounded spill budget (a monolithic 100 GB build would need
multiple TB of scratch) and doubles as a real-workload exercise of Phase 5's
refresh path at scale.

Every step records wall time, peak RSS, peak spill and the resulting database
and index sizes; the final report also runs PRAGMA ngram_compact and a spot
verification that decoded posting lists still agree with brute force.

    python3 benchmarks/bench_build.py --name e1  --replicas 1
    python3 benchmarks/bench_build.py --name e10 --replicas 10 --chunk 5
    python3 benchmarks/bench_build.py --name code --source code
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import (bench_path, free_disk_gb, gb, log, ngram_storage_schema,  # noqa: E402
                    require_duckdb, run_sql, write_json)
from gen_corpus import DEFAULT_SEED, SOURCE_DB, replica_select  # noqa: E402

#! postings, encoded bytes, segment rows, stats rows. The last is one row per
#! (gram, generation) — it equals the distinct-gram count only after a
#! compaction has rebuilt stats, and is reported as `distinct_grams` for
#! historical reasons.
INDEX_SIZE_SQL = """
SELECT coalesce(sum(rowid_count), 0)::BIGINT,
       coalesce(sum(octet_length(postings)), 0)::BIGINT,
       count(*)::BIGINT,
       (SELECT count(*) FROM {storage}.stats)::BIGINT
FROM {storage}.segments;
"""

CORPUS_SIZE_SQL = "SELECT count(*)::BIGINT, coalesce(sum(strlen(s)), 0)::BIGINT FROM docs;"


def db_bytes(path):
    total = os.path.getsize(path) if os.path.exists(path) else 0
    wal = path + ".wal"
    if os.path.exists(wal):
        total += os.path.getsize(wal)
    return total


def attach_prelude(source_db):
    return "ATTACH '%s' AS src (READ_ONLY);\n" % source_db


def step(db, sql, settings, label, timeout=None):
    result = run_sql(db, sql, timeout=timeout, settings=settings)
    log("%-22s %8.1f s  rss %6.1f GB  spill %7.1f GB" %
        (label, result.seconds, gb(result.peak_rss_bytes), gb(result.peak_spill_bytes)))
    return {
        "label": label,
        "seconds": round(result.seconds, 2),
        "peak_rss_gb": round(gb(result.peak_rss_bytes), 2),
        "peak_spill_gb": round(gb(result.peak_spill_bytes), 2),
        "stdout": result.stdout.strip(),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--name", required=True, help="database basename under the bench dir")
    parser.add_argument("--source", choices=["enwik9", "code"], default="enwik9")
    parser.add_argument("--replicas", type=int, default=1, help="enwik9 replicas (~0.92 GiB of text each)")
    parser.add_argument("--chunk", type=int, default=5, help="replicas appended+indexed per step")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--gram", type=int, default=3)
    parser.add_argument("--case-insensitive", default="true", choices=["true", "false"])
    parser.add_argument("--threads", type=int, default=24)
    parser.add_argument("--memory-limit", default="48GB")
    # the build packs a sorted stream, so relaxing order preservation is a
    # measured experiment, not a default: it can only cost extra segment rows
    # (readers union them), never correctness
    parser.add_argument("--preserve-insertion-order", default="true", choices=["true", "false"])
    parser.add_argument("--compact", action="store_true", help="run and meter PRAGMA ngram_compact at the end")
    parser.add_argument("--verify", type=int, default=3, help="spot-verify this many grams against brute force")
    parser.add_argument("--stats-every", type=int, default=1,
                        help="recount corpus/index sizes every Nth chunk (each count is a full scan)")
    parser.add_argument("--keep-corpus-only", action="store_true",
                        help="load the corpus but skip indexing (for encoding analysis runs)")
    args = parser.parse_args()

    require_duckdb()
    db = bench_path(args.name + ".db")
    source_db = bench_path(SOURCE_DB)
    if os.path.exists(db):
        sys.exit("%s already exists; remove it first" % db)
    log("free disk %.0f GB" % free_disk_gb())

    settings = {
        "threads": args.threads,
        "memory_limit": "'%s'" % args.memory_limit,
        "preserve_insertion_order": args.preserve_insertion_order,
    }
    report = {"args": vars(args), "settings": settings, "steps": []}

    source_table = "src.enwik9_lines" if args.source == "enwik9" else "src.code_lines"
    replicas = args.replicas if args.source == "enwik9" else 1
    chunks = [list(range(i, min(i + args.chunk, replicas))) for i in range(0, replicas, args.chunk)]

    for chunk_no, chunk in enumerate(chunks):
        if args.source == "code":
            selects = ["SELECT id, line AS s FROM src.code_lines"]
        else:
            selects = [replica_select(source_table, r, args.seed) for r in chunk]
        sql = attach_prelude(source_db)
        if chunk_no == 0:
            sql += "CREATE TABLE docs(id BIGINT, s VARCHAR);\n"
        for select in selects:
            sql += "INSERT INTO docs %s;\n" % select
        sql += "CHECKPOINT;\n"
        report["steps"].append(step(db, sql, settings, "append c%d (%d rep)" % (chunk_no, len(chunk)),
                                    timeout=86400))

        if args.keep_corpus_only:
            continue
        if chunk_no == 0:
            index_sql = "PRAGMA create_ngram_index('docs', 's', gram = %d, case_insensitive = %s);" % (
                args.gram, args.case_insensitive)
            label = "create index"
        else:
            index_sql = "PRAGMA ngram_refresh('docs', col = 's');"
            label = "refresh c%d" % chunk_no
        report["steps"].append(step(db, index_sql, settings, label, timeout=172800))
        report["steps"].append(step(db, "CHECKPOINT;", settings, "checkpoint c%d" % chunk_no, timeout=86400))

        report["steps"][-1]["db_bytes"] = db_bytes(db)
        # Summing octet_length over the corpus and over every postings blob is
        # two full scans of everything built so far. Cheap per chunk at small
        # scale, an hour of pure measurement overhead across a hundred chunks,
        # so it is sampled rather than run every time.
        last_chunk = chunk_no == len(chunks) - 1
        if last_chunk or chunk_no % args.stats_every == 0:
            corpus = run_sql(db, CORPUS_SIZE_SQL, settings=settings, timeout=86400).rows[0]
            report["steps"][-1]["corpus_rows"] = int(corpus[0])
            report["steps"][-1]["corpus_bytes"] = int(corpus[1])
            storage = ngram_storage_schema(db, "docs", "s", settings)
            index = run_sql(db, INDEX_SIZE_SQL.format(storage=storage), settings=settings,
                            timeout=86400).rows[0]
            report["steps"][-1]["postings"] = int(index[0])
            report["steps"][-1]["postings_bytes"] = int(index[1])
            report["steps"][-1]["segment_rows"] = int(index[2])
            report["steps"][-1]["distinct_grams"] = int(index[3])
            log("  corpus %.2f GB / %s rows, postings %s (%.2f GB blobs), db %.2f GB" %
                (gb(int(corpus[1])), corpus[0], index[0], gb(int(index[1])), gb(db_bytes(db))))
        else:
            log("  db %.2f GB" % gb(db_bytes(db)))
        write_json(bench_path(args.name + ".build.json"), report)

    if args.keep_corpus_only:
        corpus = run_sql(db, CORPUS_SIZE_SQL, settings=settings, timeout=86400).rows[0]
        report["corpus_rows"] = int(corpus[0])
        report["corpus_bytes"] = int(corpus[1])
        report["final_db_bytes"] = db_bytes(db)
        write_json(bench_path(args.name + ".build.json"), report)
        return

    stats = run_sql(db, "PRAGMA ngram_index_stats('docs');", settings=settings)
    report["stats"] = stats.stdout.strip()
    log("stats:", report["stats"])

    if args.verify:
        report["verification"] = verify(db, settings, args.verify, args.gram, args.case_insensitive == "true")

    if args.compact:
        report["steps"].append(step(db, "PRAGMA ngram_compact('docs', col = 's');", settings, "compact",
                                    timeout=259200))
        report["steps"].append(step(db, "CHECKPOINT;", settings, "checkpoint post-compact", timeout=86400))
        storage = ngram_storage_schema(db, "docs", "s", settings)
        index = run_sql(db, INDEX_SIZE_SQL.format(storage=storage), settings=settings).rows[0]
        report["post_compact"] = {
            "postings": int(index[0]), "postings_bytes": int(index[1]),
            "segment_rows": int(index[2]), "distinct_grams": int(index[3]),
            "db_bytes": db_bytes(db),
        }
        log("post-compact: %s postings, %.2f GB blobs, %s segment rows, db %.2f GB" %
            (index[0], gb(int(index[1])), index[2], gb(db_bytes(db))))
        report["stats_post_compact"] = run_sql(
            db, "PRAGMA ngram_index_stats('docs');", settings=settings).stdout.strip()

    report["final_db_bytes"] = db_bytes(db)
    write_json(bench_path(args.name + ".build.json"), report)


def verify(db, settings, count, gram_size, case_insensitive):
    """Decoded posting lists must equal brute force, exactly.

    The same gate Phase 2 used: pick grams spread over the frequency range and
    compare the distinct rowids their segments decode to against the rows a
    plain `contains` scan finds at or below the high-water mark.
    """
    storage = ngram_storage_schema(db, "docs", "s", settings)
    fold = "lower(s)" if case_insensitive else "s"
    picks = run_sql(db, """
SELECT gram FROM (
  SELECT gram, row_count, row_number() OVER (ORDER BY row_count) AS lo,
         count(*) OVER () AS n
  FROM {storage}.stats
) WHERE lo IN (1, (n/2)::BIGINT, n) LIMIT {count};
""".format(storage=storage, count=count), settings=settings).rows
    grams = [row[0] for row in picks]
    results = []
    for gram in grams:
        # the stats table stores grams already folded by the index's own
        # normalization, so the brute-force side folds the column and compares
        # the gram as stored
        literal = "'" + gram.replace("'", "''") + "'"
        sql = """
SELECT (SELECT count(DISTINCT r) FROM (
          SELECT unnest(ngram_decode_postings(postings)) AS r
          FROM {storage}.segments WHERE gram = {g}))::BIGINT AS decoded,
       (SELECT count(*) FROM docs
         WHERE rowid <= (SELECT hwm_rowid FROM {storage}.meta)
           AND contains({fold}, {g}))::BIGINT AS brute;
""".format(storage=storage, g=literal, fold=fold)
        row = run_sql(db, sql, settings=settings, timeout=86400).rows[0]
        decoded, brute = int(row[0]), int(row[1])
        log("verify gram %r: decoded %d, brute %d, %s" % (gram, decoded, brute,
                                                          "OK" if decoded == brute else "MISMATCH"))
        results.append({"gram": gram, "decoded": decoded, "brute": brute, "ok": decoded == brute})
    if any(not entry["ok"] for entry in results):
        raise SystemExit("spot verification MISMATCH: %r" % results)
    return results


if __name__ == "__main__":
    main()
