#!/usr/bin/env python3
"""Tuning sweeps for the two query-time settings.

`grams`     ngram_max_grams_per_query from 1 to 16. Each extra gram costs one
            more posting-list read and can only shrink the candidate set, so
            the question is where the reads stop paying for themselves.

`crossover` The whole candidate ladder: every needle from the picker's list,
            measured through the index (gate disabled) and through a plain
            scan. The candidate fraction where the two curves meet is what
            ngram_max_candidate_fraction should sit at or below.

`fallback`  What the gate costs once it fires: the in-scan full-scan fallback
            against the sequential scan it replaced.

Each mode merges its results into <db>.sweep.json, so the modes can be run
separately.

    python3 benchmarks/sweep_settings.py --db e10.db --what grams
    python3 benchmarks/sweep_settings.py --db e10.db --what crossover
"""

import argparse
import os
import statistics
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from bench_latency import CANDIDATES, lit, like_pattern, summarize, timed  # noqa: E402
from common import bench_path, log, read_json, require_duckdb, run_sql, write_json  # noqa: E402


def sweep_grams(db, settings, table, column, needles, repeats):
    rows = []
    for name in ("rare", "moderate", "dense"):
        needle = needles["classes"][name]["needle"]
        for k in (1, 2, 3, 4, 6, 8, 12, 16):
            variant = dict(settings, ngram_max_grams_per_query=k)
            probe = timed(db, "SELECT count(*) FROM ngram_candidates(%s, %s, %s);\n"
                          % (lit(table), lit(column), lit(needle)), variant, repeats)
            search = timed(db, "SELECT count(*) FROM ngram_search(%s, %s);\n" % (lit(table), lit(needle)),
                           variant, repeats)
            entry = {
                "class": name, "needle": needle, "max_grams": k,
                "candidates": int(probe["rows"][-1][0]),
                "probe": summarize(probe["real"]),
                "search": summarize(search["real"]),
                "matches": int(search["rows"][-1][0]),
            }
            rows.append(entry)
            log("%-9s K=%-3d candidates %10d  probe p50 %7.3f s  search p50 %7.3f s  matches %d"
                % (name, k, entry["candidates"], entry["probe"]["p50"], entry["search"]["p50"], entry["matches"]))
    # the answer must not depend on K: fewer grams only widens the candidate set
    for name in ("rare", "moderate", "dense"):
        matches = {row["matches"] for row in rows if row["class"] == name}
        if len(matches) != 1:
            raise SystemExit("K changed the answer for %s: %r" % (name, matches))
    return rows


def sweep_crossover(db, settings, table, column, total_rows, repeats):
    accel = dict(settings, ngram_auto_accelerate="true", ngram_max_candidate_fraction=1.0)
    plain = dict(settings, disabled_optimizers="'extension'")
    rows = []
    for needle in CANDIDATES:
        pattern = like_pattern(needle)
        sql = "SELECT count(*) FROM %s WHERE %s LIKE %s;\n" % (table, column, pattern)
        candidates = int(run_sql(db, "SELECT count(*) FROM ngram_candidates(%s, %s, %s);"
                                 % (lit(table), lit(column), lit(needle)),
                                 settings=settings, timeout=86400).scalar())
        index = timed(db, sql, accel, repeats)
        scan = timed(db, sql, plain, repeats)
        if int(index["rows"][-1][0]) != int(scan["rows"][-1][0]):
            raise SystemExit("MISMATCH %r: %s vs %s" % (needle, index["rows"][-1][0], scan["rows"][-1][0]))
        entry = {
            "needle": needle,
            "candidates": candidates,
            "candidate_fraction": round(candidates / total_rows, 9),
            "matches": int(index["rows"][-1][0]),
            "index_p50": round(statistics.median(index["real"]), 4),
            "scan_p50": round(statistics.median(scan["real"]), 4),
        }
        entry["ratio"] = round(entry["index_p50"] / max(entry["scan_p50"], 1e-9), 3)
        rows.append(entry)
        log("%-24r cand %10d (%.3e)  index %7.3f s  scan %7.3f s  index/scan %5.2f"
            % (needle, entry["candidates"], entry["candidate_fraction"], entry["index_p50"],
               entry["scan_p50"], entry["ratio"]))
    ordered = sorted(rows, key=lambda e: e["candidate_fraction"])
    crossover = None
    for previous, current in zip(ordered, ordered[1:]):
        if previous["ratio"] <= 1.0 < current["ratio"]:
            crossover = {"below": previous, "above": current}
    if crossover:
        log("crossover between %.3e (%.2fx) and %.3e (%.2fx) of rows"
            % (crossover["below"]["candidate_fraction"], crossover["below"]["ratio"],
               crossover["above"]["candidate_fraction"], crossover["above"]["ratio"]))
    else:
        log("no crossover in the measured range")
    return {"ladder": ordered, "crossover": crossover}


#! What the selectivity gate costs when it fires. A gated scan does not
#! re-plan: the same table function degrades to a full storage scan with the
#! filters applied natively, plus one belt-and-braces pass of the recheck
#! expression over the survivors. That is a little more work than the seq scan
#! it replaced, and this measures how much.
def sweep_fallback(db, settings, table, column, needles, repeats):
    gated = dict(settings, ngram_auto_accelerate="true", ngram_max_candidate_fraction=0.0)
    plain = dict(settings, disabled_optimizers="'extension'")
    rows = []
    for name in ("rare", "moderate", "dense"):
        needle = needles["classes"][name]["needle"]
        sql = "SELECT count(*) FROM %s WHERE %s LIKE %s;\n" % (table, column, like_pattern(needle))
        fallback = timed(db, sql, gated, repeats)
        seq = timed(db, sql, plain, repeats)
        if int(fallback["rows"][-1][0]) != int(seq["rows"][-1][0]):
            raise SystemExit("MISMATCH fallback %r" % needle)
        entry = {
            "class": name, "needle": needle,
            "fallback_p50": round(statistics.median(fallback["real"]), 4),
            "seq_scan_p50": round(statistics.median(seq["real"]), 4),
        }
        entry["ratio"] = round(entry["fallback_p50"] / max(entry["seq_scan_p50"], 1e-9), 3)
        rows.append(entry)
        log("%-9s gated fallback %7.3f s vs seq scan %7.3f s  ratio %5.2f"
            % (name, entry["fallback_p50"], entry["seq_scan_p50"], entry["ratio"]))
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", default="e10.db")
    parser.add_argument("--what", choices=["grams", "crossover", "fallback", "both"], default="both")
    parser.add_argument("--table", default="docs")
    parser.add_argument("--column", default="s")
    parser.add_argument("--threads", type=int, default=24)
    parser.add_argument("--memory-limit", default="48GB")
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--needles", default="needles.json")
    args = parser.parse_args()

    require_duckdb()
    db = bench_path(args.db) if not os.path.isabs(args.db) else args.db
    settings = {"threads": args.threads, "memory_limit": "'%s'" % args.memory_limit}
    needles = read_json(bench_path(args.needles))
    total_rows = int(run_sql(db, "SELECT count(*) FROM %s;" % args.table, settings=settings).scalar())
    # merge rather than overwrite: the modes are expensive enough to be run
    # separately, and losing the earlier one to the later one would be silent
    out_path = bench_path(os.path.basename(db).replace(".db", "") + ".sweep.json")
    report = read_json(out_path) if os.path.exists(out_path) else {}
    report.update({"db": db, "total_rows": total_rows, "settings": settings})

    if args.what in ("grams", "both"):
        report["grams"] = sweep_grams(db, settings, args.table, args.column, needles, args.repeats)
    if args.what in ("crossover", "both"):
        report["crossover"] = sweep_crossover(db, settings, args.table, args.column, total_rows, args.repeats)
    if args.what in ("fallback", "both"):
        report["fallback"] = sweep_fallback(db, settings, args.table, args.column, needles, args.repeats)

    write_json(out_path, report)


if __name__ == "__main__":
    main()
