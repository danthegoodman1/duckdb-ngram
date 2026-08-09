#!/usr/bin/env python3
"""Query latency by needle selectivity, index path vs brute force.

Extends the Phase 3 protocol: one warmup then repeated timed runs inside a
single process (so process start-up is never counted), `.timer on` for the
engine's own per-statement wall clock, p50/p95 over the samples, and a
count-equality assertion on every pair (row-level set identity is enforced by
the differential harnesses and ngram_parallel.test) — a faster wrong-count
answer is not a result.

Three measured pairs per needle:

  transparent  `WHERE s LIKE '%needle%'` with ngram_auto_accelerate=true
               against the identical query under
               `disabled_optimizers='extension'`. Same SQL, same semantics,
               same rows: this is the number a user actually sees.
  explicit     `ngram_search(...)` against `contains(lower(s), needle)`, the
               brute-force query with the case-insensitive index's semantics.
  probe        `ngram_candidates(...)`, i.e. index probe and intersection with
               no fetch, to separate probe cost from fetch+recheck cost.

Needles are chosen once by measured selectivity (`--pick`) and then reused at
every scale, so a row of the scaling table compares like with like.

    python3 benchmarks/bench_latency.py --db e1.db --pick
    python3 benchmarks/bench_latency.py --db e1.db
    python3 benchmarks/bench_latency.py --db e100.db --cold
"""

import argparse
import os
import re
import statistics
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import (bench_path, drop_caches, log, read_json, require_duckdb,  # noqa: E402
                    run_sql, write_json)

#! Literal strings drawn from English Wikipedia prose, spread over five orders
#! of magnitude of selectivity. The picker measures all of them and keeps the
#! closest match per class, so the choice is data-driven but reproducible.
CANDIDATES = [
    "the", "ion", "and t", "ing the", "which", "history", "American",
    "government", "philosophy", "encyclopedia", "Wikipedia:", "chemistry",
    "astronomical", "parliamentary", "photosynthesis", "Ethelred",
    "Schwarzschild", "monophyletic", "bicameral legislature",
    "the quick brown fox", "supercalifragilistic",
]

#! Target share of rows per class; the picker keeps the candidate whose
#! measured share is closest in log space.
CLASS_TARGETS = {"rare": 1e-7, "moderate": 3e-3, "dense": 5e-2}

TIMER = re.compile(r"^Run Time \(s\): real ([0-9.]+) user ([0-9.]+) sys ([0-9.]+)", re.M)


def lit(value):
    return "'" + value.replace("'", "''") + "'"


def like_pattern(needle):
    # the picker never proposes a needle containing % or _
    return "'%" + needle.replace("'", "''") + "%'"


def timed(db, statements, settings, repeats, warmups=1, timeout=86400):
    """Run `statements` warmups+repeats times, returning the engine's times."""
    script = ".timer on\n"
    for _ in range(warmups + repeats):
        script += statements
    result = run_sql(db, script, settings=settings, timeout=timeout, meter=False)
    times = [(float(m.group(1)), float(m.group(2))) for m in TIMER.finditer(result.stderr + result.stdout)]
    per_run = len(times) // (warmups + repeats)
    if per_run == 0:
        raise RuntimeError("no timings parsed from:\n%s\n%s" % (result.stdout[:500], result.stderr[:2000]))
    runs = [times[i * per_run:(i + 1) * per_run] for i in range(warmups + repeats)]
    measured = runs[warmups:]
    # `.timer on` prints its report to stdout between result rows
    rows = [row for row in result.rows if not row[0].startswith("Run Time (s):")]
    return {
        "real": [sum(t[0] for t in run) for run in measured],
        "user": [sum(t[1] for t in run) for run in measured],
        "rows": rows,
    }


def summarize(samples):
    ordered = sorted(samples)
    return {
        "p50": round(statistics.median(ordered), 4),
        "p95": round(ordered[min(len(ordered) - 1, int(round(0.95 * (len(ordered) - 1))))], 4),
        "min": round(ordered[0], 4),
        "n": len(ordered),
    }


def pick_needles(db, settings, table, column):
    rows = run_sql(db, "SELECT count(*) FROM %s;" % table, settings=settings).rows[0]
    total = int(rows[0])
    measured = []
    for needle in CANDIDATES:
        count = int(run_sql(db, "SELECT count(*) FROM %s WHERE contains(lower(%s), lower(%s));"
                            % (table, column, lit(needle)), settings=settings, timeout=86400).scalar())
        share = count / total if total else 0.0
        measured.append({"needle": needle, "matches": count, "share": share})
        log("candidate %-24r %10d rows  %.3e" % (needle, count, share))
    chosen = {}
    for name, target in CLASS_TARGETS.items():
        usable = [entry for entry in measured if entry["matches"] > 0]
        best = min(usable, key=lambda e: abs((e["share"] or 1e-12) / target - 1)
                   if e["share"] >= target else abs(target / max(e["share"], 1e-12) - 1))
        chosen[name] = best
    for name in chosen:
        log("%-9s -> %r (%d rows, %.3e)" % (name, chosen[name]["needle"], chosen[name]["matches"],
                                            chosen[name]["share"]))
    return {"total_rows": total, "classes": chosen, "measured": measured}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", default="e1.db")
    parser.add_argument("--table", default="docs")
    parser.add_argument("--column", default="s")
    parser.add_argument("--threads", type=int, default=24)
    parser.add_argument("--memory-limit", default="48GB")
    parser.add_argument("--repeats", type=int, default=11)
    parser.add_argument("--cold-repeats", type=int, default=3)
    parser.add_argument("--pick", action="store_true", help="(re)choose needles by measured selectivity")
    parser.add_argument("--needles", default="needles.json")
    parser.add_argument("--cold", action="store_true", help="also measure with the page cache evicted")
    parser.add_argument("--max-grams", type=int, default=None, help="override ngram_max_grams_per_query")
    parser.add_argument("--suffix", default="", help="suffix for the output file name")
    args = parser.parse_args()

    require_duckdb()
    db = bench_path(args.db) if not os.path.isabs(args.db) else args.db
    settings = {"threads": args.threads, "memory_limit": "'%s'" % args.memory_limit}
    if args.max_grams is not None:
        settings["ngram_max_grams_per_query"] = args.max_grams
    needles_path = bench_path(args.needles)

    if args.pick:
        write_json(needles_path, pick_needles(db, settings, args.table, args.column))
        return
    needles = read_json(needles_path)

    total_rows = int(run_sql(db, "SELECT count(*) FROM %s;" % args.table, settings=settings).scalar())
    report = {"db": db, "total_rows": total_rows, "settings": settings, "classes": {}}
    log("%s: %d rows" % (db, total_rows))

    for name in ("rare", "moderate", "dense"):
        needle = needles["classes"][name]["needle"]
        entry = {"needle": needle}
        pattern = like_pattern(needle)
        accel = dict(settings, ngram_auto_accelerate="true", ngram_max_candidate_fraction=1.0)
        plain = dict(settings, disabled_optimizers="'extension'")

        variants = {
            "transparent_index": ("SELECT count(*) FROM %s WHERE %s LIKE %s;\n" % (args.table, args.column, pattern),
                                  accel),
            "transparent_scan": ("SELECT count(*) FROM %s WHERE %s LIKE %s;\n" % (args.table, args.column, pattern),
                                 plain),
            "explicit_search": ("SELECT count(*) FROM ngram_search(%s, %s);\n" % (lit(args.table), lit(needle)),
                                settings),
            "brute_ci": ("SELECT count(*) FROM %s WHERE contains(lower(%s), lower(%s));\n"
                         % (args.table, args.column, lit(needle)), settings),
            "probe_only": ("SELECT count(*) FROM ngram_candidates(%s, %s, %s);\n"
                           % (lit(args.table), lit(args.column), lit(needle)), settings),
        }
        for variant, (sql, variant_settings) in variants.items():
            result = timed(db, sql, variant_settings, args.repeats)
            entry[variant] = summarize(result["real"])
            entry[variant]["user_p50"] = round(statistics.median(result["user"]), 3)
            entry[variant]["result"] = int(result["rows"][-1][0])
            log("  %-9s %-18s p50 %7.3f s  p95 %7.3f s  user/real %4.1f  rows %d"
                % (name, variant, entry[variant]["p50"], entry[variant]["p95"],
                   entry[variant]["user_p50"] / max(entry[variant]["p50"], 1e-9), entry[variant]["result"]))

        # a faster wrong answer is not a result
        if entry["transparent_index"]["result"] != entry["transparent_scan"]["result"]:
            raise SystemExit("MISMATCH transparent %r: %d vs %d"
                             % (needle, entry["transparent_index"]["result"], entry["transparent_scan"]["result"]))
        if entry["explicit_search"]["result"] != entry["brute_ci"]["result"]:
            raise SystemExit("MISMATCH explicit %r: %d vs %d"
                             % (needle, entry["explicit_search"]["result"], entry["brute_ci"]["result"]))
        entry["speedup_transparent"] = round(entry["transparent_scan"]["p50"] / max(entry["transparent_index"]["p50"], 1e-9), 2)
        entry["speedup_explicit"] = round(entry["brute_ci"]["p50"] / max(entry["explicit_search"]["p50"], 1e-9), 2)
        entry["candidates"] = entry["probe_only"]["result"]
        entry["candidate_fraction"] = round(entry["candidates"] / total_rows, 8) if total_rows else 0.0

        if args.cold:
            cold = {}
            for variant in ("transparent_index", "transparent_scan"):
                sql, variant_settings = variants[variant]
                samples = []
                for _ in range(args.cold_repeats):
                    if not drop_caches():
                        log("  (cannot drop caches; skipping cold measurement)")
                        samples = []
                        break
                    result = timed(db, sql, variant_settings, repeats=1, warmups=0)
                    samples.append(result["real"][0])
                if samples:
                    cold[variant] = summarize(samples)
                    log("  %-9s %-18s COLD p50 %7.3f s" % (name, variant, cold[variant]["p50"]))
            entry["cold"] = cold
        report["classes"][name] = entry

    name = os.path.basename(db).replace(".db", "") + args.suffix
    write_json(bench_path(name + ".latency.json"), report)


if __name__ == "__main__":
    main()
