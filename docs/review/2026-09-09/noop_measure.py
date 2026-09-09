#!/usr/bin/env python3
"""Maintenance calls with nothing to do, on the enwik9 benchmark database:
`ngram_refresh` with no row past the mark, bounded and unbounded, and
`ngram_compact` on a single-generation index, each timed in its own warm
process, plus a refresh of one appended generation and the compaction that
merges it, each timed as the first statement of its process and followed by
the same call again in that process (before any checkpoint) and in a fresh
process (after the shutdown checkpoint). The appended rows are deleted and
purged at the end; the checkpoint
then drops the fully deleted trailing row groups, which leaves the recorded
mark above the table's last rowid, so rebuild the index before measuring a
refresh on the same database again. Run from the repository root:

    python3 docs/review/2026-09-09/noop_measure.py --db ~/duckdb-ngram-bench/e1.db \
        --output docs/review/2026-09-09/noop_observations.json
"""

import argparse
import json
import re
import statistics
import subprocess

BINARY = "build/release/duckdb"
TIMER = re.compile(r"Run Time \(s\): real ([0-9.]+)")


def summary(times):
    return {"p50_ms": round(1000 * statistics.median(times), 2), "min_ms": round(1000 * min(times), 2),
            "max_ms": round(1000 * max(times), 2), "n": len(times)}


def times_in_one_process(database, statements):
    script = ".headers off\n.mode list\n.bail on\nSET threads=24; SET memory_limit='48GB';\n.timer on\n" + \
        "".join(statements)
    process = subprocess.run([BINARY, database], input=script, text=True, capture_output=True)
    if process.returncode:
        raise SystemExit(process.stderr[-2000:])
    return [float(m.group(1)) for m in TIMER.finditer(process.stdout)]


def timed(database, statement, repeats=7):
    return summary(times_in_one_process(database, [statement] * (repeats + 1))[1:])


def run(database, sql):
    process = subprocess.run([BINARY, "-csv", "-noheader", database], input="SET threads=24;\n" + sql, text=True,
                             capture_output=True)
    if process.returncode:
        raise SystemExit(process.stderr[-2000:])
    return process.stdout.strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    observations = {"git_head": subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True,
                                               text=True).stdout.strip()}
    observations["rows"] = run(args.db, "SELECT count(*) FROM docs;").splitlines()[-1]
    observations["refresh_noop"] = timed(args.db, "PRAGMA ngram_refresh('docs');\n")
    observations["refresh_noop_bounded"] = timed(args.db, "PRAGMA ngram_refresh('docs', 1000000);\n")
    observations["compact_noop"] = timed(args.db, "PRAGMA ngram_compact('docs');\n")
    run(args.db, "INSERT INTO docs SELECT id + 20000000, s FROM docs WHERE id < 100000;")
    refresh_then_noops = times_in_one_process(args.db, ["PRAGMA ngram_refresh('docs');\n"] * 8)
    observations["refresh_100k_rows"] = summary(refresh_then_noops[:1])
    observations["refresh_noop_in_refresh_process"] = summary(refresh_then_noops[1:])
    observations["refresh_noop_after_checkpoint"] = timed(args.db, "PRAGMA ngram_refresh('docs');\n")
    observations["stats_two_generations"] = run(args.db, "PRAGMA ngram_index_stats('docs');")
    merge_then_noops = times_in_one_process(args.db, ["PRAGMA ngram_compact('docs');\n"] * 8)
    observations["compact_merge"] = summary(merge_then_noops[:1])
    observations["compact_noop_in_merge_process"] = summary(merge_then_noops[1:])
    observations["compact_noop_after_checkpoint"] = timed(args.db, "PRAGMA ngram_compact('docs');\n")
    run(args.db, "DELETE FROM docs WHERE id >= 20000000;\nPRAGMA ngram_compact('docs', purge = true);\nCHECKPOINT;")
    observations["stats_after"] = run(args.db, "PRAGMA ngram_index_stats('docs');")
    for key, value in observations.items():
        print("%-24s %s" % (key, value), flush=True)
    with open(args.output, "w") as handle:
        json.dump(observations, handle, indent=1, sort_keys=True)


if __name__ == "__main__":
    main()
