#!/usr/bin/env python3
"""The transparent path's fallback against its own native baseline: the same
`ILIKE` query with the extension's optimizer disabled, on the databases
`latency_rebaseline.py` built, for the needles it declined. Appends an
`ilike_native` block to the latency observations.

    python3 docs/review/2026-09-09/ilike_baseline.py \
        --old-binary /path/to/main-d3ecccf/build/release/duckdb --new-binary build/release/duckdb \
        --work /tmp/rebaseline --output docs/review/2026-09-09/latency_observations.json
"""

import argparse
import json
import os
import re
import statistics
import subprocess

TIMER = re.compile(r"Run Time \(s\): real ([0-9.]+)")
NEEDLES = ["history", "which"]


def timed(binary, database, sql, threads, repeats=21):
    script = ".headers off\n.mode list\n.bail on\nSET threads=%d; SET memory_limit='48GB';\n%s.timer on\n" % (
        threads, "") + sql * (repeats + 1)
    process = subprocess.run([binary, "-readonly", database], input=script, text=True, capture_output=True)
    if process.returncode:
        raise SystemExit(process.stderr[-2000:])
    times = [float(m.group(1)) for m in TIMER.finditer(process.stdout)][1:]
    ordered = sorted(times)
    return {"p50_ms": round(1000 * statistics.median(ordered), 2), "min_ms": round(1000 * ordered[0], 2),
            "max_ms": round(1000 * ordered[-1], 2), "n": len(ordered)}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--old-binary", required=True)
    parser.add_argument("--new-binary", required=True)
    parser.add_argument("--work", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    with open(args.output) as handle:
        observations = json.load(handle)
    for label, binary in (("before", args.old_binary), ("after", args.new_binary)):
        database = os.path.join(args.work, label + ".db")
        block = {}
        for needle in NEEDLES:
            sql = "SELECT count(*) FROM docs WHERE s ILIKE '%%%s%%';\n" % needle
            entry = {}
            for threads in (1, 24):
                entry["native_threads_%d" % threads] = timed(
                    binary, database, "SET disabled_optimizers='extension';\n" + sql, threads)
                entry["transparent_threads_%d" % threads] = timed(
                    binary, database, "SET ngram_auto_accelerate=true;\n" + sql, threads)
            block[needle] = entry
            print("%s %-8s native %8.1f / %6.1f ms  transparent fallback %8.1f / %6.1f ms" % (
                label, needle, entry["native_threads_1"]["p50_ms"], entry["native_threads_24"]["p50_ms"],
                entry["transparent_threads_1"]["p50_ms"], entry["transparent_threads_24"]["p50_ms"]), flush=True)
        observations[label]["ilike_native"] = block
    with open(args.output, "w") as handle:
        json.dump(observations, handle, indent=1, sort_keys=True)
    print("wrote", args.output)


if __name__ == "__main__":
    main()
