#!/usr/bin/env python3
"""Record the two SQL probes beside this file: `column_probe.sql` (per-column
fetch cost by compression on the wide table) and `recheck_probe.sql` (range
against scattered access on both exact paths). Each is run once through the
release CLI against the benchmark database; every `Run Time` line is stored
in order with the statement it timed.

    python3 docs/review/2026-09-09/probe_measure.py --db ~/duckdb-ngram-bench/e1.db \
        --output docs/review/2026-09-09/probe_observations.json
"""

import argparse
import json
import os
import re
import subprocess

BINARY = "build/release/duckdb"
TIMER = re.compile(r"Run Time \(s\): real ([0-9.]+) user ([0-9.]+) sys ([0-9.]+)")
HERE = os.path.dirname(os.path.abspath(__file__))


def run(database, script_name):
    with open(os.path.join(HERE, script_name)) as handle:
        script = handle.read()
    process = subprocess.run([BINARY, "-readonly", database], input=script, text=True, capture_output=True)
    if process.returncode:
        raise SystemExit(process.stderr[-2000:])
    timed = [line.strip() for line in script.splitlines()
             if line.strip().upper().startswith("SELECT") and not line.startswith(".")]
    times = [(float(m.group(1)), float(m.group(2)) + float(m.group(3))) for m in TIMER.finditer(process.stdout)]
    # the last SELECT of each script runs with the timer off
    entries = []
    for statement, (real, cpu) in zip(timed, times):
        entries.append({"statement": statement, "real_s": real, "cpu_s": round(cpu, 4)})
    fields = dict(re.findall(r'"(Ngram [^"]+)": "([^"]*)"', process.stdout))
    return {"timings": entries, "profile": fields, "tail": process.stdout[-1500:]}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    observations = {
        "git_head": subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip(),
        "column_probe": run(args.db, "column_probe.sql"),
        "recheck_probe": run(args.db, "recheck_probe.sql"),
    }
    with open(args.output, "w") as handle:
        json.dump(observations, handle, indent=1, sort_keys=True)
    for name in ("column_probe", "recheck_probe"):
        for entry in observations[name]["timings"]:
            print("%-14s %7.3f s  %s" % (name, entry["real_s"], entry["statement"][:90]))
    print("wrote", args.output)


if __name__ == "__main__":
    main()
