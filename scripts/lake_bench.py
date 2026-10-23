#!/usr/bin/env python3
"""Benchmark an indexed DuckLake table: the index against a scan, per needle.

For each needle it runs, each in `--procs` fresh duckdb processes (cold: the
first query of the process), three plans of

    SELECT count(*), coalesce(bit_xor(hash(rowid, <column>)), 0) FROM <table> WHERE contains(<column>, <needle>)

  index   ngram_auto_accelerate on, the row-group gate open (fraction 1.0)
  gated   ngram_auto_accelerate on, the gate at its setting (--fraction)
  scan    ngram_auto_accelerate off

and, with --plans, any of

  plan         EXPLAIN of the index plan: planning alone, the extension's
               reads of the lake included
  plan-warm    EXPLAIN of the index plan after the query ran once in the process
  index-warm   the index plan run a second time in one process, timed
  scan-warm    the scan run a second time in one process, timed

and reports the median wall time, the bytes and requests DuckDB's HTTP log
records (zero for a lake on local disk), the candidates and the share of the
table's row groups they fall in as the extension's gate computes them, the
plan the gate chose, and whether every plan returned the same answer.

The prelude file holds the SQL that reaches the lake: secrets, settings and the
ATTACH, which must name the lake `lk`.

Usage: python3 scripts/lake_bench.py --prelude FILE --table lk.t --column c
                                     [--needles a,b,...] [--procs N] [--fraction F]
                                     [--duckdb PATH] [--json OUT]
"""

import argparse
import csv
import json
import os
import re
import statistics
import subprocess
import sys
import tempfile

from ngram_harness import CLI_ENV, DEFAULT_DUCKDB, sql_quote

TIMER = re.compile(r"Run Time \(s\): real ([0-9.]+)")
NEEDLES = ["quokka", "Kierkegaardian", "Kepler", "Beethoven", "Shakespeare", "Napoleon", "Netherlands",
           "football", "Germany", "England", "century", "language", "States", "[[Category:"]


def run(duckdb, script, timeout=3600):
    with tempfile.NamedTemporaryFile("w", suffix=".sql", delete=False) as f:
        f.write(script)
        path = f.name
    try:
        proc = subprocess.run([duckdb, "-no-agent", "-f", path], capture_output=True, text=True, env=CLI_ENV,
                              timeout=timeout)
    finally:
        os.unlink(path)
    if proc.returncode != 0:
        raise RuntimeError("duckdb failed: %s" % proc.stderr[-2000:])
    return proc.stdout


def measure(args, prelude, settings, query, warm=False, explain=False):
    """(seconds, answer, bytes, requests) of `query` as a process's first
    query, or with `warm` its second; with `explain`, of planning it alone."""
    log = tempfile.mktemp(suffix=".csv")
    timed = "EXPLAIN " + query if explain else query
    script = "\n".join([".bail on", ".mode csv", ".headers off", prelude] + settings + ([query] if warm else []) + [
        "CALL enable_logging('HTTP', storage = 'memory');",
        ".print @@BEGIN",
        ".timer on",
        timed,
        ".timer off",
        ".print @@END",
        # the log keeps each header's case as the server sent it
        "COPY (SELECT coalesce(response.headers['Content-Length'], response.headers['content-length']) AS length "
        "FROM duckdb_logs_parsed('HTTP') WHERE request.type = 'GET') TO %s (HEADER);" % sql_quote(log),
    ]) + "\n"
    out = run(args.duckdb, script)
    body = out.split("@@BEGIN", 1)[1].split("@@END", 1)[0]
    seconds = sum(float(m.group(1)) for m in TIMER.finditer(body))
    answer = [line for line in body.splitlines() if re.match(r"^\d+,-?\d+$", line)]
    if explain:
        answer = []
    elif len(answer) != 1:
        raise RuntimeError("expected one count,hash line, got: %s" % body[-500:])
    total = requests = 0
    if os.path.exists(log):
        with open(log) as f:
            for row in csv.DictReader(f):
                requests += 1
                total += int(row["length"] or 0)
        os.unlink(log)
    return seconds, "|".join(answer), total, requests


def candidates(args, prelude, needle):
    """The needle's candidates and the share of the table's row groups they
    fall in, as the extension's gate computed them, and the plan the gate
    picks at --fraction."""
    q = sql_quote(needle)
    query = "SELECT count(*) FROM %s WHERE contains(%s, %s)" % (args.table, args.column, q)
    out = run(args.duckdb, "\n".join([
        ".mode csv", ".headers off", prelude,
        "CALL enable_logging(level = 'info', storage = 'memory');",
        "SET ngram_auto_accelerate = true;",
        "SET ngram_lake_max_row_group_fraction = 1.0;",
        "EXPLAIN %s;" % query,
        "SELECT '@@log', message FROM duckdb_logs WHERE message LIKE 'ngram:%';",
        "SET ngram_lake_max_row_group_fraction = %s;" % args.fraction,
        "SELECT '@@gated';",
        "EXPLAIN %s;" % query,
    ]) + "\n")
    found = re.search(r"reads (\d+) candidates .* in ([0-9.]+)% of its row groups", out)
    if not found:
        raise RuntimeError("the index answered no probe for %r: %s" % (needle, out[-1000:]))
    gated = out.split("@@gated", 1)[1]
    return int(found.group(1)), float(found.group(2)), "index" if "__ngram_lake_rowids" in gated else "scan"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--prelude", required=True, help="SQL file that attaches the lake as lk")
    ap.add_argument("--table", required=True)
    ap.add_argument("--column", required=True)
    ap.add_argument("--needles", default=",".join(NEEDLES))
    ap.add_argument("--procs", type=int, default=3)
    ap.add_argument("--fraction", default="0.9", help="ngram_lake_max_row_group_fraction for the gated plan")
    ap.add_argument("--plans", default="index,gated,scan")
    ap.add_argument("--duckdb", default=DEFAULT_DUCKDB)
    ap.add_argument("--json", default=None, help="write the rows here")
    args = ap.parse_args()
    prelude = open(args.prelude).read()
    plans = {
        "index": ["SET ngram_auto_accelerate = true;", "SET ngram_lake_max_row_group_fraction = 1.0;"],
        "gated": ["SET ngram_auto_accelerate = true;",
                  "SET ngram_lake_max_row_group_fraction = %s;" % args.fraction],
        "scan": ["SET ngram_auto_accelerate = false;"],
    }
    variants = {"index": ("index", {}), "gated": ("gated", {}), "scan": ("scan", {}),
                "plan": ("index", {"explain": True}),
                "plan-warm": ("index", {"explain": True, "warm": True}), "index-warm": ("index", {"warm": True}),
                "scan-warm": ("scan", {"warm": True})}
    chosen = args.plans.split(",")
    rows = []
    print("| needle | candidates | rgs hit % | gate | " + " | ".join("%s ms | %s MB | %s GETs" % (p, p, p) for p in chosen) +
          " | index/scan | same answer |", flush=True)
    for needle in args.needles.split(","):
        query = "SELECT count(*), coalesce(bit_xor(hash(rowid, %s)), 0) FROM %s WHERE contains(%s, %s);" % (
            args.column, args.table, args.column, sql_quote(needle))
        count, share, gate = candidates(args, prelude, needle)
        row = dict(needle=needle, candidates=count, row_group_share=share, gate=gate)
        answers = set()
        for plan in chosen:
            settings, mode = variants[plan]
            runs = [measure(args, prelude, plans[settings], query, **mode) for _ in range(args.procs)]
            answers.update(r[1] for r in runs if r[1])
            row[plan] = dict(ms=1000 * statistics.median(r[0] for r in runs),
                             mb=statistics.median(r[2] for r in runs) / 1e6,
                             gets=statistics.median(r[3] for r in runs), answer=runs[0][1])
        row["same_answer"] = len(answers) == 1
        rows.append(row)
        ratio = (row["index"]["ms"] / row["scan"]["ms"]) if "index" in row and "scan" in row else float("nan")
        print("| %s | %d | %.1f | %s | %s | %.2f | %s |" % (
            needle, count, share, gate,
            " | ".join("%.0f | %.1f | %d" % (row[p]["ms"], row[p]["mb"], row[p]["gets"]) for p in chosen),
            ratio, "yes" if row["same_answer"] else "NO"), flush=True)
    if args.json:
        with open(args.json, "w") as f:
            json.dump(rows, f, indent=1)
    return 0 if all(r["same_answer"] for r in rows) else 1


if __name__ == "__main__":
    sys.exit(main())
