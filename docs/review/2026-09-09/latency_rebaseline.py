#!/usr/bin/env python3
"""Warm enwik9 latency before and after Phase 19, on one corpus and machine.

Builds one database per binary from the staged enwik9 lines (the format-4
binary from `main` at d3ecccf writes format 4, the current one format 5) and
times the same needles on both: the explicit search, the transparent rewrite,
the unaccelerated scan, and the probe alone, at 1 and 24 threads, twenty-one
measured runs after one warmup in one warm process, with count parity checked
on every pair. Run from the repository root:

    python3 docs/review/2026-09-09/latency_rebaseline.py \
        --old-binary /path/to/main-d3ecccf/build/release/duckdb \
        --new-binary build/release/duckdb \
        --source ~/duckdb-ngram-bench/corpus_source.db --work /tmp/rebaseline \
        --output docs/review/2026-09-09/latency_observations.json
"""

import argparse
import json
import os
import re
import statistics
import subprocess
import time

TIMER = re.compile(r"Run Time \(s\): real ([0-9.]+) user ([0-9.]+) sys ([0-9.]+)")
MEMORY = "48GB"
#! rare, moderate and dense by the release benchmark's classes, plus the
#! clustered-span and wide-projection shapes Phase 19 changed
NEEDLES = [("rare", "Schwarzschild"), ("moderate", "parliamentary"), ("moderate", "chemistry"),
           ("dense", "history"), ("dense", "which")]


def lit(value):
    return "'" + str(value).replace("'", "''") + "'"


def cli(binary, database, sql, threads, timeout=7200):
    prefix = ".headers off\n.mode list\n.bail on\nSET threads=%d; SET memory_limit='%s';\n" % (threads, MEMORY)
    process = subprocess.run([binary, database], input=prefix + sql, text=True, capture_output=True, timeout=timeout)
    if process.returncode:
        raise RuntimeError(process.stderr[-2000:] + "\nSQL: " + sql[-500:])
    return process.stdout


def timed(binary, database, statements, threads, repeats=21, warmups=1, settings=""):
    script = settings + ".timer on\n" + statements * (warmups + repeats)
    output = cli(binary, database, script, threads)
    times = [float(m.group(1)) for m in TIMER.finditer(output)]
    per_run = len(times) // (warmups + repeats)
    if per_run == 0:
        raise RuntimeError("no timings in: " + output[:500])
    runs = [sum(times[i * per_run:(i + 1) * per_run]) for i in range(warmups + repeats)]
    return runs[warmups:]


def summary(samples):
    ordered = sorted(samples)
    return {"p50_ms": round(1000 * statistics.median(ordered), 2),
            "p95_ms": round(1000 * ordered[min(len(ordered) - 1, int(round(0.95 * (len(ordered) - 1))))], 2),
            "min_ms": round(1000 * ordered[0], 2), "max_ms": round(1000 * ordered[-1], 2), "n": len(ordered)}


def scalar(binary, database, sql, threads):
    return cli(binary, database, sql, threads).strip().splitlines()[-1]


def build(binary, label, source, work):
    database = os.path.join(work, label + ".db")
    for suffix in ("", ".wal"):
        if os.path.exists(database + suffix):
            os.remove(database + suffix)
    start = time.time()
    cli(binary, database, "ATTACH %s AS src (READ_ONLY);\nCREATE TABLE docs AS SELECT id, line AS s FROM "
                          "src.enwik9_lines ORDER BY id;\nCHECKPOINT;\n" % lit(source), 24)
    load = time.time() - start
    build_s = timed(binary, database, "PRAGMA create_ngram_index('docs', 's');\nCHECKPOINT;\n", 24, repeats=1,
                    warmups=0)[0]
    return database, {"load_s": round(load, 2), "build_s": round(build_s, 2),
                      "version": scalar(binary, ":memory:", "PRAGMA version;", 24).split("|")[0]}


def measure(binary, database, log):
    result = {}
    rows = int(scalar(binary, database, "SELECT count(*) FROM docs;", 24))
    for cls, needle in NEEDLES:
        pattern = lit("%" + needle + "%")
        explicit = "SELECT count(*) FROM ngram_search('docs', %s);\n" % lit(needle)
        transparent = "SELECT count(*) FROM docs WHERE s ILIKE %s;\n" % pattern
        scan = "SELECT count(*) FROM docs WHERE contains(lower(s), lower(%s));\n" % lit(needle)
        probe = "SELECT count(*) FROM ngram_candidates('docs', 's', %s);\n" % lit(needle)
        expected = int(scalar(binary, database, scan, 24))
        entry = {"class": cls, "matches": expected, "share": round(expected / rows, 6)}
        for label, sql, settings in (("explicit", explicit, ""),
                                     ("transparent", transparent, "SET ngram_auto_accelerate=true;\n"),
                                     ("scan", scan, "SET disabled_optimizers='extension';\n"),
                                     ("probe", probe, "")):
            if label != "probe":
                got = int(scalar(binary, database, settings + sql, 24))
                if got != expected:
                    raise SystemExit("%s %r: %s returned %d, scan %d" % (binary, needle, label, got, expected))
            for threads in (1, 24):
                entry["%s_threads_%d" % (label, threads)] = summary(timed(binary, database, sql, threads,
                                                                          settings=settings))
        mode = cli(binary, database, "EXPLAIN (ANALYZE, FORMAT JSON) " + explicit, 24)
        match = re.search(r'"Ngram Mode": "([^"]+)"', mode)
        entry["mode"] = match.group(1) if match else "unknown"
        result[needle] = entry
        log("  %-14s %8d rows  explicit %7.2f / %6.2f ms  transparent %7.2f / %6.2f ms  scan %7.2f / %6.2f ms  "
            "probe %6.2f / %5.2f ms  %s" % (
                needle, expected, entry["explicit_threads_1"]["p50_ms"], entry["explicit_threads_24"]["p50_ms"],
                entry["transparent_threads_1"]["p50_ms"], entry["transparent_threads_24"]["p50_ms"],
                entry["scan_threads_1"]["p50_ms"], entry["scan_threads_24"]["p50_ms"],
                entry["probe_threads_1"]["p50_ms"], entry["probe_threads_24"]["p50_ms"], entry["mode"]))
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--old-binary", required=True)
    parser.add_argument("--new-binary", required=True)
    parser.add_argument("--source", required=True)
    parser.add_argument("--work", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    os.makedirs(args.work, exist_ok=True)
    observations = {"memory_limit": MEMORY, "repeats": 21, "needles": NEEDLES,
                    "git_head": subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True,
                                               text=True).stdout.strip()}

    def log(message):
        print(message, flush=True)

    for label, binary in (("before", args.old_binary), ("after", args.new_binary)):
        database, record = build(binary, label, args.source, args.work)
        log("%s: %s, load %.1f s, build %.2f s" % (label, record["version"], record["load_s"], record["build_s"]))
        record["binary"] = binary
        record["queries"] = measure(binary, database, log)
        observations[label] = record
    with open(args.output, "w") as handle:
        json.dump(observations, handle, indent=1, sort_keys=True)
    print("wrote", args.output)


if __name__ == "__main__":
    main()
