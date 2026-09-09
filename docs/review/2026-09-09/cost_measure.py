#!/usr/bin/env python3
"""The Phase 19 query cost model, measured on the enwik9 line corpus.

Reads the prepared benchmark database (10,920,423 rows, case-insensitive
trigram index) and records, on one machine:

  scan       full-scan cost per row, one thread (CPU) and every thread (wall)
  fetch      per-row cost of fetching scattered candidates by rowid, from
             needles whose candidates are spread over the corpus
  range      per-row cost of the bounded range scans that dense candidates
             take, from a copy of the corpus clustered so one needle's matches
             are contiguous, beside the same needle scattered
  projection fetch cost against the number of projected columns, on a copy
             of the corpus with extra columns
  tiny       the transparent rewrite against a plain scan on tables of 1,000
             and 100,000 rows, where fixed planning cost dominates
  crossover  index versus scan wall time as the candidate share grows, with
             the fraction gate open, at one thread and at every thread

Every query is checked for count parity with an unaccelerated scan. Run from
the repository root with the database benchmarks/gen_corpus.py stages:

    python3 docs/review/2026-09-09/cost_measure.py --db ~/duckdb-ngram-bench/e1.db \
        --output docs/review/2026-09-09/cost_observations.json
"""

import argparse
import json
import os
import re
import statistics
import subprocess

TIMER = re.compile(r"Run Time \(s\): real ([0-9.]+) user ([0-9.]+) sys ([0-9.]+)")
BINARY = "build/release/duckdb"
MEMORY = "48GB"
SPREAD_NEEDLES = ["Schwarzschild", "photosynthesis", "parliamentary", "chemistry", "government", "history"]
CROSSOVER_NEEDLES = ["parliamentary", "chemistry", "government", "history", "which", "the"]
CLUSTER_NEEDLE = "history"


def lit(value):
    return "'" + str(value).replace("'", "''") + "'"


def cli(database, sql, threads, timeout=7200):
    prefix = ".headers off\n.mode list\n.bail on\nSET threads=%d; SET memory_limit='%s';\n" % (threads, MEMORY)
    process = subprocess.run([BINARY, database], input=prefix + sql, text=True, capture_output=True, timeout=timeout)
    if process.returncode:
        raise RuntimeError(process.stderr[-2000:] + "\nSQL: " + sql[-800:])
    return process.stdout


def timed(database, statements, threads, repeats=7, warmups=1, settings=""):
    """(real, user + sys) seconds of each measured run, one warm process."""
    script = settings + ".timer on\n" + statements * (warmups + repeats)
    output = cli(database, script, threads)
    times = [(float(m.group(1)), float(m.group(2)) + float(m.group(3))) for m in TIMER.finditer(output)]
    per_run = len(times) // (warmups + repeats)
    if per_run == 0:
        raise RuntimeError("no timings in: " + output[:500])
    runs = [times[i * per_run:(i + 1) * per_run] for i in range(warmups + repeats)]
    measured = runs[warmups:]
    return [sum(t[0] for t in run) for run in measured], [sum(t[1] for t in run) for run in measured]


def p50(samples):
    return statistics.median(samples)


def scalar(database, sql, threads):
    return cli(database, sql, threads).strip().splitlines()[-1]


def profile(database, sql, threads, settings=""):
    output = cli(database, settings + "EXPLAIN (ANALYZE, FORMAT JSON) " + sql, threads)
    fields = dict(re.findall(r'"(Ngram [^"]+)": "([^"]*)"', output))
    return {k: (int(v) if v.isdigit() else v) for k, v in fields.items()}


def search_sql(table, needle):
    return "SELECT count(*) FROM ngram_search(%s, %s);\n" % (lit(table), lit(needle))


def scan_sql(table, needle):
    return "SELECT count(*) FROM %s WHERE contains(lower(s), lower(%s));\n" % (table, lit(needle))


def parity(database, table, needle, threads, settings=""):
    found = int(scalar(database, settings + search_sql(table, needle), threads))
    expected = int(scalar(database, scan_sql(table, needle), threads))
    if found != expected:
        raise SystemExit("%s %r: index %d, scan %d" % (table, needle, found, expected))
    return expected


def measure_scan(database, rows, log):
    result = {}
    for threads in (1, 24):
        real, cpu = timed(database, scan_sql("docs", "zzqzzqzz"), threads, repeats=5)
        result["threads_%d" % threads] = {"real_ms": round(1000 * p50(real), 1), "cpu_ms": round(1000 * p50(cpu), 1),
                                          "real_ns_per_row": round(1e9 * p50(real) / rows, 2),
                                          "cpu_ns_per_row": round(1e9 * p50(cpu) / rows, 2)}
        log("scan threads=%d real %.1f ms cpu %.1f ms: %.1f ns/row wall, %.1f ns/row cpu" % (
            threads, 1000 * p50(real), 1000 * p50(cpu), 1e9 * p50(real) / rows, 1e9 * p50(cpu) / rows))
    return result


def measure_fetch(database, table, needles, threads, log, label):
    """Per fetched row: search minus probe-only, over the fetched rows."""
    open_gate = "SET ngram_max_candidate_fraction=1;\n"
    result = {}
    for needle in needles:
        matches = parity(database, table, needle, threads, open_gate)
        fields = profile(database, search_sql(table, needle), threads, open_gate)
        real, cpu = timed(database, search_sql(table, needle), threads, settings=open_gate)
        probe_real, probe_cpu = timed(
            database, "SELECT count(*) FROM ngram_candidates(%s, 's', %s);\n" % (lit(table), lit(needle)), threads,
            settings=open_gate)
        fetched = fields.get("Ngram Fetched Rows", 0)
        ranged = fields.get("Ngram Range Rows", 0)
        entry = {
            "matches": matches, "mode": fields.get("Ngram Mode"), "fetched_rows": fetched, "range_rows": ranged,
            "manifest_rows_visited": fields.get("Ngram Manifest Rows Visited"),
            "admission_rows": fields.get("Ngram Admission Rows"),
            "search_real_ms": round(1000 * p50(real), 2), "search_cpu_ms": round(1000 * p50(cpu), 2),
            "probe_real_ms": round(1000 * p50(probe_real), 2), "probe_cpu_ms": round(1000 * p50(probe_cpu), 2),
        }
        rows = fetched + ranged
        if rows:
            entry["access_cpu_us_per_row"] = round(1e6 * max(p50(cpu) - p50(probe_cpu), 0) / rows, 3)
            entry["access_real_us_per_row"] = round(1e6 * max(p50(real) - p50(probe_real), 0) / rows, 3)
        result[needle] = entry
        log("%s %-16s %8d matches  fetched %8d  range %8d  search %8.2f ms (cpu %8.2f)  probe %7.2f ms  "
            "access %s us/row cpu  %s" % (label, needle, matches, fetched, ranged, entry["search_real_ms"],
                                          entry["search_cpu_ms"], entry["probe_real_ms"],
                                          entry.get("access_cpu_us_per_row", "-"), fields.get("Ngram Mode")))
    return result


def ensure_table(database, name, create_sql, log):
    """Statements go one per line: the CLI expands every pragma of a line
    before running the line, so a pragma naming a table the same line creates
    would not find it."""
    exists = scalar(database, "SELECT count(*) FROM duckdb_tables() WHERE table_name = %s;" % lit(name), 24)
    if exists == "1":
        return
    log("creating " + name)
    cli(database, create_sql.replace("; ", ";\n"), 24)


def measure_projection(database, log):
    ensure_table(database, "wide",
                 "CREATE TABLE wide AS SELECT id, s, id * 2 AS c1, id::DOUBLE AS c2, 'x' || id AS c3, id % 7 AS c4, "
                 "id::VARCHAR AS c5 FROM docs; CHECKPOINT; PRAGMA create_ngram_index('wide', 's'); CHECKPOINT;", log)
    open_gate = "SET ngram_max_candidate_fraction=1;\nSET ngram_auto_accelerate=true;\n"
    needle = "parliamentary"
    pattern = lit("%" + needle + "%")
    result = {}
    projections = {
        "count": "count(*)",
        "one": "max(c1)",
        "three": "max(c1), max(c2), max(c4)",
        "six": "max(c1), max(c2), max(c3), max(c4), max(c5), max(id)",
    }
    for label, columns in projections.items():
        sql = "SELECT %s FROM wide WHERE s LIKE %s;\n" % (columns, pattern)
        fields = profile(database, sql, 1, open_gate)
        real, cpu = timed(database, sql, 1, settings=open_gate)
        plain_real, plain_cpu = timed(database, sql, 1, settings="SET disabled_optimizers='extension';\n")
        result[label] = {"columns": columns, "mode": fields.get("Ngram Mode"),
                         "fetched_rows": fields.get("Ngram Fetched Rows"), "range_rows": fields.get("Ngram Range Rows"),
                         "index_cpu_ms": round(1000 * p50(cpu), 2), "scan_cpu_ms": round(1000 * p50(plain_cpu), 2),
                         "index_real_ms": round(1000 * p50(real), 2), "scan_real_ms": round(1000 * p50(plain_real), 2)}
        log("projection %-5s fetched %s  index cpu %8.2f ms  scan cpu %8.2f ms  %s" % (
            label, fields.get("Ngram Fetched Rows"), 1000 * p50(cpu), 1000 * p50(plain_cpu), fields.get("Ngram Mode")))
    return result


def measure_tiny(database, log):
    result = {}
    for rows in (1000, 100000):
        name = "tiny_%d" % rows
        ensure_table(database, name, "CREATE TABLE %s AS SELECT id, s FROM docs WHERE id < %d; CHECKPOINT; "
                     "PRAGMA create_ngram_index('%s', 's'); CHECKPOINT;" % (name, rows, name), log)
        needle = "Schwarzschild" if rows > 1000 else "the"
        pattern = lit("%" + needle + "%")
        sql = "SELECT count(*) FROM %s WHERE s LIKE %s;\n" % (name, pattern)
        entry = {"rows": rows, "needle": needle}
        for threads in (1, 24):
            accel_real, _ = timed(database, sql, threads, repeats=101,
                                  settings="SET ngram_auto_accelerate=true;\n")
            plain_real, _ = timed(database, sql, threads, repeats=101,
                                  settings="SET disabled_optimizers='extension';\n")
            explicit_real, _ = timed(database, search_sql(name, needle), threads, repeats=101)
            fields = profile(database, sql, threads, "SET ngram_auto_accelerate=true;\n")
            entry["threads_%d" % threads] = {
                "transparent_us": round(1e6 * p50(accel_real), 1), "scan_us": round(1e6 * p50(plain_real), 1),
                "explicit_us": round(1e6 * p50(explicit_real), 1), "mode": fields.get("Ngram Mode"),
            }
            log("tiny rows=%d threads=%d transparent %.0f us  scan %.0f us  explicit %.0f us  %s" % (
                rows, threads, 1e6 * p50(accel_real), 1e6 * p50(plain_real), 1e6 * p50(explicit_real),
                fields.get("Ngram Mode")))
        result[name] = entry
    return result


def measure_crossover(database, rows, log):
    open_gate = "SET ngram_max_candidate_fraction=1;\n"
    result = {}
    for needle in CROSSOVER_NEEDLES:
        entry = {"matches": parity(database, "docs", needle, 24, open_gate)}
        fields = profile(database, search_sql("docs", needle), 24, open_gate)
        entry["mode"] = fields.get("Ngram Mode")
        entry["candidate_share"] = round(
            int(re.search(r"<= (\d+) candidates", fields.get("Ngram Mode", "0")).group(1)) / rows, 5) \
            if "candidates" in fields.get("Ngram Mode", "") else None
        entry["admission_share"] = round(fields.get("Ngram Admission Rows", 0) / rows, 5)
        for threads in (1, 24):
            real, cpu = timed(database, search_sql("docs", needle), threads, repeats=5, settings=open_gate)
            scan_real, scan_cpu = timed(database, scan_sql("docs", needle), threads, repeats=5)
            entry["threads_%d" % threads] = {
                "index_real_ms": round(1000 * p50(real), 1), "index_cpu_ms": round(1000 * p50(cpu), 1),
                "scan_real_ms": round(1000 * p50(scan_real), 1), "scan_cpu_ms": round(1000 * p50(scan_cpu), 1),
            }
            log("crossover %-14s share %s admission %s threads=%d  index %8.1f ms  scan %8.1f ms" % (
                needle, entry["candidate_share"], entry["admission_share"], threads, 1000 * p50(real),
                1000 * p50(scan_real)))
        result[needle] = entry
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--stage", choices=["all", "scan", "fetch", "range", "projection", "tiny", "crossover"],
                        default="all")
    args = parser.parse_args()
    observations = {}
    if os.path.exists(args.output):
        with open(args.output) as handle:
            observations = json.load(handle)
    lines = []

    def log(message):
        print(message, flush=True)
        lines.append(message)

    rows = int(scalar(args.db, "SELECT count(*) FROM docs;", 24))
    observations["rows"] = rows
    observations["git_head"] = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True,
                                              text=True).stdout.strip()
    stage = args.stage
    if stage in ("all", "scan"):
        observations["scan"] = measure_scan(args.db, rows, log)
    if stage in ("all", "fetch"):
        observations["fetch_threads_1"] = measure_fetch(args.db, "docs", SPREAD_NEEDLES, 1, log, "fetch/1")
        observations["fetch_threads_24"] = measure_fetch(args.db, "docs", SPREAD_NEEDLES, 24, log, "fetch/24")
    if stage in ("all", "range"):
        ensure_table(args.db, "clustered",
                     "CREATE TABLE clustered AS SELECT id, s FROM docs ORDER BY contains(lower(s), %s) DESC, id; "
                     "CHECKPOINT; PRAGMA create_ngram_index('clustered', 's'); CHECKPOINT;" % lit(CLUSTER_NEEDLE.lower()),
                     log)
        observations["range_threads_1"] = measure_fetch(args.db, "clustered", [CLUSTER_NEEDLE, "chemistry"], 1, log,
                                                        "range/1")
        observations["range_threads_24"] = measure_fetch(args.db, "clustered", [CLUSTER_NEEDLE, "chemistry"], 24, log,
                                                         "range/24")
        # the same needle under the default gate on both layouts
        observations["range_default_gate"] = {
            table: profile(args.db, search_sql(table, CLUSTER_NEEDLE), 24)
            for table in ("clustered", "docs")
        }
        log("default gate: %s" % observations["range_default_gate"])
    if stage in ("all", "projection"):
        observations["projection"] = measure_projection(args.db, log)
    if stage in ("all", "tiny"):
        observations["tiny"] = measure_tiny(args.db, log)
    if stage in ("all", "crossover"):
        observations["crossover"] = measure_crossover(args.db, rows, log)
    observations.setdefault("log", [])
    observations["log"].extend(lines)
    with open(args.output, "w") as handle:
        json.dump(observations, handle, indent=1, sort_keys=True)
    print("wrote", args.output)


if __name__ == "__main__":
    main()
