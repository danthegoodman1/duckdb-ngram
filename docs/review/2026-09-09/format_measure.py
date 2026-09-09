#!/usr/bin/env python3
"""Format-4 versus format-5 cost, fragmented generations, and manifest scans.

Measures, on one enwik9-derived corpus and one machine, what the audit's 19B
and 19C rows ask for:

  format   build wall time, storage delta, warm query latency, refresh cost per
           generation, statistics cost, and compaction, for the format-4 build
           of `main` at d3ecccf (segments plus a statistics table, the query
           engine before Phase 19) and the current format-5 build.
  fragment query latency under 1, 5, 10 and 20 refresh generations and after
           compaction, format 5 only.
  manifest physical work of the probe's manifest collection: rows the storage
           scan reads for one `gram_key = ?` scan per key against one
           `gram_key IN (...)` scan, measured with the profiler's
           OPERATOR_ROWS_SCANNED, beside the zone-map row-group count from
           pragma_storage_info.

Every database is disposable and written under --work. Run from the
repository root:

    python3 docs/review/2026-09-09/format_measure.py \
        --old-binary /path/to/main-d3ecccf/build/release/duckdb \
        --new-binary build/release/duckdb \
        --source ~/duckdb-ngram-bench/corpus_source.db --rows 1000000 \
        --work /tmp/format-measure --output docs/review/2026-09-09/format_observations.json
"""

import argparse
import json
import os
import re
import statistics
import subprocess
import time

TIMER = re.compile(r"Run Time \(s\): real ([0-9.]+) user ([0-9.]+) sys ([0-9.]+)")
THREADS = 8
MEMORY = "16GB"
NEEDLES = ["Schwarzschild", "photosynthesis", "parliamentary", "chemistry", "history", "which"]
REFRESH_ROWS = 10000
GENERATIONS = 20


def lit(value):
    return "'" + str(value).replace("'", "''") + "'"


def cli(binary, database, sql, timeout=3600):
    prefix = ".headers off\n.mode list\n.bail on\nSET threads=%d; SET memory_limit='%s';\n" % (THREADS, MEMORY)
    process = subprocess.run([binary, database], input=prefix + sql, text=True, capture_output=True,
                             timeout=timeout)
    if process.returncode:
        raise RuntimeError(process.stderr[-2000:] + "\nSQL: " + sql[-500:])
    return process.stdout


def timed(binary, database, statements, repeats, warmups=1, settings=""):
    """Wall seconds of each measured run of `statements` in one warm process."""
    script = settings + ".timer on\n" + statements * (warmups + repeats)
    output = cli(binary, database, script)
    times = [float(m.group(1)) for m in TIMER.finditer(output)]
    per_run = len(times) // (warmups + repeats)
    if per_run == 0:
        raise RuntimeError("no timings in: " + output[:500])
    runs = [sum(times[i * per_run:(i + 1) * per_run]) for i in range(warmups + repeats)]
    return runs[warmups:]


def summary(samples):
    ordered = sorted(samples)
    return {"p50_ms": round(1000 * statistics.median(ordered), 2), "min_ms": round(1000 * ordered[0], 2),
            "max_ms": round(1000 * ordered[-1], 2), "n": len(ordered)}


def storage_bytes(database):
    total = 0
    for suffix in ("", ".wal"):
        if os.path.exists(database + suffix):
            total += os.path.getsize(database + suffix)
    return total


def scalar(binary, database, sql):
    return cli(binary, database, sql).strip().splitlines()[-1]


def explain_fields(binary, database, sql, settings=""):
    """Every `Ngram ...` field the accelerated operator renders in its profile."""
    output = cli(binary, database, settings + "EXPLAIN (ANALYZE, FORMAT JSON) " + sql)
    return dict(re.findall(r'"(Ngram [^"]+)": "([^"]*)"', output))


def query_block(binary, database, label):
    """Warm latency of every needle on the explicit and fallback paths."""
    block = {}
    for needle in NEEDLES:
        search = "SELECT count(*) FROM ngram_search('docs', %s);\n" % lit(needle)
        scan = "SELECT count(*) FROM docs WHERE contains(lower(s), lower(%s));\n" % lit(needle)
        matches = int(scalar(binary, database, scan))
        found = int(scalar(binary, database, search))
        if matches != found:
            raise SystemExit("%s: %r returned %d through the index and %d by scan" % (label, needle, found, matches))
        fields = explain_fields(binary, database, search)
        block[needle] = {
            "matches": matches,
            "mode": fields.get("Ngram Mode", "unknown"),
            "profile": fields,
            "search": summary(timed(binary, database, search, 7)),
            "scan": summary(timed(binary, database, scan, 7)),
        }
        print("  %-8s %-16s %8d rows  search %8.2f ms  scan %8.2f ms  manifest rows %s  %s" % (
            label, needle, matches, block[needle]["search"]["p50_ms"], block[needle]["scan"]["p50_ms"],
            fields.get("Ngram Manifest Rows Scanned", "?"), block[needle]["mode"]), flush=True)
    return block


def stats_block(binary, database):
    return {
        "stats": summary(timed(binary, database, "PRAGMA ngram_index_stats('docs');\n", 5)),
        "indexes": summary(timed(binary, database, "PRAGMA ngram_indexes;\n", 5)),
        "stats_row": scalar(binary, database, ".mode csv\nPRAGMA ngram_index_stats('docs');"),
    }


def measure_format(binary, label, source, rows, work):
    database = os.path.join(work, label + ".db")
    for suffix in ("", ".wal"):
        if os.path.exists(database + suffix):
            os.remove(database + suffix)
    record = {"binary": binary, "version": scalar(binary, ":memory:", "PRAGMA version;").split("|")[0]}
    load = ("ATTACH %s AS src (READ_ONLY);\n"
            "CREATE TABLE docs AS SELECT id, line AS s FROM src.enwik9_lines WHERE id < %d ORDER BY id;\n"
            "CHECKPOINT;\n" % (lit(source), rows))
    start = time.time()
    cli(binary, database, load)
    record["load_s"] = round(time.time() - start, 3)
    record["base_bytes"] = storage_bytes(database)
    build = timed(binary, database, "PRAGMA create_ngram_index('docs', 's');\nCHECKPOINT;\n", 1, warmups=0)
    record["build_s"] = round(build[0], 3)
    record["indexed_bytes"] = storage_bytes(database)
    record["index_bytes"] = record["indexed_bytes"] - record["base_bytes"]
    record["storage_tables"] = scalar(
        binary, database, "SELECT string_agg(table_name, ',' ORDER BY table_name) FROM duckdb_tables() "
                          "WHERE schema_name = '__ngram';")
    print("%s: load %.1f s, build %.2f s, index %.1f MiB, tables %s" % (
        label, record["load_s"], record["build_s"], record["index_bytes"] / 2**20, record["storage_tables"]),
        flush=True)
    record["queries_generation_1"] = query_block(binary, database, label + "/g1")
    record["stats_generation_1"] = stats_block(binary, database)
    print("  stats %.2f ms, indexes %.2f ms" % (record["stats_generation_1"]["stats"]["p50_ms"],
                                              record["stats_generation_1"]["indexes"]["p50_ms"]), flush=True)
    refreshes = []
    for generation in range(GENERATIONS):
        lo = rows + generation * REFRESH_ROWS
        insert = ("ATTACH %s AS src (READ_ONLY);\n"
                  "INSERT INTO docs SELECT id, line FROM src.enwik9_lines WHERE id >= %d AND id < %d ORDER BY id;\n"
                  % (lit(source), lo, lo + REFRESH_ROWS))
        cli(binary, database, insert)
        refreshes.append(timed(binary, database, "PRAGMA ngram_refresh('docs');\n", 1, warmups=0)[0])
        noop = timed(binary, database, "PRAGMA ngram_refresh('docs');\n", 1, warmups=0)[0]
        refreshes.append(-noop)
        if generation + 2 in (5, 10, 20):
            key = "queries_generation_%d" % (generation + 2)
            record[key] = query_block(binary, database, label + "/g%d" % (generation + 2))
            record["stats_generation_%d" % (generation + 2)] = stats_block(binary, database)
    record["refresh_s"] = [round(r, 4) for r in refreshes if r > 0]
    record["noop_refresh_s"] = [round(-r, 4) for r in refreshes if r < 0]
    record["fragmented_bytes"] = storage_bytes(database)
    print("  refresh p50 %.1f ms (no-op %.1f ms), storage after %d generations %.1f MiB" % (
        1000 * statistics.median(record["refresh_s"]), 1000 * statistics.median(record["noop_refresh_s"]),
        GENERATIONS + 1, record["fragmented_bytes"] / 2**20), flush=True)
    record["compact_s"] = round(timed(binary, database, "PRAGMA ngram_compact('docs');\nCHECKPOINT;\n", 1,
                                      warmups=0)[0], 3)
    record["noop_compact_s"] = round(timed(binary, database, "PRAGMA ngram_compact('docs');\n", 1,
                                           warmups=0)[0], 3)
    record["compacted_bytes"] = storage_bytes(database)
    record["queries_compacted"] = query_block(binary, database, label + "/compact")
    record["stats_compacted"] = stats_block(binary, database)
    print("  compact %.2f s (no-op %.3f s), storage %.1f MiB" % (
        record["compact_s"], record["noop_compact_s"], record["compacted_bytes"] / 2**20), flush=True)
    return database, record


def rows_scanned(binary, database, sql, work):
    profile = os.path.join(work, "profile.json")
    if os.path.exists(profile):
        os.remove(profile)
    script = ("PRAGMA enable_profiling='json';\nPRAGMA profiling_output=%s;\n"
              "PRAGMA custom_profiling_settings='{\"OPERATOR_ROWS_SCANNED\": \"true\", \"OPERATOR_TIMING\": "
              "\"true\", \"OPERATOR_TYPE\": \"true\", \"OPERATOR_CARDINALITY\": \"true\"}';\n%s\n"
              % (lit(profile), sql))
    cli(binary, database, script)
    with open(profile) as handle:
        tree = json.load(handle)
    scans = []

    def walk(node):
        if node.get("operator_type") == "TABLE_SCAN":
            scans.append((int(node["operator_rows_scanned"]), float(node["operator_timing"]),
                          int(node["operator_cardinality"])))
        for child in node.get("children", []):
            walk(child)

    walk(tree)
    if len(scans) != 1:
        raise RuntimeError("expected one table scan in " + sql)
    return scans[0]


def measure_manifest(binary, database, work, label):
    segments = scalar(binary, database, "SELECT '__ngram.' || table_name FROM duckdb_tables() WHERE "
                                        "schema_name = '__ngram' AND table_name LIKE 'segments_%';")
    row_groups = cli(binary, database,
                     "SELECT row_group_id, stats FROM pragma_storage_info(%s) WHERE column_name = 'gram_key' "
                     "AND segment_type <> 'VALIDITY' ORDER BY row_group_id;" % lit(segments)).strip().splitlines()
    zone_maps = []
    for line in row_groups:
        group, stats = line.split("|", 1)
        match = re.match(r"\[Min: (\d+), Max: (\d+)\]", stats)
        zone_maps.append((int(group), int(match.group(1)), int(match.group(2))))
    total_rows = int(scalar(binary, database, "SELECT count(*) FROM " + segments))
    print("manifest/%s: %s holds %d rows in %d row groups" % (label, segments, total_rows, len(zone_maps)),
          flush=True)
    result = {"segments_table": segments, "rows": total_rows, "row_groups": len(zone_maps), "needles": []}
    for needle in NEEDLES:
        keys = scalar(binary, database, "SELECT string_agg(k::VARCHAR, ',') FROM unnest(ngram_gram_keys(%s, 3, true)) "
                                        "t(k);" % lit(needle)).split(",")
        per_key = []
        for key in keys:
            scanned, seconds, returned = rows_scanned(
                binary, database, "SELECT count(*) FROM %s WHERE gram_key = %s::UHUGEINT;" % (segments, key), work)
            groups = [g for g, lo, hi in zone_maps if lo <= int(key) <= hi]
            per_key.append({"key": key, "rows_scanned": scanned, "returned": returned, "seconds": seconds,
                            "row_groups": groups})
        combined_sql = "SELECT count(*) FROM %s WHERE gram_key IN (%s);" % (
            segments, ", ".join(k + "::UHUGEINT" for k in keys))
        scanned, seconds, returned = rows_scanned(binary, database, combined_sql, work)
        union_groups = sorted({g for entry in per_key for g in entry["row_groups"]})
        per_key_scans = timed(binary, database, "".join(
            "SELECT count(*) FROM %s WHERE gram_key = %s::UHUGEINT;\n" % (segments, k) for k in keys), 7)
        combined_scans = timed(binary, database, combined_sql + "\n", 7)
        probe = timed(binary, database, "SELECT count(*) FROM ngram_candidates('docs', 's', %s);\n" % lit(needle), 7,
                      settings="SET ngram_max_candidate_fraction=1;\n")
        visited = explain_fields(binary, database, "SELECT count(*) FROM ngram_search('docs', %s);\n" % lit(needle),
                                 "SET ngram_max_candidate_fraction=1;\n")
        entry = {
            "needle": needle, "keys": len(keys),
            "per_key": per_key,
            "per_key_rows_scanned": sum(e["rows_scanned"] for e in per_key),
            "per_key_row_groups": sum(len(e["row_groups"]) for e in per_key),
            "combined_rows_scanned": scanned, "combined_returned": returned,
            "combined_row_groups": len(union_groups),
            "per_key_sql": summary(per_key_scans), "combined_sql": summary(combined_scans),
            "probe": summary(probe),
            "manifest_rows_visited": visited.get("Ngram Manifest Rows Visited"),
            "manifest_rows_scanned": visited.get("Ngram Manifest Rows Scanned"),
        }
        result["needles"].append(entry)
        print("  %-16s keys %2d  rows scanned per-key %9d (%2d groups)  combined %9d (%2d groups)  "
              "sql %6.2f / %6.2f ms  probe %6.2f ms  positioned visits %s" % (
                  needle, len(keys), entry["per_key_rows_scanned"], entry["per_key_row_groups"], scanned,
                  len(union_groups), entry["per_key_sql"]["p50_ms"], entry["combined_sql"]["p50_ms"],
                  entry["probe"]["p50_ms"], entry["manifest_rows_visited"]), flush=True)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--old-binary", required=True)
    parser.add_argument("--new-binary", required=True)
    parser.add_argument("--source", required=True)
    parser.add_argument("--rows", type=int, default=1000000)
    parser.add_argument("--work", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--stage", choices=["all", "format", "manifest"], default="all",
                        help="manifest alone reuses the databases an earlier format stage left under --work")
    args = parser.parse_args()
    os.makedirs(args.work, exist_ok=True)
    observations = {}
    if os.path.exists(args.output):
        with open(args.output) as handle:
            observations = json.load(handle)
    observations.update({"rows": args.rows, "threads": THREADS, "memory_limit": MEMORY, "needles": NEEDLES,
                         "refresh_rows": REFRESH_ROWS, "generations": GENERATIONS,
                         "git_head": subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True,
                                                    text=True).stdout.strip()})
    if args.stage in ("all", "format"):
        _, observations["format4"] = measure_format(args.old_binary, "format4", args.source, args.rows, args.work)
        _, observations["format5"] = measure_format(args.new_binary, "format5", args.source, args.rows, args.work)
    if args.stage in ("all", "manifest"):
        # one build generation, then the compacted table the format stage left
        fresh_db = os.path.join(args.work, "manifest.db")
        for suffix in ("", ".wal"):
            if os.path.exists(fresh_db + suffix):
                os.remove(fresh_db + suffix)
        cli(args.new_binary, fresh_db,
            "ATTACH %s AS src (READ_ONLY);\nCREATE TABLE docs AS SELECT id, line AS s FROM src.enwik9_lines WHERE id "
            "< %d ORDER BY id;\nCHECKPOINT;\nPRAGMA create_ngram_index('docs', 's');\nCHECKPOINT;\n"
            % (lit(args.source), args.rows))
        observations["manifest_one_generation"] = measure_manifest(args.new_binary, fresh_db, args.work, "build")
        observations["manifest_compacted"] = measure_manifest(
            args.new_binary, os.path.join(args.work, "format5.db"), args.work, "compacted")
    with open(args.output, "w") as handle:
        json.dump(observations, handle, indent=1, sort_keys=True)
    print("wrote", args.output)


if __name__ == "__main__":
    main()
