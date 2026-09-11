#!/usr/bin/env python3
"""Rows the host's filtered scan reads for one segments-table key.

Reports the column segment that holds a key and the profiler's rows-scanned
count for `gram_key = key`, `gram_key BETWEEN key AND key`, and the same
equality with a second predicate. On v1.5.5 every shape reads the key's
whole row group: the segment-level zone-map skip in
RowGroup::CheckZonemapSegments only skips the first excluded segment.

    python3 docs/review/2026-09-09/zonemap_probe.py --db /tmp/format-measure/manifest.db
"""

import argparse
import json
import os
import subprocess
import tempfile

BINARY = "build/release/duckdb"


def cli(database, sql):
    process = subprocess.run([BINARY, database], input=".headers off\n.mode list\n.bail on\nSET threads=1;\n" + sql,
                             text=True, capture_output=True)
    if process.returncode:
        raise SystemExit(process.stderr)
    return process.stdout


def scanned(database, sql, profile):
    if os.path.exists(profile):
        os.remove(profile)
    cli(database, "PRAGMA enable_profiling='json';\nPRAGMA profiling_output='%s';\n"
                  "PRAGMA custom_profiling_settings='{\"OPERATOR_ROWS_SCANNED\": \"true\", \"OPERATOR_TIMING\": "
                  "\"true\", \"OPERATOR_TYPE\": \"true\", \"OPERATOR_CARDINALITY\": \"true\"}';\n%s\n" % (profile, sql))
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
    return scans


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", required=True, help="a database holding one ngram index on docs(s)")
    parser.add_argument("--gram", default="sch")
    args = parser.parse_args()
    segments = cli(args.db, "SELECT '__ngram.' || table_name FROM duckdb_tables() WHERE schema_name = '__ngram' "
                            "AND table_name LIKE 'segments_%';").strip()
    key = cli(args.db, "SELECT ngram_gram_key('%s');" % args.gram).strip()
    print("key", key, "in", segments)
    print("column segments holding it:", cli(
        args.db, "SELECT row_group_id, segment_id, count FROM pragma_storage_info('%s') WHERE column_name='gram_key' "
                 "AND segment_type <> 'VALIDITY' AND CAST(regexp_extract(stats, 'Min: (\\\\d+)', 1) AS UHUGEINT) <= "
                 "%s::UHUGEINT AND CAST(regexp_extract(stats, 'Max: (\\\\d+)', 1) AS UHUGEINT) >= %s::UHUGEINT;"
                 % (segments, key, key)).strip())
    with tempfile.TemporaryDirectory(prefix="ngram-zonemap-") as directory:
        profile = os.path.join(directory, "profile.json")
        for label, predicate in (("equality", "gram_key = %s::UHUGEINT" % key),
                                 ("between", "gram_key >= %s::UHUGEINT AND gram_key <= %s::UHUGEINT" % (key, key)),
                                 ("equality and count", "gram_key = %s::UHUGEINT AND rowid_count > 0" % key)):
            rows = scanned(args.db, "SELECT count(*) FROM %s WHERE %s;" % (segments, predicate), profile)
            print("%-20s rows scanned, seconds, rows returned: %s" % (label, rows))


if __name__ == "__main__":
    main()
