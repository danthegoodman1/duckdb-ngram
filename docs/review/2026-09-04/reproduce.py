#!/usr/bin/env python3
"""Read-only audit of source; experiments use disposable DuckDB databases.

Run from the repository root. This reports observations, including failures;
it is not a replacement for regression tests or a release benchmark.
"""

import argparse
import hashlib
import json
from pathlib import Path
import random
import re
import statistics
import string
import subprocess
import tempfile


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def cli(binary, sql, database=":memory:", bail=True):
    prefix = ".headers off\n.mode csv\n" + (".bail on\n" if bail else "")
    return subprocess.run(
        [str(binary), database], input=prefix + sql, text=True,
        capture_output=True, timeout=90,
    )


def checked(binary, sql, database=":memory:"):
    result = cli(binary, sql, database)
    if result.returncode:
        raise RuntimeError(result.stderr)
    return result.stdout.strip()


def timings(output):
    return [
        {"wall_s": float(wall), "cpu_s": float(user) + float(system)}
        for wall, user, system in re.findall(
            r"Run Time \(s\): real ([\d.]+) user ([\d.]+) sys ([\d.]+)", output
        )
    ]


def batch_order(binary, work):
    observations = []
    for threads, transparent in ((1, False), (24, False), (24, True)):
        sql = f"""SET threads={threads};
SET ngram_max_candidate_fraction=1;
CREATE TABLE t AS SELECT i AS id,
 CASE WHEN i < 1048576 OR i % 3 = 0 THEN 'abcdefgh' ELSE 'zzzzzzzz' END AS s
 FROM range(1100000) r(i);
PRAGMA create_ngram_index('t','s',case_insensitive=false);
SET ngram_auto_accelerate={'true' if transparent else 'false'};
"""
        source = ("SELECT id FROM t WHERE contains(s,'abcdefgh')" if transparent
                  else "SELECT id FROM ngram_search('t','abcdefgh')")
        sql += f"CREATE TABLE got AS {source};\nSELECT count(*) FROM got;\n"
        sql += "SELECT count(*) FROM t;\n"
        result = cli(binary, sql, bail=False)
        observations.append({
            "threads": threads, "transparent": transparent,
            "returncode": result.returncode, "stdout": result.stdout.strip(),
            "errors": [line for line in result.stderr.splitlines() if "Error:" in line],
            "expected_count": 1065717,
        })
    return observations


def long_needle(binary, work):
    sizes = (8192, 16384, 32768, 65536, 131072)
    rng = random.Random(904)
    sql = """SET threads=1;
CREATE TABLE t AS SELECT 'abcdef' AS s;
PRAGMA create_ngram_index('t','s',gram=6,case_insensitive=false);
SET memory_limit='16MB';
"""
    for size in sizes:
        needle = "".join(rng.choices(string.ascii_letters + string.digits, k=size))
        sql += f"SET VARIABLE needle={quote(needle)};\n.timer on\n"
        sql += "SELECT count(*) FROM ngram_search('t',getvariable('needle'));\n.timer off\n"
    output = checked(binary, sql)
    return [dict(needle_bytes=size, **measurement)
            for size, measurement in zip(sizes, timings(output), strict=True)]


def range_cost(binary, work):
    sizes = (1100000, 11000000)
    sql = "SET threads=1;\nSET ngram_max_candidate_fraction=1;\n"
    for size in sizes:
        sql += f"""CREATE TABLE t{size} AS SELECT i AS id,
 CASE WHEN i < 50000 THEN 'needle' ELSE 'xxxxxxxx' END AS s FROM range({size}) r(i);
PRAGMA create_ngram_index('t{size}','s',case_insensitive=false);
.timer on
"""
        for _ in range(5):
            sql += f"SELECT count(*) FROM ngram_search('t{size}','needle');\n"
            sql += f"SELECT count(*) FROM t{size} WHERE contains(s,'needle');\n"
        sql += ".timer off\n"
    sql += "SET enable_profiling='json';\n"
    for column in ("rowid", "id"):
        sql += f"SET profiling_output={quote(work / (column + '.json'))};\n"
        sql += f"SELECT count(*) FROM t11000000 WHERE {column} BETWEEN 1000 AND 2999;\n"
    measured = timings(checked(binary, sql))
    if len(measured) != 20:
        raise RuntimeError("Expected twenty range benchmark samples")
    result = {"samples": measured, "summary": []}
    for i, size in enumerate(sizes):
        samples = measured[i * 10:(i + 1) * 10]
        result["summary"].append({
            "rows": size, "candidates": 50000,
            "indexed_cpu_p50_s": statistics.median(s["cpu_s"] for s in samples[::2]),
            "native_cpu_p50_s": statistics.median(s["cpu_s"] for s in samples[1::2]),
        })
    result["profiles"] = {}
    for column in ("rowid", "id"):
        profile = json.loads((work / (column + ".json")).read_text())
        result["profiles"][column] = {key: profile[key] for key in (
            "latency", "cpu_time", "cumulative_rows_scanned",
        )}
    return result


def registry_cost(binary, work):
    sql = """SET threads=1;
CREATE TABLE indexed(s VARCHAR);
INSERT INTO indexed VALUES ('needle');
CREATE TABLE unrelated AS SELECT i AS id FROM range(1000) r(i);
PRAGMA create_ngram_index('indexed','s');
"""
    rows = []
    # Validly encoded rows for absent owners isolate discovery cost. These
    # are synthetic allocations, not 9,999 successfully built real indexes.
    for i in range(1, 10000):
        table = f"absent_{i}"
        key = b"".join(len(s).to_bytes(8, "big") + s.encode() for s in ("main", table, "s"))
        rows.append(
            f"(2,'00000000-0000-4000-8000-{i:012x}'::UUID,from_hex('{key.hex()}'),"
            f"'main','{table}','s',5,3,false,-1,'g','t')"
        )
    sql += "INSERT INTO __ngram.registry VALUES " + ",".join(rows) + ";\n"
    for enabled in ("false", "true"):
        sql += f"SET ngram_auto_accelerate={enabled};\n.timer on\n"
        sql += "SELECT count(*) FROM unrelated WHERE id=1;\n" * 5
        sql += ".timer off\n"
    samples = timings(checked(binary, sql))
    if len(samples) != 10:
        raise RuntimeError("Expected ten registry benchmark samples")
    return {
        "registry_rows": 10000, "synthetic_orphan_rows": 9999, "samples": samples,
        "disabled_cpu_p50_s": statistics.median(s["cpu_s"] for s in samples[:5]),
        "enabled_cpu_p50_s": statistics.median(s["cpu_s"] for s in samples[5:]),
    }


def format4_drop(binary, work):
    database = str(work / "format4.duckdb")
    checked(binary, "CREATE TABLE t(s VARCHAR); INSERT INTO t VALUES ('needle');", database)
    checked(binary, "PRAGMA create_ngram_index('t','s');", database)
    ref = checked(binary, "SELECT index_id::VARCHAR FROM __ngram.registry;", database)
    stats = "stats_" + ref.replace("-", "")
    # Simulate the format-4 cleanup shape; this is not a persistence fixture
    # written by a real format-4 binary. Drop never decodes the segment data.
    checked(binary, f"CREATE TABLE __ngram.{stats}(gram VARCHAR,row_count BIGINT,segment_count BIGINT);"
            " UPDATE __ngram.registry SET format_version=4;", database)
    before = checked(binary, "PRAGMA ngram_indexes;", database)
    checked(binary, f"PRAGMA drop_ngram_index({quote(ref)}, catalog = 'format4');", database)
    return {
        "fixture_kind": "synthetic format-4 cleanup shape", "before": before,
        "after": checked(binary, "PRAGMA ngram_indexes;", database),
        "remaining_tables": checked(binary, "SELECT table_name FROM duckdb_tables() "
                                    "WHERE schema_name='__ngram' ORDER BY 1;", database),
    }


CASES = {fn.__name__: fn for fn in (batch_order, long_needle, range_cost, registry_cost, format4_drop)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=Path("build/release/duckdb"))
    parser.add_argument("--case", choices=["all", *CASES], default="all")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    digest = hashlib.sha256()
    for path in sorted(Path("src").rglob("*")):
        if path.is_file():
            digest.update(str(path).encode() + b"\0" + path.read_bytes())
    results = {
        "head": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
        "src_sha256": digest.hexdigest(),
        "binary_sha256": hashlib.file_digest(args.binary.open("rb"), "sha256").hexdigest(),
        "runtime": checked(args.binary, "SELECT * FROM pragma_version();"),
        "cases": {},
    }
    for name, function in CASES.items():
        if args.case not in ("all", name):
            continue
        with tempfile.TemporaryDirectory(prefix="ngram-review-") as directory:
            results["cases"][name] = function(args.binary, Path(directory))
    rendered = json.dumps(results, indent=2) + "\n"
    if args.output:
        args.output.write_text(rendered)
    print(rendered, end="")


if __name__ == "__main__":
    main()
