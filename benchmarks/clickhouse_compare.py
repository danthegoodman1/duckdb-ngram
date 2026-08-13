#!/usr/bin/env python3
"""Small same-machine DuckDB ngram vs ClickHouse text-index benchmark."""

import argparse
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import shutil
import socket
import statistics
import subprocess
import tempfile
import time


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = ROOT / ".tmp/clickhouse-phase15/corpus/enwik9.lines.hex.tsv"
DEFAULT_CLICKHOUSE = ROOT / ".tmp/clickhouse-phase15/bin/clickhouse"
DEFAULT_DUCKDB = ROOT / "build/release/duckdb"
INPUT_BYTES = 2_071_799_070
INPUT_SHA256 = "f9e4a271a2350049ba5e6a75d8d32977de65cea995a505d67875a7627b46385c"
CLICKHOUSE_SHA256 = "d2a2611ed56bc6d1544fafe8ece7e888ae2af6e13ea7095993a5105d8c74d4f3"
ROWS = 10_920_423
TEXT_BYTES = 986_852_975
NEEDLES = {
    "rare": "737570657263616c6966726167696c6973746963",  # supercalifragilistic
    "moderate": "57696b6970656469613a",  # Wikipedia:
    "common": "746865",  # the
}
DUCK_SETTINGS = """SET threads=24;
SET memory_limit='48GiB';
SET preserve_insertion_order=true;
SET ngram_max_grams_per_query=3;
SET ngram_max_candidate_fraction=0.01;
SET ngram_max_probe_rowids=100000000;
SET ngram_build_partitions=0;
SET ngram_auto_accelerate=false;
"""
CH_SCAN_SETTINGS = """ SETTINGS use_query_cache=0, use_query_condition_cache=0,
use_skip_indexes=0, use_skip_indexes_on_data_read=0,
query_plan_direct_read_from_text_index=0, query_plan_text_index_add_hint=0,
use_text_index_like_evaluation_by_dictionary_scan=0""".replace("\n", " ")
TIMER = re.compile(r"Run Time \(s\): real (\d+)\.(\d{3})")


class BenchError(RuntimeError):
    pass


def fail(message):
    raise BenchError(message)


def run(argv, *, input_text=None, stdin_path=None, check=True, timeout=3600):
    source = open(stdin_path, "rb") if stdin_path else None
    try:
        result = subprocess.run(
            [str(value) for value in argv], cwd=ROOT, input=input_text,
            stdin=source, text=input_text is not None or source is None,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout,
        )
    finally:
        if source:
            source.close()
    if check and result.returncode:
        fail("command failed (%d): %s\n%s" % (result.returncode, argv, result.stderr[-4000:]))
    return result


def sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as source:
        for chunk in iter(lambda: source.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def tree_size(root):
    apparent = allocated = 0
    for path in Path(root).rglob("*"):
        if path.is_file() and not path.is_symlink():
            stat = path.stat()
            apparent += stat.st_size
            allocated += stat.st_blocks * 512
    return {"apparent_bytes": apparent, "allocated_bytes": allocated}


def file_size(path):
    stat = Path(path).stat()
    return {"apparent_bytes": stat.st_size, "allocated_bytes": stat.st_blocks * 512}


def evict_files(root):
    root = Path(root)
    files = [root] if root.is_file() else sorted(
        path for path in root.rglob("*") if path.is_file() and not path.is_symlink())
    os.sync()
    total = 0
    for path in files:
        descriptor = os.open(path, os.O_RDONLY)
        try:
            size = path.stat().st_size
            os.posix_fadvise(descriptor, 0, size, os.POSIX_FADV_DONTNEED)
            total += size
        finally:
            os.close(descriptor)
    return total


def open_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def clickhouse_config(root, tcp_port, http_port):
    for name in ("data", "tmp", "logs", "user_files", "format_schemas"):
        (root / name).mkdir(parents=True, exist_ok=True)
    config = root / "config.xml"
    config.write_text("""<clickhouse>
 <logger><level>warning</level><log>{root}/logs/server.log</log>
  <errorlog>{root}/logs/server.err.log</errorlog></logger>
 <path>{root}/data/</path><tmp_path>{root}/tmp/</tmp_path>
 <user_files_path>{root}/user_files/</user_files_path>
 <format_schema_path>{root}/format_schemas/</format_schema_path>
 <listen_host>127.0.0.1</listen_host><tcp_port>{tcp}</tcp_port><http_port>{http}</http_port>
 <max_server_memory_usage>51539607552</max_server_memory_usage>
 <users><default><password/><networks><ip>127.0.0.1</ip></networks>
  <profile>default</profile><quota>default</quota></default></users>
 <profiles><default><max_threads>24</max_threads><max_insert_threads>24</max_insert_threads>
  <max_memory_usage>51539607552</max_memory_usage><use_query_cache>0</use_query_cache>
  <use_query_condition_cache>0</use_query_condition_cache></default></profiles>
 <quotas><default><interval><duration>3600</duration><queries>0</queries><errors>0</errors>
  <result_rows>0</result_rows><read_rows>0</read_rows><execution_time>0</execution_time>
 </interval></default></quotas>
</clickhouse>
""".format(root=root, tcp=tcp_port, http=http_port))
    return config


class ClickHouse:
    def __init__(self, binary, root):
        self.binary = Path(binary)
        self.root = Path(root)
        self.tcp, self.http = open_port(), open_port()
        while self.http == self.tcp:
            self.http = open_port()
        self.config = clickhouse_config(self.root, self.tcp, self.http)
        self.process = None

    def __enter__(self):
        self.process = subprocess.Popen(
            [str(self.binary), "server", "--config-file", str(self.config)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        for _ in range(300):
            if self.process.poll() is not None:
                fail("ClickHouse exited during startup")
            result = self.query("SELECT 1 FORMAT TabSeparatedRaw", check=False)
            if result.returncode == 0 and result.stdout.strip() == "1":
                return self
            time.sleep(0.05)
        fail("ClickHouse did not become ready")

    def query(self, sql, *, stdin_path=None, check=True):
        argv = [self.binary, "client", "--host", "127.0.0.1", "--port", self.tcp,
                "--database", "default", "--multiquery"]
        if stdin_path:
            argv += ["--query", sql]
            return run(argv, stdin_path=stdin_path, check=check)
        return run(argv, input_text=sql, check=check)

    def scalar(self, sql):
        value = self.query(sql + " FORMAT TabSeparatedRaw").stdout.strip()
        if not re.fullmatch(r"\d+", value):
            fail("expected integer ClickHouse result, got %r" % value[:200])
        return int(value)

    def __exit__(self, kind, value, trace):
        if self.process and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
                if kind is None:
                    fail("ClickHouse required a forced stop")
        if kind is None and self.process and self.process.returncode != 0:
            fail("ClickHouse did not stop cleanly")


def timed(function):
    start = time.monotonic_ns()
    function()
    os.sync()
    return time.monotonic_ns() - start


def duck_load(binary, database, normalized):
    sql = DUCK_SETTINGS + """CREATE TABLE docs(id BIGINT, text VARCHAR);
INSERT INTO docs SELECT id, decode(unhex(payload))
FROM read_csv('%s', delim='\\t', quote='', escape='', header=false,
 columns={'id':'BIGINT','payload':'VARCHAR'}, new_line='\\n', strict_mode=true);
CHECKPOINT;
""" % str(normalized).replace("'", "''")
    return timed(lambda: run([binary, database], input_text=sql))


def duck_build(binary, database, case_insensitive=False):
    sql = DUCK_SETTINGS + """PRAGMA create_ngram_index(
 'docs', 'text', gram=3, case_insensitive=%s);
CHECKPOINT;
""" % str(case_insensitive).lower()
    return timed(lambda: run([binary, database], input_text=sql))


def ch_index_type(case_insensitive=False):
    if case_insensitive:
        return "text(tokenizer = ngrams(3), preprocessor = lowerUTF8(text))"
    return "text(tokenizer = ngrams(3))"


def ch_table_sql(with_index=False, case_insensitive=False):
    index = ", INDEX text_ngram text TYPE " + ch_index_type(case_insensitive) if with_index else ""
    return """CREATE TABLE docs(
 id UInt64 CODEC(LZ4), text String CODEC(LZ4)%s
) ENGINE=MergeTree ORDER BY id
SETTINGS index_granularity=8192,index_granularity_bytes=0""" % index


def ch_load(server, normalized, with_index=False, case_insensitive=False):
    server.query(ch_table_sql(with_index, case_insensitive))
    insert = """INSERT INTO docs SELECT id, unhex(payload)
FROM input('id UInt64, payload String') FORMAT TabSeparated"""
    return timed(lambda: server.query(insert, stdin_path=normalized))


def ch_build(server, case_insensitive=False):
    sql = """ALTER TABLE docs ADD INDEX text_ngram text TYPE %s;
ALTER TABLE docs MATERIALIZE INDEX text_ngram SETTINGS mutations_sync=2;"""
    sql %= ch_index_type(case_insensitive)
    return timed(lambda: server.query(sql))


def ch_sizes(server):
    part = server.query("""SELECT sum(bytes_on_disk),sum(data_compressed_bytes),
sum(data_uncompressed_bytes),sum(rows) FROM system.parts
WHERE active AND database=currentDatabase() AND table='docs' FORMAT TabSeparatedRaw""").stdout.strip()
    values = [int(value) for value in part.split("\t")]
    if len(values) != 4 or values[3] != ROWS:
        fail("unexpected ClickHouse part state: %r" % part)
    index_rows = server.query("""SELECT data_compressed_bytes,data_uncompressed_bytes,marks_bytes
FROM system.data_skipping_indices WHERE database=currentDatabase()
AND table='docs' AND name='text_ngram' FORMAT TabSeparatedRaw""", check=False)
    index = [int(value) for value in index_rows.stdout.strip().split("\t")] if index_rows.stdout.strip() else [0, 0, 0]
    return {
        "table_on_disk_bytes": values[0], "table_compressed_bytes": values[1],
        "table_uncompressed_bytes": values[2], "rows": values[3],
        "index_compressed_bytes": index[0], "index_uncompressed_bytes": index[1],
        "index_marks_bytes": index[2],
    }


def pattern_hex(needle_hex):
    needle = bytes.fromhex(needle_hex)
    escaped = needle.replace(b"\\", b"\\\\").replace(b"%", b"\\%").replace(b"_", b"\\_")
    return (b"%" + escaped + b"%").hex()


def ch_query(needle_hex, indexed=True, case_insensitive=False):
    pattern = pattern_hex(needle_hex)
    if case_insensitive:
        predicate = "text ILIKE unhex('%s')" % pattern
        if indexed:
            predicate = "hasAllTokens(text,unhex('%s')) AND " % needle_hex + predicate
    else:
        predicate = "text LIKE unhex('%s')" % pattern
    sql = "SELECT count() FROM docs WHERE " + predicate
    return sql + (" SETTINGS use_query_cache=0,use_query_condition_cache=0" if indexed else CH_SCAN_SETTINGS)


def digest_command(argv, input_text=None):
    with tempfile.TemporaryFile() as output:
        result = subprocess.run(
            [str(value) for value in argv], cwd=ROOT, input=input_text, text=True,
            stdout=output, stderr=subprocess.PIPE, timeout=3600,
        )
        if result.returncode:
            fail("result-set command failed: %s\n%s" % (argv, result.stderr[-4000:]))
        output.seek(0)
        digest, rows = hashlib.sha256(), 0
        for chunk in iter(lambda: output.read(1 << 20), b""):
            digest.update(chunk)
            rows += chunk.count(b"\n")
        return {"rows": rows, "row_id_sha256": digest.hexdigest()}


def ch_result_sets(server, case_insensitive=False):
    records = []
    argv = [server.binary, "client", "--host", "127.0.0.1", "--port", server.tcp,
            "--database", "default", "--query"]
    for query_id, needle_hex in NEEDLES.items():
        for mode in ("indexed", "scan"):
            count_sql = ch_query(needle_hex, mode == "indexed", case_insensitive)
            id_sql = count_sql.replace("SELECT count()", "SELECT id", 1)
            settings = id_sql.partition(" SETTINGS ")
            id_sql = settings[0] + " ORDER BY id"
            if settings[1]:
                id_sql += " SETTINGS " + settings[2]
            id_sql += " FORMAT TabSeparatedRaw"
            records.append({"engine": "clickhouse", "query": query_id, "mode": mode,
                            **digest_command(argv + [id_sql])})
    return records


def ch_query_samples(server, samples, case_insensitive=False):
    connection = http.client.HTTPConnection("127.0.0.1", server.http, timeout=3600)
    records = []
    try:
        for query_id, needle_hex in NEEDLES.items():
            for mode in ("indexed", "scan"):
                sql = ch_query(needle_hex, mode == "indexed", case_insensitive) + " FORMAT TabSeparatedRaw"
                connection.request("POST", "/", body=sql.encode())
                response = connection.getresponse()
                body = response.read()
                if response.status != 200:
                    fail("ClickHouse warmup failed: %s" % body[-500:])
                warmup = int(body)
                values = []
                for _ in range(samples):
                    start = time.monotonic_ns()
                    connection.request("POST", "/", body=sql.encode())
                    response = connection.getresponse()
                    body = response.read()
                    elapsed = time.monotonic_ns() - start
                    if response.status != 200:
                        fail("ClickHouse timed query failed: %s" % body[-500:])
                    count = int(body)
                    if count != warmup:
                        fail("ClickHouse query count changed")
                    values.append(elapsed)
                records.append({"engine": "clickhouse", "query": query_id, "mode": mode,
                                "count": warmup, "wall_ns": values})
    finally:
        connection.close()
    return records


def ch_cold_queries(binary, root, repetition, case_insensitive=False):
    records = []
    for query_id, needle_hex in NEEDLES.items():
        for mode in ("indexed", "scan"):
            evicted = evict_files(Path(root) / "data")
            with ClickHouse(binary, root) as server:
                connection = http.client.HTTPConnection("127.0.0.1", server.http, timeout=3600)
                connection.connect()
                sql = ch_query(needle_hex, mode == "indexed", case_insensitive)
                start = time.monotonic_ns()
                connection.request("POST", "/", body=(sql + " FORMAT TabSeparatedRaw").encode())
                response = connection.getresponse()
                body = response.read()
                elapsed = time.monotonic_ns() - start
                connection.close()
                if response.status != 200:
                    fail("ClickHouse cold query failed: %s" % body[-500:])
            records.append({"engine": "clickhouse", "repetition": repetition,
                            "query": query_id, "mode": mode, "count": int(body),
                            "wall_ns": elapsed, "evicted_bytes": evicted})
    return records


def duck_query_samples(binary, database, samples, case_insensitive=False):
    script = ".headers off\n.mode list\n.timer off\n" + DUCK_SETTINGS
    order = []
    for query_id, needle_hex in NEEDLES.items():
        for mode in ("indexed", "scan"):
            if mode == "indexed":
                source = "ngram_search('docs',decode(unhex('%s')),col := 'text')" % needle_hex
            elif case_insensitive:
                source = "docs WHERE text ILIKE ('%%' || decode(unhex('%s')) || '%%')" % needle_hex
            else:
                source = "docs WHERE contains(text,decode(unhex('%s')))" % needle_hex
            script += "SELECT count(*) FROM %s;\n" % source
            for index in range(samples):
                script += ".timer on\nSELECT 'Q|%s|%s|%d|' || count(*) FROM %s;\n.timer off\n" % (
                    query_id, mode, index, source)
                order.append((query_id, mode, index))
    process = run([binary, "-readonly", database], input_text=script)
    tags = re.findall(r"^Q\|([a-z]+)\|(indexed|scan)\|(\d+)\|(\d+)$", process.stdout, re.MULTILINE)
    timers = TIMER.findall(process.stdout)
    if len(tags) != len(order) or len(timers) != len(order):
        fail("DuckDB persistent timing output differs")
    grouped = {}
    for expected, tag, timer in zip(order, tags, timers):
        if expected != (tag[0], tag[1], int(tag[2])):
            fail("DuckDB timing order differs")
        key = (tag[0], tag[1])
        grouped.setdefault(key, {"count": int(tag[3]), "wall_ns": []})
        if grouped[key]["count"] != int(tag[3]):
            fail("DuckDB query count changed")
        grouped[key]["wall_ns"].append((int(timer[0]) * 1000 + int(timer[1])) * 1_000_000)
    return [{"engine": "duckdb", "query": query_id, "mode": mode, **record}
            for (query_id, mode), record in grouped.items()]


def duck_cold_queries(binary, database, repetition, case_insensitive=False):
    records = []
    for query_id, needle_hex in NEEDLES.items():
        for mode in ("indexed", "scan"):
            if mode == "indexed":
                source = "ngram_search('docs',decode(unhex('%s')),col := 'text')" % needle_hex
            elif case_insensitive:
                source = "docs WHERE text ILIKE ('%%' || decode(unhex('%s')) || '%%')" % needle_hex
            else:
                source = "docs WHERE contains(text,decode(unhex('%s')))" % needle_hex
            evicted = evict_files(database)
            script = ".headers off\n.mode list\n.timer off\n" + DUCK_SETTINGS
            script += ".timer on\nSELECT count(*) FROM %s;\n.timer off\n" % source
            process = run([binary, "-readonly", database], input_text=script)
            values = [int(value) for value in re.findall(r"^\d+$", process.stdout, re.MULTILINE)]
            timers = TIMER.findall(process.stdout)
            if len(values) != 1 or len(timers) != 1:
                fail("DuckDB cold timing output differs")
            elapsed = (int(timers[0][0]) * 1000 + int(timers[0][1])) * 1_000_000
            records.append({"engine": "duckdb", "repetition": repetition,
                            "query": query_id, "mode": mode, "count": values[0],
                            "wall_ns": elapsed, "evicted_bytes": evicted})
    return records


def duck_result_sets(binary, database, case_insensitive=False):
    records = []
    for query_id, needle_hex in NEEDLES.items():
        for mode in ("indexed", "scan"):
            if mode == "indexed":
                source = "ngram_search('docs',decode(unhex('%s')),col := 'text')" % needle_hex
            elif case_insensitive:
                source = "docs WHERE text ILIKE ('%%' || decode(unhex('%s')) || '%%')" % needle_hex
            else:
                source = "docs WHERE contains(text,decode(unhex('%s')))" % needle_hex
            sql = ".headers off\n.mode list\n" + DUCK_SETTINGS
            sql += "SELECT id FROM %s ORDER BY id;\n" % source
            records.append({"engine": "duckdb", "query": query_id, "mode": mode,
                            **digest_command([binary, "-readonly", database], sql)})
    return records


def summarize(values):
    return {"median_ns": int(statistics.median(values)), "min_ns": min(values), "max_ns": max(values)}


def collect(args):
    work = Path(args.work).resolve()
    output = Path(args.output).resolve()
    normalized = Path(args.input).resolve()
    clickhouse = Path(args.clickhouse).resolve()
    duckdb = Path(args.duckdb).resolve()
    if args.repetitions < 1 or args.samples < 1:
        fail("repetitions and samples must be positive")
    if work.exists() or output.exists():
        fail("work/output path already exists")
    if normalized.stat().st_size != INPUT_BYTES or sha256(normalized) != INPUT_SHA256:
        fail("normalized enwik9 input differs")
    if sha256(clickhouse) != CLICKHOUSE_SHA256:
        fail("ClickHouse binary differs")
    work.mkdir(parents=True)
    stages, queries, cold_queries, result_sets = [], [], [], []
    for repetition in range(1, args.repetitions + 1):
        duck_db = work / ("duck-%d.db" % repetition)
        ch_root = work / ("clickhouse-%d" % repetition)
        engines = ("duckdb", "clickhouse") if repetition % 2 else ("clickhouse", "duckdb")
        for engine in engines:
            if engine == "duckdb":
                load_ns = duck_load(duckdb, duck_db, normalized)
                base = file_size(duck_db)
                build_ns = duck_build(duckdb, duck_db, args.case_insensitive)
                indexed = file_size(duck_db)
                stages.append({"repetition": repetition, "engine": engine, "load_ns": load_ns,
                               "build_ns": build_ns, "base_bytes": base["apparent_bytes"],
                               "indexed_bytes": indexed["apparent_bytes"],
                               "index_bytes": indexed["apparent_bytes"] - base["apparent_bytes"],
                               "base_allocated_bytes": base["allocated_bytes"],
                               "indexed_allocated_bytes": indexed["allocated_bytes"]})
                cold_queries.extend(duck_cold_queries(
                    duckdb, duck_db, repetition, args.case_insensitive))
                if repetition == args.repetitions:
                    queries.extend(duck_query_samples(
                        duckdb, duck_db, args.samples, args.case_insensitive))
                    result_sets.extend(duck_result_sets(
                        duckdb, duck_db, args.case_insensitive))
            else:
                with ClickHouse(clickhouse, ch_root) as server:
                    load_ns = ch_load(server, normalized)
                    base_logical = ch_sizes(server)
                    base = tree_size(ch_root / "data")
                    build_ns = ch_build(server, args.case_insensitive)
                    indexed_logical = ch_sizes(server)
                    indexed = tree_size(ch_root / "data")
                    stages.append({"repetition": repetition, "engine": engine, "load_ns": load_ns,
                                   "build_ns": build_ns,
                                   "base_bytes": base_logical["table_on_disk_bytes"],
                                   "indexed_bytes": indexed_logical["table_on_disk_bytes"],
                                   "index_bytes": (indexed_logical["table_on_disk_bytes"] -
                                                   base_logical["table_on_disk_bytes"]),
                                   "base_allocated_bytes": base["allocated_bytes"],
                                   "indexed_allocated_bytes": indexed["allocated_bytes"],
                                   "base_filesystem": base, "indexed_filesystem": indexed,
                                   "base_logical": base_logical,
                                   "indexed_logical": indexed_logical})
                cold_queries.extend(ch_cold_queries(
                    clickhouse, ch_root, repetition, args.case_insensitive))
                if repetition == args.repetitions:
                    with ClickHouse(clickhouse, ch_root) as server:
                        queries.extend(ch_query_samples(server, args.samples, args.case_insensitive))
                        result_sets.extend(ch_result_sets(server, args.case_insensitive))
                        plans = {query_id: server.query(
                            "EXPLAIN indexes=1 " + ch_query(
                                needle, case_insensitive=args.case_insensitive)).stdout
                                 for query_id, needle in NEEDLES.items()}
                        if "Name: text_ngram" not in plans["rare"]:
                            fail("ClickHouse rare query did not expose text_ngram in EXPLAIN")
    by_cell = {}
    for record in queries:
        by_cell[(record["engine"], record["query"], record["mode"])] = record
    for query_id in NEEDLES:
        counts = {by_cell[(engine, query_id, mode)]["count"]
                  for engine in ("duckdb", "clickhouse") for mode in ("indexed", "scan")}
        counts.update(record["count"] for record in cold_queries if record["query"] == query_id)
        if len(counts) != 1:
            fail("cross-engine count mismatch for %s: %s" % (query_id, counts))
        for engine in ("duckdb", "clickhouse"):
            for mode in ("indexed", "scan"):
                cells = [record for record in cold_queries if record["engine"] == engine and
                         record["query"] == query_id and record["mode"] == mode]
                if ({record["repetition"] for record in cells} !=
                        set(range(1, args.repetitions + 1)) or
                        any(record["wall_ns"] <= 0 or record["evicted_bytes"] <= 0
                            for record in cells)):
                    fail("cold query samples differ for %s/%s/%s" % (engine, query_id, mode))
        sets = [record for record in result_sets if record["query"] == query_id]
        if len(sets) != 4 or len({(record["rows"], record["row_id_sha256"])
                                  for record in sets}) != 1:
            fail("cross-engine result-set mismatch for %s" % query_id)
        if sets[0]["rows"] != next(iter(counts)):
            fail("result-set row count differs for %s" % query_id)
    artifact = {
        "schema": ("duckdb-ngram-clickhouse-text-ci-quick-v1" if args.case_insensitive else
                   "duckdb-ngram-clickhouse-text-quick-v1"),
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "scope": "same-machine point of reference; %d fresh load/build reps and %d warm query samples" %
                 (args.repetitions, args.samples),
        "corpus": {"rows": ROWS, "text_bytes": TEXT_BYTES, "normalized_bytes": INPUT_BYTES,
                   "normalized_sha256": INPUT_SHA256},
        "versions": {"clickhouse": run([clickhouse, "local", "--version"]).stdout.strip(),
                     "duckdb": run([duckdb, "-noheader", "-list", ":memory:", "-c", "SELECT version()"])
                     .stdout.strip(), "clickhouse_sha256": CLICKHOUSE_SHA256,
                     "duckdb_sha256": sha256(duckdb),
                     "tool_sha256": sha256(Path(__file__)),
                     "source_commit": run(["git", "rev-parse", "HEAD"]).stdout.strip()},
        "machine": {"kernel": os.uname().release, "architecture": os.uname().machine,
                    "logical_cpus": os.cpu_count()},
        "settings": {"threads": 24, "memory_limit": "48GiB",
                     "case_insensitive": args.case_insensitive,
                     "clickhouse_index": ch_index_type(args.case_insensitive),
                     "duckdb_index": "gram=3, case_insensitive=%s" %
                                     str(args.case_insensitive).lower(),
                     "query_samples": args.samples, "cold_samples": args.repetitions,
                     "cold_preparation": "sync + POSIX_FADV_DONTNEED; metadata may remain cached",
                     "repetitions": args.repetitions},
        "stages": stages,
        "queries": queries,
        "cold_queries": cold_queries,
        "result_sets": result_sets,
        "clickhouse_plans": plans,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(artifact, indent=2, sort_keys=True) + "\n")
    print("wrote", output)


def gib(value):
    return "%.3f" % (value / 1024 ** 3)


def ms(value):
    return "%.1f" % (value / 1_000_000)


def seconds(value):
    return "%.3f" % (value / 1_000_000_000)


def verdict(values, labels=("DuckDB", "ClickHouse")):
    left, right = values
    if max(left, right) / min(left, right) < 1.10:
        return "roughly tied (within 10%)"
    winner = 0 if left < right else 1
    return "%s by %.2fx" % (labels[winner], values[1 - winner] / values[winner])


def render(args):
    artifact = json.loads(Path(args.artifact).read_text())
    stages = artifact["stages"]
    case_insensitive = artifact["settings"].get("case_insensitive", False)
    qualifier = "case-insensitive " if case_insensitive else ""
    index_description = (["ClickHouse used `ngrams(3)` with `lowerUTF8(text)` preprocessing;",
                          "DuckDB used its native case-insensitive trigram index."]
                         if case_insensitive else
                         ["ClickHouse used `TYPE text(tokenizer = ngrams(3))`; DuckDB used its",
                          "native case-sensitive trigram index."])
    lines = ["# DuckDB ngram vs ClickHouse %stext index" % qualifier, "",
             "This is a bounded same-machine point of reference, not an exhaustive tuning study.",
             "Both engines used 24 threads, a 48 GiB memory setting, the same 1 GB enwik9-derived",
             "line corpus, %s3-grams, and exact substring rechecks." % qualifier, "",
             *index_description,
             "DuckDB retained its production adaptive scan fallback for dense terms.", "",
             "## Load, build, and storage", "",
             "The input file was already in the host page cache; these are warm-source ingest times.",
             "Storage is paired DuckDB database-file and ClickHouse active-table on-disk size;",
             "ClickHouse logs and temporarily retained inactive parts are excluded.", "",
             "| Engine | Load median (range) | Index build median (range) | Base | Indexed | Index delta |",
             "| --- | ---: | ---: | ---: | ---: | ---: |"]
    for engine in ("duckdb", "clickhouse"):
        rows = [row for row in stages if row["engine"] == engine]
        load = [row["load_ns"] for row in rows]
        build = [row["build_ns"] for row in rows]
        base = int(statistics.median(row["base_bytes"] for row in rows))
        indexed = int(statistics.median(row["indexed_bytes"] for row in rows))
        delta = int(statistics.median(row["index_bytes"] for row in rows))
        lines.append("| %s | %s s (%s–%s) | %s s (%s–%s) | %s GiB | %s GiB | %s GiB |" % (
            engine, seconds(statistics.median(load)), seconds(min(load)), seconds(max(load)),
            seconds(statistics.median(build)), seconds(min(build)), seconds(max(build)),
            gib(base), gib(indexed), gib(delta)))
    stage_medians = {engine: {
        "load": statistics.median(row["load_ns"] for row in stages if row["engine"] == engine),
        "build": statistics.median(row["build_ns"] for row in stages if row["engine"] == engine),
        "index": statistics.median(row["index_bytes"] for row in stages if row["engine"] == engine),
    } for engine in ("duckdb", "clickhouse")}
    lines += ["", "At a glance:", "",
              "- Load: %s." % verdict([stage_medians[engine]["load"] for engine in ("duckdb", "clickhouse")]),
              "- Index build: %s." % verdict([stage_medians[engine]["build"] for engine in ("duckdb", "clickhouse")]),
              "- Incremental index storage: %s." % verdict(
                  [stage_medians[engine]["index"] for engine in ("duckdb", "clickhouse")]),
              "", "## File-data-cold query latency", "",
              "Each observation used a stopped engine, `sync`, and `POSIX_FADV_DONTNEED` on every",
              "database/index file before a fresh process and one query with no warm-up. These three",
              "samples model file-data-cold access; filesystem metadata may remain cached.", "",
              "| Needle | Matches | DuckDB search | DuckDB scan | ClickHouse search | ClickHouse scan | Faster search |",
              "| --- | ---: | ---: | ---: | ---: | ---: | --- |"]
    cold = artifact["cold_queries"]
    for query_id in NEEDLES:
        groups = [[row["wall_ns"] for row in cold if row["engine"] == engine and
                   row["query"] == query_id and row["mode"] == mode]
                  for engine in ("duckdb", "clickhouse") for mode in ("indexed", "scan")]
        medians = [statistics.median(values) for values in groups]
        count = next(row["count"] for row in cold if row["query"] == query_id)
        display = ["%s ms (%s–%s)" % (ms(statistics.median(values)), ms(min(values)),
                                      ms(max(values))) for values in groups]
        lines.append("| %s | %d | %s | %s | %s | %s | %s |" % (
            query_id, count, *display, verdict([medians[0], medians[2]])))
    lines += ["", "## Warm query latency", "",
              "One warm-up preceded %d measured samples per cell. Sorted matching-row digests matched" %
              artifact["settings"]["query_samples"],
              "across both engines and their scan controls.", "",
              "| Needle | Matches | DuckDB search | DuckDB scan | ClickHouse search | ClickHouse scan | Faster search |",
              "| --- | ---: | ---: | ---: | ---: | ---: | --- |"]
    cells = {(row["engine"], row["query"], row["mode"]): row for row in artifact["queries"]}
    for query_id in NEEDLES:
        values = [cells[(engine, query_id, mode)] for engine in ("duckdb", "clickhouse")
                  for mode in ("indexed", "scan")]
        medians = [summarize(value["wall_ns"])["median_ns"] for value in values]
        lines.append("| %s | %d | %s ms | %s ms | %s ms | %s ms | %s |" % (
            query_id, values[0]["count"], *(ms(value) for value in medians),
            verdict([medians[0], medians[2]])))
    if case_insensitive:
        lines += ["", "The original mixed-case text was stored unchanged in both engines. DuckDB folded",
                  "its index keys; ClickHouse used `lowerUTF8` preprocessing for index tokens."]
    artifact_name = Path(args.artifact).name
    lines += ["", "Raw samples and exact versions are in",
              "[`artifacts/%s`](artifacts/%s)." % (artifact_name, artifact_name),
              "", "The result compares different architectures: ClickHouse's native text inverted index",
              "and this extension's postings index plus exact base-table recheck. It should be read as a",
              "practical reference for this corpus and machine, not a universal engine ranking.", ""]
    Path(args.output).write_text("\n".join(lines))
    print("wrote", args.output)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    collect_parser = sub.add_parser("collect")
    collect_parser.add_argument("--input", default=DEFAULT_INPUT)
    collect_parser.add_argument("--clickhouse", default=DEFAULT_CLICKHOUSE)
    collect_parser.add_argument("--duckdb", default=DEFAULT_DUCKDB)
    collect_parser.add_argument("--work", default=ROOT / ".tmp/clickhouse-quick-run")
    collect_parser.add_argument("--output", default=ROOT / "benchmarks/artifacts/enwik9-clickhouse-text-vs-ngram-v1.json")
    collect_parser.add_argument("--repetitions", type=int, default=3)
    collect_parser.add_argument("--samples", type=int, default=10)
    collect_parser.add_argument("--case-insensitive", action="store_true")
    collect_parser.set_defaults(function=collect)
    render_parser = sub.add_parser("render")
    render_parser.add_argument("--artifact", default=ROOT / "benchmarks/artifacts/enwik9-clickhouse-text-vs-ngram-v1.json")
    render_parser.add_argument("--output", default=ROOT / "benchmarks/CLICKHOUSE.md")
    render_parser.set_defaults(function=render)
    args = parser.parse_args()
    try:
        args.function(args)
    except BenchError as exc:
        print("benchmark error:", exc, file=os.sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
