#!/usr/bin/env python3
"""Collect, validate, and render the bounded Phase 14B release evidence."""

import argparse
import decimal
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import platform
import random
import re
import shutil
import struct
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
import zipfile


ROOT = Path(__file__).resolve().parents[1]
TOOL = Path(__file__).resolve()
README = ROOT / "README.md"
RESULTS = ROOT / "benchmarks" / "RESULTS.md"
ARTIFACT_REL = "benchmarks/artifacts/enwik9-current-v1.json"
COMMAND = "python3 benchmarks/release_evidence.py"
BEGIN = "<!-- BEGIN NGRAM RELEASE EVIDENCE -->"
END = "<!-- END NGRAM RELEASE EVIDENCE -->"

SCHEMA = 1
BENCHMARK_ID = "enwik9-current-v1"
ENGINE_COMMIT = "b6a388c8c39f6e51de44a8365e871a517914bc4d"
DUCKDB_GITLINK = "d8cdaa33fda8df955cc76ef58a280f68f4cd43fa"
CI_GITLINK = "72e76e99cd7fee45a99739cd118ec2db64e034ec"
DUCKDB_VERSION = "v1.5.5"
DUCKDB_SOURCE = "d8cdaa33"
ENGINE_FILES = ("CMakeLists.txt", "Makefile", "extension_config.cmake", "vcpkg.json")
SUBMODULES = ("duckdb", "extension-ci-tools")
PINNED_LINKS = {"duckdb": DUCKDB_GITLINK, "extension-ci-tools": CI_GITLINK}

SOURCE_PAGE = "https://www.mattmahoney.net/dc/textdata.html"
ARCHIVE_URL = "https://www.mattmahoney.net/dc/enwik9.zip"
REUSE_URL = "https://dumps.wikimedia.org/legal.html"
TERMS_URL = "https://foundation.wikimedia.org/wiki/Policy:Terms_of_Use"
ARCHIVE_BYTES = 322_592_222
RAW_BYTES = 1_000_000_000
RAW_MD5 = "e206c3450ac99950df65bf70ef61a12d"
RAW_SHA1 = "2996e86fb978f93cca8f566cc56998923e7fe581"
NORMALIZATION = "enwik9-lines-v1"
NORM_DOMAIN = b"enwik9-lines-v1\0"
SANITIZED_ENV = {
    "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
    "HOME": str(ROOT),
    "LC_ALL": "C",
    "TZ": "UTC",
    "CCACHE_DISABLE": "1",
}

POLL_MS = 100
QUERY_SEED = 20261402
QUERY_REPEATS = 21
NEEDLES = (
    ("rare", "737570657263616c6966726167696c6973746963"),
    ("moderate", "57696b6970656469613a"),
    ("dense", "746865"),
)
FIXED_SETTINGS = dict(
    threads=24, memory_limit="48.0 GiB", preserve_insertion_order=True,
    gram_size=3, case_insensitive=False, max_grams=3, candidate_fraction="0.01",
    probe_rowids=100_000_000, build_partitions=0, auto_accelerate=False,
)

SQL_SETTINGS = "SET threads=24; SET memory_limit='48GiB'; SET preserve_insertion_order=true;\n"
LOAD_TEMPLATE = SQL_SETTINGS + """CREATE TABLE docs(id BIGINT, text VARCHAR);
INSERT INTO docs
SELECT id, decode(unhex(payload))
FROM read_csv(<INPUT>, delim='\\t', quote='', escape='', header=false,
              columns={'id':'BIGINT','payload':'VARCHAR'}, new_line='\\n', strict_mode=true);
CHECKPOINT;
"""
BUILD_SQL = SQL_SETTINGS + """PRAGMA create_ngram_index(
    'docs', 'text', gram=3, case_insensitive=false);
CHECKPOINT;
"""
COUNT_SQL = (
    "SELECT count(*), min(id), max(id), count(DISTINCT id), "
    "sum(octet_length(encode(text))) FROM docs;"
)
SEARCH_TEMPLATE = (
    "SELECT count(*) FROM ngram_search('docs', decode(unhex(<HEX>)), col := 'text');"
)
SCAN_TEMPLATE = "SELECT count(*) FROM docs WHERE contains(text, decode(unhex(<HEX>)));"
CANDIDATE_TEMPLATE = (
    "SELECT count(*) FROM ngram_candidates('docs', 'text', decode(unhex(<HEX>)));"
)
PARITY_TEMPLATE = (
    "WITH indexed AS (SELECT id FROM ngram_search('docs', decode(unhex(<HEX>)), col := 'text')), "
    "scanned AS (SELECT id FROM docs WHERE contains(text, decode(unhex(<HEX>)))) SELECT "
    "(SELECT count(*) FROM (SELECT id FROM indexed EXCEPT ALL SELECT id FROM scanned)) AS "
    "index_minus_scan, (SELECT count(*) FROM (SELECT id FROM scanned EXCEPT ALL SELECT id FROM "
    "indexed)) AS scan_minus_index;"
)
PARITY_COLUMNS = ("index_minus_scan", "scan_minus_index")
EXPLAIN_TEMPLATE = "EXPLAIN ANALYZE " + SEARCH_TEMPLATE

STATS_COLUMNS = tuple(
    "column_name gram_size case_insensitive hwm_rowid table_max_rowid remaining_tail "
    "distinct_grams segments fragmented_keys generations posting_entries postings_bytes stale_reason".split()
)
REGISTRY_COLUMNS = tuple(
    "database_name index_ref schema_name table_name column_name format_version status reason".split()
)
#! Artifacts collected under storage format 3 recorded this listing shape. They
#! stay verifiable until the corpus is re-collected under format 4.
FORMAT3_REGISTRY_COLUMNS = tuple(
    "database_name kind index_ref schema_name table_name column_name storage_schema "
    "format_version status reason".split()
)
MACHINE_KEYS = (
    "os kernel architecture cpu_model logical_cpus memory_bytes governors filesystem mount "
    "device_model rotational c_compiler c_flags cxx_compiler cxx_flags compiler_launcher linker "
    "make cmake ninja python gnu_time"
).split()

BUILD_ARGV = ["make", "GEN=ninja", "release"]
PROTOCOL = dict(
    engine_pins=dict(
        engine_commit=ENGINE_COMMIT, duckdb_gitlink=DUCKDB_GITLINK, ci_gitlink=CI_GITLINK,
        duckdb_version=DUCKDB_VERSION, duckdb_source=DUCKDB_SOURCE,
    ),
    corpus_source=dict(
        page=SOURCE_PAGE, archive_url=ARCHIVE_URL, archive_member="enwik9",
        archive_bytes=ARCHIVE_BYTES, raw_bytes=RAW_BYTES, raw_md5=RAW_MD5, raw_sha1=RAW_SHA1,
        normalization=NORMALIZATION, input_format="id-tab-utf8hex-lf-v1",
        contract="strict UTF-8; split LF; drop empty; preserve CR/bytes; zero-based IDs",
        reuse_urls=[REUSE_URL, TERMS_URL],
    ),
    digest_contracts=dict(
        relation="SHA256(domain enwik9-lines-v1\\0 + per row LE64(id)+LE64(UTF8 bytes)+bytes)",
        engine=("SHA256(sorted tracked src plus CMakeLists.txt, Makefile, extension_config.cmake, "
                "vcpkg.json blob hashes + gitlinks + DuckDB source ID)"),
    ),
    measurement_contracts=dict(
        stage_wall="monotonic_ns around subprocess lifecycle",
        rss="GNU time -v max RSS KiB*1024 (process high-water)",
        temp="peak apparent st_size in DB.tmp sampled every 100ms; may miss brief spill",
        storage="post-CHECKPOINT DB/WAL st_size and st_blocks*512; paired whole-DB delta",
        statistics=("stage n=3: median/min/max, no p95; query n=21: nearest-rank p50/p95/min/max; "
                    "ratios use raw medians; Decimal ROUND_HALF_UP"),
        order_cache=("normalize; load (transport cache possible across pairs); relation identity; "
                     "build (source cache possible); parity/EXPLAIN; query warmups/samples"),
    ),
    build_argv=BUILD_ARGV,
    process_env=dict(SANITIZED_ENV, HOME="<REPO>"),
    timed_stage_argv=[
        "/usr/bin/time", "-v", "-o", "<RSS_FILE>", "build/release/duckdb", "<DATABASE>"
    ],
    query_argv=["build/release/duckdb", "-readonly", "<DATABASE>"],
    oracle_argv=["build/release/duckdb", "-readonly", "<OUTPUT_MODE>", "<DATABASE>", "-c", "<SQL>"],
    relation_argv=["build/release/duckdb", "-readonly", "<DATABASE>"],
    load_sql=LOAD_TEMPLATE, build_sql=BUILD_SQL, count_sql=COUNT_SQL, settings=FIXED_SETTINGS,
    search_sql=SEARCH_TEMPLATE, scan_sql=SCAN_TEMPLATE, candidate_sql=CANDIDATE_TEMPLATE,
    parity_sql=PARITY_TEMPLATE, parity_columns=list(PARITY_COLUMNS),
    explain_sql=EXPLAIN_TEMPLATE, needles=list(NEEDLES), pairs=3, query_seed=QUERY_SEED,
    query_warmups_per_cell=1, query_repeats_per_cell=QUERY_REPEATS,
    query_timer_resolution_ms=1, temp_poll_ms=POLL_MS,
    stats_columns=list(STATS_COLUMNS),
    storage_columns=["db_apparent_bytes", "db_allocated_bytes", "wal_apparent_bytes", "wal_allocated_bytes"],
    load_state_columns=["rows", "min_id", "max_id", "distinct_ids", "text_bytes", "relation_sha256"],
    build_state_columns=[*STATS_COLUMNS, "registry_row"],
    registry_columns=list(REGISTRY_COLUMNS),
    query_sample_columns=["query_id", "variant", "wall_ms", "exact_count"],
    raw_units={"wall_ns": "ns", "rss_bytes": "bytes", "sampled_peak_temp_bytes": "bytes"},
)
HEX64 = re.compile(r"^[0-9a-f]{64}$")
HEX40 = re.compile(r"^[0-9a-f]{40}$")
UTC = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
RSS = re.compile(r"Maximum resident set size \(kbytes\): (\d+)")
TIMER = re.compile(r"^Run Time \(s\): real (\d+)\.(\d{3}) user ", re.MULTILINE)
INDEX_REF = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)
class EvidenceError(RuntimeError):
    pass
def fail(message):
    raise EvidenceError(message)
def file_digest(path, algorithm="sha256"):
    digest = hashlib.new(algorithm)
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()
def run(argv, *, cwd=ROOT, env=None, stdin=None, check=True, text=True):
    process = subprocess.run(
        argv,
        cwd=cwd,
        env=SANITIZED_ENV if env is None else env,
        input=stdin,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=text,
    )
    if check and process.returncode:
        error = process.stderr if text else process.stderr.decode(errors="replace")
        fail("command failed (%s):\n%s" % (" ".join(map(str, argv)), error[-5000:]))
    return process
def git(*args, **kwargs):
    return run(["git", *args], cwd=ROOT, **kwargs)
def strict_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            fail("duplicate JSON key: %s" % key)
        result[key] = value
    return result
def reject_number(token):
    fail("invalid JSON number: %s" % token)
def load_artifact(path):
    try:
        return json.loads(
            Path(path).read_text(encoding="utf-8"),
            object_pairs_hook=strict_pairs,
            parse_float=reject_number,
            parse_constant=reject_number,
        )
    except EvidenceError:
        raise
    except Exception as exc:
        fail("invalid JSON: %s" % exc)
def exact(value, names, where):
    if type(value) is not dict:
        fail("%s must be an object" % where)
    expected = set(names)
    if set(value) != expected:
        fail(
            "%s keys differ: missing=%s extra=%s"
            % (where, sorted(expected - set(value)), sorted(set(value) - expected))
        )
def integer(value, where, minimum=0):
    if type(value) is not int or value < minimum:
        fail("invalid integer at %s (minimum %d)" % (where, minimum))
    return value
def string(value, where, pattern=None):
    if type(value) is not str or not value or (pattern and not pattern.fullmatch(value)):
        fail("invalid string at %s" % where)
    return value
def sql_literal(value):
    return "'" + str(value).replace("'", "''") + "'"
def pinned(record, values, where):
    for name, expected in values.items():
        actual = record.get(name)
        if type(actual) is not type(expected) or actual != expected:
            fail("%s.%s differs from the frozen protocol" % (where, name))
def frozen(actual, expected, where):
    encode = lambda value: json.dumps(value, sort_keys=True, separators=(",", ":"))
    if encode(actual) != encode(expected):
        fail("%s differs from the frozen protocol" % where)
def engine_paths(commit=None):
    command = ["ls-files"] if commit is None else ["ls-tree", "-r", "--name-only", commit]
    return sorted(
        path
        for path in git(*command, "--", "src", *ENGINE_FILES).stdout.splitlines()
        if path.startswith("src/") or path in ENGINE_FILES
    )
def assert_engine_clean(engine_status, links, pins_returncode, pins_output):
    if engine_status:
        fail("engine inputs are dirty or untracked:\n%s" % engine_status)
    if links != PINNED_LINKS:
        fail("pinned gitlink mismatch")
    if pins_returncode:
        fail("submodule checkout differs from the pinned gitlinks:\n%s" % pins_output)
def engine_digest(commit=None):
    records = []
    for path in engine_paths(commit):
        if commit:
            data = git("show", "%s:%s" % (commit, path), text=False).stdout
            digest = hashlib.sha256(data).hexdigest()
        else:
            digest = file_digest(ROOT / path)
        records.append("blob\t%s\t%s\n" % (digest, path))
    listing = (
        git("ls-tree", commit, "--", *SUBMODULES).stdout
        if commit
        else git("ls-files", "-s", "--", *SUBMODULES).stdout
    )
    links = {}
    for line in listing.splitlines():
        match = re.match(r"160000 (?:commit )?([0-9a-f]{40})(?: 0)?\t(.+)$", line)
        if match:
            links[match.group(2)] = match.group(1)
    if set(links) != set(SUBMODULES):
        fail("invalid engine gitlinks")
    for name, sha in links.items():
        records.append("gitlink\t%s\t%s\n" % (sha, name))
    records.append("runtime-source\t%s\tduckdb-source-id\n" % DUCKDB_SOURCE)
    digest = hashlib.sha256("".join(sorted(records)).encode()).hexdigest()
    return digest, links
def working_tree_clean(links):
    """Fail unless src/**, the engine files, and both submodule trees hold exactly what HEAD records."""
    status = git(
        "status",
        "--porcelain=v1",
        "--untracked-files=all",
        "--",
        "src",
        *ENGINE_FILES,
    ).stdout.strip()
    # scripts/verify_pins.sh is the one checkout-versus-gitlink check shared
    # with the Correctness workflow.
    pins = run(["bash", str(ROOT / "scripts" / "verify_pins.sh")], check=False)
    assert_engine_clean(status, links, pins.returncode, pins.stdout + pins.stderr)
def missing_commit_policy(exists, shallow, label):
    if not exists and not shallow:
        fail("missing %s commit in full clone" % label)
def object_exists(commit):
    return git("cat-file", "-e", commit + "^{commit}", check=False).returncode == 0
def commit_relationship(engine_commit, build_commit):
    shallow = git("rev-parse", "--is-shallow-repository").stdout.strip() == "true"
    engine_exists = object_exists(engine_commit)
    build_exists = object_exists(build_commit)
    missing_commit_policy(engine_exists, shallow, "engine")
    missing_commit_policy(build_exists, shallow, "build")
    if engine_exists and build_exists:
        if git("merge-base", "--is-ancestor", engine_commit, build_commit, check=False).returncode:
            fail("engine/build ancestry mismatch")
        return True
    return False
def validate_provenance(engine):
    """What `check` enforces on every commit: the working tree's gitlinks are the
    pinned ones and the artifact's commits are ancestors of HEAD."""
    if engine_digest()[1] != PINNED_LINKS:
        fail("current gitlinks mismatch")
    commit_relationship(engine["engine_commit"], engine["build_commit"])
    current = git("rev-parse", "HEAD").stdout.strip()
    shallow = git("rev-parse", "--is-shallow-repository").stdout.strip() == "true"
    build_exists = object_exists(engine["build_commit"])
    missing_commit_policy(build_exists, shallow, "build")
    if build_exists and git(
        "merge-base", "--is-ancestor", engine["build_commit"], current, check=False
    ).returncode:
        fail("artifact build is not current/ancestor")
def source_matches(engine, current_digest, recorded_digests, tool_digest):
    """The digest comparisons behind `check --current-source`, separated from
    the git reads so both outcomes are testable on any tree."""
    if current_digest != engine["digest_sha256"]:
        fail(
            "engine digest mismatch: the working tree's src/** and engine files digest to %s, "
            "the artifact records %s for engine commit %s; the numbers describe that commit and "
            "are re-collected on release" % (current_digest, engine["digest_sha256"], engine["engine_commit"][:12])
        )
    for commit, digest in sorted(recorded_digests.items()):
        if digest != current_digest:
            fail("recorded/current engine digest mismatch at %s" % commit[:12])
    if tool_digest != engine["tool_sha256"]:
        fail(
            "tool hash mismatch: this tool digests to %s, the artifact records %s"
            % (tool_digest, engine["tool_sha256"])
        )
def validate_source(engine):
    """What `check --current-source`, `validate`, and `collect` add to the
    provenance checks: the clean working tree and this tool hash to what the
    artifact records."""
    current_digest, links = engine_digest()
    recorded = {}
    if commit_relationship(engine["engine_commit"], engine["build_commit"]):
        recorded = {
            commit: engine_digest(commit=commit)[0]
            for commit in {engine["engine_commit"], engine["build_commit"]}
        }
    source_matches(engine, current_digest, recorded, file_digest(TOOL))
    working_tree_clean(links)
def query_order():
    generator = random.Random(QUERY_SEED)
    pairs = []
    for query_id, _hex in NEEDLES:
        first = generator.randrange(2)
        pairs.extend((query_id, (first + repeat) % 2) for repeat in range(QUERY_REPEATS))
    generator.shuffle(pairs)
    order = []
    for query_id, orientation in pairs:
        variants = ("ngram_search", "scan")
        if orientation:
            variants = variants[::-1]
        order.extend((query_id, variant) for variant in variants)
    return order
def sample_stats(values, p95=False):
    if not values or any(type(x) is not int or x < 0 for x in values):
        fail("invalid sample array")
    ordered = sorted(values)
    result = {
        "n": len(ordered),
        "median": ordered[len(ordered) // 2],
        "min": ordered[0],
        "max": ordered[-1],
    }
    if p95:
        result["p95"] = ordered[(95 * len(ordered) + 99) // 100 - 1]
    return result
def ratio(numerator, denominator, places=2):
    if denominator <= 0:
        fail("ratio inconclusive at 1 ms resolution")
    quantum = decimal.Decimal(1).scaleb(-places)
    value = decimal.Decimal(numerator) / decimal.Decimal(denominator)
    return str(value.quantize(quantum, rounding=decimal.ROUND_HALF_UP))
def summaries(artifact):
    loads = artifact["runs"][0::2]
    builds = artifact["runs"][1::2]
    apparent = [build["storage"][0] - load["storage"][0] for load, build in zip(loads, builds)]
    allocated = [build["storage"][1] - load["storage"][1] for load, build in zip(loads, builds)]
    if min(apparent) <= 0 or min(allocated) < 0:
        fail("invalid paired storage delta")
    build_wall = sample_stats([item["wall_ns"] for item in builds])
    result = {
        "load_wall": sample_stats([item["wall_ns"] for item in loads]),
        "build_wall": build_wall,
        "build_rss": sample_stats([item["rss_bytes"] for item in builds]),
        "build_temp": sample_stats([item["sampled_peak_temp_bytes"] for item in builds]),
        "apparent_delta": sample_stats(apparent),
        "allocated_delta": sample_stats(allocated),
        "build_mib_s": ratio(
            artifact["corpus"]["text_bytes"] * 1_000_000_000,
            build_wall["median"] * 1024 * 1024,
        ),
        "apparent_ratio": ratio(
            sample_stats(apparent)["median"], artifact["corpus"]["text_bytes"], 3
        ),
        "queries": {},
    }
    for query_id, _hex in NEEDLES:
        query = {}
        for variant in ("ngram_search", "scan"):
            values = [
                sample[2]
                for sample in artifact["query_samples"]
                if sample[0] == query_id and sample[1] == variant
            ]
            query[variant] = sample_stats(values, p95=True)
        query["scan_over_search"] = ratio(
            query["scan"]["median"], query["ngram_search"]["median"]
        )
        result["queries"][query_id] = query
    return result
RUN_KEYS = {
    "pair",
    "stage",
    "wall_ns",
    "rss_bytes",
    "sampled_peak_temp_bytes",
    "storage",
    "state",
}
def integer_list(value, length, where):
    if type(value) is not list or len(value) != length:
        fail("%s must be an integer list of length %d" % (where, length))
    for index, item in enumerate(value):
        integer(item, "%s[%d]" % (where, index))
def validate_run(record, pair, stage, corpus, registry_columns):
    exact(record, RUN_KEYS, "run")
    if record["pair"] != pair or record["stage"] != stage:
        fail("run pairing/order differs")
    integer(record["pair"], "run.pair", 1)
    integer(record["wall_ns"], "run.wall_ns", 1)
    integer(record["rss_bytes"], "run.rss_bytes", 1)
    integer(record["sampled_peak_temp_bytes"], "run.sampled_peak_temp_bytes")
    storage_state = record["storage"]
    integer_list(storage_state, 4, "run.storage")
    if storage_state[0] < 1 or storage_state[1] < 1 or storage_state[2:] != [0, 0]:
        fail("invalid checkpoint storage/WAL")
    state = record["state"]
    if stage == "load":
        integer_list(state[:5] if type(state) is list else state, 5, "load.state")
        if type(state) is not list or len(state) != 6:
            fail("invalid load-state shape")
        string(state[5], "load.relation_sha256", HEX64)
        expected = [corpus["rows"], 0, corpus["rows"] - 1, corpus["rows"],
                    corpus["text_bytes"], corpus["canonical_sha256"]]
        if state != expected:
            fail("relation/corpus mismatch")
        return
    if type(state) is not list or len(state) != 14:
        fail("invalid build-state shape")
    if state[:2] != ["text", 3] or type(state[2]) is not bool or state[2]:
        fail("index settings mismatch")
    for index in range(1, 12):
        if index != 2:
            integer(state[index], "build.state[%d]" % index)
    expected = [corpus["rows"] - 1, corpus["rows"] - 1, 0]
    registry = state[13]
    if type(registry) is not list or len(registry) != len(registry_columns):
        fail("invalid registry evidence")
    row = dict(zip(registry_columns, registry))
    if any(type(row[name]) is not str for name in registry_columns if name not in ("format_version", "reason")):
        fail("invalid registry strings")
    integer(row["format_version"], "registry.format", 1)
    if state[3:6] != expected or state[9] != 1 or state[12] is not None:
        fail("index maintenance gate failed")
    if [row["schema_name"], row["table_name"], row["column_name"]] != ["main", "docs", "text"]:
        fail("registry identity mismatch")
    if row["database_name"] != "run-%d" % pair:
        fail("registry database mismatch")
    format3 = registry_columns == FORMAT3_REGISTRY_COLUMNS
    if row["status"] != "READY" or row["reason"] is not None or row["format_version"] != (3 if format3 else 4):
        fail("registry status/format mismatch")
    if not INDEX_REF.fullmatch(row["index_ref"]):
        fail("registry allocation mismatch")
    if format3 and (row["kind"] != "registered"
                    or row["storage_schema"] != "__ngram_idx_" + row["index_ref"].replace("-", "_")):
        fail("registry allocation mismatch")
    for index in (6, 7, 10, 11):
        if state[index] < 1:
            fail("nonpositive index stats")
def validate_artifact(artifact, *, provenance=False, source=False, binary=None):
    """`provenance` adds the gitlink and ancestry checks; `source` adds the
    working-tree digest and tool hash checks on top of them."""
    top_keys = (
        "schema benchmark_id created_utc protocol engine corpus machine settings runs query_cells query_samples"
    ).split()
    exact(artifact, top_keys, "artifact")
    fixed_top = {"schema": SCHEMA, "benchmark_id": BENCHMARK_ID}
    pinned(artifact, fixed_top, "artifact")
    protocol = artifact["protocol"]
    registry_columns = REGISTRY_COLUMNS
    if type(protocol) is dict and protocol.get("registry_columns") == list(FORMAT3_REGISTRY_COLUMNS):
        registry_columns = FORMAT3_REGISTRY_COLUMNS
    frozen(protocol, dict(PROTOCOL, registry_columns=list(registry_columns)), "protocol")
    timestamp = string(artifact["created_utc"], "created_utc", UTC)
    try:
        parsed = time.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        fail("created_utc is not a valid UTC timestamp")
    if time.strftime("%Y-%m-%dT%H:%M:%SZ", parsed) != timestamp:
        fail("created_utc is not canonical")
    engine = artifact["engine"]
    engine_keys = (
        "engine_commit build_commit digest_sha256 extension_version tool_sha256 cli_sha256"
    ).split()
    exact(engine, engine_keys, "engine")
    for name in ("engine_commit", "build_commit"):
        string(engine[name], "engine.%s" % name, HEX40)
    for name in ("digest_sha256", "tool_sha256", "cli_sha256"):
        string(engine[name], "engine.%s" % name, HEX64)
    pinned(engine, {"engine_commit": ENGINE_COMMIT}, "engine")
    extension_version = string(engine["extension_version"], "engine.extension_version")
    if not re.fullmatch(r"[0-9a-f]{7,40}", extension_version) or not engine[
        "build_commit"
    ].startswith(extension_version):
        fail("extension/build commit mismatch")
    corpus = artifact["corpus"]
    corpus_keys = "archive_sha256 raw_sha256 rows text_bytes canonical_sha256".split()
    exact(corpus, corpus_keys, "corpus")
    for name in ("archive_sha256", "raw_sha256", "canonical_sha256"):
        string(corpus[name], "corpus.%s" % name, HEX64)
    for name in ("rows", "text_bytes"):
        integer(corpus[name], "corpus.%s" % name, 1)
    machine = artifact["machine"]
    exact(machine, MACHINE_KEYS, "machine")
    for name in set(machine) - {"logical_cpus", "memory_bytes", "rotational"}:
        string(machine[name], "machine.%s" % name)
    integer(machine["logical_cpus"], "machine.logical_cpus", 24)
    integer(machine["memory_bytes"], "machine.memory_bytes", 48 * 1024**3)
    if type(machine["rotational"]) is not bool:
        fail("machine.rotational must be boolean")
    frozen(artifact["settings"], FIXED_SETTINGS, "settings")
    runs = artifact["runs"]
    if type(runs) is not list or len(runs) != 6:
        fail("expected three run pairs")
    expected_runs = [(pair, stage) for pair in range(1, 4) for stage in ("load", "build")]
    for record, (pair, stage) in zip(runs, expected_runs):
        validate_run(record, pair, stage, corpus, registry_columns)
    build_states = [record["state"][:13] for record in runs[1::2]]
    if any(state != build_states[0] for state in build_states[1:]):
        fail("build states differ")
    if build_states[0][8] != 0:
        fail("fresh build is fragmented")
    cells, cell_map = artifact["query_cells"], {}
    if type(cells) is not list or len(cells) != len(NEEDLES):
        fail("query cells differ from the frozen manifest")
    for cell, (query_id, needle_hex) in zip(cells, NEEDLES):
        cell_keys = "query_id needle_hex matches candidates parity mode explain".split()
        exact(cell, cell_keys, "query cell")
        if (cell["query_id"], cell["needle_hex"]) != (query_id, needle_hex):
            fail("query manifest was changed or reordered")
        integer(cell["matches"], "cell.matches")
        integer(cell["candidates"], "cell.candidates")
        if cell["matches"] < 1 or cell["candidates"] < cell["matches"]:
            fail("candidate superset violation")
        integer_list(cell["parity"], 2, "cell.parity")
        if cell["parity"] != [0, 0]:
            fail("exact result-set parity differs")
        if cell["mode"] not in ("index", "full-scan-fallback"):
            fail("query mode differs")
        explain = string(cell["explain"], "cell.explain")
        observed = explain_mode(explain)
        if cell["mode"] != observed:
            fail("reported query mode differs from EXPLAIN")
        cell_map[query_id] = cell
    samples = artifact["query_samples"]
    expected_order = query_order()
    if type(samples) is not list or len(samples) != len(expected_order):
        fail("query sample count differs")
    for index, (sample, expected) in enumerate(zip(samples, expected_order)):
        if type(sample) is not list or len(sample) != 4:
            fail("invalid query sample %d" % index)
        if tuple(sample[:2]) != expected:
            fail("query interleaving differs at sample %d" % index)
        integer(sample[2], "sample.wall_ms")
        integer(sample[3], "sample.count")
        if sample[3] != cell_map[sample[0]]["matches"]:
            fail("exact ngram_search/scan count parity differs")
    summaries(artifact)
    if provenance or source:
        validate_provenance(engine)
    if source:
        validate_source(engine)
    if binary:
        validate_binary(Path(binary), engine)
def cli_json(binary, database, sql):
    readonly = [] if str(database) == ":memory:" else ["-readonly"]
    process = run([str(binary), *readonly, "-json", str(database), "-c", sql])
    try:
        return json.loads(process.stdout)
    except json.JSONDecodeError as exc:
        fail("DuckDB JSON output is invalid: %s" % exc)
VERSION_SQL = "PRAGMA version;"
EXTENSION_SQL = (
    "SELECT extension_version FROM duckdb_extensions() WHERE extension_name='ngram';"
)
def runtime_identity(binary):
    version = cli_json(binary, ":memory:", VERSION_SQL)
    extension = cli_json(binary, ":memory:", EXTENSION_SQL)
    valid = (
        len(version) == len(extension) == 1
        and version[0].get("library_version") == DUCKDB_VERSION
        and version[0].get("source_id") == DUCKDB_SOURCE
        and type(extension[0].get("extension_version")) is str
    )
    if not valid or not re.fullmatch(r"[0-9a-f]{7,40}", extension[0].get("extension_version", "")):
        fail("CLI runtime identity mismatch")
    return extension[0]["extension_version"]
def smoke_runtime_matches(runtime, current):
    return bool(re.fullmatch(r"[0-9a-f]{7,40}", runtime)) and (
        current.startswith(runtime) or ENGINE_COMMIT.startswith(runtime)
    )
def validate_binary(binary, engine):
    if not binary.is_file() or file_digest(binary) != engine["cli_sha256"]:
        fail("CLI hash differs")
    if runtime_identity(binary) != engine["extension_version"]:
        fail("CLI extension version differs")
def explain_mode(explain):
    index = "Ngram Mode" in explain and "index (" in explain
    fallback = "Ngram Mode" in explain and "full scan fallback:" in explain
    if index == fallback:
        fail("unrecognized or ambiguous ngram EXPLAIN mode")
    return "index" if index else "full-scan-fallback"
def fmt(value, scale, places=3):
    return ratio(value, scale, places)
def rendered_range(stats, scale=1):
    return "%s–%s" % (fmt(stats["min"], scale), fmt(stats["max"], scale))
def latency(stats):
    return "%d / %d ms / %d–%d ms" % (
        stats["median"], stats["p95"], stats["min"], stats["max"]
    )
def render_query_table(artifact, stats):
    cells = {cell["query_id"]: cell for cell in artifact["query_cells"]}
    lines = [
        "| needle class | ngram_search mode | exact matches | candidates | "
        "ngram_search p50 / p95 / range | scan p50 / p95 / range | scan ÷ search p50 |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for query_id, _hex in NEEDLES:
        cell = cells[query_id]
        query = stats["queries"][query_id]
        search = query["ngram_search"]
        scan = query["scan"]
        values = (
            query_id,
            cell["mode"],
            format(cell["matches"], ","),
            format(cell["candidates"], ","),
            latency(search),
            latency(scan),
            query["scan_over_search"],
        )
        lines.append("| %s | %s | %s | %s | %s | %s | %s× |" % values)
    return "\n".join(lines)
def render_block(artifact, results=False):
    validate_artifact(artifact)
    stats, corpus, engine = summaries(artifact), artifact["corpus"], artifact["engine"]
    link = "artifacts/enwik9-current-v1.json" if results else ARTIFACT_REL
    heading = "## Checked bounded benchmark" if results else "### Checked bounded benchmark"
    load, build = stats["load_wall"], stats["build_wall"]
    fields = {
        "heading": heading, "link": link, "engine": engine["engine_commit"][:12],
        "build": engine["build_commit"][:12], "rows": format(corpus["rows"], ","),
        "gib": fmt(corpus["text_bytes"], 1024**3), "load": fmt(load["median"], 10**9),
        "load_range": rendered_range(load, 10**9), "index": fmt(build["median"], 10**9),
        "index_range": rendered_range(build, 10**9), "rate": stats["build_mib_s"],
        "apparent": fmt(stats["apparent_delta"]["median"], 1024**3),
        "allocated": fmt(stats["allocated_delta"]["median"], 1024**3),
        "ratio": stats["apparent_ratio"], "rss": fmt(stats["build_rss"]["median"], 1024**3),
        "temp": fmt(stats["build_temp"]["median"], 1024**3),
        "table": render_query_table(artifact, stats),
    }
    template = """{begin}
+{heading}
+
+Same-machine observations; queries are warm-cache for a **non-default case-sensitive trigram
+index** over nonempty line-per-row `enwik9`. These are not cold-cache, large-scale, or
+shipped-default claims. Raw evidence: [`{artifact}`]({link}).
+
+- Engine commit: `{engine}`; build commit: `{build}`; DuckDB {duckdb} / source {source};
+  static-extension release CLI. The numbers describe the engine commit's `src/**` and are
+  re-collected on release; later commits keep this block until the next collection.
+- Corpus: {rows} rows, {gib} GiB of UTF-8 text; three fresh load/build pairs.
+- Timed load—fresh CLI and absent DB through create, hex decode, insert, CHECKPOINT—was
+  {load} s median ({load_range} s). Timed index build—fresh CLI through create-index and
+  CHECKPOINT—was {index} s median ({index_range} s), {rate} MiB/s of source text.
+- Paired whole-database size increase: {apparent} GiB apparent, {allocated} GiB allocated
+  (median); {ratio}× source bytes. This whole-DB effect includes allocator/checkpoint effects.
+- Build-process max RSS: {rss} GiB median. Sampled peak temp apparent file bytes: {temp} GiB
+  median, polled every 100 ms; zero means none observed, not proof that no brief spill occurred.
+  Acquisition, normalization, relation/stat checks, EXPLAIN, and parity are untimed. Loads may
+  read cached transport pages; builds follow relation identity and may read cached source pages.
+
+{table}
+
+The timed campaign adds one warmup per variant after untimed parity/EXPLAIN executions, then
+twenty-one measured observations per variant using a fixed-seed interleaving on one connection.
+Timer resolution is one millisecond. Exact ngram_search and scan counts match every observation;
+candidate counts are a separately measured lossy superset.
+`check` verifies the artifact, the pinned gitlinks, commit ancestry, and this rendered block on
+any commit; `--current-source` also requires `src/**` and the tool to hash to the artifact's
+records, which holds at the engine commit and on release tags. `validate` applies the strict
+check to any artifact path:
+
+```sh
+{command} check
+{command} check --current-source
+{command} validate --artifact {artifact}
+```
+
+Optional `--binary` validation requires the exact CLI/SHA recorded for the artifact build commit;
+an arbitrary later rebuild is rejected.
+
+For a fresh independent run:
+
+```sh
+{command} collect --output-root ./release-evidence-work \\
+  --artifact ./release-evidence-rerun.json
+{command} validate --artifact ./release-evidence-rerun.json \\
+  --binary build/release/duckdb
+```
+
+`enwik9` is the first billion bytes of the March 2006 English Wikipedia dump, obtained from
+[Matt Mahoney's canonical benchmark page]({page}). It is attributable to English Wikipedia
+contributors. SHA-256 values were freshly observed; raw MD5/SHA-1 are published there. The
+corpus is not redistributed or licensed by this repository's MIT license; do not assume
+public-domain status. Review [Wikimedia reuse guidance]({reuse}) and the
+[Wikimedia Terms of Use]({terms}).
+{end}"""
    return template.format(
        begin=BEGIN, end=END, artifact=ARTIFACT_REL, command=COMMAND, duckdb=DUCKDB_VERSION,
        source=DUCKDB_SOURCE, page=SOURCE_PAGE, reuse=REUSE_URL, terms=TERMS_URL,
        **fields,
    ).replace("\n+", "\n")
def replace_block(text, block, where):
    if text.count(BEGIN) != 1 or text.count(END) != 1:
        fail("expected one marker pair: %s" % where)
    begin = text.index(BEGIN)
    end = text.index(END)
    if end < begin or BEGIN in text[begin + len(BEGIN) : end] or END in text[:begin]:
        fail("malformed markers: %s" % where)
    return text[:begin] + block + text[end + len(END) :]
def lint_rendered(readme, results):
    def outside(text):
        return text[: text.index(BEGIN)] + text[text.index(END) + len(END) :]
    old_claims = (
        "92 GiB of text", "100 GB corpus", "1 GB of text per second",
        "historical implementation measured 413", "Fetching costs ~250",
        "at 100 GB a rare needle costs",
    )
    unowned = outside(readme) + outside(results)
    for claim in old_claims:
        if claim in unowned:
            fail("legacy performance prose outside generated block")
    if outside(results).strip() != "# ngram benchmark results":
        fail("benchmarks/RESULTS.md must contain only its heading outside the generated block")
def canonical_artifact(path):
    expected = ROOT / ARTIFACT_REL
    if Path(path).resolve() != expected.resolve() or not expected.is_file():
        fail("docs require the canonical artifact")
    return expected
def render_docs(artifact_path, output_root=None, check=False, current_source=False):
    artifact = load_artifact(canonical_artifact(artifact_path))
    validate_artifact(artifact, provenance=True, source=current_source)
    readme = replace_block(README.read_text(), render_block(artifact), "README.md")
    results = replace_block(RESULTS.read_text(), render_block(artifact, True), "benchmarks/RESULTS.md")
    lint_rendered(readme, results)
    if output_root:
        target = Path(output_root)
        if target.exists():
            fail("--output-root must not already exist")
        (target / "benchmarks").mkdir(parents=True)
        (target / "README.md").write_text(readme)
        (target / "benchmarks" / "RESULTS.md").write_text(results)
        return
    for path, text in ((README, readme), (RESULTS, results)):
        if check:
            if path.read_text() != text:
                fail("generated document is stale: %s" % path)
        else:
            temporary = path.with_name(path.name + ".tmp")
            temporary.write_text(text)
            os.replace(temporary, path)
class TempSampler(threading.Thread):
    def __init__(self, path):
        super().__init__(daemon=True)
        self.path = Path(path)
        self.peak = 0
        self.done = threading.Event()
    def size(self):
        total = 0
        if self.path.exists():
            for root, _directories, files in os.walk(self.path):
                for name in files:
                    try:
                        total += os.lstat(Path(root) / name).st_size
                    except FileNotFoundError:
                        pass
        return total
    def run(self):
        while not self.done.wait(POLL_MS / 1000):
            self.peak = max(self.peak, self.size())
        self.peak = max(self.peak, self.size())
    def stop(self):
        self.done.set()
        self.join(5)
def metered_process(binary, database, script):
    with tempfile.TemporaryDirectory(prefix="ngram-meter-") as directory:
        meter = Path(directory) / "rss"
        sampler = TempSampler(str(database) + ".tmp")
        sampler.start()
        argv = ["/usr/bin/time", "-v", "-o", str(meter), str(binary), str(database)]
        start = time.monotonic_ns()
        process = subprocess.run(
            argv, env=SANITIZED_ENV, input=script, text=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        wall_ns = time.monotonic_ns() - start
        sampler.stop()
        report = meter.read_text()
    match = RSS.search(report)
    if process.returncode or not match:
        fail("metered DuckDB process failed:\n%s" % process.stderr[-4000:])
    return wall_ns, int(match.group(1)) * 1024, sampler.peak
def storage(database):
    def one(path):
        path = Path(path)
        if not path.exists():
            return 0, 0
        info = path.stat()
        return info.st_size, info.st_blocks * 512
    result = [*one(database), *one(str(database) + ".wal")]
    if Path(str(database) + ".tmp").exists():
        fail("timed checkpoint left a temp directory")
    return result
def normalize(raw, output, verify=True):
    raw_md5 = hashlib.md5()
    raw_sha1 = hashlib.sha1()
    raw_sha256 = hashlib.sha256()
    canonical = hashlib.sha256(NORM_DOMAIN)
    rows = 0
    text_bytes = 0
    with open(raw, "rb") as source, open(output, "xb") as destination:
        for line in source:
            for digest in (raw_md5, raw_sha1, raw_sha256):
                digest.update(line)
            record = line[:-1] if line.endswith(b"\n") else line
            if not record:
                continue
            record.decode("utf-8", "strict")
            canonical.update(struct.pack("<QQ", rows, len(record)))
            canonical.update(record)
            encoded = str(rows).encode() + b"\t" + record.hex().encode() + b"\n"
            destination.write(encoded)
            rows += 1
            text_bytes += len(record)
    if verify and (Path(raw).stat().st_size, raw_md5.hexdigest(), raw_sha1.hexdigest()) != (
        RAW_BYTES, RAW_MD5, RAW_SHA1
    ):
        fail("raw enwik9 published size/MD5/SHA-1 differ")
    return {
        "raw_sha256": raw_sha256.hexdigest(),
        "rows": rows,
        "text_bytes": text_bytes,
        "canonical_sha256": canonical.hexdigest(),
    }
def validate_archive_member(entries):
    if (
        len(entries) != 1
        or entries[0].filename != "enwik9"
        or entries[0].is_dir()
        or entries[0].file_size != RAW_BYTES
        or PurePosixPath(entries[0].filename).parts != ("enwik9",)
    ):
        fail("unsafe/unexpected archive member")
    return entries[0]
def acquire(output_root):
    archive = output_root / "enwik9.zip"
    request = urllib.request.Request(
        ARCHIVE_URL, headers={"User-Agent": "duckdb-ngram-evidence/1"}
    )
    with urllib.request.urlopen(request, timeout=3600) as source, open(archive, "xb") as destination:
        shutil.copyfileobj(source, destination, 1 << 20)
    if archive.stat().st_size != ARCHIVE_BYTES:
        fail("archive size mismatch")
    raw = output_root / "enwik9"
    with zipfile.ZipFile(archive) as source:
        member = validate_archive_member(source.infolist())
        with source.open(member) as payload, open(raw, "xb") as destination:
            shutil.copyfileobj(payload, destination, 1 << 20)
    normalized = output_root / "enwik9.lines.hex.tsv"
    return archive, normalized, normalize(raw, normalized)
def relation_digest(binary, database, expected_rows):
    script = (
        ".headers off\n.mode list\n.separator |\n" + SQL_SETTINGS +
        "SELECT id, hex(encode(text)) FROM docs ORDER BY id;\n"
    )
    count, digest, process = 0, hashlib.sha256(NORM_DOMAIN), None
    with tempfile.TemporaryDirectory(prefix="ngram-relation-") as directory:
        error_path = Path(directory) / "stderr"
        try:
            with open(error_path, "wb") as errors:
                process = subprocess.Popen(
                    [str(binary), "-readonly", str(database)], env=SANITIZED_ENV,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE, stderr=errors,
                )
                process.stdin.write(script.encode())
                process.stdin.close()
                grammar = re.compile(rb"([0-9]+)\|([0-9A-F]+)\n$")
                for line in process.stdout:
                    match = grammar.fullmatch(line)
                    if not match or int(match.group(1)) != count:
                        fail("invalid relation row %d" % count)
                    payload = bytes.fromhex(match.group(2).decode())
                    digest.update(struct.pack("<QQ", count, len(payload)))
                    digest.update(payload)
                    count += 1
                process.stdout.close()
                returncode = process.wait()
        except Exception:
            if process and process.poll() is None:
                process.kill()
                process.wait()
            raise
        if returncode or count != expected_rows:
            error = error_path.read_text(errors="replace")[-1000:]
            fail("relation export failed at %d rows: %s" % (count, error))
    return digest.hexdigest()
def index_state(binary, database):
    stats = cli_json(binary, database, SQL_SETTINGS + "PRAGMA ngram_index_stats('docs');")
    registry = cli_json(binary, database, SQL_SETTINGS + "PRAGMA ngram_indexes;")
    if len(stats) != 1 or tuple(stats[0]) != STATS_COLUMNS:
        fail("stats column contract mismatch")
    if len(registry) != 1 or tuple(registry[0]) != REGISTRY_COLUMNS:
        fail("registry column contract mismatch")
    observed = registry[0]
    if (
        not INDEX_REF.fullmatch(observed["index_ref"])
        or observed["format_version"] != 4
        or observed["status"] != "READY"
        or observed["reason"] is not None
    ):
        fail("registry contract mismatch")
    row = stats[0]
    return [
        row["column_name"],
        row["gram_size"],
        row["case_insensitive"],
        row["hwm_rowid"],
        row["table_max_rowid"],
        row["remaining_tail"],
        row["distinct_grams"],
        row["segments"],
        row["fragmented_keys"],
        row["generations"],
        int(row["posting_entries"]),
        int(row["postings_bytes"]),
        row["stale_reason"],
        [observed[name] for name in REGISTRY_COLUMNS],
    ]
def clean_build():
    build_root = ROOT / "build"
    target = build_root / "release"
    if build_root.is_symlink() or target.is_symlink():
        fail("refusing symlinked build cleanup")
    resolved_root, resolved_build, resolved_target = ROOT.resolve(), build_root.resolve(), target.resolve()
    if resolved_build.parent != resolved_root or resolved_target.parent != resolved_build:
        fail("refusing unsafe build-directory cleanup")
    if resolved_target.exists():
        shutil.rmtree(resolved_target)
    process = subprocess.run(BUILD_ARGV, cwd=ROOT, env=SANITIZED_ENV)
    if process.returncode:
        fail("clean release build failed")
    binary = resolved_target / "duckdb"
    if not binary.is_file():
        fail("clean release build did not produce the CLI")
    return binary
def machine(output_root):
    def version(argv):
        output = run(argv).stdout.strip().splitlines()
        if not output:
            fail("version command returned no output: %s" % argv[0])
        return output[0]
    cpuinfo = Path("/proc/cpuinfo").read_text(errors="replace")
    meminfo = Path("/proc/meminfo").read_text()
    cpu = re.search(r"^model name\s*:\s*(.+)$", cpuinfo, re.MULTILINE)
    memory = re.search(r"^MemTotal:\s*(\d+) kB$", meminfo, re.MULTILINE)
    governor_paths = Path("/sys/devices/system/cpu").glob(
        "cpu[0-9]*/cpufreq/scaling_governor"
    )
    governors = {path.read_text().strip() for path in governor_paths}
    findmnt = ["findmnt", "-J", "-o", "SOURCE,FSTYPE,OPTIONS,TARGET", "--target", str(output_root)]
    mounts = json.loads(run(findmnt).stdout)["filesystems"]
    if len(mounts) != 1:
        fail("scratch mount is ambiguous")
    mount = mounts[0]
    source = mount["source"].split("[", 1)[0]
    options = set(mount["options"].split(","))
    block = Path("/sys/class/block") / Path(source).name
    if "rw" not in options or not source.startswith("/dev/") or not block.exists():
        fail("scratch is not writable block storage")
    device = block.resolve().parent if (block / "partition").exists() else block.resolve()
    model_path = device / "device" / "model"
    rotational_path = device / "queue" / "rotational"
    cache = {}
    for line in (ROOT / "build/release/CMakeCache.txt").read_text(errors="replace").splitlines():
        if ":" in line and "=" in line:
            cache[line.split(":", 1)[0]] = line.split("=", 1)[1]
    c_compiler, cxx_compiler = cache.get("CMAKE_C_COMPILER"), cache.get("CMAKE_CXX_COMPILER")
    c_launcher = cache.get("CMAKE_C_COMPILER_LAUNCHER")
    cxx_launcher = cache.get("CMAKE_CXX_COMPILER_LAUNCHER")
    linker = cache.get("CMAKE_LINKER")
    flags = lambda language: " ".join(filter(None, (
        cache.get("CMAKE_%s_FLAGS" % language), cache.get("CMAKE_%s_FLAGS_RELEASE" % language)
    )))
    c_flags, cxx_flags = flags("C"), flags("CXX")
    rotation = rotational_path.read_text().strip() if rotational_path.exists() else ""
    if not all((
        cpu, memory, governors, c_compiler, cxx_compiler, c_launcher, cxx_launcher, linker,
        c_flags, cxx_flags,
        model_path.exists(),
    )) or c_launcher != cxx_launcher:
        fail("incomplete machine/toolchain provenance")
    if rotation not in ("0", "1"):
        fail("invalid rotational-device value")
    return dict(
        os=platform.system(), kernel=platform.release(), architecture=platform.machine(),
        cpu_model=cpu.group(1), logical_cpus=os.cpu_count() or 0,
        memory_bytes=int(memory.group(1)) * 1024, governors=",".join(sorted(governors)),
        filesystem="%s %s %s" % (mount["source"], mount["fstype"], mount["options"]),
        mount=mount["target"], device_model=model_path.read_text().strip(),
        rotational=rotation == "1",
        c_compiler=version([c_compiler, "--version"]), c_flags=c_flags,
        cxx_compiler=version([cxx_compiler, "--version"]), cxx_flags=cxx_flags,
        compiler_launcher=version([cxx_launcher, "--version"]),
        linker=version([linker, "--version"]),
        make=version(["make", "--version"]), cmake=version(["cmake", "--version"]),
        ninja=version(["ninja", "--version"]), python=platform.python_version(),
        gnu_time=version(["/usr/bin/time", "--version"]),
    )
def effective_settings(binary):
    names = (
        ("threads", "threads"), ("memory_limit", "memory_limit"),
        ("preserve_insertion_order", "preserve_insertion_order"),
        ("ngram_max_grams_per_query", "max_grams"),
        ("ngram_max_candidate_fraction", "candidate_fraction"),
        ("ngram_max_probe_rowids", "probe_rowids"),
        ("ngram_build_partitions", "build_partitions"),
        ("ngram_auto_accelerate", "auto_accelerate"),
    )
    columns = ["current_setting('%s') AS %s" % item for item in names]
    columns[4] = columns[4].replace(") AS", ")::VARCHAR AS")
    sql = SQL_SETTINGS + "SELECT " + ",".join(columns) + ";"
    rows = cli_json(binary, ":memory:", sql)
    if len(rows) != 1:
        fail("invalid settings result")
    return dict(rows[0], gram_size=3, case_insensitive=False)
def timed_stage(binary, database, sql, pair, stage):
    wall_ns, rss_bytes, temp_bytes = metered_process(binary, database, sql)
    snapshot = storage(database)
    record = dict(
        pair=pair, stage=stage, wall_ns=wall_ns, rss_bytes=rss_bytes,
        sampled_peak_temp_bytes=temp_bytes, storage=snapshot, state=[],
    )
    return record, snapshot
def scalar(binary, database, sql):
    argv = [str(binary), "-readonly", "-noheader", "-list", str(database), "-c", SQL_SETTINGS + sql]
    output = run(argv).stdout.strip()
    if not re.fullmatch(r"\d+", output):
        fail("expected a nonnegative integer result, got %r" % output[:200])
    return int(output)
def exact_parity(binary, database, needle_hex):
    sql = PARITY_TEMPLATE.replace("<HEX>", sql_literal(needle_hex))
    rows = cli_json(binary, database, SQL_SETTINGS + sql)
    if len(rows) != 1 or tuple(rows[0]) != ("index_minus_scan", "scan_minus_index"):
        fail("invalid parity result contract")
    differences = [rows[0]["index_minus_scan"], rows[0]["scan_minus_index"]]
    integer_list(differences, 2, "parity result")
    if differences != [0, 0]:
        fail("exact result-set parity failed")
    return differences
def make_query_script():
    script = ".headers off\n.mode list\n.timer off\n" + SQL_SETTINGS
    for _query_id, needle_hex in NEEDLES:
        script += SEARCH_TEMPLATE.replace("<HEX>", sql_literal(needle_hex)) + "\n"
        script += SCAN_TEMPLATE.replace("<HEX>", sql_literal(needle_hex)) + "\n"
    needles = dict(NEEDLES)
    for index, (query_id, variant) in enumerate(query_order()):
        needle = sql_literal(needles[query_id])
        source = (
            "ngram_search('docs', decode(unhex(%s)), col := 'text')" % needle
            if variant == "ngram_search"
            else "docs WHERE contains(text, decode(unhex(%s)))" % needle
        )
        script += (
            ".timer on\nSELECT 'Q|%d|%s|%s|' || count(*) FROM %s;\n.timer off\n"
            % (index, query_id, variant, source)
        )
    return script + "SELECT 'DONE';\n"
def query_campaign(binary, database):
    process = run([str(binary), "-readonly", str(database)], stdin=make_query_script())
    tags = re.findall(
        r"^Q\|(\d+)\|([a-z]+)\|(ngram_search|scan)\|(\d+)$",
        process.stdout,
        re.MULTILINE,
    )
    timers = TIMER.findall(process.stdout)
    expected = query_order()
    if len(tags) != len(expected) or len(timers) != len(expected) or "DONE" not in process.stdout.splitlines():
        fail("invalid query campaign output")
    samples = []
    for index, (tag, timer) in enumerate(zip(tags, timers)):
        if int(tag[0]) != index or tuple(tag[1:3]) != expected[index]:
            fail("persistent query campaign tag order differs")
        wall_ms = int(timer[0]) * 1000 + int(timer[1])
        samples.append([tag[1], tag[2], wall_ms, int(tag[3])])
    return samples
def collect_pair(binary, normalized, output_root, pair, corpus):
    database = output_root / ("run-%d.db" % pair)
    if any(Path(str(database) + suffix).exists() for suffix in ("", ".wal", ".tmp")):
        fail("load database already exists")
    load_sql = LOAD_TEMPLATE.replace("<INPUT>", sql_literal(normalized))
    load, snapshot = timed_stage(binary, database, load_sql, pair, "load")
    count_argv = [
        str(binary), "-readonly", "-noheader", "-list", "-separator", "|",
        str(database), "-c", SQL_SETTINGS + COUNT_SQL,
    ]
    values = run(count_argv).stdout.strip().split("|")
    if len(values) != 5 or any(not re.fullmatch(r"\d+", value) for value in values):
        fail("invalid relation counts")
    load["state"] = [*map(int, values), relation_digest(binary, database, corpus["rows"])]
    if storage(database) != snapshot:
        fail("load gate changed storage")
    build, snapshot = timed_stage(binary, database, BUILD_SQL, pair, "build")
    build["state"] = index_state(binary, database)
    if storage(database) != snapshot:
        fail("index gate changed storage")
    return database, [load, build]
def query_cells(binary, database):
    cells = []
    for query_id, needle_hex in NEEDLES:
        sql = lambda template: template.replace("<HEX>", sql_literal(needle_hex))
        matches = scalar(binary, database, sql(SEARCH_TEMPLATE))
        candidates = scalar(binary, database, sql(CANDIDATE_TEMPLATE))
        parity = exact_parity(binary, database, needle_hex)
        if candidates < matches:
            fail("candidate superset failed: %s" % query_id)
        explain = run(
            [str(binary), "-readonly", "-box", str(database), "-c", SQL_SETTINGS + sql(EXPLAIN_TEMPLATE)]
        ).stdout
        mode = explain_mode(explain)
        cells.append(dict(
            query_id=query_id, needle_hex=needle_hex, matches=matches, candidates=candidates,
            parity=parity, mode=mode, explain=explain,
        ))
    return cells
def make_artifact(created, engine, corpus, machine_info, settings, runs, cells, samples):
    return dict(
        schema=SCHEMA, benchmark_id=BENCHMARK_ID, created_utc=created, protocol=PROTOCOL,
        engine=engine, corpus=corpus, machine=machine_info, settings=settings, runs=runs,
        query_cells=cells, query_samples=samples,
    )
def collect(args):
    output_root = Path(args.output_root).resolve()
    artifact_path = Path(args.artifact).resolve()
    if output_root.exists() or artifact_path.exists():
        fail("output root/artifact already exists")
    output_root.parent.mkdir(parents=True, exist_ok=True)
    output_root.mkdir(mode=0o700)
    digest, links = engine_digest()
    working_tree_clean(links)
    build_commit = git("rev-parse", "HEAD").stdout.strip()
    if commit_relationship(ENGINE_COMMIT, build_commit):
        if engine_digest(commit=ENGINE_COMMIT)[0] != digest:
            fail("build descendant changed engine inputs")
    tool_hash = file_digest(TOOL)
    binary = clean_build()
    cli_hash = file_digest(binary)
    extension_version = runtime_identity(binary)
    if not build_commit.startswith(extension_version):
        fail("extension/build commit mismatch")
    machine_info = machine(output_root)
    settings = effective_settings(binary)
    archive, normalized, normalized_info = acquire(output_root)
    corpus = dict(archive_sha256=file_digest(archive), **normalized_info)
    runs, query_database = [], None
    for pair in range(1, 4):
        query_database, records = collect_pair(binary, normalized, output_root, pair, corpus)
        runs.extend(records)
    cells = query_cells(binary, query_database)
    samples = query_campaign(binary, query_database)
    engine = dict(
        engine_commit=ENGINE_COMMIT, build_commit=build_commit, digest_sha256=digest,
        extension_version=extension_version, tool_sha256=tool_hash, cli_sha256=cli_hash,
    )
    artifact = make_artifact(
        time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), engine, corpus,
        machine_info, settings, runs, cells, samples,
    )
    if file_digest(TOOL) != tool_hash or file_digest(binary) != cli_hash:
        fail("tool or CLI changed during collection")
    validate_artifact(artifact, provenance=True, source=True, binary=binary)
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = artifact_path.with_name(artifact_path.name + ".tmp")
    temporary.write_text(json.dumps(artifact, indent=2, sort_keys=True) + "\n")
    os.replace(temporary, artifact_path)
    print("wrote", artifact_path)
def fixture():
    runs = []
    for pair in range(1, 4):
        common = dict(
            pair=pair, wall_ns=pair * 1_000_000_000, rss_bytes=pair * 1000,
            sampled_peak_temp_bytes=0,
        )
        runs.append(
            dict(common, stage="load", storage=[1000, 4096, 0, 0],
                 state=[10, 0, 9, 10, 1000, "3" * 64])
        )
        ref = "11111111-1111-4111-8111-111111111111"
        registry = ["run-%d" % pair, ref, "main", "docs", "text", 4, "READY", None]
        index_state_fixture = ["text", 3, False, 9, 9, 0, 8, 20, 0, 1, 100, 500, None, registry]
        runs.append(
            dict(common, stage="build", storage=[2000 + pair, 8192, 0, 0], state=index_state_fixture)
        )
    explain = "Ngram Mode\nindex (fixture)\n"
    cells = [dict(
        query_id=query_id, needle_hex=needle_hex, matches=1, candidates=2, parity=[0, 0],
        mode="index", explain=explain,
    ) for query_id, needle_hex in NEEDLES]
    samples = [
        [query_id, variant, 10 + index % 3, 1]
        for index, (query_id, variant) in enumerate(query_order())
    ]
    engine = {
            "engine_commit": ENGINE_COMMIT, "build_commit": ENGINE_COMMIT,
            "digest_sha256": "5" * 64, "extension_version": ENGINE_COMMIT[:7],
            "tool_sha256": "6" * 64, "cli_sha256": "7" * 64,
        }
    corpus = {
            "archive_sha256": "1" * 64, "raw_sha256": "2" * 64,
            "rows": 10, "text_bytes": 1000, "canonical_sha256": "3" * 64,
        }
    machine_info = {name: "fixture" for name in MACHINE_KEYS}
    machine_info.update(logical_cpus=24, memory_bytes=48 * 1024**3, rotational=False)
    return make_artifact(
        "2026-08-12T00:00:00Z", engine, corpus, machine_info,
        dict(FIXED_SETTINGS), runs, cells, samples,
    )
def expect_failure(label, function):
    try:
        function()
    except (EvidenceError, UnicodeDecodeError, zipfile.BadZipFile):
        return
    fail("negative test did not fail: %s" % label)
def cli_smoke(binary):
    if not binary.is_file():
        fail("real CLI smoke requires %s" % binary)
    runtime = runtime_identity(binary)
    current = git("rev-parse", "HEAD").stdout.strip()
    if not smoke_runtime_matches(runtime, current):
        fail("smoke CLI is unrelated to current/pinned commit")
    with tempfile.TemporaryDirectory(prefix="ngram evidence ' smoke ") as directory:
        root = Path(directory)
        raw, normalized, database = root / "raw.txt", root / "input ' \\ spaced.tsv", root / "tiny.db"
        raw.write_bytes(b"alpha\n\nStra\xc3\x9fe\r\nomega\n")
        info = normalize(raw, normalized, verify=False)
        load_sql = LOAD_TEMPLATE.replace("<INPUT>", sql_literal(normalized))
        run([str(binary), str(database)], stdin=load_sql)
        if relation_digest(binary, database, info["rows"]) != info["canonical_sha256"]:
            fail("real CLI relation digest smoke differs")
        run([str(binary), str(database)], stdin=BUILD_SQL)
        state = index_state(binary, database)
        if state[:3] != ["text", 3, False] or state[13][6] != "READY":
            fail("real CLI index metadata smoke differs")
        exact_parity(binary, database, b"alpha".hex())
        samples = query_campaign(binary, database)
        if len(samples) != len(query_order()):
            fail("real CLI persistent timer/tag smoke differs")
def tests(binary):
    artifact = fixture()
    validate_artifact(artifact)
    missing = object()
    def strict_load(text):
        return json.loads(
            text, object_pairs_hook=strict_pairs, parse_float=reject_number,
            parse_constant=reject_number,
        )
    expect_failure("duplicate key", lambda: strict_load('{"x":1,"x":2}'))
    for token in ("NaN", "Infinity", "-Infinity", "1.5"):
        expect_failure(token, lambda token=token: strict_load('{"x":%s}' % token))
    mutations = (
        ("missing", ("created_utc",), missing),
        ("extra", ("unexpected",), 1),
        ("type", ("runs", 0, "wall_ns"), True),
        ("negative", ("runs", 0, "wall_ns"), -1),
        ("commit", ("engine", "engine_commit"), "0" * 40),
        ("protocol", ("protocol", "pairs"), 2),
        ("unit", ("protocol", "raw_units", "wall_ns"), "ms"),
        ("settings", ("settings", "probe_rowids"), 1),
        ("stage", ("runs", 1, "stage"), "load"),
        ("needle", ("query_cells", 0, "needle_hex"), "00"),
        ("order", ("query_samples", 0, 1), "invalid"),
        ("empty", ("query_samples",), []),
        ("parity", ("query_samples", 0, 3), 2),
        ("candidate", ("query_cells", 0, "candidates"), 0),
        ("result", ("query_cells", 0, "parity", 0), 1),
        ("metadata", ("runs", 1, "state", 5), 1),
        ("registry", ("runs", 1, "state", 13, 2), 7),
    )
    for label, path, replacement in mutations:
        changed = json.loads(json.dumps(artifact))
        target = changed
        for part in path[:-1]:
            target = target[part]
        if replacement is missing:
            del target[path[-1]]
        else:
            target[path[-1]] = replacement
        expect_failure(label, lambda changed=changed: validate_artifact(changed))
    malformed_plan = json.loads(json.dumps(artifact))
    malformed_plan["query_cells"][0].update(mode="full-scan-fallback", explain="arbitrary plan")
    expect_failure("malformed EXPLAIN", lambda: validate_artifact(malformed_plan))
    mixed_build = json.loads(json.dumps(artifact))
    mixed_build["runs"][3]["state"][6] += 1
    expect_failure("mixed build stats", lambda: validate_artifact(mixed_build))
    fragmented = json.loads(json.dumps(artifact))
    for run_record in fragmented["runs"][1::2]: run_record["state"][8] = 1
    expect_failure("fragmented fresh build", lambda: validate_artifact(fragmented))
    expect_failure("noncanonical render", lambda: canonical_artifact(TOOL))
    expect_failure("full missing commit", lambda: missing_commit_policy(False, False, "engine"))
    missing_commit_policy(False, True, "engine")
    links = dict(PINNED_LINKS)
    expect_failure("untracked engine", lambda: assert_engine_clean("?? src/x.cpp", links, 0, ""))
    expect_failure("dirty submodule", lambda: assert_engine_clean("", links, 1, "verify_pins: duckdb differs"))
    assert_engine_clean("", links, 0, "")
    # The fixture records digests no tree produces: `check` (provenance only)
    # accepts it on this tree, `check --current-source` rejects it.
    validate_artifact(artifact, provenance=True)
    expect_failure("stale source digest", lambda: validate_artifact(artifact, source=True))
    current_digest = engine_digest()[0]
    tool_digest = file_digest(TOOL)
    matching = dict(artifact["engine"], digest_sha256=current_digest, tool_sha256=tool_digest)
    source_matches(matching, current_digest, {ENGINE_COMMIT: current_digest}, tool_digest)
    expect_failure("changed source", lambda: source_matches(matching, "0" * 64, {}, tool_digest))
    expect_failure(
        "changed recorded source",
        lambda: source_matches(matching, current_digest, {ENGINE_COMMIT: "0" * 64}, tool_digest),
    )
    expect_failure("changed tool", lambda: source_matches(matching, current_digest, {}, "0" * 64))
    if sql_literal("a ' quote \\ path") != "'a '' quote \\ path'":
        fail("DuckDB SQL string literal escaping differs")
    if any(smoke_runtime_matches(value, ENGINE_COMMIT) for value in ("", "b6", "0000000")):
        fail("unrelated smoke runtime was accepted")
    if sample_stats(list(range(21)), p95=True)["p95"] != 19 or ratio(1, 8, 2) != "0.13":
        fail("statistics convention differs")
    with tempfile.TemporaryDirectory(prefix="ngram-normalize-") as directory:
        root = Path(directory)
        raw = root / "raw"
        output = root / "out"
        raw.write_bytes(b"a\n\nStra\xc3\x9fe\r\nz\n\n")
        info = normalize(raw, output, verify=False)
        payload = bytes.fromhex(output.read_bytes().splitlines()[1].split(b"\t")[1].decode())
        if info["rows"] != 3 or payload != b"Stra\xc3\x9fe\r":
            fail("normalization golden differs")
        output.unlink()
        expect_failure("raw hash", lambda: normalize(raw, output, verify=True))
        output.unlink()
        raw.write_bytes(b"bad\xff\n")
        expect_failure("invalid UTF-8", lambda: normalize(raw, output, verify=False))
        fake = zipfile.ZipInfo("../enwik9")
        fake.file_size = RAW_BYTES
        expect_failure("unsafe archive member", lambda: validate_archive_member([fake]))
    block_hash = hashlib.sha256(render_block(artifact).encode()).hexdigest()
    if block_hash != "0ea7db47c55173105ae8a90d77e73de9e7dba7ae8c9d125e082baafd97acd384":
        fail("full rendered Markdown golden differs")
    expect_failure("missing marker", lambda: replace_block("plain", BEGIN + END, "fixture"))
    expect_failure("duplicate marker", lambda: replace_block(BEGIN + BEGIN + END, BEGIN + END, "x"))
    cli_smoke(Path(binary))
    print("release_evidence tests: PASS")
def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    children = {name: commands.add_parser(name) for name in (
        "collect", "validate", "render", "generate", "check", "test"
    )}
    children["collect"].add_argument("--output-root", required=True)
    children["collect"].add_argument("--artifact", required=True)
    children["validate"].add_argument("--artifact", required=True)
    children["validate"].add_argument("--binary")
    children["render"].add_argument("--artifact", required=True)
    children["render"].add_argument("--output-root", required=True)
    for name in ("generate", "check"):
        children[name].add_argument("--artifact", default=str(ROOT / ARTIFACT_REL))
    for name in ("render", "generate", "check"):
        children[name].add_argument(
            "--current-source", action="store_true",
            help="also require the clean working tree's src/** and this tool to hash to the artifact's records",
        )
    children["test"].add_argument("--binary", default=str(ROOT / "build/release/duckdb"))
    args = parser.parse_args()
    try:
        if args.command == "collect":
            collect(args)
        elif args.command == "validate":
            artifact = load_artifact(args.artifact)
            validate_artifact(artifact, provenance=True, source=True, binary=args.binary)
            suffix = ", current CLI" if args.binary else ""
            print("release_evidence validation: PASS (current source/tool%s)" % suffix)
        elif args.command == "render":
            render_docs(args.artifact, args.output_root, current_source=args.current_source)
        elif args.command in ("generate", "check"):
            render_docs(args.artifact, check=args.command == "check", current_source=args.current_source)
            if args.command == "check":
                scope = "current source/tool" if args.current_source else "pins, ancestry, rendered docs"
                print("release_evidence check: PASS (artifact, %s)" % scope)
        else:
            tests(args.binary)
    except EvidenceError as exc:
        print("release_evidence:", exc, file=sys.stderr)
        return 1
    return 0
if __name__ == "__main__":
    raise SystemExit(main())
