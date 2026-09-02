"""Shared plumbing for the ngram benchmark scripts.

Every benchmark drives the repo's own duckdb CLI (build/release/duckdb, which
links the extension statically) as a subprocess, so a run measures exactly the
binary the test suite exercises. Each invocation is metered: wall time, peak
RSS (via /usr/bin/time -v) and the peak size of DuckDB's spill directory
(sampled by a background thread, since the directory is deleted on exit).
"""

import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DUCKDB = os.environ.get("NGRAM_DUCKDB", os.path.join(REPO, "build", "release", "duckdb"))
BENCH_DIR = os.environ.get("NGRAM_BENCH_DIR", os.path.expanduser("~/duckdb-ngram-bench"))


def bench_path(*parts):
    os.makedirs(BENCH_DIR, exist_ok=True)
    return os.path.join(BENCH_DIR, *parts)


def dir_size(path):
    total = 0
    try:
        for root, _dirs, files in os.walk(path):
            for name in files:
                try:
                    total += os.lstat(os.path.join(root, name)).st_size
                except OSError:
                    pass
    except OSError:
        pass
    return total


class SpillSampler(threading.Thread):
    """Polls DuckDB's temp directory so the peak survives its deletion."""

    def __init__(self, temp_dir, interval=1.0):
        super().__init__(daemon=True)
        self.temp_dir = temp_dir
        self.interval = interval
        self.peak = 0
        self._stop = threading.Event()

    def run(self):
        while not self._stop.is_set():
            self.peak = max(self.peak, dir_size(self.temp_dir))
            self._stop.wait(self.interval)
        self.peak = max(self.peak, dir_size(self.temp_dir))

    def stop(self):
        self._stop.set()
        self.join(timeout=5)


class Result:
    def __init__(self, stdout, stderr, seconds, peak_rss_bytes, peak_spill_bytes, returncode):
        self.stdout = stdout
        self.stderr = stderr
        self.seconds = seconds
        self.peak_rss_bytes = peak_rss_bytes
        self.peak_spill_bytes = peak_spill_bytes
        self.returncode = returncode

    @property
    def rows(self):
        return [line.split("\x1f") for line in self.stdout.splitlines() if line]

    def scalar(self):
        rows = self.rows
        if not rows or not rows[0]:
            raise RuntimeError("expected one value, got %r" % self.stdout[:400])
        return rows[0][0]


_TIME_RSS = re.compile(r"Maximum resident set size \(kbytes\): (\d+)")


def run_sql(db_path, sql, timeout=None, settings=None, check=True, meter=True):
    """Run `sql` against `db_path` in a fresh duckdb process.

    Results come back as unit-separator-delimited rows without headers, which
    no corpus text can collide with.
    """
    prelude = ".mode list\n.separator \x1f\n.headers off\n"
    for name, value in (settings or {}).items():
        prelude += "SET %s=%s;\n" % (name, value)
    script = prelude + sql

    # /usr/bin/time writes its report to a file, not stderr: a failing query's
    # own error message must stay readable
    argv = []
    rss_file = None
    if meter and os.path.exists("/usr/bin/time"):
        rss_file = tempfile.NamedTemporaryFile(prefix="ngram-rss-", suffix=".txt", delete=False)
        rss_file.close()
        argv += ["/usr/bin/time", "-v", "-o", rss_file.name]
    argv += [DUCKDB, db_path]

    sampler = None
    if db_path != ":memory:":
        sampler = SpillSampler(db_path + ".tmp")
        sampler.start()
    start = time.time()
    proc = subprocess.run(argv, input=script, capture_output=True, text=True, timeout=timeout)
    seconds = time.time() - start
    peak_spill = 0
    if sampler:
        sampler.stop()
        peak_spill = sampler.peak
    peak_rss = 0
    if rss_file:
        try:
            with open(rss_file.name) as handle:
                match = _TIME_RSS.search(handle.read())
            if match:
                peak_rss = int(match.group(1)) * 1024
        finally:
            os.unlink(rss_file.name)
    result = Result(proc.stdout, proc.stderr, seconds, peak_rss, peak_spill, proc.returncode)
    if check and proc.returncode != 0:
        raise RuntimeError("duckdb failed (%d):\n%s" % (proc.returncode, proc.stderr[-6000:]))
    return result


_NGRAM_INDEX_REF = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")


def ngram_index_ref(db_path, table, column, settings=None):
    """Resolve one READY primary-catalog index id through the public observer."""
    catalog = run_sql(db_path, "SELECT current_database();", settings=settings, meter=False).scalar()
    rows = [
        row for row in run_sql(db_path, "PRAGMA ngram_indexes;", settings=settings, meter=False).rows
        if len(row) == 8 and row[0] == catalog and row[2] == "main"
        and row[3] == table and row[4] == column and row[5] == "4" and row[6] == "READY"
    ]
    if len(rows) != 1:
        raise RuntimeError("expected one READY format-4 ngram index for main.%s.%s" % (table, column))
    index_ref = rows[0][1]
    if not _NGRAM_INDEX_REF.fullmatch(index_ref):
        raise RuntimeError("public ngram index id is not canonical for main.%s.%s" % (table, column))
    return index_ref


def ngram_storage_table(index_ref, part):
    """`__ngram.<part>_<hex>` for part in {segments, stats}."""
    return "__ngram.%s_%s" % (part, index_ref.replace("-", ""))


def ngram_registry_row(index_ref):
    """The index's registry row, as a FROM target."""
    return "__ngram.registry WHERE index_id = '%s'::UUID" % index_ref


def drop_caches():
    """Evict the page cache so the next read is a real disk read.

    Returns True when it worked; cold-cache numbers are only reported when it
    does.
    """
    try:
        subprocess.run(["sync"], check=True, timeout=600)
        proc = subprocess.run(
            ["sudo", "-n", "sh", "-c", "echo 3 > /proc/sys/vm/drop_caches"],
            capture_output=True, text=True, timeout=120,
        )
        return proc.returncode == 0
    except Exception:
        return False


def gb(n_bytes):
    return n_bytes / (1024.0 ** 3)


def mb(n_bytes):
    return n_bytes / (1024.0 ** 2)


def log(*args):
    print("[%s]" % time.strftime("%H:%M:%S"), *args, flush=True)


def write_json(path, payload):
    tmp = path + ".tmp"
    with open(tmp, "w") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
    os.replace(tmp, path)
    log("wrote", path)


def read_json(path):
    with open(path) as handle:
        return json.load(handle)


def require_duckdb():
    if not os.path.exists(DUCKDB):
        sys.exit("duckdb binary not found at %s; run `make` first (or set NGRAM_DUCKDB)" % DUCKDB)
    return DUCKDB


def free_disk_gb(path=BENCH_DIR):
    os.makedirs(path, exist_ok=True)
    usage = shutil.disk_usage(path)
    return usage.free / (1024.0 ** 3)
