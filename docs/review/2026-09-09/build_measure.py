#!/usr/bin/env python3
"""Index build cost on enwik9: wall time, peak RSS and spill, before and after
a build change. Loads the staged enwik9 lines into a fresh database, then
meters `PRAGMA create_ngram_index` in its own process with /usr/bin/time -v
and samples the spill directory, three times. Run from the repository root:

    python3 docs/review/2026-09-09/build_measure.py --label baseline \
        --source ~/duckdb-ngram-bench/corpus_source.db --work /tmp/build-measure \
        --output docs/review/2026-09-09/build_observations.json
"""

import argparse
import json
import os
import re
import statistics
import subprocess
import threading
import time

BINARY = "build/release/duckdb"
SETTINGS = "SET threads=24; SET memory_limit='48GB'; SET preserve_insertion_order=true;\n"
RSS = re.compile(r"Maximum resident set size \(kbytes\): (\d+)")


def dir_size(path):
    total = 0
    for root, _dirs, files in os.walk(path):
        for name in files:
            try:
                total += os.lstat(os.path.join(root, name)).st_size
            except OSError:
                pass
    return total


class SpillSampler(threading.Thread):
    def __init__(self, temp_dir):
        super().__init__(daemon=True)
        self.temp_dir = temp_dir
        self.peak = 0
        self.stop_event = threading.Event()

    def run(self):
        while not self.stop_event.is_set():
            if os.path.isdir(self.temp_dir):
                self.peak = max(self.peak, dir_size(self.temp_dir))
            self.stop_event.wait(0.1)


def metered(database, sql):
    meter = database + ".rss"
    sampler = SpillSampler(database + ".tmp")
    sampler.start()
    start = time.monotonic()
    process = subprocess.run(["/usr/bin/time", "-v", "-o", meter, BINARY, database], input=SETTINGS + sql, text=True,
                             capture_output=True)
    wall = time.monotonic() - start
    sampler.stop_event.set()
    sampler.join()
    if process.returncode:
        raise SystemExit(process.stderr[-2000:])
    with open(meter) as handle:
        rss = int(RSS.search(handle.read()).group(1)) * 1024
    os.remove(meter)
    return wall, rss, sampler.peak


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--label", required=True)
    parser.add_argument("--source", required=True)
    parser.add_argument("--work", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()
    os.makedirs(args.work, exist_ok=True)
    runs = []
    for repeat in range(args.repeats):
        database = os.path.join(args.work, "%s-%d.db" % (args.label, repeat))
        for suffix in ("", ".wal"):
            if os.path.exists(database + suffix):
                os.remove(database + suffix)
        load_wall, load_rss, _ = metered(database, "ATTACH '%s' AS src (READ_ONLY);\nCREATE TABLE docs AS SELECT id, "
                                                   "line AS s FROM src.enwik9_lines ORDER BY id;\nCHECKPOINT;\n"
                                         % args.source)
        base_bytes = os.path.getsize(database)
        wall, rss, spill = metered(database, "PRAGMA create_ngram_index('docs', 's');\nCHECKPOINT;\n")
        indexed_bytes = os.path.getsize(database)
        stats = subprocess.run([BINARY, "-readonly", "-csv", "-noheader", database, "-c",
                                "PRAGMA ngram_index_stats('docs');"], capture_output=True, text=True).stdout.strip()
        run = {"load_s": round(load_wall, 2), "load_rss_bytes": load_rss, "build_s": round(wall, 2),
               "build_rss_bytes": rss, "build_spill_bytes": spill, "index_bytes": indexed_bytes - base_bytes,
               "stats": stats}
        runs.append(run)
        print("%s run %d: build %.2f s, rss %.2f GiB, spill %.2f GiB, index %.3f GiB" % (
            args.label, repeat, wall, rss / 2**30, spill / 2**30, run["index_bytes"] / 2**30), flush=True)
        for suffix in ("", ".wal"):
            if os.path.exists(database + suffix):
                os.remove(database + suffix)
    observations = {}
    if os.path.exists(args.output):
        with open(args.output) as handle:
            observations = json.load(handle)
    observations[args.label] = {
        "git_head": subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip(),
        "runs": runs,
        "build_s_median": round(statistics.median(r["build_s"] for r in runs), 2),
        "build_rss_median_bytes": int(statistics.median(r["build_rss_bytes"] for r in runs)),
    }
    with open(args.output, "w") as handle:
        json.dump(observations, handle, indent=1, sort_keys=True)
    print("wrote", args.output)


if __name__ == "__main__":
    main()
