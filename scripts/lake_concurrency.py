#!/usr/bin/env python3
"""Concurrency harness for ngram indexes on DuckLake tables.

Writer, maintainer, reader and DDL processes share one lake whose catalog is a
SQLite database (in WAL mode, so readers never block the writer), as several
machines would share a lake. Each worker runs one duckdb process per
transaction until the deadline:

  writers      insert, update and delete rows of the indexed table
  maintainers  ngram_refresh, ngram_compact (with and without purge), DuckLake
               file merges, rewrites, inline flushes and snapshot expiry
  readers      inside one transaction, compare ngram_search with a scan and
               each accelerated LIKE/ILIKE/contains query with the same query
               planned without the extension's optimizer, as multisets
  ddl          create and drop other tables; now and then ALTER the indexed
               table and rebuild its index

It checks that every reader comparison agrees, that a maintenance call that
loses to a concurrent one fails with a conflict error and succeeds when run
again, that writers fail only with conflicts, and that readers used the index
for a share of their queries. A final quiescent pass refreshes the index and
compares every needle once more.

Usage: python3 scripts/lake_concurrency.py [--seconds N] [--writers N] [--maintainers N]
                                           [--readers N] [--rows N] [--seed S]
                                           [--tokenizer ngram|sparse] [--duckdb PATH] [--keep]
Needs a build with DuckLake and the SQLite scanner linked (NGRAM_DUCKLAKE=1).
Exit code 0 iff every check passes.
"""

import argparse
import csv
import os
import random
import shutil
import sys
import tempfile
import threading
import time
import traceback

from ngram_harness import Cli, DEFAULT_DUCKDB, REPO, multiset_mismatch, sql_quote

RUNS_DIR = os.path.join(REPO, "build", "lake_runs")
WORDS = ["tent", "ten", "cent", "often", "entered", "content", "contentment", "connection", "reset", "peer",
         "Tent", "TENT", "needle", "haystack", "gram", "index", "duck", "lake", "row", "tail", "öft", "ÖFT"]
# writers splice these into rows; no generated row contains them otherwise
MARKERS = ["QzMarkAlpha", "QzMarkBeta", "QzMarkGamma"]
# DuckLake's commit conflict, and the maintenance scripts' own re-check
CONFLICTS = ("Transaction conflict", "changed since this call was planned")
# other errors that name a conflict, retried as DuckLake retries them, and
# counted apart
OTHER_CONFLICT = "conflict"
# the SQLite catalog's busy timeout ran out: contention in the catalog
# database rather than a conflict between transactions
BUSY = ("database is locked",)
MAX_ATTEMPTS = 25


def row_text(rng):
    words = [rng.choice(WORDS) for _ in range(rng.randint(1, 8))]
    if rng.random() < 0.05:
        words.insert(rng.randrange(len(words) + 1), rng.choice(MARKERS))
    return " ".join(words)


def needles(rng):
    """Words, substrings of words, multi-word phrases and markers."""
    result = set(rng.sample(WORDS, 4)) | set(rng.sample(MARKERS, 1))
    for _ in range(3):
        word = rng.choice(WORDS + MARKERS)
        start = rng.randrange(len(word))
        result.add(word[start:start + rng.randint(3, 6)])
    result.add(rng.choice(WORDS) + " " + rng.choice(WORDS))
    return sorted(n for n in result if n)


class Lake:
    def __init__(self, cli, root, args):
        self.cli = cli
        self.root = root
        self.args = args
        catalog = os.path.join(root, "catalog.sqlite")
        # in WAL mode an open reading transaction leaves another free to commit
        self.attach = "ATTACH %s AS lk (DATA_PATH %s, BUSY_TIMEOUT 60000, META_JOURNAL_MODE 'wal');\n" % (
            sql_quote("ducklake:sqlite:" + catalog), sql_quote(os.path.join(root, "data") + "/"))
        self.lock = threading.Lock()
        self.failures = []
        self.stats = {}
        self.rebuild_lock = threading.Lock()

    def run(self, script):
        code, out, err = self.cli.run(None, self.attach + script, allow_error=True, attached=False)
        return code == 0, out, err

    def count(self, key, n=1):
        with self.lock:
            self.stats[key] = self.stats.get(key, 0) + n

    def fail(self, message):
        with self.lock:
            self.failures.append(message)

    def until_committed(self, label, script, tolerated=(), required=True):
        """Runs `script` until it commits, and returns the error that ended
        the attempt, or None. A failure must be a conflict, the catalog busy,
        or an error in `tolerated`, which ends the attempt; with `required`,
        losing MAX_ATTEMPTS conflicts in a row fails the run."""
        for attempt in range(MAX_ATTEMPTS):
            ok, out, err = self.run(script)
            if ok:
                self.count(label + " committed")
                if attempt:
                    self.count(label + " committed after a conflict")
                return None
            if any(fragment.lower() in err.lower() for fragment in tolerated):
                self.count(label + " found no maintainable index")
                return err
            if any(fragment in err for fragment in BUSY):
                self.count(label + " found the catalog busy")
            elif any(fragment in err for fragment in CONFLICTS):
                self.count(label + " conflicts")
            elif OTHER_CONFLICT in err:
                self.count(label + " other conflicts")
            else:
                self.fail("%s failed with a non-conflict error: %s" % (label, err.strip()[-400:]))
                return err
            time.sleep(random.random() * 0.2)
        if required:
            self.fail("%s lost %d conflicts in a row" % (label, MAX_ATTEMPTS))
        else:
            self.count(label + " gave up after %d conflicts" % MAX_ATTEMPTS)
        return "gave up"

    def rebuild(self, tokenizer, label):
        """Drops and creates the index, one rebuild at a time: a concurrent
        rebuild may have done either step first."""
        with self.rebuild_lock:
            self.count(label)
            self.until_committed("rebuild drop", "PRAGMA drop_ngram_index('lk.corpus', 's');\n",
                                 ("no ngram index exists",))
            self.until_committed("rebuild create", "PRAGMA create_ngram_index('lk.corpus', 's', tokenizer = %s);\n"
                                 % sql_quote(tokenizer))


def predicate(quoted):
    return "contains(lower(s), lower(%s))" % quoted


def reader_script(rng, log=False):
    """One reader transaction: every comparison and three EXPLAINs read one
    snapshot. The scan each comparison is compared with runs with the rewrite
    off, so it never reads the index itself; with `log`, the transaction also
    prints the extension's log lines, which say why a query read the index or
    did not."""
    gate = rng.choice(["1.0", "0.6"])
    statements = ["CALL enable_logging(level = 'info', storage = 'memory');"] if log else []
    statements += ["BEGIN TRANSACTION;",
                   "SET ngram_auto_accelerate = false;",
                   "SET ngram_lake_max_row_group_fraction = %s;" % gate]
    chosen = needles(rng)
    for needle in chosen:
        q = sql_quote(needle)
        statements.append("SELECT 'diff', %s, (%s);" % (sql_quote("search@%s:%s" % (gate, needle)), multiset_mismatch(
            "SELECT * FROM ngram_search('lk.corpus', %s)" % q, "SELECT * FROM lk.corpus WHERE %s" % predicate(q))))
    statements.append("SET ngram_auto_accelerate = true;")
    for needle in rng.sample(chosen, 3):
        for form, pred in (("like", "s LIKE %s" % sql_quote("%" + needle + "%")),
                           ("ilike", "s ILIKE %s" % sql_quote("%" + needle + "%")),
                           ("contains", "contains(s, %s)" % sql_quote(needle))):
            statements += [
                "SET disabled_optimizers = '';",
                "CREATE OR REPLACE TEMP TABLE r_acc AS SELECT * FROM lk.corpus WHERE %s;" % pred,
                "SET disabled_optimizers = 'extension';",
                "CREATE OR REPLACE TEMP TABLE r_plain AS SELECT * FROM lk.corpus WHERE %s;" % pred,
                "SET disabled_optimizers = '';",
                "SELECT 'diff', %s, (%s);" % (sql_quote("%s@%s:%s" % (form, gate, needle)),
                                              multiset_mismatch("TABLE r_acc", "TABLE r_plain")),
            ]
    probe = sql_quote(next((n for n in chosen if len(n) >= 6), "connection"))
    statements += ["SET ngram_lake_max_row_group_fraction = 1.0;",
                   "SET ngram_auto_accelerate = false;",
                   "SELECT '@@oracle';",
                   "EXPLAIN SELECT * FROM lk.corpus WHERE %s;" % predicate(probe),
                   "SELECT '@@search';",
                   "EXPLAIN SELECT * FROM ngram_search('lk.corpus', %s);" % probe,
                   "SET ngram_auto_accelerate = true;",
                   "SELECT '@@contains';",
                   "EXPLAIN SELECT * FROM lk.corpus WHERE contains(s, %s);" % probe,
                   "SELECT '@@end';",
                   "COMMIT;"]
    if log:
        statements.append("SELECT 'log', message FROM duckdb_logs WHERE message LIKE 'ngram:%' ORDER BY timestamp;")
    return "\n".join(statements) + "\n"


def read_diffs(lake, out, label):
    """Counts the transaction's comparisons, and returns whether ngram_search
    and the rewrite both read the index with the gate open. A scan oracle
    that read the index fails the run."""
    plans = {}
    for part in out.split("@@")[1:]:
        name, _, text = part.partition("\n")
        plans[name.strip()] = text
    if "__ngram_lake_rowids" in plans.get("oracle", ""):
        lake.fail("%s: the scan oracle read the index" % label)
    for line in csv.reader(out.splitlines()):
        if len(line) == 3 and line[0] == "diff":
            lake.count("reader comparisons at gate " + line[1].split("@", 1)[1].split(":", 1)[0])
            if int(line[2]) != 0:
                lake.fail("%s: %r -> %s mismatching rows" % (label, line[1], line[2]))
    return all("__ngram_lake_rowids" in plans.get(shape, "") for shape in ("search", "contains"))


def writer(lake, worker_no, seed, deadline, initial_rows):
    rng = random.Random(seed)
    next_id = (worker_no + 1) * 10 ** 9
    while time.time() < deadline:
        rows = []
        for _ in range(rng.randint(1, 40)):
            rows.append("(%d, %s)" % (next_id, sql_quote(row_text(rng))))
            next_id += 1
        script = "BEGIN TRANSACTION;\nINSERT INTO lk.corpus (id, s) VALUES %s;\n" % ", ".join(rows)
        if rng.random() < 0.6:
            ids = rng.sample(range(initial_rows), 5)
            script += "UPDATE lk.corpus SET s = %s WHERE id IN (%s);\n" % (
                sql_quote(row_text(rng) + " " + rng.choice(MARKERS)), ", ".join(map(str, ids)))
        if rng.random() < 0.4:
            script += "DELETE FROM lk.corpus WHERE id IN (%s);\n" % ", ".join(
                str(rng.randrange(initial_rows)) for _ in range(3))
        script += "COMMIT;\n"
        ok, _, err = lake.run(script)
        if ok:
            lake.count("writer commits")
        elif any(fragment in err for fragment in BUSY):
            lake.count("writer found the catalog busy")
        elif any(fragment in err for fragment in CONFLICTS):
            lake.count("writer conflicts")
        else:
            lake.fail("writer %d: %s" % (worker_no, err.strip()[-400:]))


# (label, script, weight, whether it is an ngram call): an ngram call that
# loses MAX_ATTEMPTS conflicts in a row fails the run; DuckLake's own
# maintenance may lose to steady writers, which is DuckLake's business
MAINTENANCE = [
    ("refresh", "PRAGMA ngram_refresh('lk.corpus');\n", 6, True),
    ("compact", "PRAGMA ngram_compact('lk.corpus');\n", 2, True),
    ("compact purge", "PRAGMA ngram_compact('lk.corpus', purge = true);\n", 1, True),
    ("merge", "CALL ducklake_merge_adjacent_files('lk');\n", 2, False),
    ("rewrite", "CALL ducklake_rewrite_data_files('lk', 'corpus', delete_threshold => 0);\n", 1, False),
    ("flush", "CALL ducklake_flush_inlined_data('lk');\n", 1, False),
    ("expire", "CALL ducklake_expire_snapshots('lk', older_than => now() - INTERVAL 60 SECOND);\n", 1, False),
]
# while the DDL worker rebuilds the index, or between its ALTER and the
# rebuild, maintenance finds no index it may maintain; that ends the attempt
INDEX_GONE = ("no ngram index exists", "the table was altered after the index's last refresh")
# DuckLake expired snapshots the staleness check needs, and a schema change
# makes it read them: the remedy is a rebuild
EXPIRED = "were expired before a refresh"


def maintainer(lake, worker_no, seed, deadline, tokenizer):
    rng = random.Random(seed)
    while time.time() < deadline:
        label, script, _, ngram = rng.choices(MAINTENANCE, [weight for _, _, weight, _ in MAINTENANCE])[0]
        error = lake.until_committed(label, script, INDEX_GONE + (EXPIRED,), required=ngram)
        if error and EXPIRED in error:
            lake.rebuild(tokenizer, "rebuilds after expiry")
        time.sleep(rng.random() * 0.5)


def reader(lake, worker_no, seed, deadline):
    rng = random.Random(seed)
    while time.time() < deadline:
        ok, out, err = lake.run(reader_script(rng))
        if not ok and "no ngram index exists" in err:
            # a rebuild is between its drop and its create
            lake.count("reader transactions during a rebuild")
            continue
        if not ok:
            lake.fail("reader %d: %s" % (worker_no, err.strip()[-400:]))
            continue
        lake.count("reader transactions")
        if read_diffs(lake, out, "reader %d" % worker_no):
            lake.count("reader transactions that read the index")


def ddl(lake, seed, deadline, tokenizer):
    rng = random.Random(seed)
    while time.time() < deadline:
        time.sleep(1 + rng.random() * 3)
        if rng.random() < 0.85:
            # a rerun after a conflict repeats both statements
            name = "lk.other_%d" % rng.randrange(10 ** 9)
            lake.until_committed("ddl elsewhere",
                                 "CREATE TABLE IF NOT EXISTS %s (x INTEGER);\nDROP TABLE IF EXISTS %s;\n" % (name, name))
        else:
            lake.until_committed("alter", "ALTER TABLE lk.corpus ADD COLUMN extra_%d INTEGER;\n"
                                 % rng.randrange(10 ** 9))
            lake.rebuild(tokenizer, "rebuilds after an ALTER")


def guarded(lake, name, target, *args):
    """Runs a worker; an exception it raises fails the run."""
    try:
        target(lake, *args)
    except Exception:
        lake.fail("%s died: %s" % (name, traceback.format_exc()[-1500:]))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=int, default=300)
    ap.add_argument("--writers", type=int, default=2)
    ap.add_argument("--maintainers", type=int, default=2)
    ap.add_argument("--readers", type=int, default=3)
    ap.add_argument("--rows", type=int, default=20000)
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--tokenizer", choices=["ngram", "sparse"], default="ngram")
    ap.add_argument("--duckdb", default=DEFAULT_DUCKDB, help="duckdb binary with DuckLake linked")
    ap.add_argument("--keep", action="store_true", help="keep the lake under build/lake_runs")
    args = ap.parse_args()
    seed = args.seed if args.seed is not None else random.SystemRandom().randint(0, 2 ** 31)
    print("seed: %d" % seed, flush=True)
    rng = random.Random(seed)

    os.makedirs(RUNS_DIR, exist_ok=True)
    root = tempfile.mkdtemp(prefix="concurrency-%d-" % seed, dir=RUNS_DIR)
    lake = Lake(Cli(args.duckdb, timeout=900), root, args)
    try:
        values = ", ".join("(%d, %s)" % (i, sql_quote(row_text(rng))) for i in range(args.rows))
        ok, _, err = lake.run("CREATE TABLE lk.corpus (id BIGINT, s VARCHAR);\n"
                              "CALL lk.set_option('parquet_row_group_size', 1024, table_name => 'corpus');\n"
                              "INSERT INTO lk.corpus VALUES %s;\n"
                              "PRAGMA create_ngram_index('lk.corpus', 's', tokenizer = %s);\n"
                              % (values, sql_quote(args.tokenizer)))
        if not ok:
            print("setup failed: %s" % err.strip()[-2000:])
            return 1

        deadline = time.time() + args.seconds
        def worker(name, target, *worker_args):
            return threading.Thread(target=guarded, args=(lake, name, target) + worker_args)

        threads = [worker("writer %d" % i, writer, i, rng.random(), deadline, args.rows) for i in range(args.writers)]
        threads += [worker("maintainer %d" % i, maintainer, i, rng.random(), deadline, args.tokenizer)
                    for i in range(args.maintainers)]
        threads += [worker("reader %d" % i, reader, i, rng.random(), deadline) for i in range(args.readers)]
        threads.append(worker("ddl", ddl, rng.random(), deadline, args.tokenizer))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        # quiescent: the index must be usable, and agree with the scan
        lake.until_committed("final refresh", "PRAGMA ngram_refresh('lk.corpus');\n", INDEX_GONE + (EXPIRED,))
        ok, out, err = lake.run("SELECT status FROM ngram_indexes() WHERE table_name = 'corpus';\n")
        if not ok or out.strip().splitlines()[-1:] != ["READY"]:
            lake.rebuild(args.tokenizer, "final rebuilds")
        for _ in range(3):
            ok, out, err = lake.run(reader_script(rng, log=True))
            if not ok:
                lake.fail("final reader: %s" % err.strip()[-400:])
            elif not read_diffs(lake, out, "final reader"):
                log = [line for line in out.splitlines() if line.startswith("log,")]
                lake.fail("the final reader's probe did not read a READY index: %s" % " | ".join(log[-3:]))
        # outside a rebuild a reader reads the index unless an ALTER has made
        # it stale, a window the DDL worker closes within seconds
        read = lake.stats.get("reader transactions that read the index", 0)
        if read * 2 < lake.stats.get("reader transactions", 0):
            lake.fail("only %d of %d reader transactions read the index"
                      % (read, lake.stats.get("reader transactions", 0)))
    finally:
        if not args.keep:
            shutil.rmtree(root, ignore_errors=True)

    for key in sorted(lake.stats):
        print("  %-48s %d" % (key, lake.stats[key]))
    print("TOTAL: %d failures (seed %d)" % (len(lake.failures), seed))
    for failure in lake.failures[:50]:
        print("FAIL:", failure)
    return 1 if lake.failures else 0


if __name__ == "__main__":
    sys.exit(main())
