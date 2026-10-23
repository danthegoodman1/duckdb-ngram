#!/usr/bin/env python3
"""Differential harness for ngram indexes on DuckLake tables.

Each trial builds a randomized lake table, indexes it, and applies a random
sequence of steps: inserts; updates of the indexed column and of another one;
deletes; refresh; compaction with and without purge; DuckLake's file merges,
rewrites and inline flushes; snapshot expiry; DDL elsewhere in the lake; ALTERs
of the indexed table; rebuilds; and transactions that write and then commit or
roll back. After every step it checks, for many needles, that

    ngram_search(t, needle)               == the scan's answer
    LIKE / ILIKE / contains, accelerated  == the same query without the extension's optimizer
    ngram_candidates(t, col, needle)      >= the scan's matches          (index READY)

as multisets; that the table holds what the harness wrote; that the index's
status follows from the steps (stale after an ALTER of its table, READY when
nothing since its last maintenance could make it stale); and that the rewrite
reads the index for a probeable needle whenever the index is READY, so no
comparison passes vacuously. The row-group gate is randomized per check.

Usage: python3 scripts/lake_differential.py [--trials N] [--steps N] [--rows N] [--seed S]
                                            [--trial-seed S] [--tokenizer KIND]
                                            [--catalog duckdb|sqlite] [--duckdb PATH] [--keep]
Needs a build with DuckLake linked (NGRAM_DUCKLAKE=1). Exit code 0 iff every
check passes. Seeds are printed and reproducible.
"""

import argparse
import csv
import os
import random
import shutil
import sys
import tempfile

from differential_search import ALPHABETS, make_needles, make_patterns, make_row
from ngram_harness import Cli, DEFAULT_DUCKDB, REPO, multiset_mismatch, sql_quote

NULL_SENTINEL = "__NGRAM_NULL__"
RUNS_DIR = os.path.join(REPO, "build", "lake_runs")
# no generated row contains the marker, so its appearance is checked exactly
MARKER = "QzLakeMarker4471"
STEPS = [
    ("insert", 12), ("update_s", 10), ("update_n", 4), ("delete", 6), ("refresh", 10), ("compact", 4),
    ("compact_purge", 3), ("merge", 3), ("rewrite", 3), ("flush", 3), ("expire", 2), ("ddl_elsewhere", 2),
    ("alter", 1), ("drop_add", 1), ("txn_commit", 4), ("txn_rollback", 3), ("read_txn", 2),
]


class Trial:
    def __init__(self, cli, rng, root, catalog_kind, args):
        self.cli = cli
        self.rng = rng
        self.root = root
        if catalog_kind == "sqlite":
            self.attach = "ATTACH %s AS lk (DATA_PATH %s);\n" % (
                sql_quote("ducklake:sqlite:" + os.path.join(root, "catalog.sqlite")),
                sql_quote(os.path.join(root, "data") + "/"))
        else:
            self.attach = "ATTACH %s AS lk (DATA_PATH %s);\n" % (
                sql_quote("ducklake:" + os.path.join(root, "catalog.ducklake")),
                sql_quote(os.path.join(root, "data") + "/"))
        self.alphabet = rng.choice(list(ALPHABETS))
        tokenizer = args.tokenizer if args.tokenizer != "mixed" else rng.choice(["ngram", "sparse"])
        if tokenizer == "ngram":
            self.min_gram = self.max_gram = rng.choice([2, 3, 3, 4])
            self.options = "gram = %d" % self.min_gram
        else:
            self.min_gram = rng.choice([3, 3, 4])
            self.max_gram = max(self.min_gram, rng.choice([self.min_gram, 5, 8]))
            self.options = "tokenizer = 'sparse', min_gram = %d, max_gram = %d" % (self.min_gram, self.max_gram)
        self.case_insensitive = rng.random() < 0.6
        self.options += ", case_insensitive = %s" % ("true" if self.case_insensitive else "false")
        self.row_group_size = rng.choice([64, 256, 1024])
        self.inline_limit = rng.choice([0, 0, 25])
        self.label = "alphabet=%s %s row_group=%d inline=%d" % (
            self.alphabet, self.options, self.row_group_size, self.inline_limit)
        self.rows = {}
        self.next_id = 0
        self.n_rows = rng.randint(max(args.rows // 4, 200), args.rows)
        # what the steps since the index's last maintenance imply about it:
        # an ALTER of its table makes it stale; snapshot expiry and DDL
        # anywhere in the lake together may (the history that would clear
        # the DDL is gone)
        self.altered = False
        self.expired = False
        self.ddl = False
        self.checks = 0
        self.index_reads = 0
        self.folded_probes = 0
        self.skipped_probes = 0
        self.failures = []

    # -- running scripts ------------------------------------------------

    def run(self, script, allow_error=False):
        code, out, err = self.cli.run(None, self.attach + script, allow_error=True, attached=False)
        if code != 0 and not allow_error:
            raise RuntimeError("duckdb failed:\n%s\n--- script ---\n%s" % (err[-3000:], script[:3000]))
        return code, out, err

    def expect_error(self, script, fragment):
        code, _, err = self.run(script, allow_error=True)
        if code == 0 or fragment not in err:
            self.failures.append("expected an error containing %r from %r, got exit %d: %s"
                                 % (fragment, script.strip()[:200], code, err.strip()[-300:]))

    # -- the table -------------------------------------------------------

    def new_rows(self, count):
        result = []
        for _ in range(count):
            result.append((self.next_id, make_row(self.rng, self.alphabet, self.n_rows)))
            self.next_id += 1
        return result

    def insert_sql(self, rows):
        path = os.path.join(self.root, "rows-%d.csv" % self.next_id)
        with open(path, "w", newline="") as f:
            # quoted, so an empty string stays apart from the NULL sentinel
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            for row_id, s in rows:
                writer.writerow([row_id, NULL_SENTINEL if s is None else s, row_id % 7])
        return ("INSERT INTO lk.corpus (id, s, n) SELECT * FROM read_csv(%s, header = false, nullstr = %s, "
                "columns = {'id': 'BIGINT', 's': 'VARCHAR', 'n': 'INTEGER'}, quote = '\"', escape = '\"');\n"
                % (sql_quote(path), sql_quote(NULL_SENTINEL)))

    def update_rows(self):
        """New values for a few live rows' indexed column: fresh text, the
        marker spliced in, or NULL."""
        ids = self.rng.sample(sorted(self.rows), min(len(self.rows), self.rng.randint(1, 20)))
        result = []
        for row_id in ids:
            r = self.rng.random()
            if r < 0.3:
                s = (self.rows[row_id] or "") + " " + MARKER
            elif r < 0.4:
                s = None
            else:
                s = make_row(self.rng, self.alphabet, self.n_rows)
            result.append((row_id, s))
        return result

    def update_sql(self, updates):
        values = ", ".join("(%d, %s)" % (row_id, "NULL::VARCHAR" if s is None else sql_quote(s))
                           for row_id, s in updates)
        return ("UPDATE lk.corpus SET s = v.s FROM (VALUES %s) v(id, s) WHERE corpus.id = v.id;\n" % values)

    def delete_ids(self):
        return self.rng.sample(sorted(self.rows), min(len(self.rows), self.rng.randint(1, max(len(self.rows) // 20, 1))))

    def create_index(self):
        self.run("PRAGMA create_ngram_index('lk.corpus', 's', %s);\n" % self.options)
        self.altered = self.expired = self.ddl = False

    def setup(self):
        rows = self.new_rows(self.n_rows)
        self.run("CREATE TABLE lk.corpus (id BIGINT, s VARCHAR, n INTEGER);\n"
                 "CALL lk.set_option('parquet_row_group_size', %d, table_name => 'corpus');\n"
                 "CALL lk.set_option('data_inlining_row_limit', %d, table_name => 'corpus');\n"
                 % (self.row_group_size, self.inline_limit) + self.insert_sql(rows))
        self.rows.update(rows)
        self.create_index()

    # -- steps -----------------------------------------------------------

    def step(self, name):
        if name == "insert":
            rows = self.new_rows(self.rng.randint(1, max(self.n_rows // 10, 2)))
            self.run(self.insert_sql(rows))
            self.rows.update(rows)
        elif name == "update_s" and self.rows:
            updates = self.update_rows()
            self.run(self.update_sql(updates))
            self.rows.update(updates)
        elif name == "update_n":
            self.run("UPDATE lk.corpus SET n = n + 1 WHERE id %% 5 = %d;\n" % self.rng.randrange(5))
        elif name == "delete" and self.rows:
            ids = self.delete_ids()
            self.run("DELETE FROM lk.corpus WHERE id IN (%s);\n" % ", ".join(map(str, ids)))
            for row_id in ids:
                del self.rows[row_id]
        elif name in ("refresh", "compact", "compact_purge"):
            pragma = {"refresh": "ngram_refresh('lk.corpus')", "compact": "ngram_compact('lk.corpus')",
                      "compact_purge": "ngram_compact('lk.corpus', purge = true)"}[name]
            if self.altered:
                self.expect_error("PRAGMA %s;\n" % pragma, "cannot be maintained")
            else:
                code, _, err = self.run("PRAGMA %s;\n" % pragma, allow_error=True)
                if code != 0:
                    if self.expired and self.ddl and "cannot be maintained" in err:
                        self.rebuild()
                    else:
                        self.failures.append("%s failed: %s" % (pragma, err.strip()[-300:]))
                elif name == "refresh":
                    # a refresh advances the covered snapshot past the expired
                    # history and records the schema version
                    self.expired = self.ddl = False
                else:
                    # a compact records the schema version but keeps the
                    # covered snapshot, so an expiry gap after it surfaces at
                    # the next schema change
                    self.ddl = False
        elif name == "merge":
            self.run("CALL ducklake_merge_adjacent_files('lk');\n")
        elif name == "rewrite":
            self.run("CALL ducklake_rewrite_data_files('lk', 'corpus', delete_threshold => 0);\n")
        elif name == "flush":
            self.run("CALL ducklake_flush_inlined_data('lk');\n")
        elif name == "expire":
            self.run("CALL ducklake_expire_snapshots('lk', older_than => now());\n"
                     "CALL ducklake_cleanup_old_files('lk', cleanup_all => true);\n")
            self.expired = True
        elif name == "ddl_elsewhere":
            self.run("CREATE TABLE lk.other_%d (x INTEGER);\n" % self.rng.randrange(10 ** 9))
            self.ddl = True
        elif name == "alter":
            self.run("ALTER TABLE lk.corpus ADD COLUMN extra_%d INTEGER;\n" % self.rng.randrange(10 ** 9))
            self.altered = True
        elif name == "drop_add":
            # every row reads the new column's default, and no change feed
            # entry records it
            default = self.rng.choice(["default " + MARKER, "plain default"])
            self.run("ALTER TABLE lk.corpus DROP COLUMN s;\n"
                     "ALTER TABLE lk.corpus ADD COLUMN s VARCHAR DEFAULT %s;\n" % sql_quote(default))
            self.rows = {row_id: default for row_id in self.rows}
            self.altered = True
        elif name in ("txn_commit", "txn_rollback") and self.rows:
            rows = self.new_rows(self.rng.randint(1, 30))
            updates = self.update_rows()
            ids = [i for i in self.delete_ids() if i not in dict(updates)]
            script = ("BEGIN;\n" + self.insert_sql(rows) + self.update_sql(updates) +
                      "DELETE FROM lk.corpus WHERE id IN (%s);\n" % ", ".join(map(str, ids or [-1])))
            # inside the writing transaction the answers include its own rows
            local = dict(self.rows)
            local.update(rows)
            local.update(updates)
            for row_id in ids:
                local.pop(row_id, None)
            script += self.check_sql(local, in_txn=True)
            script += "COMMIT;\n" if name == "txn_commit" else "ROLLBACK;\n"
            self.read_checks(self.run(script)[1], "in " + name, local)
            if name == "txn_commit":
                self.rows = local
        elif name == "read_txn":
            self.read_checks(self.run("BEGIN;\n" + self.check_sql(self.rows) + "COMMIT;\n")[1],
                             "read txn", self.rows)

    def rebuild(self):
        self.run("PRAGMA drop_ngram_index('lk.corpus', 's');\n")
        self.create_index()

    # -- checks ----------------------------------------------------------

    def predicate(self, quoted):
        if self.case_insensitive:
            return "contains(lower(s), lower(%s))" % quoted
        return "contains(s, %s)" % quoted

    def check_sql(self, rows, in_txn=False, candidates=False):
        """The comparisons of one check. The scan each one is compared with
        runs with the rewrite off: on a case-sensitive index the oracle
        contains(s, needle) is itself a shape the rewrite accelerates.
        ngram_search reads the index whatever ngram_auto_accelerate says.
        Inside a transaction that wrote the lake every query scans, so only
        ngram_search is compared there; ngram_candidates answers only through
        a usable index."""
        corpus = [s for s in rows.values() if s]
        needles = make_needles(self.rng, corpus, self.min_gram, self.max_gram, 8) if corpus else []
        needles = [n for n in needles if n] + [MARKER, MARKER.lower()]
        script = ["SET ngram_lake_max_row_group_fraction = %s;" % self.rng.choice(["1.0", "0.6", "0.05"]),
                  "SET ngram_auto_accelerate = false;"]
        for needle in needles:
            q = sql_quote(needle)
            script.append("SELECT 'diff', %s, (%s);" % (q, multiset_mismatch(
                "SELECT * FROM ngram_search('lk.corpus', %s)" % q,
                "SELECT * FROM lk.corpus WHERE %s" % self.predicate(q))))
            if candidates and len(needle) >= self.min_gram:
                script.append("SELECT 'diff', %s, count(*) FROM (SELECT rowid FROM lk.corpus WHERE %s EXCEPT "
                              "SELECT rowid FROM ngram_candidates('lk.corpus', 's', %s));"
                              % (sql_quote("candidates:" + needle), self.predicate(q), q))
        if not in_txn:
            script.append("SET ngram_auto_accelerate = true;")
            for _, pattern in make_patterns(self.rng, needles, 6):
                for form, pred in (("like", "s LIKE %s" % sql_quote("%" + pattern + "%")),
                                   ("ilike", "s ILIKE %s" % sql_quote("%" + pattern + "%")),
                                   ("contains", "contains(s, %s)" % sql_quote(pattern))):
                    script += [
                        "SET disabled_optimizers = '';",
                        "CREATE OR REPLACE TEMP TABLE r_acc AS SELECT * FROM lk.corpus WHERE %s;" % pred,
                        "SET disabled_optimizers = 'extension';",
                        "CREATE OR REPLACE TEMP TABLE r_plain AS SELECT * FROM lk.corpus WHERE %s;" % pred,
                        "SET disabled_optimizers = '';",
                        "SELECT 'diff', %s, (%s);" % (sql_quote(form + ":" + pattern),
                                                      multiset_mismatch("TABLE r_acc", "TABLE r_plain")),
                    ]
        script.append("SELECT 'count', count(*), count(s), coalesce(sum(strlen(s)), 0) FROM lk.corpus;")
        return "\n".join(script) + "\n"

    def read_checks(self, out, label, rows):
        for line in csv.reader(out.splitlines()):
            if not line:
                continue
            if line[0] == "diff":
                self.checks += 1
                if int(line[2]) != 0:
                    self.failures.append("%s: %r -> %s mismatching rows" % (label, line[1], line[2]))
            elif line[0] == "count":
                self.checks += 1
                expected = [len(rows), sum(1 for s in rows.values() if s is not None),
                            sum(len(s.encode()) for s in rows.values() if s is not None)]
                if [int(v) for v in line[1:4]] != expected:
                    self.failures.append("%s: the table holds %s, the harness wrote %s" % (label, line[1:4], expected))

    def status(self):
        rows = [line for line in csv.reader(self.run(
            "SELECT status, coalesce(reason, '') FROM ngram_indexes() WHERE table_name = 'corpus';\n")[1].splitlines())
            if line]
        return rows[0] if rows else ["MISSING", ""]

    def check(self, label):
        status, reason = self.status()
        altered = reason.startswith("the table was altered")
        expired = reason.startswith("snapshots after the index's last refresh were expired")
        if self.altered:
            # expiry can remove the ALTER's snapshot from the history the
            # check reads, which then reports the expiry
            if status != "SCAN_ONLY" or not (altered or (expired and self.expired)):
                self.failures.append("%s: an ALTER left the index %s (%s)" % (label, status, reason))
        elif status == "SCAN_ONLY" and self.expired and self.ddl and expired:
            pass
        elif status != "READY":
            self.failures.append("%s: the index is %s (%s) with no ALTER since its build" % (label, status, reason))
        ready = status == "READY"

        self.read_checks(self.run(self.check_sql(self.rows, candidates=ready))[1], label, self.rows)

        probe = self.probe_needle()
        if probe is None:
            self.skipped_probes += 1
            return
        q = sql_quote(probe)
        shapes = [("oracle", False, "SELECT * FROM lk.corpus WHERE %s" % self.predicate(q)),
                  ("ngram_search", False, "SELECT * FROM ngram_search('lk.corpus', %s)" % q),
                  ("contains", True, "SELECT * FROM lk.corpus WHERE contains(s, %s)" % q)]
        script = "CALL enable_logging(level = 'info', storage = 'memory');\n"
        script += "SET ngram_lake_max_row_group_fraction = 1.0;\n"
        for shape, accelerate, query in shapes:
            script += ("SET ngram_auto_accelerate = %s;\nCALL truncate_duckdb_logs();\nSELECT '@@%s';\n"
                       "EXPLAIN %s;\nSELECT '@@log', message FROM duckdb_logs WHERE message LIKE 'ngram:%%';\n"
                       % ("true" if accelerate else "false", shape, query))
        out = self.run(script)[1]
        sections = {}
        for part in out.split("@@")[1:]:
            name, _, text = part.partition("\n")
            if name.startswith("log"):
                # a log line reads @@log,<message>
                sections[current + ":log"] = sections.get(current + ":log", "") + part
            else:
                current = name.strip()
                sections[current] = text
        self.checks += 1
        if "__ngram_lake_rowids" in sections.get("oracle", ""):
            self.failures.append("%s: the scan oracle for %r read the index" % (label, probe))
        if not ready:
            return
        for shape in ("ngram_search", "contains"):
            self.checks += 1
            plan, log = sections.get(shape, ""), sections.get(shape + ":log", "")
            if "__ngram_lake_rowids" in plan:
                self.index_reads += 1
            elif shape == "contains" and "ngram:" not in log and "contains(" not in plan:
                # the column's statistics proved the filter true for every row
                # (after drop_add every row holds the default) and the planner
                # dropped it before the extension saw it
                self.folded_probes += 1
            else:
                self.failures.append("%s: a READY index did not answer %s for %r: %s"
                                     % (label, shape, probe, log.strip()))

    def probe_needle(self):
        live = [s for s in self.rows.values() if s and len(s) >= self.max_gram + 1]
        if not live:
            return None
        s = self.rng.choice(live)
        start = self.rng.randint(0, len(s) - self.max_gram - 1)
        return s[start:start + self.max_gram + 1]


def one_trial(trial_no, seed, args, cli):
    rng = random.Random(seed)
    os.makedirs(RUNS_DIR, exist_ok=True)
    root = tempfile.mkdtemp(prefix="differential-%d-" % seed, dir=RUNS_DIR)
    trial = Trial(cli, rng, root, args.catalog, args)
    print("trial %d (seed %d): %s rows=%d" % (trial_no, seed, trial.label, trial.n_rows), flush=True)
    try:
        trial.setup()
        trial.check("after create")
        names = [name for name, _ in STEPS]
        weights = [weight for _, weight in STEPS]
        for step_no in range(args.steps):
            name = rng.choices(names, weights)[0]
            trial.step(name)
            trial.check("step %d (%s)" % (step_no, name))
            if trial.altered and rng.random() < 0.5:
                trial.rebuild()
                trial.check("step %d (rebuild)" % step_no)
            if len(trial.failures) > 20:
                break
    except RuntimeError as error:
        trial.failures.append(str(error))
    finally:
        if not args.keep:
            shutil.rmtree(root, ignore_errors=True)
    print("  %d checks, %d index reads (%d probes folded by the planner, %d without a probe needle), %d failures"
          % (trial.checks, trial.index_reads, trial.folded_probes, trial.skipped_probes, len(trial.failures)),
          flush=True)
    return trial


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trials", type=int, default=6)
    ap.add_argument("--steps", type=int, default=30)
    ap.add_argument("--rows", type=int, default=3000)
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--tokenizer", choices=["ngram", "sparse", "mixed"], default="mixed")
    ap.add_argument("--catalog", choices=["duckdb", "sqlite"], default="duckdb",
                    help="the lake's catalog database")
    ap.add_argument("--duckdb", default=DEFAULT_DUCKDB, help="duckdb binary with DuckLake linked")
    ap.add_argument("--keep", action="store_true", help="keep each trial's lake under build/lake_runs")
    ap.add_argument("--trial-seed", type=int, default=None, help="run the one trial a run printed with this seed")
    args = ap.parse_args()
    cli = Cli(args.duckdb, timeout=600)

    seed = args.seed if args.seed is not None else random.SystemRandom().randint(0, 2 ** 31)
    print("master seed: %d" % seed, flush=True)
    master = random.Random(seed)
    trial_seeds = [master.randint(0, 2 ** 31) for _ in range(args.trials)]
    if args.trial_seed is not None:
        trial_seeds = [args.trial_seed]
    checks = reads = 0
    failures = []
    for trial_no, trial_seed in enumerate(trial_seeds):
        trial = one_trial(trial_no, trial_seed, args, cli)
        checks += trial.checks
        reads += trial.index_reads
        failures += ["trial %d: %s" % (trial_no, f) for f in trial.failures]
    print("TOTAL: %d checks, %d index reads, %d failures (master seed %d)" % (checks, reads, len(failures), seed))
    for failure in failures[:50]:
        print("FAIL:", failure)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
