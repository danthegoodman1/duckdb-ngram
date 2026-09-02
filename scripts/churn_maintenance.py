#!/usr/bin/env python3
"""Churn harness for ngram index maintenance.

Interleaves INSERT / DELETE / UPDATE / CHECKPOINT / PRAGMA ngram_refresh
(bounded and unbounded) / PRAGMA ngram_compact against a live index, reopening
the database between every step (each step runs in its own duckdb process), and
verifies the maintenance contract after every step:

  I1  exact search, always: ngram_search results equal the brute-force answer
      after every mutation, checkpoint, reopen, and maintenance operation.

  I2  maintenance uncertainty is surfaced: the harness looks at the
      index between steps the way an operator would — PRAGMA ngram_index_stats
      reports stale_reason, and the maintenance pragmas refuse — and rebuilds
      when told to. Query fallback must remain exact before that rebuild.

The corpus is created with a small row-group size so that a few thousand rows
span many row groups, making vacuum and trailing-rowid-reuse schedules easier
to reach in a bounded run.

--no-stale-expected turns the harness into a false-alarm gate instead. It omits
operations that may legitimately make incremental maintenance uncertain, but
keeps an indexed-column update confined to rows past the mark. It FAILS on any
detector verdict; ordinary mode accepts a conservative verdict and rebuilds.

Usage: python3 scripts/churn_maintenance.py [--rounds N] [--rows N] [--seed S]
                                            [--duckdb PATH] [--no-stale-expected]
Exit code 0 iff every check passes. Seeds are printed and reproducible;
iteration is sorted, so runs do not depend on PYTHONHASHSEED.
"""

import argparse
import csv
import os
import random
import re
import subprocess
import sys
import tempfile

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DUCKDB = os.path.join(REPO, "build", "release", "duckdb")
ROW_GROUP_SIZE = 2048
# words the rows are built from; UPDATE splices in MARKER, which no generated
# row contains, so indexed-column update exhaustiveness is directly checked
WORDS = ["tent", "ten", "cent", "often", "entered", "content", "connection",
         "reset", "peer", "needle", "haystack", "gram", "index", "duck", "row"]
MARKER = "grendel"
NEEDLES = ["tent", "ten", "ent", "connection", "duck", "gram", MARKER, "xyzzy"]


def sql_quote(s):
    return "'" + s.replace("'", "''") + "'"


def predicate(needle):
    return "contains(lower(s), lower(%s))" % sql_quote(needle)


class Db:
    """One duckdb process per script: every step reopens the database, which
    also exercises checkpoint-on-close and WAL replay."""

    def __init__(self, path):
        self.path = path

    def run(self, script, allow_error=False, attached=True):
        proc = subprocess.run([DUCKDB] + ([self.path] if attached else []),
                              input=".headers off\n.mode csv\n" + script,
                              capture_output=True, text=True, timeout=900)
        if proc.returncode != 0 and not allow_error:
            raise RuntimeError("duckdb failed:\n%s\n--- script ---\n%s" % (proc.stderr[-4000:], script[:2000]))
        return proc.returncode, proc.stdout, proc.stderr

    def index_ref(self):
        _, catalog_out, _ = self.run("SELECT current_database();")
        catalog = next(csv.reader(catalog_out.splitlines()))[0]
        _, out, _ = self.run("PRAGMA ngram_indexes;")
        rows = [row for row in csv.reader(out.splitlines())
                if len(row) == 8 and row[0] == catalog
                and row[2:5] == ["main", "corpus", "s"] and row[5] == "4" and row[6] == "READY"]
        if len(rows) != 1:
            raise RuntimeError("expected one READY format-4 corpus.s index")
        ref = rows[0][1]
        if not re.fullmatch(r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}", ref):
            raise RuntimeError("public corpus.s index id is not a canonical UUIDv4")
        return ref


def rows_sql(rng, start, count):
    values = []
    for i in range(count):
        n = rng.randint(1, 6)
        text = " ".join(rng.choice(WORDS) for _ in range(n))
        values.append("(%d, %s)" % (start + i, sql_quote(text)))
    return "INSERT INTO corpus VALUES %s;" % ", ".join(values)


def verification_script():
    """Per needle, check both directions of the exact-search invariant."""
    out = []
    for needle in NEEDLES:
        q = sql_quote(needle)
        out.append("SELECT 'extra', %s, count(*) FROM "
                   "(SELECT * FROM ngram_search('corpus', %s) EXCEPT SELECT * FROM corpus WHERE %s);"
                   % (q, q, predicate(needle)))
        out.append("SELECT 'missing', %s, count(*) FROM "
                   "(SELECT * FROM corpus WHERE %s EXCEPT SELECT * FROM ngram_search('corpus', %s));"
                   % (q, predicate(needle), q))
    return "\n".join(out)


def parse_counts(out):
    return [(row[0], row[1], int(row[2]))
            for row in csv.reader(out.splitlines())
            if len(row) == 3 and row[0] in ("extra", "missing")]


class Churn:
    def __init__(self, db, rng, rows, strict=False):
        self.db = db
        self.rng = rng
        #! no detector may fire: the run only does things that keep an index valid
        self.strict = strict
        self.next_id = rows
        self.checks = 0
        self.failures = []
        self.stale_states = 0
        self.detected = 0
        self.ops = {}
        self.reasons = {}

    def rebuild(self):
        self.db.run("PRAGMA drop_ngram_index('corpus', 's');\n"
                    "PRAGMA create_ngram_index('corpus', 's');")

    def maintenance(self, statement):
        """Run a maintenance pragma. A refusal is a detection, after which the
        harness does what the error message tells the user to do."""
        code, _, err = self.db.run(statement, allow_error=True)
        if code == 0:
            return
        if "cannot be maintained incrementally" in err:
            if self.strict:
                self.failures.append("false alarm: %s refused on an index nothing invalidated: %s"
                                     % (statement.strip(), err.strip()[-300:]))
                self.rebuild()
                return
            self.detected += 1
            self.stale_states += 1
            self.rebuild()
            return
        self.failures.append("%s failed unexpectedly: %s" % (statement.strip(), err.strip()[-400:]))

    def step(self, round_no):
        ops = ["insert", "insert", "checkpoint", "refresh", "refresh", "compact", "compact_purge"]
        if not self.strict:
            # Deletes/checkpoints may make incremental maintenance uncertain;
            # indexed-column updates exercise the guard's delete+insert path.
            ops += ["delete", "update"]
        else:
            # A covered-column UPDATE confined to rows past the high-water
            # mark. The guard turns it into delete+append and the replacement
            # remains in the scanned tail, so any detector verdict is a false
            # alarm. This keeps mutation in strict mode instead of making that
            # mode append-only.
            #
            # A tail-confined DELETE is deliberately NOT here: refresh advances
            # the mark over deleted rowids on purpose (otherwise an increment of
            # nothing but deleted rows would stall a bounded loop), so the gap
            # ends up below the mark, and the next checkpoint's vacuum then
            # can create a trailing-reuse risk that belongs in ordinary mode.
            ops += ["tail_update"]
        op = self.rng.choice(sorted(ops))
        self.ops[op] = self.ops.get(op, 0) + 1
        if op == "insert":
            n = self.rng.randint(50, 400)
            self.db.run(rows_sql(self.rng, self.next_id, n))
            self.next_id += n
        elif op == "delete":
            modulus = self.rng.randint(3, 11)
            self.db.run("DELETE FROM corpus WHERE id %% %d = %d;" % (modulus, self.rng.randrange(modulus)))
        elif op == "update":
            modulus = self.rng.randint(7, 23)
            self.db.run("UPDATE corpus SET s = s || ' %s' WHERE id %% %d = %d;"
                        % (MARKER, modulus, self.rng.randrange(modulus)))
        elif op == "tail_update":
            modulus = self.rng.randint(7, 23)
            self.db.run("UPDATE corpus SET s = s || ' %s' "
                        "WHERE rowid > (SELECT hwm_rowid FROM __ngram.registry WHERE index_id = '%s'::UUID) "
                        "AND id %% %d = %d;"
                        % (MARKER, self.db.index_ref(), modulus, self.rng.randrange(modulus)))
        elif op == "checkpoint":
            self.db.run("CHECKPOINT;")
        elif op == "refresh":
            roll = self.rng.random()
            if roll < 0.25:
                self.maintenance("PRAGMA ngram_refresh('corpus', col = 's');")
            elif roll < 0.65:
                # A bounded refresh, deliberately stopped part-way: one to three
                # increments of a bound too small to swallow the tail. The
                # invariants do not move — rows past the mark are tail, and the
                # tail scan is what makes a half-caught-up index exact — so an
                # interrupted catch-up is exactly as answerable as a finished
                # one, which is the claim worth churning.
                bound = self.rng.choice([1, 3, 17, 200])
                calls = self.rng.randint(1, 3)
                named = self.rng.random() < 0.5
                for _ in range(calls):
                    if named:
                        self.maintenance("PRAGMA ngram_refresh('corpus', max_rows = %d);" % bound)
                    else:
                        self.maintenance("PRAGMA ngram_refresh('corpus', %d);" % bound)
            else:
                self.maintenance("PRAGMA ngram_refresh('corpus');")
        elif op == "compact":
            self.maintenance("PRAGMA ngram_compact('corpus');")
        elif op == "compact_purge":
            self.maintenance("PRAGMA ngram_compact('corpus', purge = true);")
        return op

    def look_at_index(self):
        """What an operator sees between steps: the stats pragma's staleness
        verdict. Returns True when it reported the index unusable (and the
        harness has rebuilt in response)."""
        code, out, err = self.db.run("PRAGMA ngram_index_stats('corpus');", allow_error=True)
        if code != 0:
            self.failures.append("ngram_index_stats failed: %s" % err.strip()[-400:])
            return False
        stale = []
        for row in csv.reader(out.splitlines()):
            if len(row) == 13 and row[12] not in ("", "NULL"):
                stale.append(row[12])
                self.reasons[row[12][:60]] = self.reasons.get(row[12][:60], 0) + 1
        if stale:
            if self.strict:
                self.failures.append("false alarm: ngram_index_stats reports %r on an index nothing invalidated"
                                     % stale[0])
            else:
                self.detected += 1
                self.stale_states += 1
            self.rebuild()
        return bool(stale)

    def verify(self, round_no, op):
        code, out, err = self.db.run(verification_script(), allow_error=True)
        if code != 0:
            self.failures.append("round %d (%s): verification failed: %s" % (round_no, op, err.strip()[-400:]))
            self.look_at_index()
            return
        missing = 0
        for kind, needle, count in parse_counts(out):
            self.checks += 1
            if kind == "extra" and count:
                self.failures.append("round %d (%s): I1 violated, needle %r returned %d rows brute force does not"
                                     % (round_no, op, needle, count))
            elif kind == "missing":
                missing += count
        # Inspect maintenance health only after querying the pre-rebuild state:
        # an unsafe guard must take its exact fallback before stats triggers a
        # conservative rebuild.
        reported = self.look_at_index()
        if not missing:
            return
        if reported:
            self.failures.append("round %d (%s): %d rows missing before a detector-triggered rebuild"
                                 % (round_no, op, missing))
        else:
            self.failures.append("round %d (%s): I1 violated, %d matching rows were omitted"
                                 % (round_no, op, missing))


def main():
    global DUCKDB
    ap = argparse.ArgumentParser()
    ap.add_argument("--rounds", type=int, default=40)
    ap.add_argument("--rows", type=int, default=4000)
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--duckdb", default=DUCKDB)
    ap.add_argument("--no-stale-expected", action="store_true",
                    help="only run operations that keep an index valid, and fail on any detector verdict")
    args = ap.parse_args()
    DUCKDB = args.duckdb

    seed = args.seed if args.seed is not None else random.SystemRandom().randint(0, 2**31)
    print("master seed: %d%s" % (seed, " (no-stale-expected: any detector verdict fails)"
                                      if args.no_stale_expected else ""), flush=True)
    rng = random.Random(seed)

    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "churn.db")
        db = Db(path)
        # a small row-group size makes a few thousand rows span many row
        # groups, so deletes produce real vacuum merges
        # created through ATTACH so the row-group size can be set; it is stored
        # in the file, so every later process opening the path inherits it
        db.run("ATTACH %s AS churn (ROW_GROUP_SIZE %d);\nUSE churn;\n"
               "CREATE TABLE corpus(id BIGINT, s VARCHAR);" % (sql_quote(path), ROW_GROUP_SIZE),
               attached=False)
        db.run(rows_sql(rng, 0, args.rows))
        db.run("PRAGMA create_ngram_index('corpus', 's');")
        churn = Churn(db, rng, args.rows, strict=args.no_stale_expected)

        for round_no in range(args.rounds):
            op = churn.step(round_no)
            if os.environ.get("CHURN_VERBOSE"):
                print("  round %d: %s" % (round_no, op), flush=True)
            churn.verify(round_no, op)
            if (round_no + 1) % 10 == 0:
                print("  round %d: %d checks, %d failures, %d un-maintainable states (%d reported)"
                      % (round_no + 1, churn.checks, len(churn.failures), churn.stale_states, churn.detected),
                      flush=True)

        _, out, _ = db.run("SELECT count(*), (SELECT count(DISTINCT row_group_id) FROM pragma_storage_info('corpus')) "
                           "FROM corpus;")
        final = out.strip().splitlines()[-1] if out.strip() else "?"

    print("TOTAL: %d checks, %d failures, %d un-maintainable states "
          "(%d reported by a detector), ops %s, final rows/row-groups %s "
          "(master seed %d)"
          % (churn.checks, len(churn.failures), churn.stale_states, churn.detected,
             sorted(churn.ops.items()), final, seed))
    print("detector reasons: %s" % sorted(churn.reasons.items()))
    for f in churn.failures[:50]:
        print("FAIL:", f)
    return 1 if churn.failures else 0


if __name__ == "__main__":
    sys.exit(main())
