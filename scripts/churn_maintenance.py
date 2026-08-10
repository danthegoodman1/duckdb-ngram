#!/usr/bin/env python3
"""Churn harness for ngram index maintenance.

Interleaves INSERT / DELETE / UPDATE / CHECKPOINT / PRAGMA ngram_refresh
(bounded and unbounded) / PRAGMA ngram_compact against a live index, reopening
the database between every step (each step runs in its own duckdb process), and
verifies the maintenance contract after every step:

  I1  no false positives, ever: ngram_search results are a subset of the
      brute-force answer in every state, however stale the index is. A
      violation is always a bug.

  I2  exhaustive results whenever the contract promises them: after any
      sequence of appends, deletes, refreshes and compactions, the index path
      must return exactly what brute force returns.

  I3  staleness is surfaced, not silent: a state the index cannot answer
      exhaustively must be reported by a detector. The harness looks at the
      index between steps the way an operator would — PRAGMA ngram_index_stats
      reports stale_reason, and the maintenance pragmas refuse — and rebuilds
      when told to. A state that is neither exhaustive nor reported is a
      failure, except for the one gap the contract documents as undetectable
      (an in-place UPDATE that no checkpoint has folded into the storage of an
      earlier row group), which is counted and reported.

The corpus is created with a small row-group size so that a few thousand rows
span many row groups: that is what makes deletes trigger real vacuum merges,
and what gives the row-group and column-storage detectors something to see.

--no-stale-expected turns the harness into a false-alarm gate instead. It drops
the operations that legitimately invalidate an index — deletes, which the next
checkpoint vacuums out from under the index's postings once refresh has carried
the mark past them, and in-place updates below the mark — but keeps an in-place
update confined to rows past the mark, which cannot invalidate anything. Then
it FAILS on any detector verdict at all: a refusal, a query error, or a
stale_reason in the stats pragma. Appending, refreshing, compacting, and
mutating rows the index does not cover must never make a detector fire, and the
ordinary mode cannot notice if they start to, because it treats every verdict
as legitimate and rebuilds.

Usage: python3 scripts/churn_maintenance.py [--rounds N] [--rows N] [--seed S]
                                            [--duckdb PATH] [--no-stale-expected]
Exit code 0 iff every check passes. Seeds are printed and reproducible;
iteration is sorted, so runs do not depend on PYTHONHASHSEED.
"""

import argparse
import csv
import os
import random
import subprocess
import sys
import tempfile

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DUCKDB = os.path.join(REPO, "build", "release", "duckdb")
ROW_GROUP_SIZE = 2048
# words the rows are built from; UPDATE splices in MARKER, which no generated
# row contains, so an unrepaired in-place update is always visible as a miss
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


def rows_sql(rng, start, count):
    values = []
    for i in range(count):
        n = rng.randint(1, 6)
        text = " ".join(rng.choice(WORDS) for _ in range(n))
        values.append("(%d, %s)" % (start + i, sql_quote(text)))
    return "INSERT INTO corpus VALUES %s;" % ", ".join(values)


def verification_script():
    """Per needle: rows the index path returns that brute force does not (I1),
    and rows brute force returns that the index path does not (I2)."""
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
        #! an in-place UPDATE happened; only a rebuild can repair it
        self.updated_in_place = False
        self.stale_states = 0
        self.detected = 0
        self.undetected_updates = 0
        self.ops = {}
        self.reasons = {}

    def rebuild(self):
        self.db.run("PRAGMA drop_ngram_index('corpus', 's');\n"
                    "PRAGMA create_ngram_index('corpus', 's');")
        self.updated_in_place = False

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
            # a delete is vacuumed by the next checkpoint (and every step here
            # closes the database), and an in-place update is unrepairable:
            # both legitimately invalidate the index
            ops += ["delete", "update"]
        else:
            # An in-place UPDATE confined to rows past the high-water mark.
            # Those rows are not indexed — every query reads them by scanning —
            # and v1.5.5 updates in place (verified: row count, max rowid and
            # the updated row's own rowid are unchanged across the update and
            # the checkpoint after it), so this leaves no gap for a vacuum to
            # close and cannot invalidate anything. Any detector verdict after
            # it is a false alarm. Without it, strict mode drops mutation
            # entirely and structurally cannot reach the state where a false
            # alarm lives, which is how a bounded refresh recording witnesses
            # over tail rows got past this harness once already.
            #
            # A tail-confined DELETE is deliberately NOT here: refresh advances
            # the mark over deleted rowids on purpose (otherwise an increment of
            # nothing but deleted rows would stall a bounded loop), so the gap
            # ends up below the mark, and the next checkpoint's vacuum then
            # shifts indexed rows down out from under the index's postings.
            # That is genuine staleness — the documented "rebuild after a
            # checkpoint has vacuumed deleted rows" case — and it belongs in the
            # ordinary mode, where it already is.
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
            self.updated_in_place = True
        elif op == "tail_update":
            modulus = self.rng.randint(7, 23)
            self.db.run("UPDATE corpus SET s = s || ' %s' "
                        "WHERE rowid > (SELECT hwm_rowid FROM ngram_main_corpus.meta_s) AND id %% %d = %d;"
                        % (MARKER, modulus, self.rng.randrange(modulus)))
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
        reported = self.look_at_index()
        code, out, err = self.db.run(verification_script(), allow_error=True)
        if code != 0:
            if "is stale and cannot be used" in err:
                if self.strict:
                    self.failures.append("round %d (%s): false alarm, the query path refused on an index nothing "
                                         "invalidated: %s" % (round_no, op, err.strip()[-300:]))
                else:
                    # the query path itself refused: also a detection
                    self.detected += 1
                    self.stale_states += 1
                self.rebuild()
                return
            self.failures.append("round %d (%s): verification failed: %s" % (round_no, op, err.strip()[-400:]))
            return
        missing = 0
        for kind, needle, count in parse_counts(out):
            self.checks += 1
            if kind == "extra" and count:
                self.failures.append("round %d (%s): I1 violated, needle %r returned %d rows brute force does not"
                                     % (round_no, op, needle, count))
            elif kind == "missing":
                missing += count
        if not missing:
            return
        if reported:
            self.failures.append("round %d (%s): %d rows missing after a rebuild the detector asked for"
                                 % (round_no, op, missing))
        elif self.strict:
            self.failures.append("round %d (%s): I2 violated, %d rows missing in a run that only appended"
                                 % (round_no, op, missing))
        elif self.updated_in_place:
            # the documented gap: an in-place UPDATE the storage detector could
            # not see yet (no checkpoint has folded it into an earlier row
            # group). Only a rebuild repairs it, which is what the contract
            # tells the user to do.
            self.undetected_updates += 1
            self.stale_states += 1
            self.rebuild()
        else:
            self.failures.append("round %d (%s): I2 violated, %d rows missing with no detector firing"
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
          "(%d reported by a detector, %d undetected in-place updates), ops %s, final rows/row-groups %s "
          "(master seed %d)"
          % (churn.checks, len(churn.failures), churn.stale_states, churn.detected,
             churn.undetected_updates, sorted(churn.ops.items()), final, seed))
    print("detector reasons: %s" % sorted(churn.reasons.items()))
    for f in churn.failures[:50]:
        print("FAIL:", f)
    return 1 if churn.failures else 0


if __name__ == "__main__":
    sys.exit(main())
