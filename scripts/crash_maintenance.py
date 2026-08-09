#!/usr/bin/env python3
"""Crash-interruption test for ngram index maintenance.

PRAGMA ngram_refresh and PRAGMA ngram_compact expand to several statements,
which duckdb's statement preprocessor wraps in one BEGIN/COMMIT, so a crash
must leave the index either wholly before or wholly after the operation. This
script checks that the hard way: it runs the pragma in a child duckdb process,
kills it with SIGKILL at an offset spread across the operation's measured
duration, reopens the database, and verifies that

  * the database opens and the index tables are readable (WAL replay works),
  * the recorded state is exactly the pre-operation or the post-operation one
    (no half-applied generation, no advanced high-water mark without the
    postings that justify it),
  * ngram_search still equals brute force for every needle, whichever state
    survived.

Each kill runs against a fresh copy of the same pristine database, so the
offsets are comparable and a failure is reproducible from the printed offset.
The timing of a kill is inherently not deterministic — the point of the sweep
is to hit many different phases of the operation, not the same one twice.

Usage: python3 scripts/crash_maintenance.py [--kills N] [--rows N] [--seed S]
                                            [--duckdb PATH] [--keep]
Exit code 0 iff every reopened database passed.
"""

import argparse
import os
import random
import shutil
import signal
import subprocess
import sys
import tempfile
import time

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DUCKDB = os.path.join(REPO, "build", "release", "duckdb")
WORDS = ["tent", "ten", "cent", "often", "entered", "content", "connection",
         "reset", "peer", "needle", "haystack", "gram", "index", "duck", "row"]
NEEDLES = ["tent", "ten", "connection", "gram", "xyzzy"]


def sql_quote(s):
    return "'" + s.replace("'", "''") + "'"


def run(db, script, allow_error=False, timeout=900):
    proc = subprocess.run([DUCKDB, db], input=".headers off\n.mode csv\n" + script,
                          capture_output=True, text=True, timeout=timeout)
    if proc.returncode != 0 and not allow_error:
        raise RuntimeError("duckdb failed on %s:\n%s" % (db, proc.stderr[-3000:]))
    return proc.returncode, proc.stdout, proc.stderr


def build_corpus(db, rng, rows):
    values = []
    for i in range(rows):
        text = " ".join(rng.choice(WORDS) for _ in range(rng.randint(2, 8)))
        values.append("(%d, %s)" % (i, sql_quote(text)))
    run(db, "CREATE TABLE corpus(id BIGINT, s VARCHAR);\nINSERT INTO corpus VALUES %s;" % ", ".join(values))
    run(db, "PRAGMA create_ngram_index('corpus', 's');")


def append_tail(db, rng, start, rows):
    values = []
    for i in range(rows):
        text = " ".join(rng.choice(WORDS) for _ in range(rng.randint(2, 8)))
        values.append("(%d, %s)" % (start + i, sql_quote(text)))
    run(db, "INSERT INTO corpus VALUES %s;" % ", ".join(values))


def index_state(db):
    """The facts a half-applied maintenance operation would disagree on."""
    _, out, _ = run(db, "SELECT (SELECT hwm_rowid FROM ngram_main_corpus.meta_s), "
                        "(SELECT count(*) FROM ngram_main_corpus.segments_s), "
                        "(SELECT coalesce(sum(rowid_count), 0) FROM ngram_main_corpus.segments_s), "
                        "(SELECT count(*) FROM ngram_main_corpus.stats_s);")
    line = [l for l in out.strip().splitlines() if l]
    return line[-1] if line else None


def differential(db):
    """Rows the index path and brute force disagree on, both directions."""
    script = []
    for needle in NEEDLES:
        q = sql_quote(needle)
        pred = "contains(lower(s), lower(%s))" % q
        script.append("SELECT count(*) FROM ("
                      "(SELECT * FROM ngram_search('corpus', %s) EXCEPT SELECT * FROM corpus WHERE %s) UNION ALL "
                      "(SELECT * FROM corpus WHERE %s EXCEPT SELECT * FROM ngram_search('corpus', %s)));"
                      % (q, pred, pred, q))
    code, out, err = run(db, "\n".join(script), allow_error=True)
    if code != 0:
        return None, err.strip()[-300:]
    return sum(int(l) for l in out.strip().splitlines() if l.strip().isdigit()), None


def timed_pragma(db, pragma):
    start = time.time()
    run(db, pragma)
    return time.time() - start


def kill_during(db, pragma, delay):
    """Start the pragma in a child process and SIGKILL it after `delay`."""
    proc = subprocess.Popen([DUCKDB, db], stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL, text=True)
    proc.stdin.write(pragma + "\n")
    proc.stdin.flush()
    time.sleep(delay)
    killed = proc.poll() is None
    if killed:
        os.kill(proc.pid, signal.SIGKILL)
    else:
        proc.stdin.close()
    proc.wait(timeout=60)
    return killed


def main():
    global DUCKDB
    ap = argparse.ArgumentParser()
    ap.add_argument("--kills", type=int, default=12)
    ap.add_argument("--rows", type=int, default=60000)
    ap.add_argument("--tail", type=int, default=20000)
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--duckdb", default=DUCKDB)
    ap.add_argument("--keep", action="store_true", help="keep the working directory")
    args = ap.parse_args()
    DUCKDB = args.duckdb

    seed = args.seed if args.seed is not None else random.SystemRandom().randint(0, 2**31)
    print("master seed: %d" % seed, flush=True)
    rng = random.Random(seed)

    tmp = tempfile.mkdtemp(prefix="ngram-crash-")
    failures = []
    try:
        for pragma in ("PRAGMA ngram_refresh('corpus');", "PRAGMA ngram_compact('corpus', purge = true);"):
            pristine = os.path.join(tmp, "pristine.db")
            for suffix in ("", ".wal"):
                if os.path.exists(pristine + suffix):
                    os.remove(pristine + suffix)
            build_corpus(pristine, rng, args.rows)
            append_tail(pristine, rng, args.rows, args.tail)
            if "compact" in pragma:
                # give compaction something to merge: a refreshed tail leaves a
                # second generation of segment rows behind
                run(pristine, "PRAGMA ngram_refresh('corpus');")
                append_tail(pristine, rng, args.rows + args.tail, args.tail)
                run(pristine, "PRAGMA ngram_refresh('corpus');")
            before = index_state(pristine)

            # measure the uninterrupted operation on a copy, to spread the kills
            timing_db = os.path.join(tmp, "timing.db")
            shutil.copyfile(pristine, timing_db)
            duration = timed_pragma(timing_db, pragma)
            after = index_state(timing_db)
            print("%s: uninterrupted %.3fs, state before=%s after=%s" % (pragma, duration, before, after), flush=True)
            if before == after:
                failures.append("%s does not change the index state; the crash test would be vacuous" % pragma)
                continue

            for i in range(args.kills):
                # spread the kills across the operation, plus a couple past its
                # end (which land in commit / shutdown)
                delay = duration * (i + 1) / max(args.kills - 2, 1)
                work = os.path.join(tmp, "kill_%d.db" % i)
                shutil.copyfile(pristine, work)
                killed = kill_during(work, pragma, delay)
                code, out, err = run(work, "SELECT 1;", allow_error=True)
                if code != 0:
                    failures.append("%s kill@%.3fs: database does not reopen: %s" % (pragma, delay, err[-300:]))
                    continue
                state = index_state(work)
                if state not in (before, after):
                    failures.append("%s kill@%.3fs: index state %s is neither pre (%s) nor post (%s)"
                                    % (pragma, delay, state, before, after))
                mismatches, error = differential(work)
                if error is not None:
                    failures.append("%s kill@%.3fs: search failed after reopen: %s" % (pragma, delay, error))
                elif mismatches:
                    failures.append("%s kill@%.3fs: %d rows differ from brute force after reopen"
                                    % (pragma, delay, mismatches))
                print("  kill@%.3fs killed=%s state=%s -> %s" %
                      (delay, killed, "pre" if state == before else ("post" if state == after else state),
                       "ok" if not error and not mismatches else "FAILED"), flush=True)
                if not args.keep:
                    for suffix in ("", ".wal"):
                        if os.path.exists(work + suffix):
                            os.remove(work + suffix)
    finally:
        if not args.keep:
            shutil.rmtree(tmp, ignore_errors=True)
        else:
            print("working directory kept at %s" % tmp)

    print("TOTAL: %d failures (master seed %d)" % (len(failures), seed))
    for f in failures:
        print("FAIL:", f)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
