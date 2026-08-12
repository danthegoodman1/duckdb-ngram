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

The bounded scenario is the same question asked of a catch-up loop: a child
process runs a sequence of PRAGMA ngram_refresh(table, max_rows) calls and is
killed part-way through it. There the surviving state must be one of the
increment boundaries the uninterrupted loop passes through — never a partial
one, and never behind the increment before the kill — and resuming the loop
afterwards must land on exactly the index the uninterrupted loop produced,
postings and all. That is what "a crash costs one increment" means, measured
rather than asserted.

Each kill runs against a fresh copy of the same pristine database, so the
offsets are comparable and a failure is reproducible from the printed offset.
The timing of a kill is inherently not deterministic — the point of the sweep
is to hit many different phases of the operation, not the same one twice.

Usage: python3 scripts/crash_maintenance.py [--kills N] [--rows N] [--tail N]
                                            [--bound N] [--seed S]
                                            [--duckdb PATH] [--keep]
Exit code 0 iff every reopened database passed.
"""

import argparse
import csv
import os
import random
import re
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


def storage_schema(db):
    _, catalog_out, _ = run(db, "SELECT current_database();")
    catalog = next(csv.reader(catalog_out.splitlines()))[0]
    _, out, _ = run(db, "PRAGMA ngram_indexes;")
    rows = [row for row in csv.reader(out.splitlines())
            if len(row) == 10 and row[0] == catalog and row[1] == "registered"
            and row[3:6] == ["main", "corpus", "s"] and row[7] == "3"
            and row[8] in ("READY", "SCAN_ONLY")]
    if len(rows) != 1:
        raise RuntimeError("expected one canonical registered corpus.s allocation")
    index_ref, schema = rows[0][2], rows[0][6]
    if (not re.fullmatch(r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}", index_ref)
            or not re.fullmatch(r"__ngram_idx_[0-9a-f]{8}_[0-9a-f]{4}_4[0-9a-f]{3}_[89ab][0-9a-f]{3}_[0-9a-f]{12}", schema)
            or schema != "__ngram_idx_" + index_ref.replace("-", "_")):
        raise RuntimeError("public corpus.s allocation identity is not canonical")
    return schema


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
    storage = storage_schema(db)
    _, out, _ = run(db, "SELECT (SELECT hwm_rowid FROM {0}.meta), "
                        "(SELECT count(*) FROM {0}.segments), "
                        "(SELECT coalesce(sum(rowid_count), 0) FROM {0}.segments), "
                        "s.n, s.hash_sum, s.hash_xor FROM (SELECT count(*) AS n, "
                        "coalesce(sum(hash(encode(gram), row_count, segment_count))::VARCHAR, '0') AS hash_sum, "
                        "coalesce(bit_xor(hash(encode(gram), row_count, segment_count))::VARCHAR, '0') AS hash_xor "
                        "FROM {0}.stats) s;".format(storage))
    line = [l for l in out.strip().splitlines() if l]
    return line[-1] if line else None


def postings_digest(db):
    """The decoded index itself: every (gram, rowid) posting, summarised so two
    databases can be compared without materialising both."""
    storage = storage_schema(db)
    _, out, _ = run(db, "SELECT count(*), coalesce(sum(hash(gram || ':' || r))::VARCHAR, '0') "
                        "FROM ngram_unpack_postings("
                        "(SELECT gram, segment_no, postings FROM {0}.segments));".format(storage))
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


def bounded_scenario(tmp, rng, args, failures):
    """Kill a bounded catch-up loop part-way through and make it account for
    what it lost: the surviving state must be one the uninterrupted loop passes
    through, and resuming must reach the very index the uninterrupted loop
    reached."""
    pristine = os.path.join(tmp, "bounded_pristine.db")
    for suffix in ("", ".wal"):
        if os.path.exists(pristine + suffix):
            os.remove(pristine + suffix)
    build_corpus(pristine, rng, args.rows)
    append_tail(pristine, rng, args.rows, args.tail)
    # two extra calls so the loop provably ends with an empty tail, the way a
    # caller looping on remaining_tail would
    calls = (args.tail + args.bound - 1) // args.bound + 2
    script = "PRAGMA ngram_refresh('corpus', %d);\n" % args.bound * calls

    # every state the uninterrupted loop passes through, increment by increment
    ref = os.path.join(tmp, "bounded_ref.db")
    shutil.copyfile(pristine, ref)
    states = [index_state(ref)]
    for _ in range(calls):
        run(ref, "PRAGMA ngram_refresh('corpus', %d);" % args.bound)
        state = index_state(ref)
        if state != states[-1]:
            states.append(state)
    final_state = states[-1]
    final_digest = postings_digest(ref)

    # and the wall time of that loop run inside one process, to spread the kills
    timing_db = os.path.join(tmp, "bounded_timing.db")
    shutil.copyfile(pristine, timing_db)
    duration = timed_pragma(timing_db, script)
    print("bounded loop (max_rows=%d, %d calls): uninterrupted %.3fs, %d distinct states"
          % (args.bound, calls, duration, len(states)), flush=True)
    if len(states) < 3:
        failures.append("the bounded loop passes through %d states; the crash test would be vacuous" % len(states))
        return

    for i in range(args.kills):
        delay = duration * (i + 1) / max(args.kills - 2, 1)
        work = os.path.join(tmp, "bkill_%d.db" % i)
        shutil.copyfile(pristine, work)
        killed = kill_during(work, script, delay)
        code, out, err = run(work, "SELECT 1;", allow_error=True)
        if code != 0:
            failures.append("bounded kill@%.3fs: database does not reopen: %s" % (delay, err[-300:]))
            continue
        state = index_state(work)
        where = states.index(state) if state in states else None
        if where is None:
            failures.append("bounded kill@%.3fs: index state %s is not an increment boundary of the loop (%s)"
                            % (delay, state, states))
        mismatches, error = differential(work)
        if error is not None:
            failures.append("bounded kill@%.3fs: search failed after reopen: %s" % (delay, error))
        elif mismatches:
            failures.append("bounded kill@%.3fs: %d rows differ from brute force after reopen" % (delay, mismatches))
        # resume the loop from wherever the crash left it
        run(work, script)
        resumed, resumed_digest = index_state(work), postings_digest(work)
        if resumed != final_state:
            failures.append("bounded kill@%.3fs: resumed loop ends at %s, not at %s"
                            % (delay, resumed, final_state))
        if resumed_digest != final_digest:
            failures.append("bounded kill@%.3fs: resumed postings %s differ from uninterrupted %s"
                            % (delay, resumed_digest, final_digest))
        after_mismatches, after_error = differential(work)
        if after_error is not None or after_mismatches:
            failures.append("bounded kill@%.3fs: after resuming, %s"
                            % (delay, after_error or ("%d rows differ from brute force" % after_mismatches)))
        print("  kill@%.3fs killed=%s survived at increment %s/%d -> resumed %s" %
              (delay, killed, "?" if where is None else where, len(states) - 1,
               "ok" if resumed == final_state and resumed_digest == final_digest else "FAILED"), flush=True)
        if not args.keep:
            for suffix in ("", ".wal"):
                if os.path.exists(work + suffix):
                    os.remove(work + suffix)


def main():
    global DUCKDB
    ap = argparse.ArgumentParser()
    ap.add_argument("--kills", type=int, default=12)
    ap.add_argument("--rows", type=int, default=60000)
    ap.add_argument("--tail", type=int, default=20000)
    ap.add_argument("--bound", type=int, default=2000,
                    help="max_rows for the bounded catch-up loop scenario")
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
        for pragma in ("PRAGMA ngram_refresh('corpus');", "PRAGMA ngram_compact('corpus');",
                       "PRAGMA ngram_compact('corpus', purge = true);"):
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
        bounded_scenario(tmp, rng, args, failures)
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
