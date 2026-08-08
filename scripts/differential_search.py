#!/usr/bin/env python3
"""Property-based differential harness for the ngram explicit query path.

For each trial, builds a randomized corpus (alphabet, size, gram size, case
sensitivity, NULL/empty fractions all randomized), indexes it, and checks that

    SELECT * FROM ngram_search(t, needle) == SELECT * FROM t WHERE <recheck predicate>

as a multiset (rows carry a unique id), plus the candidates superset invariant

    ngram_candidates(t, col, needle) >= true matches among indexed rows

for randomized needles: substrings sampled from real rows (guaranteed matches,
lengths crossing the gram boundary), random non-matching strings, gram-splice
traps (grams present in the corpus, substring likely absent), case-flipped
variants, and the empty needle. Each corpus is rechecked after tail inserts
(past the high-water mark), after deletes, and inside an uncommitted writing
transaction.

Usage: python3 scripts/differential_search.py [--trials N] [--rows N] [--seed S]
Exit code 0 iff every check passes.
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
NULL_SENTINEL = "__NGRAM_NULL__"

ALPHABETS = {
    "hex": "0123456789abcdef",
    "letters": "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ ",
    "dense": "ab ",
    "unicode": "abcĲĳÖöẞßΣσς füt ",
    "words": None,  # word-based corpus
}

WORDS = [
    "tent", "ten", "cent", "often", "entered", "content", "contentment",
    "connection", "reset", "peer", "Tent", "TENT", "öft", "ÖFT", "sträße",
    "needle", "haystack", "gram", "index", "duck", "db", "row", "tail",
]


def sql_quote(s):
    return "'" + s.replace("'", "''") + "'"


def make_row(rng, alphabet_name, rows):
    r = rng.random()
    if r < 0.04:
        return None
    if r < 0.08:
        return ""
    if alphabet_name == "words":
        n = rng.randint(1, 8)
        return " ".join(rng.choice(WORDS) for _ in range(n))
    alphabet = ALPHABETS[alphabet_name]
    n = rng.randint(1, 40)
    return "".join(rng.choice(alphabet) for _ in range(n))


def make_needles(rng, corpus, gram_size, count):
    needles = set()
    non_null = [s for s in corpus if s]
    # substrings of real rows: guaranteed matches at assorted lengths,
    # including exactly gram_size-1 (short-needle fallback) and gram_size
    for _ in range(count):
        s = rng.choice(non_null)
        cps = list(s)
        if not cps:
            continue
        for length in (gram_size - 1, gram_size, gram_size + 1, rng.randint(1, 12)):
            if length < 1 or length > len(cps):
                continue
            start = rng.randint(0, len(cps) - length)
            needles.add("".join(cps[start:start + length]))
    # gram splices: two real substrings concatenated — the grams exist
    # somewhere, the concatenation usually does not (false-positive traps)
    for _ in range(count):
        a = rng.choice(non_null)
        b = rng.choice(non_null)
        if len(a) >= gram_size and len(b) >= gram_size:
            i = rng.randint(0, len(a) - gram_size)
            j = rng.randint(0, len(b) - gram_size)
            needles.add(a[i:i + gram_size] + b[j:j + gram_size])
    # random strings, mostly absent
    for _ in range(count // 2):
        n = rng.randint(1, 10)
        needles.add("".join(rng.choice("abcxyz019ö") for _ in range(n)))
    # case flips (slice a sorted copy: set order varies with PYTHONHASHSEED and
    # would break reproducing a run from its printed seed)
    ordered = sorted(needles)
    needles.update(n.upper() for n in ordered[:count // 2])
    needles.update(n.lower() for n in ordered[:count // 2])
    needles.add("")
    # drop anything with characters that complicate CSV/SQL transport
    return sorted(n for n in needles if "\n" not in n and "\r" not in n)[:120]


def differential_sql(needle, case_insensitive):
    q = sql_quote(needle)
    if case_insensitive:
        pred = "contains(lower(s), lower(%s))" % q
    else:
        pred = "contains(s, %s)" % q
    return (
        "SELECT 'diff', %s, count(*) FROM ("
        "(SELECT * FROM ngram_search('corpus', %s) EXCEPT SELECT * FROM corpus WHERE %s)"
        " UNION ALL "
        "(SELECT * FROM corpus WHERE %s EXCEPT SELECT * FROM ngram_search('corpus', %s)));"
        % (q, q, pred, pred, q)
    )


def candidates_sql(needle, case_insensitive):
    q = sql_quote(needle)
    if case_insensitive:
        pred = "contains(lower(s), lower(%s))" % q
    else:
        pred = "contains(s, %s)" % q
    return (
        "SELECT 'cand', %s, count(*) FROM ("
        "SELECT rowid FROM corpus WHERE %s AND rowid <= (SELECT hwm_rowid FROM ngram_main_corpus.meta_s)"
        " EXCEPT SELECT rowid FROM ngram_candidates('corpus', 's', %s));"
        % (q, pred, q)
    )


def run_duckdb(db_path, script):
    proc = subprocess.run(
        [DUCKDB, db_path],
        input=".headers off\n.mode csv\n" + script,
        capture_output=True, text=True, timeout=600,
    )
    if proc.returncode != 0:
        raise RuntimeError("duckdb failed:\n%s" % proc.stderr[-4000:])
    return proc.stdout


def check_output(out, label, failures):
    ok = 0
    for line in csv.reader(out.splitlines()):
        if not line or line[0] not in ("diff", "cand"):
            continue
        kind, needle, mismatches = line[0], line[1], int(line[2])
        if mismatches != 0:
            failures.append("%s %s: needle %r -> %d mismatching rows" % (label, kind, needle, mismatches))
        else:
            ok += 1
    return ok


def one_trial(trial, rng, rows):
    alphabet = rng.choice(list(ALPHABETS))
    gram_size = rng.choice([2, 3, 3, 4])
    case_insensitive = rng.random() < 0.6
    n_rows = rng.randint(max(rows // 4, 200), rows)
    print("trial %d: alphabet=%s gram=%d case_insensitive=%s rows=%d"
          % (trial, alphabet, gram_size, case_insensitive, n_rows), flush=True)

    corpus = [make_row(rng, alphabet, n_rows) for _ in range(n_rows)]
    tail = [make_row(rng, alphabet, n_rows) for _ in range(n_rows // 3)]
    needles = make_needles(rng, corpus, gram_size, 25)

    failures = []
    checks = 0
    with tempfile.TemporaryDirectory() as tmp:
        csv_path = os.path.join(tmp, "corpus.csv")
        with open(csv_path, "w", newline="") as f:
            w = csv.writer(f)
            for i, s in enumerate(corpus):
                w.writerow([i, NULL_SENTINEL if s is None else s])
        db_path = os.path.join(tmp, "trial.db")

        setup = [
            "CREATE TABLE corpus(id BIGINT, s VARCHAR);",
            "COPY corpus FROM %s (FORMAT csv, HEADER false, NULL %s);"
            % (sql_quote(csv_path), sql_quote(NULL_SENTINEL)),
        ]
        run_duckdb(db_path, "\n".join(setup))
        run_duckdb(db_path, "PRAGMA create_ngram_index('corpus', 's', gram = %d, case_insensitive = %s);"
                   % (gram_size, "true" if case_insensitive else "false"))

        phase_checks = [differential_sql(n, case_insensitive) for n in needles]
        phase_checks += [candidates_sql(n, case_insensitive) for n in needles if len(n) >= gram_size]

        # phase 1: committed, fully indexed corpus
        checks += check_output(run_duckdb(db_path, "\n".join(phase_checks)), "committed", failures)

        # phase 2: tail rows past the high-water mark
        inserts = ["INSERT INTO corpus VALUES (%d, %s);"
                   % (len(corpus) + i, "NULL" if s is None else sql_quote(s)) for i, s in enumerate(tail)]
        run_duckdb(db_path, "\n".join(inserts))
        checks += check_output(run_duckdb(db_path, "\n".join(phase_checks)), "tail", failures)

        # phase 3: inside an uncommitted writing transaction (insert + delete)
        txn = ["BEGIN;",
               "INSERT INTO corpus SELECT 10000000 + id, s FROM corpus WHERE s IS NOT NULL LIMIT 500;",
               "DELETE FROM corpus WHERE id % 5 = 2;"]
        txn += [differential_sql(n, case_insensitive) for n in needles[:40]]
        txn.append("ROLLBACK;")
        checks += check_output(run_duckdb(db_path, "\n".join(txn)), "in-txn", failures)

        # phase 4: deletes across indexed and tail rows, checked in the same
        # session. Identity holds for deletes until a checkpoint vacuums them:
        # vacuum merges row groups, squeezing out deleted gaps and MOVING the
        # rowids of surviving rows, which strands the index's postings until
        # PRAGMA ngram_refresh (Phase 5). Closing the database checkpoints, so
        # the deletes and their checks must share one session.
        deleted = ["DELETE FROM corpus WHERE id % 3 = 1;"] + phase_checks
        checks += check_output(run_duckdb(db_path, "\n".join(deleted)), "deleted", failures)

        # phase 5: after close+reopen the deletes above have been vacuumed and
        # surviving rowids may have moved. The index can now legitimately miss
        # moved rows (the Phase 5 refresh contract), but recheck must still
        # guarantee zero false positives: search results ⊆ brute force, always.
        subset = []
        for n in needles:
            q = sql_quote(n)
            if case_insensitive:
                pred = "contains(lower(s), lower(%s))" % q
            else:
                pred = "contains(s, %s)" % q
            subset.append(
                "SELECT 'diff', %s, count(*) FROM "
                "(SELECT * FROM ngram_search('corpus', %s) EXCEPT SELECT * FROM corpus WHERE %s);"
                % (q, q, pred))
        checks += check_output(run_duckdb(db_path, "\n".join(subset)), "post-vacuum-subset", failures)

    return checks, failures


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trials", type=int, default=8)
    ap.add_argument("--rows", type=int, default=5000)
    ap.add_argument("--seed", type=int, default=None)
    args = ap.parse_args()

    seed = args.seed if args.seed is not None else random.SystemRandom().randint(0, 2**31)
    print("master seed: %d" % seed, flush=True)
    rng = random.Random(seed)

    total_checks = 0
    all_failures = []
    for trial in range(args.trials):
        checks, failures = one_trial(trial, rng, args.rows)
        total_checks += checks
        all_failures.extend(failures)
        print("  %d checks, %d failures" % (checks, len(failures)), flush=True)

    print("TOTAL: %d checks, %d failures (master seed %d)" % (total_checks, len(all_failures), seed))
    for f in all_failures[:50]:
        print("FAIL:", f)
    return 1 if all_failures else 0


if __name__ == "__main__":
    sys.exit(main())
