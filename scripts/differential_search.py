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
(past the high-water mark), after below-HWM indexed-column updates, after
deletes/vacuum, and inside an uncommitted writing transaction.

With --transparent the harness exercises the Phase 4 optimizer rewrite
instead of the explicit functions: every needle becomes plain
`LIKE '%needle%'`, `contains(...)`, and `ILIKE '%needle%'` queries run with
`ngram_auto_accelerate=true`, compared against the same query executed under
`SET disabled_optimizers='extension'` (the built-in kill switch). Needles may
contain LIKE wildcards; both sides interpret them identically, which doubles
as coverage for multi-segment patterns and the `_` fallback. Each trial also
asserts via EXPLAIN that the rewrite actually fires for a probeable needle,
so the comparison can never go vacuously green. The candidate-fraction gate
is randomized per trial (default 0.05 vs 1.0) to exercise both the index
path and the in-scan fallback.

Usage: python3 scripts/differential_search.py [--trials N] [--rows N] [--seed S]
                                              [--transparent]
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
UPDATE_MARKER = "QzPhaseElevenMarker917"


def set_duckdb_binary(path):
    global DUCKDB
    DUCKDB = path

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


def make_patterns(rng, needles, count):
    """LIKE patterns for transparent mode: plain needles plus synthesized
    wildcard patterns (multi-segment '%', single-char '_'). Both sides of the
    comparison run the same query, so any pattern is safe to throw at it."""
    plain = [n for n in needles if n][:count]
    patterns = []
    for n in plain:
        patterns.append(("plain", n))
    for _ in range(max(count // 3, 4)):
        if len(plain) < 2:
            break
        a, b = rng.choice(plain), rng.choice(plain)
        patterns.append(("multiseg", a + "%" + b))
    for _ in range(max(count // 4, 3)):
        n = rng.choice(plain)
        if len(n) < 2:
            continue
        i = rng.randrange(len(n))
        patterns.append(("underscore", n[:i] + "_" + n[i + 1:]))
    return patterns


def transparent_checks(patterns):
    """Per pattern, compare accelerated vs disabled_optimizers='extension'
    execution of the same plain query, for LIKE / contains / ILIKE forms.
    Session settings (ngram_auto_accelerate etc.) are the caller's business."""
    statements = []
    for kind, pattern in patterns:
        forms = [("like", "s LIKE %s" % sql_quote("%" + pattern + "%")),
                 ("ilike", "s ILIKE %s" % sql_quote("%" + pattern + "%"))]
        if kind == "plain":
            forms.append(("contains", "contains(s, %s)" % sql_quote(pattern)))
        for form, pred in forms:
            tag = "%s/%s:%s" % (form, kind, pattern)
            statements += [
                "SET disabled_optimizers='';",
                "CREATE OR REPLACE TEMP TABLE r_acc AS SELECT * FROM corpus WHERE %s;" % pred,
                "SET disabled_optimizers='extension';",
                "CREATE OR REPLACE TEMP TABLE r_plain AS SELECT * FROM corpus WHERE %s;" % pred,
            ]
            statements.append(
                "SELECT 'diff', %s, count(*) FROM ((TABLE r_acc EXCEPT TABLE r_plain)"
                " UNION ALL (TABLE r_plain EXCEPT TABLE r_acc));" % sql_quote(tag))
    statements.append("SET disabled_optimizers='';")
    return statements


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


def one_trial(trial, rng, rows, transparent):
    alphabet = rng.choice(list(ALPHABETS))
    gram_size = rng.choice([2, 3, 3, 4])
    case_insensitive = rng.random() < 0.6
    n_rows = rng.randint(max(rows // 4, 200), rows)
    # transparent mode randomizes the candidate-fraction gate: the default
    # (0.05) trips often on small corpora (exercising the in-scan fallback),
    # 1.0 keeps the index path hot
    gate = rng.choice(["default", "off"]) if transparent else None
    print("trial %d: alphabet=%s gram=%d case_insensitive=%s rows=%d%s"
          % (trial, alphabet, gram_size, case_insensitive, n_rows,
             " transparent(gate=%s)" % gate if transparent else ""), flush=True)

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

        if transparent:
            session_settings = ["SET ngram_auto_accelerate=true;"]
            if gate == "off":
                session_settings.append("SET ngram_max_candidate_fraction=1.0;")
            patterns = make_patterns(rng, needles, 25)
            phase_checks = session_settings + transparent_checks(patterns)
            txn_checks = session_settings + transparent_checks(patterns[:8])

            # the rewrite must actually fire for a probeable needle, or every
            # comparison below would be vacuously green
            probe = next((n for n in needles if len(n) >= gram_size), None)
            if probe is not None:
                out = run_duckdb(db_path, "\n".join(
                    session_settings +
                    ["EXPLAIN SELECT * FROM corpus WHERE contains(s, %s);" % sql_quote(probe)]))
                if "NGRAM_INDEX_SCAN" in out:
                    checks += 1
                else:
                    failures.append("explain: rewrite did not fire for needle %r" % probe)
        else:
            phase_checks = [differential_sql(n, case_insensitive) for n in needles]
            phase_checks += [candidates_sql(n, case_insensitive) for n in needles if len(n) >= gram_size]
            txn_checks = [differential_sql(n, case_insensitive) for n in needles[:40]]

        # phase 1: committed, fully indexed corpus
        checks += check_output(run_duckdb(db_path, "\n".join(phase_checks)), "committed", failures)

        # phase 1b: introduce and then remove a marker through two committed
        # updates below the HWM. Both directions must remain exhaustive.
        if transparent:
            update_checks = session_settings + transparent_checks([("plain", UPDATE_MARKER)])
        else:
            update_checks = [differential_sql(UPDATE_MARKER, case_insensitive),
                             candidates_sql(UPDATE_MARKER, case_insensitive)]
        run_duckdb(db_path, "UPDATE corpus SET s = coalesce(s, '') || %s WHERE id = 0;"
                   % sql_quote(" " + UPDATE_MARKER))
        checks += check_output(run_duckdb(db_path, "\n".join(update_checks)),
                               "update-introduce", failures)
        run_duckdb(db_path, "UPDATE corpus SET s = replace(s, %s, '') WHERE id = 0;"
                   % sql_quote(UPDATE_MARKER))
        checks += check_output(run_duckdb(db_path, "\n".join(update_checks)),
                               "update-remove", failures)

        # phase 2: tail rows past the high-water mark
        inserts = ["INSERT INTO corpus VALUES (%d, %s);"
                   % (len(corpus) + i, "NULL" if s is None else sql_quote(s)) for i, s in enumerate(tail)]
        run_duckdb(db_path, "\n".join(inserts))
        checks += check_output(run_duckdb(db_path, "\n".join(phase_checks)), "tail", failures)

        # phase 3: inside an uncommitted writing transaction (insert + delete)
        txn = ["BEGIN;",
               "INSERT INTO corpus SELECT 10000000 + id, s FROM corpus WHERE s IS NOT NULL LIMIT 500;",
               "DELETE FROM corpus WHERE id % 5 = 2;"]
        txn += txn_checks
        txn.append("ROLLBACK;")
        checks += check_output(run_duckdb(db_path, "\n".join(txn)), "in-txn", failures)

        # phase 4: deletes across indexed and tail rows, checked in the same
        # session and then after close/checkpoint below.
        deleted = ["DELETE FROM corpus WHERE id % 3 = 1;"] + phase_checks
        checks += check_output(run_duckdb(db_path, "\n".join(deleted)), "deleted", failures)

        # phase 5: close/reopen may vacuum deleted trailing groups and reuse
        # their rowids. The guard must preserve acceleration or force a full
        # scan; either way both query paths remain exactly equal to brute force.
        if transparent:
            post_vacuum = session_settings + transparent_checks(patterns)
        else:
            post_vacuum = phase_checks
        checks += check_output(run_duckdb(db_path, "\n".join(post_vacuum)),
                               "post-vacuum", failures)

    return checks, failures


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trials", type=int, default=8)
    ap.add_argument("--rows", type=int, default=5000)
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--transparent", action="store_true",
                    help="exercise the Phase 4 optimizer rewrite (plain LIKE/contains/ILIKE queries, "
                         "accelerated vs disabled_optimizers='extension') instead of the explicit functions")
    ap.add_argument("--duckdb", default=DUCKDB, help="duckdb binary to run (e.g. a DEBUG build)")
    args = ap.parse_args()
    set_duckdb_binary(args.duckdb)

    seed = args.seed if args.seed is not None else random.SystemRandom().randint(0, 2**31)
    print("master seed: %d" % seed, flush=True)
    rng = random.Random(seed)

    total_checks = 0
    all_failures = []
    for trial in range(args.trials):
        checks, failures = one_trial(trial, rng, args.rows, args.transparent)
        total_checks += checks
        all_failures.extend(failures)
        print("  %d checks, %d failures" % (checks, len(failures)), flush=True)

    print("TOTAL: %d checks, %d failures (master seed %d)" % (total_checks, len(all_failures), seed))
    for f in all_failures[:50]:
        print("FAIL:", f)
    return 1 if all_failures else 0


if __name__ == "__main__":
    sys.exit(main())
