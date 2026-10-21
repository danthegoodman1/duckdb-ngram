#!/usr/bin/env python3
"""Check sparse_grams() against a brute-force reference of the definition.

The reference tests every substring directly: a range of min_gram to max_gram
codepoints is a sparse gram when every (min_gram - 1)-codepoint window strictly
inside it weighs less than both of its border windows. The extension enumerates
the same set in one pass over a stack, so the two must agree on every input;
small alphabets make repeated windows, whose equal weights exercise the tie
rule. Case folding is off: the reference does not model utf8proc's lowercase.

    python3 scripts/sparse_grams_check.py --duckdb build/release/duckdb --cases 3000 --seed 1
"""

import argparse
import json
import os
import random
import subprocess
import sys

MASK = (1 << 64) - 1
ALPHABETS = ["ab", "abc", "abcd", "a b", "xyz.", "éa", "aé😀", "abcdefghijklmnopqrstuvwxyz ", "Ωωάé€😀a "]


def fmix64(value):
    value ^= value >> 33
    value = (value * 0xFF51AFD7ED558CCD) & MASK
    value ^= value >> 33
    value = (value * 0xC4CEB9FE1A85EC53) & MASK
    value ^= value >> 33
    return value


def packed(codepoint):
    value = 0
    for byte in codepoint.encode("utf-8"):
        value = (value << 8) | byte
    return value


def weight(window):
    result = fmix64(((packed(window[0]) << 32) | packed(window[1])) & MASK)
    for codepoint in window[2:]:
        result = fmix64(result ^ packed(codepoint))
    return result


def reference(text, min_gram, max_gram):
    span = min_gram - 1
    weights = [weight(text[i:i + span]) for i in range(len(text) - span + 1)]
    grams = []
    for first in range(len(text)):
        for end in range(first + min_gram, min(len(text), first + max_gram) + 1):
            left, right = first, end - span
            inner = weights[left + 1:right]
            if all(w < min(weights[left], weights[right]) for w in inner):
                grams.append(text[first:end])
    # sparse_grams orders by first codepoint, then length
    return grams


def random_case(generator):
    alphabet = generator.choice(ALPHABETS)
    length = generator.randrange(0, 40)
    text = "".join(generator.choice(alphabet) for _ in range(length))
    min_gram = generator.choice([3, 3, 3, 4, 5])
    max_gram = generator.choice([min_gram, min_gram + 1, 8, 16, 64])
    return text, min_gram, max(max_gram, min_gram)


def literal(text):
    return "'" + text.replace("'", "''") + "'"


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--duckdb", default="build/release/duckdb")
    parser.add_argument("--cases", type=int, default=3000)
    parser.add_argument("--seed", type=int, default=1)
    args = parser.parse_args()
    generator = random.Random(args.seed)
    cases = [random_case(generator) for _ in range(args.cases)]
    cases += [("chester ", 3, 16), ("aaaaaaaa", 3, 16), ("abababab", 3, 16), ("ab", 3, 8), ("abc", 3, 8)]
    values = ", ".join("(%d, %s, %d, %d)" % (i, literal(t), lo, hi) for i, (t, lo, hi) in enumerate(cases))
    sql = ("SELECT id, to_json(sparse_grams(t, lo, hi, false)) FROM (VALUES %s) v(id, t, lo, hi) ORDER BY id;"
           % values)
    env = dict(os.environ, DUCKDB_AGENT_MODE="0")
    output = subprocess.run([args.duckdb, "-noheader", "-list", "-separator", "\t"], input=sql, text=True,
                            capture_output=True, env=env, check=True).stdout
    failures = 0
    checked = 0
    for line in output.splitlines():
        index, grams = line.split("\t", 1)
        text, min_gram, max_gram = cases[int(index)]
        expected = reference(text, min_gram, max_gram)
        actual = json.loads(grams)
        checked += 1
        if actual != expected:
            failures += 1
            if failures <= 5:
                print("MISMATCH %r min=%d max=%d\n  expected %s\n  actual   %s" %
                      (text, min_gram, max_gram, expected, actual))
    if checked != len(cases):
        print("expected %d results, got %d" % (len(cases), checked))
        return 1
    print("sparse_grams: %d cases, %d mismatches (seed %d)" % (checked, failures, args.seed))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
