#!/usr/bin/env bash
#
# The exact sequence that produced RESULTS.md, in order.
#
# Scale down with NGRAM_REPLICAS: the shape of every result holds at smaller
# sizes, and 100 replicas is ~92 GiB of text plus ~98 GiB of index, which most
# machines do not have room for. NGRAM_REPLICAS=10 needs ~20 GB and about 40
# minutes; the default needs ~200 GB and about seven hours.
#
#   NGRAM_REPLICAS=10 bash benchmarks/run_all.sh
#
set -o pipefail
cd "$(dirname "$0")/.." || exit 1
BIG=${NGRAM_REPLICAS:-100}
B=${NGRAM_BENCH_DIR:-$HOME/duckdb-ngram-bench}
mkdir -p "$B"

run() { echo "### $*"; "$@" || exit 1; }

# ---- corpora -------------------------------------------------------------
run python3 benchmarks/gen_corpus.py both

# ---- builds --------------------------------------------------------------
# 1 GB, and the same corpus with no index at all (the base-table baseline the
# on-disk index ratio is measured against).
run python3 benchmarks/bench_build.py --name e1  --replicas 1 --chunk 1 --compact --verify 3
run python3 benchmarks/bench_build.py --name e1c --replicas 1 --chunk 1 --keep-corpus-only
run python3 benchmarks/bench_build.py --name code --source code --compact --verify 3

# 10 GB in two 5-replica steps: an initial build and one refresh of the same
# size, which is what shows refresh cost tracking the tail rather than the
# table. It is also the deliberately oversized chunk — see RESULTS.md.
run python3 benchmarks/bench_build.py --name e10 --replicas 10 --chunk 5 --compact --verify 3

# The big one, one replica per step.
run python3 benchmarks/bench_build.py --name e100 --replicas "$BIG" --chunk 1 \
        --compact --verify 3 --stats-every 10

# ---- analysis and measurement -------------------------------------------
run python3 benchmarks/analyze_encoding.py --db e1.db
run python3 benchmarks/analyze_encoding.py --db code.db --memory-limit 16GB
run python3 benchmarks/analyze_encoding.py --db e10.db --skip-shifts

run python3 benchmarks/bench_latency.py --db e1.db --pick
run python3 benchmarks/bench_latency.py --db e1.db   --cold --repeats 11 --cold-repeats 3
run python3 benchmarks/bench_latency.py --db e10.db  --cold --repeats 9  --cold-repeats 3
run python3 benchmarks/bench_latency.py --db e100.db --cold --repeats 5  --cold-repeats 2

# The pre-parallel scan ran on one thread whatever the thread setting was, so
# threads=1 reproduces it against the same 24-thread brute-force baseline.
run python3 benchmarks/bench_latency.py --db e10.db  --threads 1 --repeats 5 --suffix .t1
run python3 benchmarks/bench_latency.py --db e100.db --threads 1 --repeats 3 --suffix .t1

run python3 benchmarks/sweep_settings.py --db e10.db  --what both      --repeats 5
run python3 benchmarks/sweep_settings.py --db e100.db --what crossover --repeats 3
run python3 benchmarks/sweep_settings.py --db e100.db --what fallback  --repeats 3

# Gram size: rebuild the 1 GB corpus at 2 and 4 and measure both.
for g in 2 4; do
  rm -f "$B/e1g$g.db" "$B/e1g$g.db.wal"; rm -rf "$B/e1g$g.db.tmp"
  run python3 benchmarks/bench_build.py --name "e1g$g" --replicas 1 --chunk 1 --gram "$g" --verify 2
  run python3 benchmarks/bench_latency.py --db "e1g$g.db" --repeats 7
done

echo "results are in $B/*.json; RESULTS.md records them"
