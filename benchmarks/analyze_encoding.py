#!/usr/bin/env python3
"""Where the index's bytes go, and what a different encoding would save.

Reconstructs the exact posting stream from a built index (ngram_unpack_postings
is the codec's own inverse) and re-derives the on-disk byte total from first
principles. The model is validated against the real blob bytes before any
alternative is trusted: if `model` and `actual` disagree, nothing below it
means anything.

Reported:
  * delta-width histogram (how many postings need 1..5 varint bytes)
  * the split between payload, per-segment headers, and DuckDB storage overhead
  * bit-packed delta blocks at several block sizes, including a per-block
    choice between bit-packing and varints
  * a roaring-style bitmap/array split per 2^16 rowid chunk
  * the effect of segment granularity (SEGMENT_SHIFT) on total bytes

    python3 benchmarks/analyze_encoding.py --db e1.db
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import (bench_path, log, mb, ngram_storage_schema, require_duckdb,  # noqa: E402
                    run_sql, write_json)

#! LEB128 byte count of a non-negative value.
VB = ("CASE WHEN {x} < 128 THEN 1 WHEN {x} < 16384 THEN 2 WHEN {x} < 2097152 THEN 3 "
      "WHEN {x} < 268435456 THEN 4 WHEN {x} < 34359738368 THEN 5 "
      "WHEN {x} < 4398046511104 THEN 6 ELSE 7 END")
#! Bits needed to represent a value (0 needs a nominal 1).
BITS = "CASE WHEN {x} <= 0 THEN 1 ELSE (floor(log2({x})) + 1)::BIGINT END"

BLOCK = 32  #! finest block granularity; larger blocks are rolled up from it


def build_blocks(db, settings, storage):
    """One heavy pass: per 32-delta block, its width, count and varint cost."""
    segments = "%s.segments" % storage
    sql = """
CREATE OR REPLACE TABLE blk AS
WITH p AS (
  SELECT gram, segment_no, r
  FROM ngram_unpack_postings((SELECT gram, segment_no, postings FROM {segments}))
), d AS (
  SELECT gram, segment_no, r,
         r - lag(r) OVER w AS delta,
         row_number() OVER w AS rn
  FROM p WINDOW w AS (PARTITION BY gram, segment_no ORDER BY r)
)
SELECT gram, segment_no,
       ((rn - 2) // {block})::BIGINT AS blk,
       count(*)::BIGINT AS n,
       max({bits})::BIGINT AS maxbits,
       sum({vb})::BIGINT AS varint_bytes
FROM d WHERE rn >= 2
GROUP BY gram, segment_no, blk;

CREATE OR REPLACE TABLE seghdr AS
WITH p AS (
  SELECT gram, segment_no, r
  FROM ngram_unpack_postings((SELECT gram, segment_no, postings FROM {segments}))
)
SELECT gram, segment_no, count(*)::BIGINT AS n, min(r)::BIGINT AS first_rowid
FROM p GROUP BY gram, segment_no;
""".format(segments=segments, block=BLOCK,
           bits=BITS.format(x="delta"), vb=VB.format(x="delta"))
    result = run_sql(db, sql, settings=settings, timeout=86400)
    log("materialized block/header tables in %.1f s" % result.seconds)
    return result.seconds


def scalar_row(db, settings, sql):
    return run_sql(db, sql, settings=settings, timeout=86400).rows[0]


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", default="e1.db")
    parser.add_argument("--table", default="docs")
    parser.add_argument("--column", default="s")
    parser.add_argument("--threads", type=int, default=24)
    parser.add_argument("--memory-limit", default="48GB")
    parser.add_argument("--skip-build", action="store_true", help="reuse blk/seghdr from a previous run")
    parser.add_argument("--keep-tables", action="store_true",
                        help="leave the blk/seghdr scratch tables behind for a --skip-build re-run")
    parser.add_argument("--skip-shifts", action="store_true",
                        help="skip the roaring and SEGMENT_SHIFT sweeps (each is another full re-sort "
                             "of every posting; only worth paying once)")
    args = parser.parse_args()

    require_duckdb()
    db = bench_path(args.db) if not os.path.isabs(args.db) else args.db
    settings = {"threads": args.threads, "memory_limit": "'%s'" % args.memory_limit}
    storage_schema = ngram_storage_schema(db, args.table, args.column, settings)
    segments = "%s.segments" % storage_schema
    report = {"db": db}

    actual = scalar_row(db, settings, """
SELECT sum(octet_length(postings))::BIGINT, count(*)::BIGINT, sum(rowid_count)::BIGINT FROM {seg};
""".format(seg=segments))
    report["actual_blob_bytes"] = int(actual[0])
    report["segment_rows"] = int(actual[1])
    report["postings"] = int(actual[2])
    log("actual: %.1f MB of blobs, %s segment rows, %s postings"
        % (mb(int(actual[0])), actual[1], actual[2]))

    # what the segments table really costs on disk, blobs plus DuckDB's own
    # per-column storage
    storage = scalar_row(db, settings, """
SELECT coalesce(sum(count_distinct_blocks), 0)::BIGINT FROM (
  SELECT count(DISTINCT block_id) AS count_distinct_blocks
  FROM pragma_storage_info('{seg}') WHERE block_id IS NOT NULL);
""".format(seg=segments))
    report["segments_table_blocks"] = int(storage[0])
    report["segments_table_bytes"] = int(storage[0]) * 262144
    log("segments table occupies %d blocks = %.1f MB on disk"
        % (int(storage[0]), mb(int(storage[0]) * 262144)))

    if not args.skip_build:
        report["analysis_seconds"] = build_blocks(db, settings, storage_schema)

    # ---- model validation: rebuild the current format's byte total ----
    model = scalar_row(db, settings, """
SELECT (SELECT sum(varint_bytes) FROM blk)::BIGINT AS delta_bytes,
       (SELECT sum(1 + {vbn} + {vbf}) FROM seghdr)::BIGINT AS header_bytes;
""".format(vbn=VB.format(x="n"), vbf=VB.format(x="first_rowid")))
    delta_bytes, header_bytes = int(model[0]), int(model[1])
    modelled = delta_bytes + header_bytes
    report["model"] = {"delta_bytes": delta_bytes, "header_bytes": header_bytes, "total": modelled}
    log("model: %.1f MB deltas + %.1f MB headers = %.1f MB (actual %.1f MB, %+.4f%%)"
        % (mb(delta_bytes), mb(header_bytes), mb(modelled), mb(report["actual_blob_bytes"]),
           100.0 * (modelled - report["actual_blob_bytes"]) / report["actual_blob_bytes"]))
    report["model_error_pct"] = round(
        100.0 * (modelled - report["actual_blob_bytes"]) / report["actual_blob_bytes"], 4)

    # ---- delta-width histogram ----
    # per delta, not per block: block maxima would overstate the wide tail
    hist = run_sql(db, """
SELECT bytes, count(*)::BIGINT AS postings FROM (
  SELECT {vb} AS bytes FROM (
    SELECT r - lag(r) OVER (PARTITION BY gram, segment_no ORDER BY r) AS delta
    FROM ngram_unpack_postings((SELECT gram, segment_no, postings FROM {seg}))
  ) WHERE delta IS NOT NULL
) GROUP BY bytes ORDER BY bytes;
""".format(vb=VB.format(x="delta"), seg=segments), settings=settings, timeout=86400).rows
    report["delta_width_histogram"] = {int(row[0]): int(row[1]) for row in hist}
    total_deltas = sum(report["delta_width_histogram"].values())
    log("delta width histogram (bytes -> share):")
    for width, count in sorted(report["delta_width_histogram"].items()):
        log("   %d byte: %13d  %5.2f%%" % (width, count, 100.0 * count / total_deltas))

    # ---- bit-packed blocks, rolled up from the 32-delta granularity ----
    report["bitpack"] = {}
    for block_size in (32, 64, 128, 256):
        group = "(blk // %d)" % (block_size // BLOCK)
        row = scalar_row(db, settings, """
SELECT sum(1 + ceil(n * maxbits / 8.0))::BIGINT AS packed,
       sum(least(1 + ceil(n * maxbits / 8.0), 1 + varint_bytes))::BIGINT AS hybrid
FROM (SELECT sum(n) AS n, max(maxbits) AS maxbits, sum(varint_bytes) AS varint_bytes
      FROM blk GROUP BY gram, segment_no, {group});
""".format(group=group))
        packed = int(row[0]) + header_bytes
        hybrid = int(row[1]) + header_bytes
        report["bitpack"][block_size] = {
            "packed_bytes": packed,
            "packed_saving_pct": round(100.0 * (modelled - packed) / modelled, 2),
            "hybrid_bytes": hybrid,
            "hybrid_saving_pct": round(100.0 * (modelled - hybrid) / modelled, 2),
        }
        log("bit-pack B=%3d: packed %.1f MB (%.1f%% saved), hybrid %.1f MB (%.1f%% saved)"
            % (block_size, mb(packed), report["bitpack"][block_size]["packed_saving_pct"],
               mb(hybrid), report["bitpack"][block_size]["hybrid_saving_pct"]))

    if args.skip_shifts:
        if not args.keep_tables:
            run_sql(db, "DROP TABLE IF EXISTS blk; DROP TABLE IF EXISTS seghdr; CHECKPOINT;", settings=settings)
        write_json(bench_path(os.path.basename(db).replace(".db", "") + ".encoding.json"), report)
        return

    # ---- roaring-style: bitmap vs sorted 16-bit array per 2^16 rowid chunk ----
    roaring = scalar_row(db, settings, """
SELECT sum(least(8192, 2 * cnt) + 4)::BIGINT FROM (
  SELECT gram, (r >> 16) AS chunk, count(*) AS cnt
  FROM ngram_unpack_postings((SELECT gram, segment_no, postings FROM {seg}))
  GROUP BY gram, chunk);
""".format(seg=segments), )
    roaring_bytes = int(roaring[0])
    report["roaring_bytes"] = roaring_bytes
    report["roaring_saving_pct"] = round(100.0 * (modelled - roaring_bytes) / modelled, 2)
    log("roaring-style: %.1f MB (%.1f%% saved)" % (mb(roaring_bytes), report["roaring_saving_pct"]))

    # ---- segment granularity ----
    report["segment_shift"] = {}
    for shift in (16, 18, 20, 22, 24, 26):
        row = scalar_row(db, settings, """
WITH p AS (
  SELECT gram, (r >> {shift}) AS seg, r
  FROM ngram_unpack_postings((SELECT gram, segment_no, postings FROM {seg}))
), d AS (
  SELECT gram, seg, r, r - lag(r) OVER w AS delta, row_number() OVER w AS rn
  FROM p WINDOW w AS (PARTITION BY gram, seg ORDER BY r)
)
SELECT (SELECT count(*) FROM (SELECT DISTINCT gram, seg FROM p))::BIGINT AS segs,
       (SELECT sum(CASE WHEN rn = 1 THEN 1 + {vbr} ELSE {vbd} END) FROM d)::BIGINT AS body,
       (SELECT sum({vbn}) FROM (SELECT count(*) AS n FROM p GROUP BY gram, seg))::BIGINT AS counts;
""".format(shift=shift, seg=segments, vbr=VB.format(x="r"),
           vbd=VB.format(x="delta"), vbn=VB.format(x="n")), )
        segs, body, counts = int(row[0]), int(row[1]), int(row[2])
        total = body + counts
        report["segment_shift"][shift] = {
            "segments": segs, "bytes": total,
            "delta_vs_current_pct": round(100.0 * (total - modelled) / modelled, 2),
        }
        log("SEGMENT_SHIFT=%2d: %9d segments, %.1f MB (%+.2f%% vs current)"
            % (shift, segs, mb(total), report["segment_shift"][shift]["delta_vs_current_pct"]))

    if not args.keep_tables:
        # the scratch tables are written into the analysed database; leaving
        # them behind would inflate every later size measurement of it
        run_sql(db, "DROP TABLE IF EXISTS blk; DROP TABLE IF EXISTS seghdr; CHECKPOINT;", settings=settings)

    write_json(bench_path(os.path.basename(db).replace(".db", "") + ".encoding.json"), report)


if __name__ == "__main__":
    main()
