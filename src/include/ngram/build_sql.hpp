//===----------------------------------------------------------------------===//
// ngram/build_sql.hpp: the generated create, refresh, compact, and drop scripts, and the partition planning they share.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "ngram/catalog.hpp"

namespace duckdb {

class TableCatalogEntry;

namespace ngram {

struct ObservedIndex;

//===----------------------------------------------------------------------===//
// Partitioned packing
//
// Build, refresh and compact turn a (gram, segment_no, rowid) pair stream into
// one segment row per key with the ngram_pack_segment aggregate under a plain
// GROUP BY: DuckDB's radix-partitioned hash aggregate is the only shape
// measured to use the whole machine (a global ORDER BY into a streaming packer
// saturates at ~5 of 24 threads, benchmarks/RESULTS.md §2). Aggregate states
// cannot spill, so the stream is fed one partition at a time, each sized to
// the memory limit. Partitions are segment-aligned rowid ranges: since
// segment_no = rowid >> SEGMENT_SHIFT, a range holds every rowid of every key
// it touches, so each key is grouped once and the output is one row per key,
// byte for byte what the ordered packer produced.
//===----------------------------------------------------------------------===//

//! Rowid ranges (inclusive) splitting [lo, hi] into approximately `partitions`
//! segment-aligned pieces; floor-sized ranges can overshoot the request when
//! the segment count does not divide. Always at least one range. With
//! `open_ended` the last range runs to MAX_ROW_ID - 1 so rows committed
//! between the pragma callback and the statement are indexed too, which keeps
//! the recorded mark exact; a bounded refresh passes false to stop at `hi`.
vector<pair<int64_t, int64_t>> SegmentAlignedRanges(int64_t lo, int64_t hi, idx_t partitions, bool open_ended = true);

//! One statement filling `packed` (gram, segment_no, postings, rowid_count,
//! min_rowid, max_rowid) from `pair_source`, a SELECT of (gram, segment_no, r).
//! The first partition creates the table, the rest append to it.
string PackPartitionStatement(const string &packed, bool first, const string &pair_source);

//! The packing statements for the non-NULL values of `column_name` in each of
//! `ranges`, one PackPartitionStatement per range.
string PackRangesStatements(const string &packed, const ResolvedTarget &target, const string &column_name,
                            const GramOptions &options, const vector<pair<int64_t, int64_t>> &ranges);

//! How many partitions a pair stream of `estimated_pairs` should be split into
//! for its grouping pass to fit the session's memory limit. Honours the
//! ngram_build_partitions setting when it is set to a non-zero value.
idx_t BuildPartitionCount(ClientContext &context, int64_t estimated_pairs);

//! Grams the rows in [min_rowid, max_rowid] of `column` will produce, estimated
//! from a sample of those rows. Only used to size partitions, and biased to
//! overestimate (byte lengths rather than character lengths, deleted rows
//! counted), because too many partitions costs a little time and too few costs
//! an out-of-memory error.
int64_t EstimateGramCount(ClientContext &context, TableCatalogEntry &table, const string &column, int64_t min_rowid,
                          int64_t max_rowid, idx_t gram_size);

//! The script that builds an index on `target`'s column with `options`.
string CreateIndexScript(ClientContext &context, const ResolvedTarget &target, const GramOptions &options);

//! The script that removes `index` (a registry row, its storage, and the guard
//! when this was its last reference).
string DropIndexScript(ClientContext &context, const ObservedIndex &index);

//! The script that indexes the rows past each index's high-water mark, for
//! every indexed column of `target` or the one named by `only_column`. With
//! `bounded`, each column covers at most `max_rows` more rowids and the script
//! ends with a progress row.
string RefreshScript(ClientContext &context, const ResolvedTarget &target, const string &only_column, bool bounded,
                     int64_t max_rows);

//! The script that merges each key's segment rows back into one; with `purge`
//! it also drops postings of rows that are no longer live.
string CompactScript(ClientContext &context, const ResolvedTarget &target, const string &only_column, bool purge);

} // namespace ngram
} // namespace duckdb
