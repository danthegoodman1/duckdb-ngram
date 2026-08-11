//===----------------------------------------------------------------------===//
//                         ngram
//
// ngram/index_pragmas.hpp
//
// create_ngram_index / drop_ngram_index / ngram_index_stats pragmas, plus the
// shadow-storage naming scheme and target resolution shared with the query
// path (ngram_search / ngram_candidates).
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {

class TableCatalogEntry;

namespace ngram {

//! Postings are bucketed by rowid range: segment_no = rowid >> SEGMENT_SHIFT.
//! Build and query must agree on this constant.
constexpr int64_t SEGMENT_SHIFT = 20;

//! The schema holding a base table's shadow tables (ngram_<schema>_<table>)
string ShadowSchemaName(const string &schema, const string &table);

//! Per-column shadow tables inside the shadow schema (ngram_<schema>_<table>)
string MetaTableName(const string &column);
string SegmentsTableName(const string &column);
string StatsTableName(const string &column);

struct ResolvedTarget {
	string catalog_name;
	string schema_name;
	string table_name;
	string column_name;
	string shadow_schema;
	//! The resolved base table; valid for the duration of the resolving statement.
	optional_ptr<TableCatalogEntry> entry;
};

//! Resolve a user-supplied table name (optionally schema/catalog-qualified) to a
//! base table. Throws binder errors for views and temporary tables. With
//! require_column set, additionally validates the column for index builds
//! (exists, VARCHAR, no user column shadowing rowid). column_name comes back in
//! the catalog's spelling whenever the column exists on the table, so generated
//! names and name comparisons are casing-stable.
ResolvedTarget ResolveTarget(ClientContext &context, const string &table_input, const string &column_name,
                             bool require_column);

//! Whether a table named `name` exists in the target's shadow schema.
bool ShadowTableExists(ClientContext &context, const ResolvedTarget &target, const string &name);

//! Names of meta_* tables currently present in the target's shadow schema.
vector<string> ExistingMetaTables(ClientContext &context, const ResolvedTarget &target);

//! Quote an identifier / a string literal, or qualify a built-in function so
//! generated SQL cannot resolve a same-named macro from the current schema.
string Ident(const string &name);
string Lit(const string &value);
string SystemFunction(const string &name);

//! Rowids at or above this value are transaction-local: they are reassigned at
//! commit, so the index must never record them (duckdb MAX_ROW_ID).
constexpr int64_t LOCAL_ROWID_START = 36028797018960000LL;

//===----------------------------------------------------------------------===//
// Partitioned packing
//
// Build, refresh and compact all turn a (gram, segment_no, rowid) pair stream
// into one segment row per (gram, segment_no). They do it with the
// ngram_pack_segment aggregate under a plain GROUP BY, which runs on DuckDB's
// radix-partitioned hash aggregate — the only shape measured to use the whole
// machine (a global ORDER BY feeding the streaming packer saturates at ~5 of
// 24 threads whatever it is given, benchmarks/RESULTS.md §2).
//
// Aggregate states cannot spill, so the pair stream is fed to the aggregate one
// partition at a time and each partition's states are sized to fit the memory
// limit. Partitions are rowid ranges aligned to the segment boundary: since
// segment_no = rowid >> SEGMENT_SHIFT, such a range holds every rowid of every
// key it touches, so each key is grouped exactly once and the output is one row
// per key — the same rows, byte for byte, the ordered packer produced.
//===----------------------------------------------------------------------===//

//! Rowid ranges (inclusive) splitting [lo, hi] into at most `partitions`
//! segment-aligned pieces. Always returns at least one range.
//!
//! With `open_ended` (the default) the last range runs to LOCAL_ROWID_START - 1
//! so that rows committed between the pragma callback and the statement are
//! still indexed, which keeps the recorded high-water mark exact. A bounded
//! refresh passes false instead: its whole point is to stop at `hi`, and the
//! mark it records stops there with it.
vector<pair<int64_t, int64_t>> SegmentAlignedRanges(int64_t lo, int64_t hi, idx_t partitions, bool open_ended = true);

//! One statement filling `packed` (gram, segment_no, postings, rowid_count,
//! min_rowid, max_rowid) from `pair_source`, a SELECT of (gram, segment_no, r).
//! The first partition creates the table, the rest append to it.
string PackPartitionStatement(const string &packed, bool first, const string &pair_source);

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

void RegisterIndexPragmas(ExtensionLoader &loader);

} // namespace ngram
} // namespace duckdb
