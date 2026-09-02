//===----------------------------------------------------------------------===//
//                         ngram
//
// ngram/index_pragmas.hpp
//
// create_ngram_index / drop_ngram_index / ngram_index_stats pragmas, plus the
// registry (one row per index, holding its metadata) and the storage-table
// naming shared with the query path (ngram_search / ngram_candidates).
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

class TableCatalogEntry;

namespace ngram {

struct MetaInfo;

//! Postings are bucketed by rowid range: segment_no = rowid >> SEGMENT_SHIFT.
//! Build and query must agree on this constant.
constexpr int64_t SEGMENT_SHIFT = 20;

//! The schema holding the registry and every index's two storage tables.
constexpr const char *NGRAM_SCHEMA = "__ngram";

struct ResolvedTarget {
	string catalog_name;
	string schema_name;
	string table_name;
	string column_name;
	//! The resolved base table; valid for the duration of the resolving statement.
	optional_ptr<TableCatalogEntry> entry;
};

//! One registry row's identity: the index id, the column it indexes, and the
//! registry table it was read from. Storage tables are named by the id.
struct IndexLocation {
	string index_ref;
	string column_name;
	idx_t registry_oid = 0;
	string guard_name;
	string guard_token;

	string Hex() const {
		return StringUtil::Replace(index_ref, "-", "");
	}
	string SegmentsTable() const {
		return "segments_" + Hex();
	}
	string StatsTable() const {
		return "stats_" + Hex();
	}
};

//! Resolve a user-supplied table name (optionally schema/catalog-qualified) to a
//! base table. Throws binder errors for views and temporary tables. With
//! require_column set, additionally validates the column for index builds
//! (exists, VARCHAR, no user column shadowing rowid). column_name comes back in
//! the catalog's spelling whenever the column exists on the table, so generated
//! names and name comparisons are casing-stable.
ResolvedTarget ResolveTarget(ClientContext &context, const string &table_input, const string &column_name,
                             bool require_column);

//! The registry rows owned by `target`: the one for its column, or every row of
//! its table when column_name is empty. A row this extension cannot use (other
//! format, corrupt values) raises with its reason; `lenient` skips such rows
//! and an unreadable registry instead, for the optimizer's decline path.
vector<IndexLocation> ExistingIndexes(ClientContext &context, const ResolvedTarget &target, bool lenient = false);
void RequireUniqueIndexColumns(const vector<IndexLocation> &indexes);

//! Registry rows of `target`'s table, other than `except_ref`, that record
//! `guard_name`. The table's guard is dropped only when this is zero.
idx_t OtherGuardReferences(ClientContext &context, const ResolvedTarget &target, const string &guard_name,
                           const string &except_ref);

void ValidateRegistryForCreate(ClientContext &context, const string &catalog_name, idx_t expected_registry_oid,
                               bool expected_bootstrap);

//! False means the registry row is gone. A replaced registry table or a row
//! whose owner changed raises by default, which is what the maintenance and
//! candidate paths need. The exhaustive read paths pass `changed_is_absent`
//! so a replaced identity also reads as false and the query falls back to the
//! exact scan.
bool IndexLocationAvailable(ClientContext &context, const ResolvedTarget &target, const IndexLocation &location,
                            bool changed_is_absent = false);

//! The metadata of `location`'s registry row (gram options, high-water mark,
//! guard name and token). Throws when the row is gone or unusable.
MetaInfo ReadMeta(ClientContext &context, const string &catalog_name, const IndexLocation &location);

//! Committed rowid space the table has allocated (tombstoned rows included),
//! i.e. max committed rowid + 1.
int64_t TableTotalRows(TableCatalogEntry &table);

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

//! Rowid ranges (inclusive) splitting [lo, hi] into approximately `partitions`
//! segment-aligned pieces. Floor-sized ranges can overshoot that request when
//! the segment count is not divisible by it. Always returns at least one range.
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
