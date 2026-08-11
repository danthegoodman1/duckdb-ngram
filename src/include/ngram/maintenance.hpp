//===----------------------------------------------------------------------===//
//                         ngram
//
// ngram/maintenance.hpp
//
// Index maintenance: the ngram_refresh / ngram_compact pragmas and the
// staleness detectors they share with the query paths. A detector answers one
// question: is this index *certainly* stale, i.e. is there proof that its
// postings no longer describe the table? A "certainly stale" verdict gates
// errors and refusals, so it must never fire on a healthy index; the reverse
// is not true, and the residual undetectable states are documented on
// CertainStaleReason and SampleStaleReason.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "ngram/search_core.hpp"

namespace duckdb {

class TableCatalogEntry;

namespace ngram {

//! Meta layout this extension version writes and reads. Bumped whenever the
//! meta table gains or loses a column; every reader rejects other versions
//! with a rebuild-required error rather than misreading them.
constexpr int64_t NGRAM_FORMAT_VERSION = 2;

//! How many rows an index records as witnesses of where its postings point.
constexpr idx_t ROW_SAMPLE_COUNT = 32;

//! Internal execution-time functions used by generated maintenance scripts.
constexpr const char *NGRAM_MAINTENANCE_GUARD = "__ngram_maintenance_guard";
constexpr const char *NGRAM_ROW_SAMPLES = "__ngram_row_samples";

//! Facts about a base table that the index records at build/refresh and
//! re-derives later to detect staleness. All of it is O(1) or O(columns), so
//! every query path can afford it.
struct TableFingerprint {
	//! Committed rowid space the table has allocated (tombstoned rows
	//! included), i.e. max committed rowid + 1. Grows with appends and shrinks
	//! only when a checkpoint vacuums rows away.
	int64_t total_rows = 0;
	//! Ordered column names and types, length-prefixed so that a string prefix
	//! is exactly a column-list prefix. Names are case-folded: identifiers
	//! match case-insensitively, so a case-only rename is not a change.
	string schema_fingerprint;
	//! Catalog identity of the table, and of the attached database it lives in.
	//! Both are minted from a process-wide counter — on every CREATE, and on
	//! every ATTACH — so a recorded pair says something only while the same
	//! instance is running AND the same attach incarnation is still current:
	//! a DETACH + re-ATTACH renumbers the table too, and would otherwise look
	//! exactly like a DROP + re-CREATE.
	int64_t table_oid = 0;
	int64_t catalog_oid = 0;
	//! Identifies the running database instance (see InstanceId).
	string instance_id;
	//! (name, type) per logical column, for the indexed column's own shape.
	vector<pair<string, string>> columns;

	//! Whether `meta` was recorded by this very table object: same instance,
	//! same attach incarnation, same catalog entry. When it holds, the table
	//! cannot have been re-created since, whatever else changed about it.
	bool ProvesSameTable(const MetaInfo &meta) const;
	//! The table's current type for `column`, or "" when it no longer has one
	//! by that name (matched case-insensitively, like every identifier).
	string ColumnType(const string &column) const;
};

TableFingerprint ComputeTableFingerprint(ClientContext &context, TableCatalogEntry &table);

//! Witnesses of where the index's postings point: rowids spread over the
//! indexed range paired with a hash of the indexed column's value there,
//! recorded as "<rowid>:<hash>,...". Checking them re-reads those rows, so it
//! costs one row fetch per sample and belongs to the maintenance pragmas
//! rather than to every query.
string BuildRowSampleDigest(ClientContext &context, TableCatalogEntry &table, const string &column, int64_t max_rowid);

//! Why the index described by `meta` is certainly stale, or "" when no
//! staleness can be proven from table metadata alone. Proof, not suspicion:
//! every verdict holds only for states an append-only history cannot reach.
string CertainStaleReason(const MetaInfo &meta, const TableFingerprint &now);

//! The deeper check: re-read the rows `meta` recorded as witnesses and report
//! the first one that no longer holds the value the index was built from. A
//! changed value there is proof that the index's postings for that rowid
//! describe something else — a checkpoint vacuum moved the row out from under
//! them, or the row was updated in place. Returns "" when nothing is provable:
//! a deleted witness, or one that still hashes the same, says nothing.
//!
//! What stays undetectable on duckdb v1.5.5, and is therefore documented
//! rather than caught: an in-place UPDATE that misses every witness (the
//! engine has no trigger or change feed to enumerate updated rows), and a
//! checkpoint vacuum whose row motion happens to leave every witness holding
//! an equal value.
string SampleStaleReason(ClientContext &context, TableCatalogEntry &table, const MetaInfo &meta);

//! A value unique to the open database instance, stable for its lifetime and
//! regenerated by the next one. Recorded next to a table's catalog oid, which
//! only means something within the instance that handed it out.
string InstanceId(ClientContext &context);

//! Registers the ngram_refresh and ngram_compact pragmas.
void RegisterMaintenance(ExtensionLoader &loader);

} // namespace ngram
} // namespace duckdb
