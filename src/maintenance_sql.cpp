//===----------------------------------------------------------------------===//
// maintenance_sql.cpp: the generated ngram_refresh and ngram_compact scripts (declared in ngram/build_sql.hpp).
//===----------------------------------------------------------------------===//

#include "ngram/build_sql.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/planner/table_filter_set.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/local_storage.hpp"
#include "ngram/fence.hpp"
#include "ngram/index_state.hpp"
#include "ngram/search_core.hpp"

#include <algorithm>
#include <limits>

namespace duckdb {
namespace ngram {

//! Resolve every index that the pragma should operate on: all indexed columns
//! of the table, or the single column named by `only_column`. Reads and
//! validates each registry row and refuses outright when the guard cannot
//! prove the index maintainable.
static vector<MaintenanceColumn> ResolveMaintenanceColumns(ClientContext &context, const char *fn,
                                                           const ResolvedTarget &target, const string &only_column) {
	auto indexes = ExistingIndexes(context, target);
	RequireUniqueIndexColumns(indexes);
	if (indexes.empty()) {
		throw CatalogException("%s: no ngram index exists on %s", fn, target.table_name);
	}
	if (!only_column.empty()) {
		vector<IndexLocation> filtered;
		for (auto &location : indexes) {
			if (StringUtil::CIEquals(location.column_name, only_column)) {
				filtered.push_back(location);
			}
		}
		if (filtered.empty()) {
			throw CatalogException("%s: no ngram index exists on %s.%s", fn, target.table_name, only_column);
		}
		indexes = std::move(filtered);
	}
	std::sort(indexes.begin(), indexes.end(), [](const IndexLocation &left, const IndexLocation &right) {
		return left.column_name < right.column_name;
	});

	vector<MaintenanceColumn> result;
	for (auto &location : indexes) {
		result.push_back(ResolveMaintenanceColumn(context, fn, target, location));
	}
	return result;
}

static PreparedMaintenance PrepareMaintenance(const char *fn, const ResolvedTarget &target,
                                              const MaintenanceColumn &column) {
	PreparedMaintenance prepared;
	prepared.fn = fn;
	prepared.target = target;
	prepared.location = column.location;
	prepared.meta = column.meta;
	return prepared;
}

static string MaintenanceCall(ClientContext &context, uint64_t group, const char *fn, const ResolvedTarget &target,
                              const MaintenanceColumn &column) {
	return PreparedMaintenanceCall(context, group, PrepareMaintenance(fn, target, column));
}

//! The statements that move `packed`'s rows into the segments table as
//! `generation`: sorted into a scratch table by key, so the run prunes by
//! zone map, then appended at execution through the fence's append call,
//! which inserts nothing when the packed delta is empty.
static string AppendPackedStatements(ClientContext &context, uint64_t group, const char *fn,
                                     const ResolvedTarget &target, const MaintenanceColumn &column,
                                     const string &packed, const string &generation) {
	auto sorted_name = ScratchTableName("sorted");
	auto sorted = Ident(sorted_name);
	auto applied = ScratchName("applied");
	string script;
	script += "CREATE TEMP TABLE " + sorted + " AS SELECT gram_key, segment_no, " + generation +
	          " AS generation, postings, rowid_count, min_rowid, max_rowid FROM " + packed +
	          " ORDER BY gram_key, segment_no;\n";
	script += "CREATE TEMP TABLE " + applied + " AS SELECT " +
	          PreparedAppendCall(context, group, PrepareMaintenance(fn, target, column), sorted_name) +
	          " AS appended;\n";
	script += "DROP TABLE " + applied + ";\n";
	script += "DROP TABLE " + sorted + ";\n";
	return script;
}

//! The registry row of one index, as a FROM/UPDATE target.
static string RegistryRow(const ResolvedTarget &target, const MaintenanceColumn &column) {
	return Registry(target.catalog_name) + " WHERE index_id = " + Lit(column.location.index_ref) + "::UUID";
}

//===----------------------------------------------------------------------===//
// PRAGMA ngram_refresh
//
// Indexes the rows between the recorded high-water mark and the table's last
// committed rowid as a new generation of segment rows (readers union every row
// of a key) and advances the mark; the constant rowid filter lets zone maps
// skip row groups below it. With max_rows the call stops partway, commits, and
// reports remaining_tail for the caller's loop: one call is one transaction,
// because the statement preprocessor wraps a whole expansion or nothing.
// Updates of guard-covered columns are delete+insert, so their replacements
// sit in this tail like appends.
//===----------------------------------------------------------------------===//

//! The highest rowid a bounded refresh starting from `hwm` may cover: the span
//! hwm + max_rows, snapped down to a segment boundary. A span rather than a
//! live-row count keeps every partition range a literal, which is what lets a
//! partition's scan skip row groups by zone map, and bounds the work in the
//! safe direction because deletes leave rowid gaps. An end on a segment
//! boundary means each (gram, segment_no) of the tail is produced whole by one
//! call, so the loop writes the same segment rows one unbounded refresh would
//! have, differing only in generation numbers. A bound smaller than the rest of
//! the mark's own segment is honoured as given, since one segment of long rows
//! can be more text than the caller wants in one transaction; such an
//! increment splits that segment's keys across two generations, which a later
//! ngram_compact merges.
static int64_t BoundedRefreshEnd(int64_t hwm, int64_t max_rows) {
	if (max_rows >= MAX_ROW_ID || hwm > MAX_ROW_ID - 1 - max_rows) {
		// the requested span runs past the committed rowid space; there is
		// nothing left to bound
		return MAX_ROW_ID - 1;
	}
	auto target = hwm + max_rows;
	auto aligned = (((target + 1) >> SEGMENT_SHIFT) << SEGMENT_SHIFT) - 1;
	return aligned > hwm ? aligned : target;
}

//! The mark a refresh records. Unbounded, it is the highest committed rowid
//! the partitions covered. Bounded, it is bound_end itself once some committed
//! row past bound_end proves the slots at or below it are settled; otherwise
//! the unbounded rule restricted to this increment, which is conservative.
//! Either way the tail is then empty, so the caller's loop ends, and advancing
//! over an increment whose rows were all deleted keeps it from spinning.
//!
//! Why the proof is needed and enough: rowids are handed out to a
//! transaction's appended rows while it commits, under the table's append lock
//! (DataTable::AppendLock sets row_start from the current row count), reached
//! through LocalStorage::Commit -> Flush. With a WAL that runs inside
//! DuckTransaction::WriteToWAL, which DuckTransactionManager::CommitTransaction
//! calls holding the WAL lock, and info.commit_id is taken in the same WAL-lock
//! critical section; without a WAL (in-memory, NO_WAL_WRITES) the allocation
//! happens in DuckTransaction::Commit under the transaction lock that covers
//! the commit id. One lock serializes both events for every commit of a
//! database, so commit-id order and rowid order are one order. A slot between
//! the highest row this transaction sees and the table's allocated end may
//! still belong to an append that commits after us, and a mark over it would
//! bury rows no later refresh looks at; but a visible row past bound_end has a
//! commit id below our snapshot, hence so does every transaction holding a
//! lower slot, so everything at or below bound_end is visible-or-deleted and
//! the partitions above indexed all of it.
static string RefreshedHighWaterMark(const string &base, const string &tail_predicate, bool stops_short,
                                     int64_t bound_end) {
	auto committed_max = "(SELECT " + SystemFunction("max") + "(rowid) FROM " + base + " WHERE " + tail_predicate;
	if (!stops_short) {
		return "coalesce(" + committed_max + "), hwm_rowid)";
	}
	auto bound_str = to_string(bound_end);
	return "CASE WHEN EXISTS (SELECT 1 FROM " + base + " WHERE rowid > " + bound_str + " AND rowid < " +
	       to_string(MAX_ROW_ID) + ") THEN " + bound_str + " ELSE coalesce(" + committed_max +
	       " AND rowid <= " + bound_str + "), hwm_rowid) END";
}

//! Progress, read back from what this transaction just committed: rows_indexed
//! counts the committed rows the mark newly covers (NULL values included; they
//! are covered but contribute no grams) and remaining_tail what a query still
//! answers with a tail scan, the committed rows past the mark in this
//! statement's snapshot; rows this transaction appended have no committed
//! rowid yet and are in neither. Both are counted in the packing pass's
//! snapshot, so they reconcile with what was indexed exactly. The old mark is
//! a literal: the execution-time check already refused the script if the row
//! lost it.
static string RefreshSummaryRow(const ResolvedTarget &target, const MaintenanceColumn &column) {
	auto base = target.Qualified();
	auto recorded = "(SELECT hwm_rowid FROM " + RegistryRow(target, column) + ")";
	return "SELECT " + Lit(column.column_name) + " AS column_name, (SELECT " + SystemFunction("count") + "(*) FROM " +
	       base + " WHERE rowid > " + to_string(column.meta.hwm_rowid) + " AND rowid <= " + recorded +
	       ") AS rows_indexed, " + recorded + " AS hwm_rowid, (SELECT " + SystemFunction("count") + "(*) FROM " + base +
	       " WHERE rowid > " + recorded + " AND rowid < " + to_string(MAX_ROW_ID) + ") AS remaining_tail";
}

string RefreshScript(ClientContext &context, const ResolvedTarget &target, const string &only_column, bool bounded,
                     int64_t max_rows) {
	optional_ptr<TableCatalogEntry> entry = target.entry;
	auto total_rows = TableTotalRows(*entry);
	auto columns = ResolveMaintenanceColumns(context, "ngram_refresh", target, only_column);
	auto base = target.Qualified();

	string script;
	auto fence = ScratchName("fence");
	auto group = NewMaintenanceGroup(context);
	bool first_fence = true;
	vector<string> summary_rows;
	for (auto &column : columns) {
		auto segments = StorageTable(target.catalog_name, column.location.SegmentsTable());
		auto packed = ScratchName("refresh_packed");
		// rows past the high-water mark, excluding this transaction's local
		// rows: their rowids are reassigned at commit, so indexing them would
		// record postings for rowids that never exist
		auto tail_predicate = "rowid > " + to_string(column.meta.hwm_rowid) + " AND rowid < " + to_string(MAX_ROW_ID);
		// A bound only changes anything while it stops short of the table's
		// committed end; a bound that covers the whole tail generates the
		// unbounded script, so "loop until remaining_tail is 0" costs exactly
		// one call when the tail already fits.
		auto bound_end = bounded ? BoundedRefreshEnd(column.meta.hwm_rowid, max_rows) : MAX_ROW_ID - 1;
		auto stops_short = bound_end < total_rows - 1;
		auto range_end = stops_short ? bound_end : total_rows - 1;

		auto fence_call = MaintenanceCall(context, group, "ngram_refresh", target, column);
		if (first_fence) {
			script += "CREATE TEMP TABLE " + fence + " AS SELECT " + fence_call + " AS ignored;\n";
			first_fence = false;
		} else {
			script += "INSERT INTO " + fence + " SELECT " + fence_call + ";\n";
		}
		// No rowid past the mark had been allocated when the pragma expanded
		// (total_rows counts allocated rowids, deleted ones included), so no
		// committed row can lie there: the call still validates the index
		// through its fence, inside its transaction, and reports; it packs and
		// writes nothing. A row committed after the expansion stays in the
		// tail until the next refresh, as it would under any bound that
		// stopped before it. A tail whose rows were all deleted keeps its
		// allocated rowids and takes the packing path, which indexes nothing.
		if (column.meta.hwm_rowid >= total_rows - 1) {
			summary_rows.push_back(RefreshSummaryRow(target, column));
			continue;
		}
		// One statement per rowid-range partition of the tail. Unbounded, the
		// ranges cover exactly what tail_predicate covers, including rows
		// committed between this script being generated and being run, so the
		// high-water mark the UPDATE below records never runs ahead of what was
		// indexed. Bounded, they stop at bound_end and so does the mark.
		auto partitions = BuildPartitionCount(context, EstimateGramCount(context, *entry, column.column_name,
		                                                                 column.meta.hwm_rowid + 1, range_end,
		                                                                 column.meta.options.gram_size));
		script +=
		    PackRangesStatements(packed, target, column.column_name, column.meta.options,
		                         SegmentAlignedRanges(column.meta.hwm_rowid + 1, range_end, partitions, !stops_short));
		// A new generation of segment rows for keys the index already holds;
		// readers union every row of a (gram_key, segment_no), compaction merges.
		// Written in key order like every other generation, so the probe's
		// `gram_key = ?` filter keeps pruning row groups by zone map. The delta
		// is empty when every row past the mark was deleted; the append call
		// then writes nothing.
		script += AppendPackedStatements(context, group, "ngram_refresh", target, column, packed,
		                                 "(SELECT coalesce(" + SystemFunction("max") + "(generation), 0) + 1 FROM " +
		                                     segments + ")::INTEGER");
		script += "UPDATE " + Registry(target.catalog_name) +
		          " SET hwm_rowid = " + RefreshedHighWaterMark(base, tail_predicate, stops_short, bound_end) +
		          " WHERE index_id = " + Lit(column.location.index_ref) + "::UUID;\n";
		script += "DROP TABLE " + packed + ";\n";
		summary_rows.push_back(RefreshSummaryRow(target, column));
	}
	script += "DROP TABLE " + fence + ";\n";
	// The pragma's only output, one progress row per index, placed last so
	// that the preprocessor's BEGIN/COMMIT around the still multi-statement
	// expansion, the crash atomicity this pragma rests on, stays in place.
	script += StringUtil::Join(summary_rows, " UNION ALL ") + " ORDER BY column_name;\n";
	return script;
}

//===----------------------------------------------------------------------===//
// PRAGMA ngram_compact
//
// Merges the segment rows that share a (gram_key, segment_no), one per refresh
// generation, back into one row per key. Index-only; dead postings stay, and
// recheck discards them. purge := true rewrites every key against the base
// snapshot and drops every posting whose rowid is no longer live.
//===----------------------------------------------------------------------===//

//! Whether the segments table can hold two rows of one (gram_key, segment_no).
//! Every generation writes a key of a segment at most once and compaction
//! leaves every row at generation 0, so fragmentation needs rows of two
//! generations. The table's column statistics answer that without a scan;
//! rows this transaction appended are not in them and count as a second
//! generation, as does any statistic the host cannot give.
static bool MayHoldFragmentedKeys(ClientContext &context, const ResolvedTarget &target,
                                  const MaintenanceColumn &column) {
	auto &entry = ResolveExistingTable(context, target.catalog_name, NGRAM_SCHEMA, column.location.SegmentsTable(),
	                                   "ngram index segments table");
	auto &storage = entry.GetStorage();
	if (LocalStorage::Get(DuckTransaction::Get(context, entry.ParentCatalog())).Find(storage)) {
		return true;
	}
	if (!entry.ColumnExists("generation")) {
		return true;
	}
	auto &generation = entry.GetColumn("generation");
	auto stats = storage.GetStatistics(context, entry.GetStorageIndex(ColumnIndex(generation.Logical().index)));
	if (!stats || !NumericStats::HasMinMax(*stats)) {
		return true;
	}
	return NumericStats::Min(*stats) != NumericStats::Max(*stats);
}

string CompactScript(ClientContext &context, const ResolvedTarget &target, const string &only_column,
                     bool purge_everywhere) {
	auto columns = ResolveMaintenanceColumns(context, "ngram_compact", target, only_column);
	auto base = target.Qualified();

	string script;
	auto fence = ScratchName("fence");
	auto group = NewMaintenanceGroup(context);
	bool first_fence = true;
	for (auto &column : columns) {
		auto segments = StorageTable(target.catalog_name, column.location.SegmentsTable());
		auto keys = ScratchName("compact_keys");
		auto key_check = ScratchName("compact_key_check");
		auto selected = ScratchName("compact_source");
		auto live = ScratchName("compact_live");
		auto packed = ScratchName("compact_packed");

		auto fence_call = MaintenanceCall(context, group, "ngram_compact", target, column);
		if (first_fence) {
			script += "CREATE TEMP TABLE " + fence + " AS SELECT " + fence_call + " AS ignored;\n";
			first_fence = false;
		} else {
			script += "INSERT INTO " + fence + " SELECT " + fence_call + ";\n";
		}
		// a merge of one generation has nothing to merge: validate and stop
		if (!purge_everywhere && !MayHoldFragmentedKeys(context, target, column)) {
			continue;
		}
		script += "CREATE TEMP TABLE " + keys + " AS SELECT gram_key, segment_no FROM " + segments +
		          " GROUP BY gram_key, segment_no" +
		          (purge_everywhere ? "" : " HAVING " + SystemFunction("count") + "(*) > 1") + ";\n";
		script += "CREATE TEMP TABLE " + key_check + " AS SELECT CASE WHEN " + SystemFunction("count") +
		          "(*) = 0 THEN true ELSE " + SystemFunction("error") +
		          "('ngram: malformed segments-table key') END AS valid FROM " + keys +
		          " WHERE gram_key IS NULL OR segment_no IS NULL OR segment_no < 0 OR segment_no > " +
		          to_string(column.meta.hwm_rowid < 0 ? -1 : column.meta.hwm_rowid >> SEGMENT_SHIFT) + ";\n";
		// The persistent table is key-ordered for query pruning, so scanning it
		// once per rowid partition multiplies reads. Copy only selected encoded
		// rows into a spillable segment-ordered source once, before decoding.
		script += "CREATE TEMP TABLE " + selected +
		          " AS SELECT s.gram_key, s.segment_no, s.postings, s.rowid_count, s.min_rowid, s.max_rowid, "
		          "s.generation::BIGINT AS generation FROM " +
		          segments + " s WHERE EXISTS (SELECT 1 FROM " + keys +
		          " k WHERE k.gram_key = s.gram_key AND k.segment_no = s.segment_no) ORDER BY s.segment_no, "
		          "s.gram_key;\n";
		if (purge_everywhere) {
			// DuckDB v1.5.5 cannot physically prune a base scan on the rowid
			// pseudo-column. Materialize the relevant live rowids in one pass;
			// the ordered one-BIGINT temp then prunes each packing range.
			script += "CREATE TEMP TABLE " + live + " AS SELECT rowid AS r FROM " + base +
			          " WHERE rowid >= 0 AND rowid <= " + to_string(column.meta.hwm_rowid) + " AND " +
			          Ident(column.column_name) + " IS NOT NULL" +
			          " AND EXISTS (SELECT 1 FROM (SELECT DISTINCT segment_no FROM " + keys +
			          ") k WHERE k.segment_no = rowid >> " + to_string(SEGMENT_SHIFT) + ") ORDER BY r;\n";
		}
		// Each key is decoded and re-packed wholly in one bounded range. A
		// purging rowid survives only when the base snapshot put it in `live`;
		// merge-only compaction retains dead postings for recheck. Without a
		// persisted pair count, auto requests the shared 4096 partitions, which
		// the segment-aligned split turns into about one range per segment; an
		// explicit ngram_build_partitions setting is still honoured.
		auto partitions = BuildPartitionCount(context, std::numeric_limits<int64_t>::max());
		auto ranges = SegmentAlignedRanges(0, column.meta.hwm_rowid, partitions, false);
		for (idx_t i = 0; i < ranges.size(); i++) {
			auto segment_lo = to_string(ranges[i].first >> SEGMENT_SHIFT);
			auto segment_hi = to_string(ranges[i].second >> SEGMENT_SHIFT);
			auto source = "SELECT gram_key, segment_no, r FROM " + SystemFunction("ngram_unpack_postings") +
			              "((SELECT gram_key, segment_no, postings, rowid_count, min_rowid, max_rowid, generation, " +
			              to_string(column.meta.hwm_rowid) + "::BIGINT AS hwm FROM " + selected +
			              " WHERE segment_no >= " + segment_lo + " AND segment_no <= " + segment_hi + "))";
			if (purge_everywhere) {
				source += " WHERE r IN (SELECT r FROM " + live + " WHERE r >= " + to_string(ranges[i].first) +
				          " AND r <= " + to_string(ranges[i].second) + ")";
			}
			script += PackPartitionStatement(packed, i == 0, source);
		}
		script += "DELETE FROM " + segments + " WHERE EXISTS (SELECT 1 FROM " + keys +
		          " k WHERE k.gram_key = " + segments + ".gram_key AND k.segment_no = " + segments + ".segment_no);\n";
		// Re-inserted in key order, so the merged rows prune by zone map for
		// the probe exactly as the generations they replace did. A merge with
		// nothing fragmented has nothing to insert and goes through the append
		// call, which writes nothing then; a purge rewrites every key and keeps
		// the parallel batch insert, which no empty batch insert into this table
		// can precede in a transaction (docs/upstream/duckdb-empty-batch-insert.md).
		if (purge_everywhere) {
			script += "INSERT INTO " + segments +
			          " SELECT gram_key, segment_no, 0, postings, rowid_count, min_rowid, max_rowid FROM " + packed +
			          " ORDER BY gram_key, segment_no;\n";
		} else {
			script += AppendPackedStatements(context, group, "ngram_compact", target, column, packed, "0::INTEGER");
		}
		// every surviving row joins generation 0: one generation is the state
		// the next call recognizes as nothing to merge, and the next refresh
		// numbers its rows from there
		script += "UPDATE " + segments + " SET generation = 0 WHERE generation <> 0;\n";
		script += "DROP TABLE " + keys + ";\n";
		script += "DROP TABLE " + key_check + ";\n";
		script += "DROP TABLE " + selected + ";\n";
		if (purge_everywhere) {
			script += "DROP TABLE " + live + ";\n";
		}
		script += "DROP TABLE " + packed + ";\n";
	}
	script += "DROP TABLE " + fence + ";\n";
	return script;
}

} // namespace ngram
} // namespace duckdb
