#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "ngram/index_pragmas.hpp"
#include "ngram/maintenance.hpp"
#include "ngram/search_core.hpp"

#include <algorithm>

namespace duckdb {
namespace ngram {

//===----------------------------------------------------------------------===//
// Staleness detection
//
// The index is exhaustive as long as every rowid it recorded still holds the
// row it recorded it for, and every row it has never seen sits past the
// high-water mark where the tail scan finds it. Two engine behaviours can
// break that between queries: a checkpoint vacuum moves surviving rowids, and
// DROP TABLE + CREATE under the same name hands the shadow schema to a
// different table. Neither can be repaired by an incremental refresh, so the
// detectors below turn them into errors and refusals instead of silent misses.
//
// What a query path reads is metadata only: the table's allocated rowid count
// and catalog oid are O(1) and its column list is O(columns). The maintenance
// pragmas additionally re-read a handful of recorded rows (see below), which
// costs one row fetch per witness and is far too much per query.
//===----------------------------------------------------------------------===//

//! FNV-1a over a value: the digests below are persisted, so the mixing must
//! be fixed here rather than inherited from a standard-library hash that may
//! differ per build or platform.
static void MixHash(uint64_t &hash, uint64_t value) {
	for (idx_t byte = 0; byte < 8; byte++) {
		hash ^= (value >> (byte * 8)) & 0xFF;
		hash *= 1099511628211ULL;
	}
}

static uint64_t HashBytes(const char *data, idx_t size) {
	uint64_t hash = 14695981039346656037ULL;
	for (idx_t i = 0; i < size; i++) {
		hash ^= static_cast<uint8_t>(data[i]);
		hash *= 1099511628211ULL;
	}
	// length too: a hash of the bytes alone cannot tell "" from a NULL
	MixHash(hash, size);
	return hash;
}

//! Stands in for a NULL value. A string could in principle hash to it too;
//! the cost of that coincidence is one witness that fails to notice a change,
//! which is the same direction as a deleted witness.
static constexpr uint64_t NULL_VALUE_HASH = 0;

//! Length-prefixed so a string prefix is exactly a column-list prefix: a
//! column name can otherwise contain any separator we might pick. Names are
//! folded because identifiers match case-insensitively — two columns cannot
//! differ by case alone, and a case-only rename changes no data.
static void AppendColumnFingerprint(string &result, const string &name, const string &type) {
	auto folded = StringUtil::Lower(name);
	result += to_string(folded.size()) + ":" + folded + to_string(type.size()) + ":" + type;
}

//! A value unique to one open database instance. It lives in the instance's
//! object cache rather than in a setting: a setting's default is fixed when
//! the extension registers it and would survive a close/reopen inside the same
//! process, which is exactly the case (a restart renumbers catalog oids) where
//! comparing oids must stop.
struct InstanceIdEntry : public ObjectCacheEntry {
	InstanceIdEntry() : id(UUID::ToString(UUID::GenerateRandomUUID())) {
	}

	string id;

	static string ObjectType() {
		return "ngram_instance_id";
	}
	string GetObjectType() override {
		return ObjectType();
	}
	//! no memory estimate: the entry must never be evicted, or an index
	//! written earlier in this session would stop being comparable
	optional_idx GetEstimatedCacheMemory() const override {
		return optional_idx();
	}
};

string InstanceId(ClientContext &context) {
	auto entry = ObjectCache::GetObjectCache(context).GetOrCreate<InstanceIdEntry>("ngram_instance_id");
	return entry ? entry->id : string();
}

TableFingerprint ComputeTableFingerprint(ClientContext &context, TableCatalogEntry &table) {
	TableFingerprint result;
	result.table_oid = NumericCast<int64_t>(table.oid);
	result.catalog_oid = NumericCast<int64_t>(table.ParentCatalog().GetOid());
	result.instance_id = InstanceId(context);
	for (auto &col : table.GetColumns().Logical()) {
		auto type = col.Type().ToString();
		AppendColumnFingerprint(result.schema_fingerprint, col.Name(), type);
		result.columns.emplace_back(col.Name(), type);
	}
	result.total_rows = NumericCast<int64_t>(table.Cast<DuckTableEntry>().GetStorage().GetTotalRows());
	return result;
}

bool TableFingerprint::ProvesSameTable(const MetaInfo &meta) const {
	// oids are handed out from one process-wide counter, so a recorded pair is
	// comparable only inside the instance that recorded it, and only while the
	// database it named is still the same attach incarnation
	return !meta.instance_id.empty() && meta.instance_id == instance_id && meta.catalog_oid == catalog_oid &&
	       meta.table_oid == table_oid;
}

string TableFingerprint::ColumnType(const string &column) const {
	for (auto &entry : columns) {
		// identifiers match case-insensitively, and a case-only rename leaves
		// every value and rowid alone: it must not read as a vanished column
		if (StringUtil::CIEquals(entry.first, column)) {
			return entry.second;
		}
	}
	return string();
}

//===----------------------------------------------------------------------===//
// Row witnesses
//
// The structural facts above cannot see the one thing that hurts most: a
// checkpoint vacuum that moved rowids and was then papered over by appends.
// Rowids are handed out again from the start after a vacuum, so the refilled
// table can be indistinguishable from the healthy one in every piece of
// metadata — row counts, row-group boundaries, even the column's block layout
// (all verified on v1.5.5). What a vacuum cannot restore is the *contents* of
// the rowids the index recorded: shifting rows down puts different values at
// them.
//
// So the index records a handful of (rowid, value hash) witnesses spread over
// the indexed range, and maintenance re-reads them. A witness whose value
// changed is proof the index's postings for that rowid describe a different
// row. A witness that reads the same, or that has since been deleted, proves
// nothing — which is why this only ever adds detections and never contradicts
// a healthy index.
//===----------------------------------------------------------------------===//

//! Read the indexed column of one rowid. Returns false when the row is not
//! visible (deleted, or never committed), in which case it is no witness.
static bool ReadRowHash(ClientContext &context, DuckTableEntry &table, const ColumnDefinition &column, int64_t rowid,
                        uint64_t &result) {
	if (rowid < 0 || rowid >= LOCAL_ROWID_START) {
		// a witness is always a committed rowid; fetching a transaction-local
		// one goes through local storage, which raises an internal error when
		// the transaction that owned it is gone
		return false;
	}
	auto &storage = table.GetStorage();
	auto &transaction = DuckTransaction::Get(context, table.ParentCatalog());
	vector<StorageIndex> column_ids {table.GetStorageIndex(ColumnIndex(column.Logical().index))};
	DataChunk chunk;
	chunk.Initialize(Allocator::Get(context), vector<LogicalType> {column.Type()});
	Vector row_ids(LogicalType::ROW_TYPE, 1);
	FlatVector::GetData<row_t>(row_ids)[0] = NumericCast<row_t>(rowid);
	ColumnFetchState fetch_state;
	storage.Fetch(transaction, chunk, column_ids, row_ids, 1, fetch_state);
	if (chunk.size() != 1) {
		return false;
	}
	auto value = chunk.GetValue(0, 0);
	if (value.IsNull()) {
		result = NULL_VALUE_HASH;
		return true;
	}
	auto text = StringValue::Get(value);
	result = HashBytes(text.data(), text.size());
	return true;
}

//! The rowids to witness: evenly spread over [0, max_rowid], ends included.
//! `max_rowid` is the end of the range the caller is recording coverage of, not
//! the end of the table: a witness past the high-water mark would describe a row
//! the index holds no postings for (see SampleStaleReason).
static vector<int64_t> SampleRowids(int64_t max_rowid) {
	vector<int64_t> result;
	if (max_rowid < 0) {
		return result;
	}
	auto count = MinValue<int64_t>(NumericCast<int64_t>(ROW_SAMPLE_COUNT), max_rowid + 1);
	for (int64_t i = 0; i < count; i++) {
		// spread so that a shift anywhere in the table disturbs a witness
		auto rowid = count == 1 ? max_rowid : (max_rowid * i) / (count - 1);
		if (result.empty() || result.back() != rowid) {
			result.push_back(rowid);
		}
	}
	return result;
}

string BuildRowSampleDigest(ClientContext &context, TableCatalogEntry &table, const string &column, int64_t max_rowid) {
	if (!table.ColumnExists(column)) {
		return string();
	}
	auto &duck_table = table.Cast<DuckTableEntry>();
	auto &column_def = table.GetColumn(column);
	string result;
	for (auto rowid : SampleRowids(max_rowid)) {
		uint64_t hash = 0;
		if (!ReadRowHash(context, duck_table, column_def, rowid, hash)) {
			continue;
		}
		if (!result.empty()) {
			result += ",";
		}
		result += to_string(rowid) + ":" + to_string(hash);
	}
	return result;
}

string SampleStaleReason(ClientContext &context, TableCatalogEntry &table, const MetaInfo &meta) {
	if (meta.row_samples.empty() || meta.column_name.empty() || !table.ColumnExists(meta.column_name)) {
		return string();
	}
	auto &duck_table = table.Cast<DuckTableEntry>();
	auto &column_def = table.GetColumn(meta.column_name);
	for (auto &entry : StringUtil::Split(meta.row_samples, ",")) {
		auto parts = StringUtil::Split(entry, ":");
		if (parts.size() != 2) {
			throw InvalidInputException("ngram: index records malformed row samples (%s); the index is malformed",
			                            meta.row_samples);
		}
		int64_t rowid;
		uint64_t recorded;
		try {
			rowid = std::stoll(parts[0]);
			recorded = std::stoull(parts[1]);
		} catch (std::exception &) {
			throw InvalidInputException("ngram: index records malformed row samples (%s); the index is malformed",
			                            meta.row_samples);
		}
		if (rowid > meta.hwm_rowid) {
			// Past the high-water mark the index holds no postings at all, so
			// whatever this row says now, it says nothing about where the
			// index's postings point — the tail scan reads that row live on
			// every query. The state the witnesses exist to catch is a vacuum
			// that shifted indexed rows, and a vacuum can only move a row at or
			// below the mark when a deleted gap sits at or below the mark, which
			// disturbs the witnesses there. So ignoring these costs no detection
			// and removes a whole class of false refusals: a bounded refresh
			// leaves a tail behind by design, and an ordinary UPDATE or a
			// vacuumed DELETE in that tail must not make the index
			// unmaintainable. It also disarms witnesses written above the mark
			// by an earlier version of this extension, which the recording side
			// can no longer retract.
			continue;
		}
		uint64_t current = 0;
		if (!ReadRowHash(context, duck_table, column_def, rowid, current)) {
			// deleted or not visible: the index over-approximates for it, which
			// recheck already handles, and it says nothing about the rest
			continue;
		}
		if (current != recorded) {
			return StringUtil::Format(
			    "row id %lld no longer holds the value the index recorded for it (a checkpoint vacuum moved rows out "
			    "from under the index's postings, or the row was updated in place)",
			    rowid);
		}
	}
	return string();
}

string CertainStaleReason(const MetaInfo &meta, const TableFingerprint &now) {
	auto same_instance = !meta.instance_id.empty() && meta.instance_id == now.instance_id;
	auto same_catalog = same_instance && meta.catalog_oid == now.catalog_oid;
	if (same_catalog && meta.table_oid != now.table_oid) {
		// Same running instance, same attach incarnation, different catalog
		// entry: the table this index was built for is gone. Table oids
		// survive every ALTER (verified on v1.5.5) and are only handed out
		// again on CREATE, so within one incarnation this is proof. The
		// catalog oid has to match too — ATTACH mints fresh oids for
		// everything it loads, so after a DETACH + re-ATTACH the table's oid
		// differs for a perfectly healthy index.
		return "the table was dropped and re-created (or replaced) after the index was built, so the index describes "
		       "rows that no longer exist";
	}
	if (now.ProvesSameTable(meta)) {
		// This is provably the same table object the index was built from, so
		// it cannot have been re-created and the rest of the column list says
		// nothing about the index's health: an ALTER that renames, drops or
		// retypes some *other* column leaves every indexed rowid and value
		// where it was. Only the indexed column's own shape matters.
		auto current_type = now.ColumnType(meta.column_name);
		if (current_type.empty()) {
			return "the indexed column " + meta.column_name + " no longer exists on the table";
		}
		if (!meta.column_type.empty() && current_type != meta.column_type) {
			return "the indexed column " + meta.column_name + " is now " + current_type + ", not " + meta.column_type;
		}
	} else if (!meta.schema_fingerprint.empty() && now.schema_fingerprint.rfind(meta.schema_fingerprint, 0) != 0) {
		// Identity cannot be proven here (another session, another attach
		// incarnation), and the column list is then the only signal that
		// separates "the same table, altered" from "a different table under
		// the same name". Columns appended at the end keep every recorded
		// column in place and preserve rowids, so they are tolerated; anything
		// else is treated as a re-creation.
		return "the table's column list changed after the index was built, or a table with a different column list "
		       "now carries its name (columns were removed, renamed, retyped or reordered)";
	}
	if (meta.hwm_rowid >= 0 && meta.hwm_rowid < LOCAL_ROWID_START && now.total_rows <= meta.hwm_rowid) {
		// the table shrank below the index's coverage: rowids the index refers
		// to are gone, and the rowid space is being handed out again
		return StringUtil::Format("the table now holds %lld committed rowids but the index covers rowids up to %lld "
		                          "(rows were removed by a checkpoint vacuum, or the table was re-created)",
		                          now.total_rows, meta.hwm_rowid);
	}
	return string();
}

//===----------------------------------------------------------------------===//
// Shared pragma scaffolding
//===----------------------------------------------------------------------===//

//! An indexed column of a base table, resolved against both the catalog and
//! the shadow tables, with its meta row already read and validated.
struct MaintenanceColumn {
	string column_name;
	MetaInfo meta;
};

static string RequireStringParam(const Value &value, const char *fn, const char *name) {
	if (value.IsNull()) {
		throw BinderException("%s: parameter %s cannot be NULL", fn, name);
	}
	auto text = value.ToString();
	if (text.empty()) {
		throw BinderException("%s: parameter %s cannot be empty", fn, name);
	}
	return text;
}

//! Resolve every index that the pragma should operate on: all indexed columns
//! of the table, or the single column named by `only_column`. Reads and
//! validates each meta row (format version, ownership) and refuses outright
//! when a detector proves the index cannot be maintained incrementally.
static vector<MaintenanceColumn> ResolveMaintenanceColumns(ClientContext &context, const char *fn,
                                                           ResolvedTarget &target, const string &only_column,
                                                           const TableFingerprint &fingerprint) {
	vector<string> names;
	for (auto &meta_name : ExistingMetaTables(context, target)) {
		names.push_back(meta_name.substr(strlen("meta_")));
	}
	if (names.empty()) {
		throw CatalogException("%s: no ngram index exists on %s", fn, target.table_name);
	}
	if (!only_column.empty()) {
		vector<string> filtered;
		for (auto &name : names) {
			if (StringUtil::CIEquals(name, only_column)) {
				filtered.push_back(name);
			}
		}
		if (filtered.empty()) {
			throw CatalogException("%s: no ngram index exists on %s.%s", fn, target.table_name, only_column);
		}
		names = std::move(filtered);
	}
	std::sort(names.begin(), names.end());

	auto &transaction = DuckTransaction::Get(context, target.entry->ParentCatalog());
	vector<MaintenanceColumn> result;
	for (auto &column_name : names) {
		if (!target.entry->ColumnExists(column_name)) {
			throw CatalogException("%s: the ngram index on %s references column %s, which no longer exists; drop the "
			                       "index with PRAGMA drop_ngram_index",
			                       fn, target.table_name, column_name);
		}
		auto &meta_entry = ResolveExistingTable(context, target.catalog_name, target.shadow_schema,
		                                        MetaTableName(column_name), "ngram index meta table");
		ShadowTarget shadow {target.schema_name, target.table_name, column_name, target.shadow_schema};
		MaintenanceColumn column;
		column.column_name = column_name;
		column.meta = ReadMeta(context, transaction, meta_entry, shadow);
		auto reason = CertainStaleReason(column.meta, fingerprint);
		if (reason.empty()) {
			// the pragmas can afford to re-read the recorded witnesses; the
			// query paths cannot, which is why refresh and compact are where a
			// vacuum that appends have papered over gets caught
			reason = SampleStaleReason(context, *target.entry, column.meta);
		}
		if (!reason.empty()) {
			// A rebuild can take hours at scale, so the user chooses when to
			// pay for it: refusing here keeps the stale index exactly as it
			// is, which is misses-only, instead of cementing the damage by
			// advancing the high-water mark over moved rows.
			throw InvalidInputException("%s: the ngram index on %s.%s cannot be maintained incrementally because %s. "
			                            "Rebuild it: PRAGMA drop_ngram_index('%s', '%s') then PRAGMA "
			                            "create_ngram_index('%s', '%s')",
			                            fn, target.table_name, column_name, reason, target.table_name, column_name,
			                            target.table_name, column_name);
		}
		result.push_back(std::move(column));
	}
	return result;
}

//! A statement that fails the whole script unless the meta row still says what
//! the generated SQL assumes. The pragma callback reads meta in its own
//! transaction, before the transaction the script runs in, so a concurrent
//! refresh could have advanced the high-water mark in between; rather than
//! writing a stale hwm the script stops and asks for a retry.
static string MetaUnchangedGuard(const ResolvedTarget &target, const string &meta_qualified, const string &column_name,
                                 int64_t hwm, const char *fn) {
	return SilentGuard("CASE WHEN (SELECT count(*) FROM " + meta_qualified + " WHERE format_version = " +
	                   to_string(NGRAM_FORMAT_VERSION) + " AND schema_name IS NOT DISTINCT FROM " +
	                   Lit(target.schema_name) + " AND table_name IS NOT DISTINCT FROM " + Lit(target.table_name) +
	                   " AND hwm_rowid = " + to_string(hwm) + ") <> 1 THEN error(" +
	                   Lit(string(fn) + ": the index on " + target.table_name + "." + column_name +
	                       " changed while the statement was being prepared; run it again") +
	                   ") END");
}

//! Re-record the table facts the detectors compare against. Written as
//! literals: the pragma callback reads them before the script runs, so a
//! concurrent append can only make them older than the state the script
//! commits, and every comparison against them stays conservative.
static string FingerprintAssignments(const TableFingerprint &fingerprint, const string &column_name,
                                     const string &row_samples) {
	return "table_oid = " + to_string(fingerprint.table_oid) + ", catalog_oid = " + to_string(fingerprint.catalog_oid) +
	       ", instance_id = " + Lit(fingerprint.instance_id) +
	       ", schema_fingerprint = " + Lit(fingerprint.schema_fingerprint) +
	       ", column_type = " + Lit(fingerprint.ColumnType(column_name)) + ", row_samples = " + Lit(row_samples);
}

//===----------------------------------------------------------------------===//
// PRAGMA ngram_refresh
//
// Indexes the rows between the recorded high-water mark and the table's last
// committed rowid, appends them to the segments table as a new generation
// (readers already union every segment row of a key), and advances the mark.
// Cost is proportional to that tail: the rowid filter is a constant, so row
// groups below the mark are skipped by their zone maps.
//
// With a max_rows bound the call stops partway instead, commits that progress,
// and reports it (see BoundedRefreshEnd and the summary row below); the caller
// loops until remaining_tail is 0. One call is still one transaction — the
// statement preprocessor's auto-wrap is what makes a refresh crash-atomic, and
// it wraps a whole expansion or nothing — so the loop cannot live in here.
//
// Rows updated in place below the mark are NOT re-indexed. duckdb v1.5.5
// updates rows in place and offers no trigger or change-feed to find them
// (CREATE TRIGGER is a parser error), so there is no sound incremental way to
// know which rows changed; that gap is closed by a rebuild, not by refresh.
//===----------------------------------------------------------------------===//

//! The highest rowid a bounded refresh starting from `hwm` may cover.
//!
//! max_rows is spent as a rowid span rather than as a live-row count, and the
//! span is then snapped down to a segment boundary. Three reasons:
//!
//! * A live-row count could only be evaluated when the script runs, so every
//!   partition range would have to be written against a session variable
//!   instead of a literal — and literal rowid ranges are exactly what lets a
//!   partition's scan skip row groups by their zone maps (see the partitioning
//!   note in ngram/index_pragmas.hpp).
//! * A span bounds the work in the safe direction: deletes leave rowid gaps, so
//!   the rows actually indexed are at most max_rows, never more.
//! * segment_no = rowid >> SEGMENT_SHIFT, so an end at a segment boundary means
//!   every (gram, segment_no) of the tail is produced whole by exactly one call
//!   of the loop. The loop then writes the same segment rows, byte for byte,
//!   that one unbounded refresh would have written; only their generation
//!   numbers differ. Snapping down never spends more than max_rows.
//!
//! Bounds smaller than the remainder of the mark's own segment are honoured as
//! given rather than rounded up to it: a segment is 2^SEGMENT_SHIFT rowids, and
//! on long rows one segment can be more text than the caller is willing to put
//! in a single transaction, which is the very thing the bound exists to avoid.
//! Such an increment ends mid-segment and splits that segment's keys across two
//! generations — fragmentation a later ngram_compact merges away.
static int64_t BoundedRefreshEnd(int64_t hwm, int64_t max_rows) {
	if (max_rows >= LOCAL_ROWID_START || hwm > LOCAL_ROWID_START - 1 - max_rows) {
		// the requested span runs past the committed rowid space; there is
		// nothing left to bound
		return LOCAL_ROWID_START - 1;
	}
	auto target = hwm + max_rows;
	auto aligned = (((target + 1) >> SEGMENT_SHIFT) << SEGMENT_SHIFT) - 1;
	return aligned > hwm ? aligned : target;
}

static int64_t RequireRowBound(const Value &value) {
	if (value.IsNull()) {
		throw BinderException("ngram_refresh: parameter max_rows cannot be NULL");
	}
	auto rows = value.GetValue<int64_t>();
	if (rows < 1) {
		throw InvalidInputException("ngram_refresh: max_rows must be at least 1, got %lld", rows);
	}
	return rows;
}

static string RefreshNgramIndexQuery(ClientContext &context, const FunctionParameters &parameters) {
	auto table_input = parameters.values[0].ToString();
	string only_column;
	bool bounded = false;
	int64_t max_rows = 0;
	if (parameters.values.size() > 1) {
		max_rows = RequireRowBound(parameters.values[1]);
		bounded = true;
	}
	for (auto &entry : parameters.named_parameters) {
		if (entry.first == "col") {
			only_column = RequireStringParam(entry.second, "ngram_refresh", "col");
		} else if (entry.first == "max_rows") {
			if (bounded) {
				throw BinderException("ngram_refresh: max_rows was given twice, positionally and by name");
			}
			max_rows = RequireRowBound(entry.second);
			bounded = true;
		} else {
			throw BinderException("ngram_refresh: unknown named parameter %s", entry.first);
		}
	}

	auto target = ResolveTarget(context, table_input, string(), false);
	if (!target.entry->IsDuckTable()) {
		throw BinderException("ngram_refresh: %s is not a DuckDB base table", table_input);
	}
	auto fingerprint = ComputeTableFingerprint(context, *target.entry);
	auto columns = ResolveMaintenanceColumns(context, "ngram_refresh", target, only_column, fingerprint);

	auto base = Ident(target.catalog_name) + "." + Ident(target.schema_name) + "." + Ident(target.table_name);
	auto shadow = Ident(target.catalog_name) + "." + Ident(target.shadow_schema);
	auto local_start = to_string(LOCAL_ROWID_START);

	string script;
	vector<string> summary_rows;
	for (auto &column : columns) {
		auto meta = shadow + "." + Ident(MetaTableName(column.column_name));
		auto segments = shadow + "." + Ident(SegmentsTableName(column.column_name));
		auto stats = shadow + "." + Ident(StatsTableName(column.column_name));
		auto quoted_column = Ident(column.column_name);
		auto hwm = to_string(column.meta.hwm_rowid);
		auto gram_str = to_string(column.meta.options.gram_size);
		auto ci_str = column.meta.options.case_insensitive ? "true" : "false";
		// __ngram_ is this extension's reserved temp-table namespace
		auto packed = Ident("__ngram_refresh_packed_" + target.shadow_schema + "_" + column.column_name);
		// rows past the high-water mark, excluding this transaction's local
		// rows: their rowids are reassigned at commit, so indexing them would
		// record postings for rowids that never exist
		auto tail_predicate = "rowid > " + hwm + " AND rowid < " + local_start;
		// A bound only changes anything while it stops short of the table's
		// committed end; a bound that covers the whole tail generates the
		// unbounded script, so "loop until remaining_tail is 0" costs exactly
		// one call when the tail already fits.
		auto bound_end = bounded ? BoundedRefreshEnd(column.meta.hwm_rowid, max_rows) : LOCAL_ROWID_START - 1;
		auto stops_short = bound_end < fingerprint.total_rows - 1;
		auto range_end = stops_short ? bound_end : fingerprint.total_rows - 1;

		script += OwnershipGuard(target, meta);
		script += MetaUnchangedGuard(target, meta, column.column_name, column.meta.hwm_rowid, "ngram_refresh");
		// One statement per rowid-range partition of the tail. Unbounded, the
		// ranges cover exactly what tail_predicate covers, including rows
		// committed between this script being generated and being run, so the
		// high-water mark the UPDATE below records never runs ahead of what was
		// indexed. Bounded, they stop at bound_end and so does the mark.
		auto partitions = BuildPartitionCount(context, EstimateGramCount(context, *target.entry, column.column_name,
		                                                                 column.meta.hwm_rowid + 1, range_end,
		                                                                 column.meta.options.gram_size));
		auto ranges = SegmentAlignedRanges(column.meta.hwm_rowid + 1, range_end, partitions, !stops_short);
		for (idx_t i = 0; i < ranges.size(); i++) {
			script += PackPartitionStatement(
			    packed, i == 0,
			    "SELECT rowid AS r, rowid >> " + to_string(SEGMENT_SHIFT) + " AS segment_no, unnest(trigrams(" +
			        quoted_column + ", " + gram_str + ", " + ci_str + ")) AS gram FROM " + base +
			        " WHERE rowid >= " + to_string(ranges[i].first) + " AND rowid <= " + to_string(ranges[i].second) +
			        " AND " + quoted_column + " IS NOT NULL");
		}
		// a new generation of segment rows for keys the index already holds;
		// readers union every row of a (gram, segment_no), compaction merges.
		// Written in gram order like every other generation, so the probe's
		// `gram = ?` filter keeps pruning row groups by zone map.
		script += "INSERT INTO " + segments +
		          " SELECT gram, segment_no, (SELECT coalesce(max(generation), 0) + 1 FROM " + segments +
		          "), postings, rowid_count, min_rowid, max_rowid FROM " + packed + " ORDER BY gram, segment_no;\n";
		// stats rows are summed per gram by the probe, so appending deltas is
		// enough; compaction folds them back into one row per gram
		script += "INSERT INTO " + stats + " SELECT gram, sum(rowid_count)::BIGINT, count(*)::BIGINT FROM " + packed +
		          " GROUP BY gram;\n";
		// Witnesses are drawn over the range this call records coverage of, not
		// over the whole table: past the mark the index holds no postings, so a
		// witness there would report an ordinary tail UPDATE — or a vacuumed
		// tail DELETE — as an index that cannot be maintained, which is the one
		// state a bounded refresh is in by design. Unbounded this is the same
		// value it always was, so that path is untouched.
		//
		// range_end is an upper bound on the mark rather than the mark itself,
		// which is only known when the UPDATE below runs. The two differ only in
		// the fallback branch, and only over rowids that hold no visible row
		// when the script commits, so a witness lands in the gap solely when a
		// row was visible to this callback and deleted before the script's
		// snapshot. SampleStaleReason ignores witnesses past the recorded mark,
		// which closes that race and every other way one could get there.
		auto row_samples = BuildRowSampleDigest(context, *target.entry, column.column_name, range_end);
		// Unbounded, the new mark is the highest committed rowid the partitions
		// just covered. Bounded, it is bound_end itself — but only once some
		// committed row past bound_end proves the rowid slots at or below it are
		// settled.
		//
		// Why that proof is needed, and why it is enough: rowids are handed out
		// to a transaction's appended rows while it commits, under the table's
		// append lock (DataTable::AppendLock, src/storage/data_table.cpp, sets
		// row_start from the table's current row count), reached through
		// LocalStorage::Commit -> LocalStorage::Flush. On a database with a WAL
		// that runs inside DuckTransaction::WriteToWAL, which
		// DuckTransactionManager::CommitTransaction calls with the transaction
		// lock released and the WAL lock held; info.commit_id is taken after it
		// returns, still inside that same WAL-lock critical section, which ends
		// only once the commit is finished. On a database without a WAL
		// (in-memory, or NO_WAL_WRITES) the allocation happens instead in
		// DuckTransaction::Commit, under the transaction lock that already
		// covers the commit id. Either way one lock serializes both events for
		// every committing transaction, and ShouldWriteToWAL is a per-database
		// property, so a database never mixes the two regimes. Commit-id order
		// and rowid order are therefore one and the same order. So a rowid slot
		// in the gap between the highest
		// row this transaction can see and the table's allocated end may still
		// belong to an append that commits after us — recording a mark over it
		// would bury rows that no later refresh would ever look at again. But if
		// we can see any row past bound_end, its committer's commit id is below
		// our snapshot, hence so is that of every transaction holding a lower
		// slot: everything at or below bound_end is visible-or-deleted, and the
		// partitions above indexed all of it.
		//
		// When nothing is visible past bound_end the mark falls back to the
		// unbounded rule restricted to this increment, which is conservative;
		// the tail is then empty afterwards either way, so the loop still ends.
		// Advancing over an increment whose rows were all deleted is what keeps
		// that loop from spinning on a mark that never moves.
		string new_hwm;
		if (stops_short) {
			auto bound_str = to_string(bound_end);
			new_hwm = "CASE WHEN EXISTS (SELECT 1 FROM " + base + " WHERE rowid > " + bound_str + " AND rowid < " +
			          local_start + ") THEN " + bound_str + " ELSE coalesce((SELECT max(rowid) FROM " + base +
			          " WHERE " + tail_predicate + " AND rowid <= " + bound_str + "), hwm_rowid) END";
		} else {
			new_hwm = "coalesce((SELECT max(rowid) FROM " + base + " WHERE " + tail_predicate + "), hwm_rowid)";
		}
		script += "UPDATE " + meta + " SET hwm_rowid = " + new_hwm + ", " +
		          FingerprintAssignments(fingerprint, column.column_name, row_samples) + ";\n";
		script += "DROP TABLE " + packed + ";\n";

		if (bounded) {
			// Progress, read back from what this transaction just committed:
			// rows_indexed counts the committed rows the mark newly covers
			// (rows whose value is NULL included — they are covered, they just
			// contribute no grams), and remaining_tail counts what a query is
			// still answering with a tail scan. Both are counted in the same
			// transaction, and so in the same snapshot, as the packing pass, so
			// they reconcile with what was indexed exactly. The old mark is a
			// literal: MetaUnchangedGuard above has already refused the whole
			// script if the meta row no longer held it.
			auto recorded = "(SELECT hwm_rowid FROM " + meta + ")";
			summary_rows.push_back("SELECT " + Lit(column.column_name) + " AS column_name, (SELECT count(*) FROM " +
			                       base + " WHERE rowid > " + hwm + " AND rowid <= " + recorded +
			                       ") AS rows_indexed, " + recorded + " AS hwm_rowid, (SELECT count(*) FROM " + base +
			                       " WHERE rowid > " + recorded + " AND rowid < " + local_start +
			                       ") AS remaining_tail");
		}
	}
	if (!summary_rows.empty()) {
		// The pragma's only output row (guards assign to a session variable and
		// the rest of the script returns counts, not results), and the last
		// statement of an expansion that is still several statements long, so
		// the preprocessor's BEGIN/COMMIT — the crash atomicity this pragma
		// rests on — is untouched.
		script += StringUtil::Join(summary_rows, " UNION ALL ") + " ORDER BY column_name;\n";
	}
	return script;
}

//===----------------------------------------------------------------------===//
// PRAGMA ngram_compact
//
// Merges the segment rows that share a (gram, segment_no) — every refresh
// appends a new generation, and an index written by an older version of this
// extension could also hold per-thread partial segments — back into one row
// per key, and drops postings whose rowid no longer exists. Results never
// change: readers already union duplicate rows, and a posting for a deleted
// row is already filtered by recheck.
//
// purge := true widens the rewrite from the fragmented keys to every key, so
// dead postings are removed everywhere rather than only where the merge was
// going to rewrite the blob anyway.
//===----------------------------------------------------------------------===//

static string CompactNgramIndexQuery(ClientContext &context, const FunctionParameters &parameters) {
	auto table_input = parameters.values[0].ToString();
	string only_column;
	bool purge_everywhere = false;
	for (auto &entry : parameters.named_parameters) {
		if (entry.first == "col") {
			only_column = RequireStringParam(entry.second, "ngram_compact", "col");
		} else if (entry.first == "purge") {
			if (entry.second.IsNull()) {
				throw BinderException("ngram_compact: parameter purge cannot be NULL");
			}
			purge_everywhere = entry.second.GetValue<bool>();
		} else {
			throw BinderException("ngram_compact: unknown named parameter %s", entry.first);
		}
	}

	auto target = ResolveTarget(context, table_input, string(), false);
	if (!target.entry->IsDuckTable()) {
		throw BinderException("ngram_compact: %s is not a DuckDB base table", table_input);
	}
	auto fingerprint = ComputeTableFingerprint(context, *target.entry);
	auto columns = ResolveMaintenanceColumns(context, "ngram_compact", target, only_column, fingerprint);

	auto base = Ident(target.catalog_name) + "." + Ident(target.schema_name) + "." + Ident(target.table_name);
	auto shadow = Ident(target.catalog_name) + "." + Ident(target.shadow_schema);

	string script;
	for (auto &column : columns) {
		auto meta = shadow + "." + Ident(MetaTableName(column.column_name));
		auto segments = shadow + "." + Ident(SegmentsTableName(column.column_name));
		auto stats = shadow + "." + Ident(StatsTableName(column.column_name));
		auto keys = Ident("__ngram_compact_keys_" + target.shadow_schema + "_" + column.column_name);
		auto packed = Ident("__ngram_compact_packed_" + target.shadow_schema + "_" + column.column_name);

		script += OwnershipGuard(target, meta);
		script += MetaUnchangedGuard(target, meta, column.column_name, column.meta.hwm_rowid, "ngram_compact");
		script += "CREATE OR REPLACE TEMP TABLE " + keys + " AS SELECT gram, segment_no FROM " + segments +
		          " GROUP BY gram, segment_no" + (purge_everywhere ? "" : " HAVING count(*) > 1") + ";\n";
		// decode the selected keys, drop postings for rowids that no longer
		// exist, and re-pack. A rowid is dropped only when the base table has
		// no such row in this transaction's snapshot, so a live posting can
		// never be lost; readers on older snapshots still see the pre-compact
		// segment rows through MVCC. Partitioned by segment_no, the same
		// boundary the build partitions on, so each key is re-packed whole in
		// exactly one statement. The estimate covers every posting in the index,
		// which is what a purging compaction re-encodes; a plain compaction
		// touches only the fragmented keys and so is over-partitioned rather
		// than under.
		auto partitions =
		    BuildPartitionCount(context, EstimateGramCount(context, *target.entry, column.column_name, 0,
		                                                   column.meta.hwm_rowid, column.meta.options.gram_size));
		auto ranges = SegmentAlignedRanges(0, column.meta.hwm_rowid, partitions);
		for (idx_t i = 0; i < ranges.size(); i++) {
			script += PackPartitionStatement(
			    packed, i == 0,
			    "SELECT gram, segment_no, r FROM ngram_unpack_postings((SELECT s.gram, s.segment_no, s.postings FROM " +
			        segments + " s WHERE s.segment_no >= " + to_string(ranges[i].first >> SEGMENT_SHIFT) +
			        " AND s.segment_no <= " + to_string(ranges[i].second >> SEGMENT_SHIFT) +
			        " AND EXISTS (SELECT 1 FROM " + keys +
			        " k WHERE k.gram = s.gram AND k.segment_no = s.segment_no))) WHERE r IN (SELECT rowid FROM " +
			        base + ")");
		}
		script += "DELETE FROM " + segments + " WHERE EXISTS (SELECT 1 FROM " + keys + " k WHERE k.gram = " + segments +
		          ".gram AND k.segment_no = " + segments + ".segment_no);\n";
		// re-inserted in gram order, so the merged rows prune by zone map for
		// the probe exactly as the generations they replace did
		script += "INSERT INTO " + segments +
		          " SELECT gram, segment_no, 0, postings, rowid_count, min_rowid, max_rowid FROM " + packed +
		          " ORDER BY gram, segment_no;\n";
		// stats are cheap to rebuild exactly (two small columns) and the merge
		// changed both the per-gram row counts and the segment counts
		script += "DELETE FROM " + stats + ";\n";
		script += "INSERT INTO " + stats + " SELECT gram, sum(rowid_count)::BIGINT, count(*)::BIGINT FROM " + segments +
		          " GROUP BY gram;\n";
		script += "UPDATE " + meta + " SET " +
		          FingerprintAssignments(
		              fingerprint, column.column_name,
		              BuildRowSampleDigest(context, *target.entry, column.column_name, column.meta.hwm_rowid)) +
		          ";\n";
		script += "DROP TABLE " + keys + ";\n";
		script += "DROP TABLE " + packed + ";\n";
	}
	return script;
}

void RegisterMaintenance(ExtensionLoader &loader) {
	// two overloads so the bound can be written either way: positionally,
	// PRAGMA ngram_refresh('t', 100000), or by name, max_rows = 100000 (pragma
	// named parameters take =, not :=)
	PragmaFunctionSet refresh_set("ngram_refresh");
	for (auto &arguments :
	     vector<vector<LogicalType>> {{LogicalType::VARCHAR}, {LogicalType::VARCHAR, LogicalType::BIGINT}}) {
		auto refresh = PragmaFunction::PragmaCall("ngram_refresh", RefreshNgramIndexQuery, arguments);
		refresh.named_parameters["col"] = LogicalType::VARCHAR;
		refresh.named_parameters["max_rows"] = LogicalType::BIGINT;
		refresh_set.AddFunction(std::move(refresh));
	}
	loader.RegisterFunction(std::move(refresh_set));

	auto compact = PragmaFunction::PragmaCall("ngram_compact", CompactNgramIndexQuery, {LogicalType::VARCHAR});
	compact.named_parameters["col"] = LogicalType::VARCHAR;
	compact.named_parameters["purge"] = LogicalType::BOOLEAN;
	loader.RegisterFunction(compact);
}

} // namespace ngram
} // namespace duckdb
