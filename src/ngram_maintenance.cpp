#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/enums/database_modification_type.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/storage_lock.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "ngram/index_pragmas.hpp"
#include "ngram/maintenance.hpp"
#include "ngram/rowid_guard.hpp"
#include "ngram/search_core.hpp"

#include <algorithm>
#include <limits>
#include <mutex>

namespace duckdb {
namespace ngram {

//===----------------------------------------------------------------------===//
// Shared pragma scaffolding
//===----------------------------------------------------------------------===//

//! An indexed column of a base table, resolved against the registry, with its
//! metadata already read and its guard verdict already passed.
struct MaintenanceColumn {
	string column_name;
	IndexLocation location;
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

static string ScratchName(const char *purpose) {
	return Ident(string("__ngram_") + purpose + "_" + UUID::ToString(UUID::GenerateRandomUUID()));
}

//! Read and validate one indexed column. This is used once while expanding the
//! pragma for useful early errors and again after the generated script owns its
//! transaction-lifetime vacuum fence, where it becomes the correctness check.
static MaintenanceColumn ResolveMaintenanceColumn(ClientContext &context, const char *fn, ResolvedTarget &target,
                                                  const IndexLocation &location) {
	auto &column_name = location.column_name;
	if (!target.entry->ColumnExists(column_name)) {
		throw CatalogException("%s: the ngram index on %s references column %s, which no longer exists; drop the "
		                       "index with PRAGMA drop_ngram_index",
		                       fn, target.table_name, column_name);
	}
	if (!IndexLocationAvailable(context, target, location)) {
		throw InvalidInputException("%s: index storage is unavailable", fn);
	}
	MaintenanceColumn column;
	column.column_name = column_name;
	column.location = location;
	column.meta = ReadMeta(context, target.catalog_name, location);
	auto reason = RowIdGuardReason(context, target.entry->Cast<DuckTableEntry>(), column.meta);
	if (!reason.empty()) {
		throw InvalidInputException("%s: the ngram index on %s.%s cannot be maintained incrementally because %s. "
		                            "Rebuild it: PRAGMA drop_ngram_index('%s', '%s') then PRAGMA "
		                            "create_ngram_index('%s', '%s')",
		                            fn, target.table_name, column_name, reason, target.table_name, column_name,
		                            target.table_name, column_name);
	}
	return column;
}

//! Resolve every index that the pragma should operate on: all indexed columns
//! of the table, or the single column named by `only_column`. Reads and
//! validates each registry row and refuses outright when the guard cannot
//! prove the index maintainable.
static vector<MaintenanceColumn> ResolveMaintenanceColumns(ClientContext &context, const char *fn,
                                                           ResolvedTarget &target, const string &only_column) {
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

static string MaintenanceGuardCall(const char *fn, const ResolvedTarget &target, const MaintenanceColumn &column) {
	return SystemFunction(NGRAM_MAINTENANCE_GUARD) + "(" + Lit(fn) + ", " + Lit(target.catalog_name) + ", " +
	       Lit(target.schema_name) + ", " + Lit(target.table_name) + ", " + Lit(column.column_name) + ", false, " +
	       to_string(column.meta.hwm_rowid) + ", " + to_string(column.meta.options.gram_size) + ", " +
	       (column.meta.options.case_insensitive ? "true" : "false") + ", '', '', " + Lit(column.location.index_ref) +
	       ", " + to_string(column.location.registry_oid) + ", false)";
}

static string StorageTable(const ResolvedTarget &target, const string &table) {
	return Ident(target.catalog_name) + "." + Ident(NGRAM_SCHEMA) + "." + Ident(table);
}

//! The registry row of one index, as a FROM/UPDATE target.
static string RegistryRow(const ResolvedTarget &target, const MaintenanceColumn &column) {
	return StorageTable(target, "registry") + " WHERE index_id = " + Lit(column.location.index_ref) + "::UUID";
}

//! DuckTransaction drops its own vacuum lock just before running an automatic
//! checkpoint during commit. Keep one extra shared lock until DuckDB's
//! post-commit callback, which runs after that checkpoint, or until rollback.
class MaintenanceFenceState final : public ClientContextState {
private:
	struct HeldFence {
		HeldFence(DuckTransactionManager *manager_p, unique_ptr<StorageLockKey> vacuum_lock_p)
		    : manager(manager_p), vacuum_lock(std::move(vacuum_lock_p)) {
		}

		DuckTransactionManager *manager;
		unique_ptr<StorageLockKey> vacuum_lock;
		unique_ptr<StorageLockKey> creation_lock;
		DataTable *creation_table = nullptr;
		string creation_schema_name;
		string creation_table_name;
		string creation_column_name;
		string creation_guard_name;
		transaction_t transaction_id = 0;
	};

public:
	void Acquire(DuckTransactionManager &manager) {
		lock_guard<mutex> guard(fences_lock);
		for (auto &held : fences) {
			if (held.manager == &manager) {
				return;
			}
		}
		auto vacuum_lock = manager.SharedVacuumLock();
		fences.emplace_back(&manager, std::move(vacuum_lock));
	}

	void HoldCreationBarrier(DuckTransaction &transaction, DataTable &original_table,
	                         unique_ptr<StorageLockKey> creation_lock, string schema_name, string table_name,
	                         string column_name, string guard_name) {
		lock_guard<mutex> guard(fences_lock);
		for (auto &held : fences) {
			if (held.manager != &transaction.GetTransactionManager()) {
				continue;
			}
			if (held.creation_lock) {
				throw InvalidInputException("ngram creation barrier is already held for this catalog");
			}
			held.creation_lock = std::move(creation_lock);
			held.creation_table = &original_table;
			held.creation_schema_name = std::move(schema_name);
			held.creation_table_name = std::move(table_name);
			held.creation_column_name = std::move(column_name);
			held.creation_guard_name = std::move(guard_name);
			held.transaction_id = transaction.transaction_id;
			return;
		}
		throw InvalidInputException("ngram creation barrier has no maintenance fence");
	}

	void FinishCreation(DuckTransaction &transaction, DataTable &current_table, const string &schema_name,
	                    const string &table_name, const string &column_name, const string &guard_name) {
		lock_guard<mutex> guard(fences_lock);
		for (auto &held : fences) {
			if (held.manager != &transaction.GetTransactionManager() || !held.creation_lock) {
				continue;
			}
			if (held.transaction_id != transaction.transaction_id || !held.creation_table) {
				throw TransactionException("ngram creation barrier belongs to a different transaction");
			}
			if (!StringUtil::CIEquals(held.creation_schema_name, schema_name) ||
			    !StringUtil::CIEquals(held.creation_table_name, table_name) ||
			    !StringUtil::CIEquals(held.creation_column_name, column_name) ||
			    held.creation_guard_name != guard_name) {
				throw TransactionException("ngram creation barrier does not match the fresh rowid guard");
			}
			if (held.creation_table->IsMainTable() || !current_table.IsMainTable() ||
			    held.creation_table == &current_table) {
				throw TransactionException("ngram creation cannot finish because the base table was not replaced");
			}
			held.creation_lock.reset();
			held.creation_table = nullptr;
			held.creation_schema_name.clear();
			held.creation_table_name.clear();
			held.creation_column_name.clear();
			held.creation_guard_name.clear();
			held.transaction_id = 0;
			return;
		}
		throw TransactionException("ngram creation barrier is not held for this catalog");
	}

	bool OwnsCreationBarrier(DuckTransactionManager &manager) {
		lock_guard<mutex> guard(fences_lock);
		for (auto &held : fences) {
			if (held.manager == &manager && held.creation_lock) {
				return true;
			}
		}
		return false;
	}

	void TransactionCommit(MetaTransaction &, ClientContext &) override {
		Clear();
	}
	void TransactionRollback(MetaTransaction &, ClientContext &) override {
		Clear();
	}

private:
	void Clear() {
		lock_guard<mutex> guard(fences_lock);
		fences.clear();
	}

	mutex fences_lock;
	vector<HeldFence> fences;
};

bool ContextOwnsCreationBarrier(ClientContext &context, DuckTransactionManager &manager) {
	auto state = context.registered_state->Get<MaintenanceFenceState>("ngram_maintenance_fence");
	return state && state->OwnsCreationBarrier(manager);
}

static void AcquireMaintenanceFence(ClientContext &context, Catalog &catalog) {
	auto &transaction = DuckTransaction::Get(context, catalog);
	transaction.SetModifications(DatabaseModificationType::INSERT_DATA);
	auto state = context.registered_state->GetOrCreate<MaintenanceFenceState>("ngram_maintenance_fence");
	state->Acquire(transaction.GetTransactionManager());
}

//! Exclude every pre-existing writer before replacing the DataTable. An
//! exclusive upgrade alone is not enough for an old transaction that already
//! committed after this snapshot, so the timestamp check is deliberately
//! made only after the upgrade succeeds.
static void PrepareCreationTransaction(DuckTransaction &transaction) {
	auto undo = transaction.GetUndoProperties();
	if (undo.has_updates || undo.has_deletes || undo.has_catalog_changes) {
		throw TransactionException(
		    "create_ngram_index cannot follow updates, deletes, or catalog changes in the same transaction; "
		    "commit or roll back those changes and retry");
	}
	transaction.SetModifications(DatabaseModificationType::CREATE_INDEX);
}

static unique_ptr<StorageLockKey> AcquireCreationBarrier(DuckTransaction &transaction) {
	PrepareCreationTransaction(transaction);
	auto lock = transaction.TryGetCheckpointLock();
	if (!lock) {
		throw TransactionException(
		    "create_ngram_index could not exclude an active writer; roll back this transaction and retry");
	}
	if (transaction.GetTransactionManager().GetLastCommit() > transaction.start_time) {
		throw TransactionException(
		    "create_ngram_index started before a recently committed writer; roll back this transaction and retry");
	}
	return lock;
}

//! Acquires the vacuum fence, then repeats the checks that were necessarily
//! done once during pragma expansion. The transaction owns its normal copy;
//! MaintenanceFenceState bridges the small commit/autocheckpoint interval.
static void MaintenanceGuardFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto output = FlatVector::GetData<bool>(result);
	for (idx_t row = 0; row < args.size(); row++) {
		auto fn = args.GetValue(0, row).ToString();
		auto catalog_name = args.GetValue(1, row).ToString();
		auto schema_name = args.GetValue(2, row).ToString();
		auto table_name = args.GetValue(3, row).ToString();
		auto column_name = args.GetValue(4, row).ToString();
		auto creating = args.GetValue(5, row).GetValue<bool>();
		auto expected_hwm = args.GetValue(6, row).GetValue<int64_t>();
		auto expected_gram_size = args.GetValue(7, row).GetValue<int64_t>();
		auto expected_case_insensitive = args.GetValue(8, row).GetValue<bool>();
		auto guard_name = args.GetValue(9, row).ToString();
		auto guard_token = args.GetValue(10, row).ToString();
		IndexLocation location;
		location.index_ref = args.GetValue(11, row).ToString();
		location.column_name = column_name;
		location.registry_oid = NumericCast<idx_t>(args.GetValue(12, row).GetValue<int64_t>());
		auto expected_registry_bootstrap = args.GetValue(13, row).GetValue<bool>();

		auto &catalog = Catalog::GetCatalog(context, catalog_name);
		AcquireMaintenanceFence(context, catalog);
		if (fn == "drop_ngram_index_by_id") {
			ResolvedTarget drop_target {catalog_name, schema_name, table_name, column_name, nullptr};
			if (!IndexLocationAvailable(context, drop_target, location)) {
				throw InvalidInputException("%s: index storage was removed after preparation", fn);
			}
			if (!guard_name.empty() &&
			    OtherGuardReferences(context, drop_target, guard_name, location.index_ref) != 0) {
				throw InvalidInputException("%s: another ngram index now shares the rowid guard; run it again", fn);
			}
			output[row] = true;
			continue;
		}
		unique_ptr<StorageLockKey> creation_lock;
		if (creating && guard_token.empty()) {
			creation_lock = AcquireCreationBarrier(DuckTransaction::Get(context, catalog));
		}

		auto qualified = Ident(catalog_name) + "." + Ident(schema_name) + "." + Ident(table_name);
		auto target = ResolveTarget(context, qualified, column_name, true);
		auto &table = target.entry->Cast<DuckTableEntry>();
		if (creating) {
			ValidateRegistryForCreate(context, catalog_name, location.registry_oid, expected_registry_bootstrap);
			// Enumerate again behind the fence: another connection may have
			// registered an index on this table after pragma preprocessing.
			auto table_target = target;
			table_target.column_name.clear();
			auto siblings = ExistingIndexes(context, table_target);
			for (auto &sibling : siblings) {
				if (StringUtil::CIEquals(sibling.column_name, column_name)) {
					throw InvalidInputException(
					    "An ngram index already exists on %s.%s (%s); use drop_ngram_index first", target.table_name,
					    column_name, sibling.index_ref);
				}
				if (guard_token.empty() || sibling.guard_name != guard_name || sibling.guard_token != guard_token) {
					throw InvalidInputException(
					    "%s: the ngram indexes on %s changed while the statement was being prepared; run it again", fn,
					    table_name);
				}
			}
			if (creation_lock) {
				auto fence_state =
				    context.registered_state->GetOrCreate<MaintenanceFenceState>("ngram_maintenance_fence");
				fence_state->HoldCreationBarrier(DuckTransaction::Get(context, catalog), table.GetStorage(),
				                                 std::move(creation_lock), target.schema_name, target.table_name,
				                                 column_name, guard_name);
			} else {
				if (siblings.empty()) {
					throw InvalidInputException(
					    "%s: the ngram indexes on %s changed while the statement was being prepared; run it again", fn,
					    table_name);
				}
				MetaInfo shared;
				shared.column_name = column_name;
				shared.guard_name = guard_name;
				shared.guard_token = guard_token;
				auto reason = RowIdGuardReason(context, table, shared);
				if (!reason.empty()) {
					throw InvalidInputException("%s: the rowid guard shared by the ngram indexes on %s cannot cover %s "
					                            "(%s); drop the table's ngram indexes and rebuild them",
					                            fn, table_name, column_name, reason);
				}
			}
		} else {
			auto column = ResolveMaintenanceColumn(context, fn.c_str(), target, location);
			if (column.meta.hwm_rowid != expected_hwm ||
			    column.meta.options.gram_size != NumericCast<idx_t>(expected_gram_size) ||
			    column.meta.options.case_insensitive != expected_case_insensitive) {
				throw InvalidInputException("%s: the index on %s.%s changed while the statement was being prepared; "
				                            "run it again",
				                            fn, target.table_name, column_name);
			}
		}
		output[row] = true;
	}
}

//! Prove the scan-free physical guard was installed before releasing the
//! creation EXCLUSIVE, and that ADD/DROP replaced the original DataTable.
static void CreationFinishFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto output = FlatVector::GetData<string_t>(result);
	for (idx_t row = 0; row < args.size(); row++) {
		auto catalog_name = args.GetValue(0, row).ToString();
		auto schema_name = args.GetValue(1, row).ToString();
		auto table_name = args.GetValue(2, row).ToString();
		auto column_name = args.GetValue(3, row).ToString();
		auto guard_name = args.GetValue(4, row).ToString();
		auto &catalog = Catalog::GetCatalog(context, catalog_name);
		auto &table = ResolveExistingTable(context, catalog_name, schema_name, table_name, "ngram index base table");
		auto token = InstalledRowIdGuardToken(table, column_name, guard_name);
		auto context_state = context.registered_state->GetOrCreate<MaintenanceFenceState>("ngram_maintenance_fence");
		context_state->FinishCreation(DuckTransaction::Get(context, catalog), table.GetStorage(), schema_name,
		                              table_name, column_name, guard_name);
		output[row] = StringVector::AddString(result, token);
	}
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
// The rowid guard turns indexed-column updates into delete+insert, so their
// live replacements appear in this tail like ordinary appends.
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
	auto total_rows = TableTotalRows(*target.entry);
	auto columns = ResolveMaintenanceColumns(context, "ngram_refresh", target, only_column);

	auto base = Ident(target.catalog_name) + "." + Ident(target.schema_name) + "." + Ident(target.table_name);
	auto local_start = to_string(LOCAL_ROWID_START);

	string script;
	auto guard = ScratchName("guard");
	bool first_guard = true;
	vector<string> summary_rows;
	for (auto &column : columns) {
		auto segments = StorageTable(target, column.location.SegmentsTable());
		auto stats = StorageTable(target, column.location.StatsTable());
		auto quoted_column = Ident(column.column_name);
		auto hwm = to_string(column.meta.hwm_rowid);
		auto gram_str = to_string(column.meta.options.gram_size);
		auto ci_str = column.meta.options.case_insensitive ? "true" : "false";
		auto packed = ScratchName("refresh_packed");
		auto folded_stats = ScratchName("refresh_stats");
		// rows past the high-water mark, excluding this transaction's local
		// rows: their rowids are reassigned at commit, so indexing them would
		// record postings for rowids that never exist
		auto tail_predicate = "rowid > " + hwm + " AND rowid < " + local_start;
		// A bound only changes anything while it stops short of the table's
		// committed end; a bound that covers the whole tail generates the
		// unbounded script, so "loop until remaining_tail is 0" costs exactly
		// one call when the tail already fits.
		auto bound_end = bounded ? BoundedRefreshEnd(column.meta.hwm_rowid, max_rows) : LOCAL_ROWID_START - 1;
		auto stops_short = bound_end < total_rows - 1;
		auto range_end = stops_short ? bound_end : total_rows - 1;

		auto guard_call = MaintenanceGuardCall("ngram_refresh", target, column);
		if (first_guard) {
			script += "CREATE TEMP TABLE " + guard + " AS SELECT " + guard_call + " AS ignored;\n";
			first_guard = false;
		} else {
			script += "INSERT INTO " + guard + " SELECT " + guard_call + ";\n";
		}
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
			    "SELECT rowid AS r, rowid >> " + to_string(SEGMENT_SHIFT) + " AS segment_no, " +
			        SystemFunction("unnest") + "(" + SystemFunction("trigrams") + "(" + quoted_column + ", " +
			        gram_str + ", " + ci_str + ")) AS gram FROM " + base +
			        " WHERE rowid >= " + to_string(ranges[i].first) + " AND rowid <= " + to_string(ranges[i].second) +
			        " AND " + quoted_column + " IS NOT NULL");
		}
		// a new generation of segment rows for keys the index already holds;
		// readers union every row of a (gram, segment_no), compaction merges.
		// Written in gram order like every other generation, so the probe's
		// `gram = ?` filter keeps pruning row groups by zone map.
		script += "INSERT INTO " + segments + " SELECT gram, segment_no, (SELECT coalesce(" + SystemFunction("max") +
		          "(generation), 0) + 1 FROM " + segments + "), postings, rowid_count, min_rowid, max_rowid FROM " +
		          packed + " ORDER BY " + SystemFunction("encode") + "(gram), segment_no;\n";
		// Fold the existing stats and this delta into one byte-ordered row per
		// gram in a single statement, then replace the table. Refresh generations
		// are individually ordered but DuckDB can place several small appends in
		// one row group, widening that row group's zone map to the whole gram
		// domain, so the whole table is rewritten in one global order. Historical
		// rows are validated on the way; delta rows are fresh and each is one
		// segment. Only the encoded key is grouped so session collations cannot
		// merge byte-distinct grams.
		//
		// The delta is folded here rather than appended to stats first because
		// DuckDB v1.5.5 leaves a table reading zero rows for the rest of the
		// process after an empty batch INSERT ... ORDER BY, a DELETE, and a batch
		// INSERT of at least 122,880 rows into it in one transaction
		// (docs/upstream/duckdb-empty-batch-insert.md). A refresh with an empty
		// tail has an empty delta, so no statement may insert into stats before
		// the DELETE below.
		auto invalid_stats = "gram IS NULL OR row_count IS NULL OR segment_count IS NULL OR row_count <= 0 OR "
		                     "segment_count <= 0";
		auto stats_error = SystemFunction("error") + "('ngram: invalid stats row; the index is malformed')";
		script += "CREATE TEMP TABLE " + folded_stats + " AS SELECT " + SystemFunction("decode") +
		          "(gram_key) AS gram, " + SystemFunction("sum") + "(checked_row_count)::BIGINT AS row_count, " +
		          SystemFunction("sum") + "(checked_segment_count)::BIGINT AS segment_count FROM (SELECT " +
		          SystemFunction("encode") + "(gram) AS gram_key, CASE WHEN " + invalid_stats + " THEN " + stats_error +
		          " ELSE row_count END AS checked_row_count, CASE WHEN " + invalid_stats + " THEN " + stats_error +
		          " ELSE segment_count END AS checked_segment_count FROM " + stats + " UNION ALL SELECT " +
		          SystemFunction("encode") +
		          "(gram) AS gram_key, rowid_count::BIGINT AS checked_row_count, "
		          "1::BIGINT AS checked_segment_count FROM " +
		          packed + ") GROUP BY gram_key ORDER BY gram_key;\n";
		script += "DELETE FROM " + stats + ";\n";
		script += "INSERT INTO " + stats + " SELECT * FROM " + folded_stats + " ORDER BY " + SystemFunction("encode") +
		          "(gram);\n";
		script += "DROP TABLE " + folded_stats + ";\n";
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
			          local_start + ") THEN " + bound_str + " ELSE coalesce((SELECT " + SystemFunction("max") +
			          "(rowid) FROM " + base + " WHERE " + tail_predicate + " AND rowid <= " + bound_str +
			          "), hwm_rowid) END";
		} else {
			new_hwm = "coalesce((SELECT " + SystemFunction("max") + "(rowid) FROM " + base + " WHERE " +
			          tail_predicate + "), hwm_rowid)";
		}
		script += "UPDATE " + StorageTable(target, "registry") + " SET hwm_rowid = " + new_hwm +
		          " WHERE index_id = " + Lit(column.location.index_ref) + "::UUID;\n";
		script += "DROP TABLE " + packed + ";\n";

		if (bounded) {
			// Progress, read back from what this transaction just committed:
			// rows_indexed counts the committed rows the mark newly covers
			// (rows whose value is NULL included — they are covered, they just
			// contribute no grams), and remaining_tail counts what a query is
			// still answering with a tail scan. Both are counted in the same
			// transaction, and so in the same snapshot, as the packing pass, so
			// they reconcile with what was indexed exactly. The old mark is a
			// literal: the execution-time maintenance guard above has already
			// refused the whole script if the registry row no longer held it.
			auto recorded = "(SELECT hwm_rowid FROM " + RegistryRow(target, column) + ")";
			summary_rows.push_back(
			    "SELECT " + Lit(column.column_name) + " AS column_name, (SELECT " + SystemFunction("count") +
			    "(*) FROM " + base + " WHERE rowid > " + hwm + " AND rowid <= " + recorded + ") AS rows_indexed, " +
			    recorded + " AS hwm_rowid, (SELECT " + SystemFunction("count") + "(*) FROM " + base +
			    " WHERE rowid > " + recorded + " AND rowid < " + local_start + ") AS remaining_tail");
		}
	}
	script += "DROP TABLE " + guard + ";\n";
	if (!summary_rows.empty()) {
		// The pragma's only output row (guard writes go to an invocation-scoped
		// temp table and the rest of the script returns counts, not results), and
		// the last statement of an expansion that is still several statements long, so
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
// appends a new generation — back into one row per key. It is index-only and
// retains dead postings; results stay exact because readers already union
// duplicate rows and recheck filters dead rows.
//
// purge := true rewrites every key and consults the base snapshot, removing
// every posting whose rowid is no longer live.
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
	auto columns = ResolveMaintenanceColumns(context, "ngram_compact", target, only_column);

	auto base = Ident(target.catalog_name) + "." + Ident(target.schema_name) + "." + Ident(target.table_name);

	string script;
	auto guard = ScratchName("guard");
	bool first_guard = true;
	for (auto &column : columns) {
		auto segments = StorageTable(target, column.location.SegmentsTable());
		auto stats = StorageTable(target, column.location.StatsTable());
		auto keys = ScratchName("compact_keys");
		auto key_guard = ScratchName("compact_key_guard");
		auto selected = ScratchName("compact_source");
		auto live = ScratchName("compact_live");
		auto packed = ScratchName("compact_packed");

		auto guard_call = MaintenanceGuardCall("ngram_compact", target, column);
		if (first_guard) {
			script += "CREATE TEMP TABLE " + guard + " AS SELECT " + guard_call + " AS ignored;\n";
			first_guard = false;
		} else {
			script += "INSERT INTO " + guard + " SELECT " + guard_call + ";\n";
		}
		script += "CREATE TEMP TABLE " + keys + " AS SELECT " + SystemFunction("decode") +
		          "(gram_key) AS gram, segment_no FROM (SELECT " + SystemFunction("encode") +
		          "(gram) AS gram_key, segment_no FROM " + segments + ") GROUP BY gram_key, segment_no" +
		          (purge_everywhere ? "" : " HAVING " + SystemFunction("count") + "(*) > 1") + ";\n";
		script += "CREATE TEMP TABLE " + key_guard + " AS SELECT CASE WHEN " + SystemFunction("count") +
		          "(*) = 0 THEN true ELSE " + SystemFunction("error") +
		          "('ngram: malformed segments-table key') END AS valid FROM " + keys +
		          " WHERE gram IS NULL OR segment_no IS NULL OR " + SystemFunction("length") +
		          "(gram) != " + to_string(column.meta.options.gram_size) + " OR segment_no < 0 OR segment_no > " +
		          to_string(column.meta.hwm_rowid < 0 ? -1 : column.meta.hwm_rowid >> SEGMENT_SHIFT) + ";\n";
		// The persistent table is gram-ordered for query pruning, so scanning it
		// once per rowid partition multiplies reads. Copy only selected encoded
		// rows into a spillable segment-ordered source once, before decoding.
		script += "CREATE TEMP TABLE " + selected +
		          " AS SELECT s.gram, s.segment_no, s.postings, s.rowid_count, s.min_rowid, s.max_rowid, "
		          "s.generation::BIGINT AS generation FROM " +
		          segments + " s WHERE EXISTS (SELECT 1 FROM " + keys + " k WHERE " + SystemFunction("encode") +
		          "(k.gram) = " + SystemFunction("encode") +
		          "(s.gram) AND k.segment_no = s.segment_no) ORDER BY s.segment_no, " + SystemFunction("encode") +
		          "(s.gram);\n";
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
		// merge-only compaction deliberately retains dead postings for recheck.
		// There is no persisted total-pair counter, and consulting the base or
		// scanning global stats here would merely hide read amplification in
		// pragma preprocessing. Auto requests up to the shared 4096 partitions;
		// the segment-aligned floor split emits one range per segment below that
		// scale and approximately that many ranges above it. An explicit
		// ngram_build_partitions setting is still honoured.
		auto partitions = BuildPartitionCount(context, std::numeric_limits<int64_t>::max());
		auto ranges = SegmentAlignedRanges(0, column.meta.hwm_rowid, partitions, false);
		for (idx_t i = 0; i < ranges.size(); i++) {
			auto segment_lo = to_string(ranges[i].first >> SEGMENT_SHIFT);
			auto segment_hi = to_string(ranges[i].second >> SEGMENT_SHIFT);
			auto source = "SELECT gram, segment_no, r FROM " + SystemFunction("ngram_unpack_postings") +
			              "((SELECT gram, segment_no, postings, rowid_count, min_rowid, max_rowid, generation, " +
			              to_string(column.meta.hwm_rowid) + "::BIGINT AS hwm FROM " + selected +
			              " WHERE segment_no >= " + segment_lo + " AND segment_no <= " + segment_hi + "))";
			if (purge_everywhere) {
				source += " WHERE r IN (SELECT r FROM " + live + " WHERE r >= " + to_string(ranges[i].first) +
				          " AND r <= " + to_string(ranges[i].second) + ")";
			}
			script += PackPartitionStatement(packed, i == 0, source);
		}
		script += "DELETE FROM " + segments + " WHERE EXISTS (SELECT 1 FROM " + keys + " k WHERE " +
		          SystemFunction("encode") + "(k.gram) = " + SystemFunction("encode") + "(" + segments +
		          ".gram) AND k.segment_no = " + segments + ".segment_no);\n";
		// re-inserted in gram order, so the merged rows prune by zone map for
		// the probe exactly as the generations they replace did
		script += "INSERT INTO " + segments +
		          " SELECT gram, segment_no, 0, postings, rowid_count, min_rowid, max_rowid FROM " + packed +
		          " ORDER BY " + SystemFunction("encode") + "(gram), segment_no;\n";
		// Stats are rebuilt from the merged segment metadata: the merge changed
		// both the per-gram row counts and the segment counts, and the two
		// small columns are cheap to recompute exactly. The rebuild runs even
		// when no key was selected. Nothing inserts into stats before this
		// DELETE, and nothing deletes from segments after the possibly empty
		// insert above, so neither table takes the v1.5.5 empty-insert, delete,
		// reinsert shape that empties a table in-process
		// (docs/upstream/duckdb-empty-batch-insert.md).
		script += "DELETE FROM " + stats + ";\n";
		script += "INSERT INTO " + stats + " SELECT " + SystemFunction("decode") + "(gram_key), " +
		          SystemFunction("sum") + "(rowid_count)::BIGINT, " + SystemFunction("count") +
		          "(*)::BIGINT FROM (SELECT " + SystemFunction("encode") + "(gram) AS gram_key, rowid_count FROM " +
		          segments + ") GROUP BY gram_key ORDER BY gram_key;\n";
		script += "DROP TABLE " + keys + ";\n";
		script += "DROP TABLE " + key_guard + ";\n";
		script += "DROP TABLE " + selected + ";\n";
		if (purge_everywhere) {
			script += "DROP TABLE " + live + ";\n";
		}
		script += "DROP TABLE " + packed + ";\n";
	}
	script += "DROP TABLE " + guard + ";\n";
	return script;
}

void RegisterMaintenance(ExtensionLoader &loader) {
	auto guard = ScalarFunction(NGRAM_MAINTENANCE_GUARD,
	                            {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                             LogicalType::VARCHAR, LogicalType::BOOLEAN, LogicalType::BIGINT, LogicalType::BIGINT,
	                             LogicalType::BOOLEAN, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                             LogicalType::BIGINT, LogicalType::BOOLEAN},
	                            LogicalType::BOOLEAN, MaintenanceGuardFunction);
	guard.stability = FunctionStability::VOLATILE;
	guard.SetFallible();
	loader.RegisterFunction(guard);

	auto finish = ScalarFunction(
	    NGRAM_CREATION_FINISH,
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	    LogicalType::VARCHAR, CreationFinishFunction);
	finish.stability = FunctionStability::VOLATILE;
	finish.SetFallible();
	loader.RegisterFunction(finish);

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
