#include "ngram/index_state.hpp"

#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "ngram/catalog.hpp"
#include "ngram/fence.hpp"
#include "ngram/rowid_guard.hpp"

#include <algorithm>

namespace duckdb {
namespace ngram {

//! The state of the guard named `guard_name` on `table`, bound or not, or null
//! with the reason it cannot be read. A pending persisted seal is compared
//! against `checkpoint_iteration` when one is given.
static unique_ptr<RowIdGuardState> ReadGuardState(DuckTableEntry &table, const string &column_name,
                                                  const string &guard_name, string &reason,
                                                  optional_idx checkpoint_iteration = optional_idx()) {
	if (guard_name.empty()) {
		reason = "the index does not record a valid rowid guard";
		return nullptr;
	}
	auto &storage = table.GetStorage();
	auto has_column = table.ColumnExists(column_name);
	StorageIndex expected_column;
	if (has_column) {
		expected_column = table.GetStorageIndex(ColumnIndex(table.GetColumn(column_name).Logical().index));
	}
	for (auto &entry : storage.GetDataTableInfo()->GetIndexes().IndexEntries()) {
		auto &index = *entry.index;
		if (index.GetIndexName() != guard_name) {
			continue;
		}
		if (entry.bind_state.load() == IndexBindState::BINDING) {
			reason = "the recorded rowid guard is in an uncertain bind state";
			return nullptr;
		}
		if (!has_column || index.GetIndexType() != NGRAM_ROWID_GUARD_TYPE ||
		    std::find(index.GetColumnIds().begin(), index.GetColumnIds().end(), expected_column.GetPrimaryIndex()) ==
		        index.GetColumnIds().end()) {
			reason = "the recorded rowid guard has the wrong type or column dependency";
			return nullptr;
		}
		if (index.IsBound()) {
			return make_uniq<RowIdGuardState>(ReadBoundGuardState(index, checkpoint_iteration));
		}
		auto &unbound = index.Cast<UnboundIndex>();
		auto stored = ReadStoredGuardState(unbound.GetStorageInfo());
		if (checkpoint_iteration.IsValid() &&
		    (!stored.checkpoint_iteration.IsValid() ||
		     stored.checkpoint_iteration.GetIndex() != checkpoint_iteration.GetIndex())) {
			stored.unsafe_reuse = true;
		}
		bool buffered;
		{
			// DataTable::AppendToIndexes buffers replay chunks under entry.lock
			// while IndexEntries() holds only the list lock; take the same lock
			// to read the buffer.
			lock_guard<mutex> entry_guard(entry.lock);
			buffered = unbound.HasBufferedReplays();
		}
		if (buffered) {
			// The exact rowid effects are deliberately left to DuckDB's replay
			// binder; until then, one full scan is the only safe answer.
			stored.unsafe_reuse = true;
		}
		auto result = make_uniq<RowIdGuardState>();
		result->token = std::move(stored.token);
		result->max_seen = stored.max_seen;
		result->unsafe_reuse = stored.unsafe_reuse;
		result->protection_compatible = stored.protection_compatible;
		result->column_ids = index.GetColumnIds();
		return result;
	}
	reason = "the recorded rowid guard is missing";
	return nullptr;
}

string InstalledRowIdGuardToken(DuckTableEntry &table, const string &column_name, const string &guard_name) {
	RequirePinnedRuntime();
	string reason;
	auto guard = ReadGuardState(table, column_name, guard_name, reason);
	if (!guard) {
		throw TransactionException("ngram creation cannot finish because the fresh rowid guard is unavailable: %s",
		                           reason);
	}
	if (!guard->protection_compatible) {
		throw TransactionException(
		    "ngram creation cannot finish because the fresh rowid guard is incompatible with this extension build");
	}
	return guard->token;
}

string RowIdGuardDropReason(ClientContext &context, DuckTableEntry &table, const string &guard_name,
                            const string &guard_token, bool bind_unbound) {
	if (guard_name.empty() || guard_token.empty()) {
		return "the index does not record a valid rowid guard name and token";
	}

	auto &storage = table.GetStorage();
	EntryLookupInfo lookup(CatalogType::INDEX_ENTRY, guard_name);
	auto catalog_entry = Catalog::GetEntry(context, table.ParentCatalog().GetName(), table.ParentSchema().name, lookup,
	                                       OnEntryNotFound::RETURN_NULL);
	if (catalog_entry) {
		auto &index_entry = catalog_entry->Cast<DuckIndexEntry>();
		if (index_entry.index_type != NGRAM_ROWID_GUARD_TYPE ||
		    &index_entry.GetDataTableInfo() != storage.GetDataTableInfo().get()) {
			return "an index with the recorded guard name belongs to a different table or type";
		}
	}

	bool needs_bind = false;
	for (auto &entry : storage.GetDataTableInfo()->GetIndexes().IndexEntries()) {
		auto &index = *entry.index;
		if (index.GetIndexName() != guard_name) {
			continue;
		}
		if (!catalog_entry) {
			return "the recorded rowid guard exists in table storage but not in the index catalog";
		}
		if (entry.bind_state.load() == IndexBindState::BINDING) {
			return "the recorded rowid guard is being bound; retry the drop";
		}
		if (index.GetIndexType() != NGRAM_ROWID_GUARD_TYPE) {
			return "the recorded rowid guard has the wrong type";
		}
		string token;
		if (index.IsBound()) {
			token = ReadBoundGuardState(index, optional_idx()).token;
		} else {
			auto &options = index.Cast<UnboundIndex>().GetStorageInfo().options;
			auto token_entry = options.find(NGRAM_GUARD_TOKEN_OPTION);
			if (token_entry == options.end() || token_entry->second.IsNull()) {
				return "the unbound rowid guard does not persist an incarnation token";
			}
			token = StringValue::Get(token_entry->second);
			needs_bind = true;
		}
		if (token != guard_token) {
			return "the recorded rowid guard was dropped and re-created";
		}
		if (!needs_bind) {
			return string();
		}
		break;
	}
	if (needs_bind) {
		if (!bind_unbound) {
			return "the recorded rowid guard remained unbound; retry the drop";
		}
		// BindIndexes binds every guard of this type. Pre-screen all of them so
		// no malformed expression can leave DuckDB's v1.5.5 bind state poisoned.
		// Persisted state/source mismatches are intentionally tolerated by
		// GuardCreateInstance and become bound quarantine guards.
		if (!CanBindRowIdGuards(table, false)) {
			return "an unbound rowid guard cannot be safely bound; retry after other guard activity finishes";
		}
		try {
			storage.GetDataTableInfo()->BindIndexes(context, NGRAM_ROWID_GUARD_TYPE);
		} catch (std::exception &ex) {
			return StringUtil::Format("the recorded rowid guard could not be bound safely: %s",
			                          ErrorData(ex).RawMessage());
		}
		// Success is safe only after re-reading the catalog and physical entry as
		// BOUND. A bound index cannot later enter the raw-pointer BINDING path.
		return RowIdGuardDropReason(context, table, guard_name, guard_token, false);
	}
	if (catalog_entry) {
		return "the recorded rowid guard is cataloged but missing from table storage";
	}
	return string();
}

//! The guard `meta` records, when it is exactly that incarnation and usable.
static unique_ptr<RowIdGuardState> ReadExactGuard(DuckTableEntry &table, const MetaInfo &meta, string &reason,
                                                  optional_idx checkpoint_iteration = optional_idx()) {
	if (!RowIdGuardRuntimeCompatible()) {
		reason = HostRuntimeMismatchReason();
		return nullptr;
	}
	if (meta.guard_token.empty()) {
		reason = "the index does not record a valid rowid guard";
		return nullptr;
	}
	auto state = ReadGuardState(table, meta.column_name, meta.guard_name, reason, checkpoint_iteration);
	if (!state) {
		return nullptr;
	}
	if (state->token != meta.guard_token) {
		reason = "the recorded rowid guard was dropped and re-created";
		return nullptr;
	}
	if (!state->protection_compatible) {
		reason = "the recorded rowid guard is incompatible with this extension build";
		return nullptr;
	}
	return state;
}

string RowIdGuardReason(ClientContext &context, DuckTableEntry &table, const MetaInfo &meta) {
	if (!RowIdGuardRuntimeCompatible()) {
		return HostRuntimeMismatchReason();
	}
	auto &manager = DuckTransaction::Get(context, table.ParentCatalog()).GetTransactionManager();
	unique_ptr<StorageLockKey> checkpoint_lock;
	if (!ContextOwnsCreationBarrier(context, manager)) {
		checkpoint_lock = manager.SharedCheckpointLock();
	}
	auto checkpoint_iteration = CheckpointIteration(table.GetStorage().db);
	string reason;
	auto state = ReadExactGuard(table, meta, reason, checkpoint_iteration);
	if (!state) {
		return reason;
	}
	if (state->max_seen < meta.hwm_rowid) {
		return "the rowid guard has not observed the index high-water mark";
	}
	if (state->unsafe_reuse) {
		return "the rowid guard cannot exclude reuse of rowids already covered by the ngram index";
	}
	return string();
}

static IndexVerdict Changed(string reason) {
	IndexVerdict verdict;
	verdict.availability = IndexAvailability::CHANGED;
	verdict.reason = std::move(reason);
	return verdict;
}

//! The verdict for `location` against the registry as it stands. Every
//! identity change here is a plan that outlived its row; callers decide whether
//! to raise on the replacement or to scan instead. With `guard` the row must be
//! readable and the rowid guard of `target.entry` is consulted.
static IndexVerdict Verdict(ClientContext &context, const ResolvedTarget &target, const IndexLocation &location,
                            bool guard) {
	RegistrySnapshot registry;
	try {
		registry = ReadRegistry(context, target.catalog_name);
	} catch (CatalogException &ex) {
		return Changed(ex.what());
	} catch (InvalidInputException &ex) {
		return Changed(ex.what());
	}
	IndexVerdict verdict;
	if (!registry.oid) {
		return verdict;
	}
	if (registry.oid != location.registry_oid) {
		return Changed("ngram: registry changed after the index operation was prepared");
	}
	for (auto &row : registry.rows) {
		if (row.index_ref != location.index_ref) {
			continue;
		}
		if (!StringUtil::CIEquals(row.schema_name, target.schema_name) ||
		    !StringUtil::CIEquals(row.table_name, target.table_name) ||
		    !StringUtil::CIEquals(row.column_name, location.column_name)) {
			return Changed(StringUtil::Format("ngram: registry row %s changed after the index operation was prepared",
			                                  location.index_ref));
		}
		verdict.availability = IndexAvailability::AVAILABLE;
		verdict.meta = row.meta;
		if (!guard) {
			return verdict;
		}
		if (!row.error.empty()) {
			throw InvalidInputException("ngram: the index on %s.%s (%s) is unusable: %s", row.table_name,
			                            row.column_name, row.index_ref, row.error);
		}
		optional_ptr<TableCatalogEntry> table = target.entry;
		verdict.reason = RowIdGuardReason(context, table->Cast<DuckTableEntry>(), verdict.meta);
		return verdict;
	}
	return verdict;
}

IndexVerdict LocateIndex(ClientContext &context, const ResolvedTarget &target, const IndexLocation &location) {
	return Verdict(context, target, location, false);
}

IndexVerdict ValidateIndex(ClientContext &context, const ResolvedTarget &target, const IndexLocation &location) {
	return Verdict(context, target, location, true);
}

MaintenanceColumn ResolveMaintenanceColumn(ClientContext &context, const char *fn, const ResolvedTarget &target,
                                           const IndexLocation &location) {
	auto &column_name = location.column_name;
	if (!target.entry->ColumnExists(column_name)) {
		throw CatalogException("%s: the ngram index on %s references column %s, which no longer exists; drop the "
		                       "index with PRAGMA drop_ngram_index",
		                       fn, target.table_name, column_name);
	}
	auto verdict = ValidateIndex(context, target, location);
	if (verdict.availability == IndexAvailability::CHANGED) {
		throw InvalidInputException(verdict.reason);
	}
	if (verdict.availability == IndexAvailability::ABSENT) {
		throw InvalidInputException("%s: index storage is unavailable", fn);
	}
	if (!verdict.reason.empty()) {
		throw InvalidInputException("%s: the ngram index on %s.%s cannot be maintained incrementally because %s. "
		                            "Rebuild it: PRAGMA drop_ngram_index('%s', '%s') then PRAGMA "
		                            "create_ngram_index('%s', '%s')",
		                            fn, target.table_name, column_name, verdict.reason, target.table_name, column_name,
		                            target.table_name, column_name);
	}
	MaintenanceColumn column;
	column.column_name = column_name;
	column.location = location;
	column.meta = std::move(verdict.meta);
	return column;
}

static void ClassifyBase(ClientContext &context, const MetaInfo &meta, ObservedIndex &observed) {
	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, observed.table_name);
	auto base =
	    Catalog::GetEntry(context, observed.catalog_name, observed.schema_name, lookup, OnEntryNotFound::RETURN_NULL);
	if (!base || base->type != CatalogType::TABLE_ENTRY || !base->Cast<TableCatalogEntry>().IsDuckTable()) {
		observed.status = "ORPHAN";
		observed.reason = "base table is missing or no longer an ordinary DuckDB table";
		return;
	}
	auto &table = base->Cast<DuckTableEntry>();
	if (!table.ColumnExists(observed.location.column_name)) {
		observed.status = "ORPHAN";
		observed.reason = "indexed column is missing";
		return;
	}
	auto reason = RowIdGuardReason(context, table, meta);
	if (!reason.empty()) {
		observed.status = "SCAN_ONLY";
		observed.reason = "exact rowid guard is unavailable: " + reason;
		return;
	}
	observed.status = "READY";
}

vector<ObservedIndex> ObserveCatalog(ClientContext &context, const string &catalog_name) {
	vector<ObservedIndex> result;
	RegistrySnapshot registry;
	string registry_error;
	try {
		registry = ReadRegistry(context, catalog_name);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		if (error.Type() != ExceptionType::CATALOG && error.Type() != ExceptionType::INVALID_INPUT) {
			throw;
		}
		registry_error = error.Message();
	}
	auto malformed = [&](const string &index_ref, string reason) {
		ObservedIndex observed;
		observed.catalog_name = catalog_name;
		observed.location.index_ref = index_ref;
		observed.status = "MALFORMED";
		observed.reason = std::move(reason);
		return observed;
	};
	// Storage tables present in the schema, by index id: (segments, stats).
	unordered_map<string, pair<bool, bool>> storage;
	vector<ObservedIndex> foreign;
	auto schema = Catalog::GetSchema(context, catalog_name, NGRAM_SCHEMA, OnEntryNotFound::RETURN_NULL);
	if (schema) {
		schema->Scan(context, CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
			string index_ref;
			bool segments = false;
			bool table = entry.type == CatalogType::TABLE_ENTRY && entry.Cast<TableCatalogEntry>().IsDuckTable();
			if (table && StringUtil::CIEquals(entry.name, REGISTRY_TABLE)) {
				return;
			}
			if (!table || !ParseStorageName(entry.name, index_ref, segments)) {
				foreign.push_back(
				    malformed(entry.name, StringUtil::Format("%s.%s is not a storage table of this extension",
				                                             NGRAM_SCHEMA, entry.name)));
				return;
			}
			(segments ? storage[index_ref].first : storage[index_ref].second) = true;
		});
	}
	for (auto &row : registry.rows) {
		ObservedIndex observed;
		observed.catalog_name = catalog_name;
		observed.schema_name = row.schema_name;
		observed.table_name = row.table_name;
		observed.location = LocationOf(row, registry.oid);
		observed.format_version = row.format_version;
		observed.legacy = registry.legacy_shape;
		// The row consumes its storage entry; whatever remains in storage
		// afterwards has no registry row. Read the entry before erasing it.
		bool storage_complete = false;
		auto tables = storage.find(row.index_ref);
		if (tables != storage.end()) {
			storage_complete = tables->second.first && tables->second.second;
			storage.erase(tables);
		}
		if (!row.error.empty()) {
			observed.status = "MALFORMED";
			observed.reason = row.error;
		} else if (!storage_complete) {
			observed.status = "MALFORMED";
			observed.reason = "a storage table is missing";
		} else {
			ClassifyBase(context, row.meta, observed);
		}
		result.push_back(std::move(observed));
	}
	for (auto &orphaned : storage) {
		result.push_back(
		    malformed(orphaned.first, registry_error.empty() ? "storage has no registry row" : registry_error));
	}
	result.insert(result.end(), std::make_move_iterator(foreign.begin()), std::make_move_iterator(foreign.end()));
	return result;
}

ObservedIndex FindObserved(ClientContext &context, const string &catalog_name, const string &index_ref) {
	if (!IsCanonicalUUID(index_ref)) {
		throw InvalidInputException("ngram: index reference must be a canonical lowercase UUID");
	}
	auto database = DatabaseManager::Get(context).GetDatabase(context, catalog_name);
	if (!database || !database->GetCatalog().IsDuckCatalog() || !database->HasStorageManager()) {
		throw CatalogException("ngram: %s is not an attached DuckDB catalog", catalog_name);
	}
	for (auto &observed : ObserveCatalog(context, database->GetName())) {
		if (observed.location.index_ref == index_ref) {
			return observed;
		}
	}
	throw CatalogException("ngram: index %s does not exist in catalog %s", index_ref, catalog_name);
}

} // namespace ngram
} // namespace duckdb
