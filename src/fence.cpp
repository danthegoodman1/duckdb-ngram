#include "ngram/fence.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "ngram/index_state.hpp"
#include "ngram/search_core.hpp"

#include <functional>
#include <mutex>

namespace duckdb {
namespace ngram {

//! Per-connection maintenance state: the prepared checks generated scripts
//! consume by handle, and the fences their execution holds. DuckTransaction
//! drops its own vacuum lock just before running an automatic checkpoint
//! during commit, so one extra shared lock is kept until DuckDB's post-commit
//! callback, which runs after that checkpoint, or until rollback.
class MaintenanceState final : public ClientContextState {
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

	struct Handle {
		//! The transaction that expanded the pragma. The preprocessor expands
		//! every pragma of one query string in one transaction, so a
		//! registration from a later transaction means the earlier scripts have
		//! run or never will, and it drops their leftovers.
		transaction_t expansion_id;
		//! The script the handle belongs to: the handles one pragma callback
		//! registers (one per column of a refresh or compact) run in one
		//! transaction.
		uint64_t group;
		//! The transaction running the group's script, known once it consumes
		//! the group's first handle; its end drops the handles it never reached.
		optional_idx script_id;
		PreparedMaintenance prepared;
	};

	static transaction_t CurrentTransaction(ClientContext &context) {
		return context.transaction.ActiveTransaction().global_transaction_id;
	}

	void EraseIf(const std::function<bool(const Handle &)> &stale) {
		for (auto entry = handles.begin(); entry != handles.end();) {
			entry = stale(entry->second) ? handles.erase(entry) : std::next(entry);
		}
	}

public:
	uint64_t NewGroup() {
		lock_guard<mutex> guard(state_lock);
		return ++next_group;
	}

	string Register(ClientContext &context, uint64_t group, PreparedMaintenance prepared) {
		lock_guard<mutex> guard(state_lock);
		auto expansion_id = CurrentTransaction(context);
		EraseIf([&](const Handle &held) { return held.expansion_id != expansion_id; });
		auto handle = UUID::ToString(UUID::GenerateRandomUUID());
		handles.emplace(handle, Handle {expansion_id, group, optional_idx(), std::move(prepared)});
		return handle;
	}

	PreparedMaintenance Take(ClientContext &context, const string &handle) {
		lock_guard<mutex> guard(state_lock);
		auto entry = handles.find(handle);
		if (entry == handles.end()) {
			throw InvalidInputException("ngram: maintenance handle %s is unknown; a generated maintenance statement "
			                            "only runs inside the pragma that produced it",
			                            handle);
		}
		auto prepared = std::move(entry->second.prepared);
		auto group = entry->second.group;
		handles.erase(entry);
		auto script_id = CurrentTransaction(context);
		for (auto &held : handles) {
			if (held.second.group == group) {
				held.second.script_id = script_id;
			}
		}
		return prepared;
	}

	void Acquire(DuckTransactionManager &manager) {
		lock_guard<mutex> guard(state_lock);
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
		lock_guard<mutex> guard(state_lock);
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
		lock_guard<mutex> guard(state_lock);
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
		lock_guard<mutex> guard(state_lock);
		for (auto &held : fences) {
			if (held.manager == &manager && held.creation_lock) {
				return true;
			}
		}
		return false;
	}

	//! A handle waits through the expansion transaction's commit and through
	//! any unrelated statement run before its script; the end of the
	//! transaction that consumed a handle of its group drops what that script
	//! never reached.
	void TransactionCommit(MetaTransaction &transaction, ClientContext &) override {
		TransactionEnd(transaction);
	}
	void TransactionRollback(MetaTransaction &transaction, ClientContext &) override {
		TransactionEnd(transaction);
	}

private:
	void TransactionEnd(MetaTransaction &transaction) {
		lock_guard<mutex> guard(state_lock);
		fences.clear();
		EraseIf([&](const Handle &held) {
			return held.script_id.IsValid() && held.script_id.GetIndex() == transaction.global_transaction_id;
		});
	}

	mutex state_lock;
	vector<HeldFence> fences;
	unordered_map<string, Handle> handles;
	uint64_t next_group = 0;
};

static constexpr const char *MAINTENANCE_STATE = "ngram_maintenance";

static shared_ptr<MaintenanceState> GetMaintenanceState(ClientContext &context) {
	return context.registered_state->GetOrCreate<MaintenanceState>(MAINTENANCE_STATE);
}

bool ContextOwnsCreationBarrier(ClientContext &context, DuckTransactionManager &manager) {
	auto state = context.registered_state->Get<MaintenanceState>(MAINTENANCE_STATE);
	return state && state->OwnsCreationBarrier(manager);
}

uint64_t NewMaintenanceGroup(ClientContext &context) {
	return GetMaintenanceState(context)->NewGroup();
}

string PreparedMaintenanceCall(ClientContext &context, uint64_t group, PreparedMaintenance prepared) {
	prepared.target.entry = nullptr;
	return SystemFunction(NGRAM_MAINTENANCE_GUARD) + "(" +
	       Lit(GetMaintenanceState(context)->Register(context, group, std::move(prepared))) + ")";
}

static void AcquireMaintenanceFence(ClientContext &context, Catalog &catalog) {
	auto &transaction = DuckTransaction::Get(context, catalog);
	transaction.SetModifications(DatabaseModificationType::INSERT_DATA);
	GetMaintenanceState(context)->Acquire(transaction.GetTransactionManager());
}

//! Exclude every pre-existing writer before replacing the DataTable. An
//! exclusive upgrade alone is not enough for an old transaction that already
//! committed after this snapshot, so the timestamp check is deliberately
//! made only after the upgrade succeeds.
static unique_ptr<StorageLockKey> AcquireCreationBarrier(DuckTransaction &transaction) {
	auto undo = transaction.GetUndoProperties();
	if (undo.has_updates || undo.has_deletes || undo.has_catalog_changes) {
		throw TransactionException(
		    "create_ngram_index cannot follow updates, deletes, or catalog changes in the same transaction; "
		    "commit or roll back those changes and retry");
	}
	transaction.SetModifications(DatabaseModificationType::CREATE_INDEX);
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

//! The drop script's checks: the registry row is still the one the drop was
//! prepared against, no other row took a reference on the guard since, and the
//! guard about to be dropped is the recorded incarnation.
static void CheckPreparedDrop(ClientContext &context, const PreparedMaintenance &prepared) {
	auto &fn = prepared.fn;
	auto &target = prepared.target;
	auto located = LocateIndex(context, target, prepared.location);
	if (located.availability == IndexAvailability::CHANGED) {
		throw InvalidInputException(located.reason);
	}
	if (located.availability == IndexAvailability::ABSENT) {
		throw InvalidInputException("%s: index storage was removed after preparation", fn);
	}
	if (!prepared.drop_guard) {
		return;
	}
	auto &guard_name = prepared.location.guard_name;
	if (OtherGuardReferences(context, target, guard_name, prepared.location.index_ref) != 0) {
		throw InvalidInputException("%s: another ngram index now shares the rowid guard; run it again", fn);
	}
	auto &table = ResolveExistingTable(context, target.catalog_name, target.schema_name, target.table_name,
	                                   "ngram index base table");
	auto reason = RowIdGuardDropReason(context, table, guard_name, prepared.location.guard_token);
	if (!reason.empty()) {
		throw InvalidInputException("ngram rowid guard validation failed: %s", reason);
	}
}

//! The create script's checks: the registry is in the state the script
//! assumes, no index appeared on the column, and a shared guard still covers
//! it. A first index takes the creation barrier here and holds it until
//! __ngram_creation_finish proves the fresh guard.
static void CheckPreparedCreate(ClientContext &context, Catalog &catalog, const PreparedMaintenance &prepared) {
	auto &fn = prepared.fn;
	auto &column_name = prepared.location.column_name;
	auto &guard_name = prepared.location.guard_name;
	auto &guard_token = prepared.location.guard_token;
	unique_ptr<StorageLockKey> creation_lock;
	if (guard_token.empty()) {
		creation_lock = AcquireCreationBarrier(DuckTransaction::Get(context, catalog));
	}
	auto target = ResolveTarget(context, prepared.target.Qualified(), column_name, true);
	auto &table = target.entry->Cast<DuckTableEntry>();
	ValidateRegistryForCreate(context, target.catalog_name, prepared.location.registry_oid, prepared.bootstrap);
	// Enumerate again behind the fence: another connection may have registered
	// an index on this table after pragma preprocessing.
	auto table_target = target;
	table_target.column_name.clear();
	auto siblings = ExistingIndexes(context, table_target);
	for (auto &sibling : siblings) {
		if (StringUtil::CIEquals(sibling.column_name, column_name)) {
			throw InvalidInputException("An ngram index already exists on %s.%s (%s); use drop_ngram_index first",
			                            target.table_name, column_name, sibling.index_ref);
		}
		if (guard_token.empty() || sibling.guard_name != guard_name || sibling.guard_token != guard_token) {
			throw InvalidInputException(
			    "%s: the ngram indexes on %s changed while the statement was being prepared; run it again", fn,
			    target.table_name);
		}
	}
	if (creation_lock) {
		GetMaintenanceState(context)->HoldCreationBarrier(DuckTransaction::Get(context, catalog), table.GetStorage(),
		                                                  std::move(creation_lock), target.schema_name,
		                                                  target.table_name, column_name, guard_name);
		return;
	}
	if (siblings.empty()) {
		throw InvalidInputException(
		    "%s: the ngram indexes on %s changed while the statement was being prepared; run it again", fn,
		    target.table_name);
	}
	MetaInfo shared;
	shared.column_name = column_name;
	shared.guard_name = guard_name;
	shared.guard_token = guard_token;
	auto reason = RowIdGuardReason(context, table, shared);
	if (!reason.empty()) {
		throw InvalidInputException("%s: the rowid guard shared by the ngram indexes on %s cannot cover %s (%s); drop "
		                            "the table's ngram indexes and rebuild them",
		                            fn, target.table_name, column_name, reason);
	}
}

//! Refresh and compact: the index is still maintainable and its metadata is
//! what the script was generated from.
static void CheckPreparedMaintenance(ClientContext &context, const PreparedMaintenance &prepared) {
	auto &column_name = prepared.location.column_name;
	auto target = ResolveTarget(context, prepared.target.Qualified(), column_name, true);
	auto column = ResolveMaintenanceColumn(context, prepared.fn.c_str(), target, prepared.location);
	if (column.meta.hwm_rowid != prepared.meta.hwm_rowid ||
	    column.meta.options.gram_size != prepared.meta.options.gram_size ||
	    column.meta.options.case_insensitive != prepared.meta.options.case_insensitive) {
		throw InvalidInputException(
		    "%s: the index on %s.%s changed while the statement was being prepared; run it again", prepared.fn,
		    target.table_name, column_name);
	}
}

//! Acquires the vacuum fence, then repeats the checks that were necessarily
//! done once during pragma expansion. The transaction owns its normal copy;
//! MaintenanceState bridges the small commit/autocheckpoint interval.
static void MaintenanceGuardFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto output = FlatVector::GetData<bool>(result);
	for (idx_t row = 0; row < args.size(); row++) {
		auto prepared = GetMaintenanceState(context)->Take(context, args.GetValue(0, row).ToString());
		auto &catalog = Catalog::GetCatalog(context, prepared.target.catalog_name);
		AcquireMaintenanceFence(context, catalog);
		switch (prepared.kind) {
		case PreparedMaintenance::Kind::DROP:
			CheckPreparedDrop(context, prepared);
			break;
		case PreparedMaintenance::Kind::CREATE:
			CheckPreparedCreate(context, catalog, prepared);
			break;
		case PreparedMaintenance::Kind::MAINTAIN:
			CheckPreparedMaintenance(context, prepared);
			break;
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
		GetMaintenanceState(context)->FinishCreation(DuckTransaction::Get(context, catalog), table.GetStorage(),
		                                             schema_name, table_name, column_name, guard_name);
		output[row] = StringVector::AddString(result, token);
	}
}

void RegisterFence(ExtensionLoader &loader) {
	auto check =
	    ScalarFunction(NGRAM_MAINTENANCE_GUARD, {LogicalType::VARCHAR}, LogicalType::BOOLEAN, MaintenanceGuardFunction);
	check.stability = FunctionStability::VOLATILE;
	check.SetFallible();
	loader.RegisterFunction(check);

	auto finish = ScalarFunction(
	    NGRAM_CREATION_FINISH,
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	    LogicalType::VARCHAR, CreationFinishFunction);
	finish.stability = FunctionStability::VOLATILE;
	finish.SetFallible();
	loader.RegisterFunction(finish);
}

} // namespace ngram
} // namespace duckdb
