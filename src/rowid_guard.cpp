#include "ngram/rowid_guard.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/vector/sequence_vector.hpp"
#include "duckdb/execution/index/unbound_index.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/connection_manager.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/planner/extension_callback.hpp"
#include "duckdb/planner/operator/logical_create_index.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/index.hpp"
#include "duckdb/storage/single_file_block_manager.hpp"
#include "duckdb/storage/table/index_entry.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"

#include <atomic>

namespace duckdb {
namespace ngram {

static constexpr const char *OPTION_MAX_SEEN = "ngram_guard_max_seen";
static constexpr const char *OPTION_UNSAFE = "ngram_guard_unsafe_reuse";
static constexpr int64_t GUARD_VERSION = 1;
static constexpr const char *DUCKDB_VERSION = "v2.0.0";
//! The DuckDB commit the guard is pinned to. A host reports an abbreviation of
//! it as pragma_version().source_id whose length follows the build's git
//! configuration: eight characters from a full clone, ten in the official binary.
static constexpr const char *DUCKDB_SOURCE_COMMIT = "e366461e30fb95d68c0d53a50f050dd0b043fc71";
static constexpr idx_t MIN_SOURCE_ID_LENGTH = 7;
//! The source tag persisted in every guard's storage options and compared
//! exactly on read. Guards already on disk carry this literal, so it stays
//! fixed independently of how the host abbreviates the commit.
static constexpr const char *DUCKDB_SOURCE_ID = "e366461e";
static constexpr const char *OPTION_VERSION = "ngram_guard_version";
static constexpr const char *OPTION_SOURCE = "ngram_duckdb_source_id";
static constexpr const char *OPTION_CHECKPOINT = "ngram_guard_checkpoint_iteration";
static constexpr const char *OPTION_PROTECTION = "ngram_guard_protection_compatible";

enum class HostRuntimeState : uint8_t { UNKNOWN, COMPATIBLE, INCOMPATIBLE };

static atomic<HostRuntimeState> host_runtime_state {HostRuntimeState::UNKNOWN};

//! True when source is an abbreviation of DUCKDB_SOURCE_COMMIT with at least
//! MIN_SOURCE_ID_LENGTH characters.
static bool SourceIdMatchesPinnedCommit(const string &source) {
	const string commit(DUCKDB_SOURCE_COMMIT);
	return source.size() >= MIN_SOURCE_ID_LENGTH && source.size() <= commit.size() &&
	       commit.compare(0, source.size(), source) == 0;
}

void InitializeRowIdGuardHostRuntime(ExtensionLoader &loader) {
	bool compatible = false;
	try {
		// This cataloged table function executes inside the host binary. Calling
		// DuckDB::SourceID() from a static-linked DSO only identifies the DSO.
		Connection connection(loader.GetDatabaseInstance());
		auto result = connection.Query("SELECT library_version, source_id FROM system.main.pragma_version()");
		if (!result->HasError()) {
			auto version = result->GetValue(0, 0).ToString();
			auto source = result->GetValue(1, 0).ToString();
			compatible = version == DUCKDB_VERSION && SourceIdMatchesPinnedCommit(source);
		}
	} catch (std::exception &) {
	}
	if (!compatible) {
		host_runtime_state.store(HostRuntimeState::INCOMPATIBLE);
		return;
	}
	auto expected = HostRuntimeState::UNKNOWN;
	host_runtime_state.compare_exchange_strong(expected, HostRuntimeState::COMPATIBLE);
}

uint64_t CheckpointIteration(AttachedDatabase &db) {
	auto &storage = db.GetStorageManager();
	return storage.InMemory() ? 0 : storage.GetBlockManager().Cast<SingleFileBlockManager>().GetCheckpointIteration();
}

//! The header checkpoint iteration as an append may read it, or invalid when
//! no value can be read safely. An in-memory database has no header although
//! its checkpoints still vacuum trailing deleted row groups. While a file
//! checkpoint is active, its WriteHeader may be incrementing the plain counter
//! on another thread; the active-checkpoint flag is atomic. A checkpoint that
//! starts after this check cannot reach WriteHeader before the calling commit
//! ends: it must first take every table's exclusive checkpoint lock, and the
//! committing writer holds its own table's shared lock through
//! DataTable::AppendLock until its flush finishes.
static optional_idx ObservableCheckpointIteration(AttachedDatabase &db) {
	auto &storage = db.GetStorageManager();
	if (storage.InMemory()) {
		return optional_idx();
	}
	auto &manager = db.GetTransactionManager().Cast<DuckTransactionManager>();
	if (manager.GetActiveCheckpoint().IsValid()) {
		return optional_idx();
	}
	return storage.GetBlockManager().Cast<SingleFileBlockManager>().GetCheckpointIteration();
}

bool RowIdGuardRuntimeCompatible() {
	return host_runtime_state.load() == HostRuntimeState::COMPATIBLE;
}

string HostRuntimeMismatchReason() {
	return StringUtil::Format("the host runtime does not match pinned DuckDB %s commit %s", DUCKDB_VERSION,
	                          DUCKDB_SOURCE_COMMIT);
}

static const Value &RequireOption(const case_insensitive_map_t<Value> &options, const char *name) {
	auto entry = options.find(name);
	if (entry == options.end() || entry->second.IsNull()) {
		throw InvalidInputException("ngram rowid guard is missing required option %s", name);
	}
	return entry->second;
}

StoredGuardState ReadStoredGuardState(const IndexStorageInfo &storage) {
	StoredGuardState result;
	try {
		if (!storage.IsValid()) {
			return result;
		}
		result.token = StringValue::Get(RequireOption(storage.options, NGRAM_GUARD_TOKEN_OPTION));
		result.max_seen = RequireOption(storage.options, OPTION_MAX_SEEN).GetValue<int64_t>();
		result.unsafe_reuse = RequireOption(storage.options, OPTION_UNSAFE).GetValue<bool>();
		auto version = RequireOption(storage.options, OPTION_VERSION).GetValue<int64_t>();
		auto source = StringValue::Get(RequireOption(storage.options, OPTION_SOURCE));
		auto protection = RequireOption(storage.options, OPTION_PROTECTION).GetValue<bool>();
		if (!RowIdGuardRuntimeCompatible() || version != GUARD_VERSION || source != DUCKDB_SOURCE_ID || !protection) {
			result.unsafe_reuse = true;
			return result;
		}
		if (result.token.empty()) {
			result.unsafe_reuse = true;
			return result;
		}
		result.checkpoint_iteration = RequireOption(storage.options, OPTION_CHECKPOINT).GetValue<uint64_t>();
		result.protection_compatible = true;
		return result;
	} catch (std::exception &) {
		result.unsafe_reuse = true;
		result.protection_compatible = false;
		return result;
	}
}

class RowIdGuard final : public BoundIndex {
public:
	RowIdGuard(const Identifier &name, const vector<column_t> &column_ids, TableIOManager &io_manager,
	           const vector<unique_ptr<Expression>> &expressions, AttachedDatabase &db, string token_p,
	           int64_t max_seen_p, bool unsafe_reuse_p, bool protection_compatible_p,
	           optional_idx checkpoint_iteration_p, optional_idx advance_iteration_p)
	    : BoundIndex(name, NGRAM_ROWID_GUARD_TYPE, IndexConstraintType::NONE, column_ids, io_manager, expressions, db),
	      token(std::move(token_p)), max_seen(max_seen_p), unsafe_reuse(unsafe_reuse_p),
	      protection_compatible(protection_compatible_p), checkpoint_iteration(checkpoint_iteration_p),
	      advance_iteration(advance_iteration_p) {
	}

	ErrorData Append(IndexLock &, DataChunk &chunk, Vector &row_ids) override {
		if (chunk.size() == 0) {
			return ErrorData();
		}
		if (row_ids.GetVectorType() == VectorType::SEQUENCE_VECTOR) {
			int64_t start, increment;
			SequenceVector::GetSequence(row_ids, start, increment);
			if (increment == 1) {
				Observe(start, start + NumericCast<int64_t>(chunk.size() - 1));
				return ErrorData();
			}
		}
		row_ids.Flatten();
		auto ids = FlatVector::GetData<row_t>(row_ids);
		auto first = NumericCast<int64_t>(ids[0]);
		auto last = first;
		for (idx_t i = 1; i < chunk.size(); i++) {
			auto rowid = NumericCast<int64_t>(ids[i]);
			first = MinValue(first, rowid);
			last = MaxValue(last, rowid);
		}
		Observe(first, last);
		return ErrorData();
	}

	ErrorData Insert(IndexLock &lock, DataChunk &chunk, Vector &row_ids) override {
		return Append(lock, chunk, row_ids);
	}

	idx_t TryDelete(IndexLock &, DataChunk &entries, Vector &, optional_ptr<SelectionVector>,
	                optional_ptr<SelectionVector>) override {
		return entries.size();
	}

	bool MergeIndexes(IndexLock &, BoundIndex &) override {
		return false;
	}

	void Vacuum(IndexLock &) override {
	}

	idx_t GetInMemorySize(IndexLock &) const override {
		return sizeof(*this) + token.size();
	}

	void Verify(IndexLock &) override {
	}

	string ToString(IndexLock &, bool) override {
		return NGRAM_ROWID_GUARD_TYPE;
	}

	void VerifyAllocations(IndexLock &) override {
	}

	// The BoundIndex default throws; DEBUG builds call this on every bound index
	// after appends (duckdb/src/storage/local_storage.cpp:626, data_table.cpp:430 and :1352).
	void VerifyBuffers(IndexLock &) override {
	}

	void ResetStorage(IndexLock &) override {
		unsafe_reuse = true;
	}

	string GetConstraintViolationMessage(VerifyExistenceType, idx_t, DataChunk &) const override {
		return "ngram rowid guard has no constraints";
	}

	IndexStorageInfo SerializeToDisk(QueryContext, const case_insensitive_map_t<Value> &) override {
		annotated_lock_guard<annotated_mutex> guard(lock);
		auto compatible = protection_compatible && RowIdGuardRuntimeCompatible();
		auto current_iteration = compatible ? CheckpointIteration(db) : 0;
		if (compatible) {
			ApplyCheckpointSeal(current_iteration);
		}
		// WriteHeader advances the iteration exactly once after index metadata is
		// serialized. A future unbound checkpoint can only copy this old seal.
		return Serialize(max_seen, compatible ? unsafe_reuse : true, compatible ? current_iteration + 1 : 0,
		                 compatible);
	}

	IndexStorageInfo SerializeToWAL(const case_insensitive_map_t<Value> &) override {
		annotated_lock_guard<annotated_mutex> guard(lock);
		auto compatible = protection_compatible && RowIdGuardRuntimeCompatible();
		auto current_iteration = compatible ? CheckpointIteration(db) : 0;
		if (compatible) {
			ApplyCheckpointSeal(current_iteration);
		}
		// The CREATE INDEX WAL record is replayed without installing the index
		// until that transaction commits. Persist the live commit-time state:
		// creator-local rows are already included and are not replayed through
		// this index; later WAL transactions buffer against the unbound index.
		auto result = Serialize(max_seen, compatible ? unsafe_reuse : true, current_iteration, compatible);
		result.buffers.emplace_back();
		return result;
	}

	RowIdGuardState GetState(optional_idx current_iteration) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		if (current_iteration.IsValid()) {
			ApplyCheckpointSeal(current_iteration.GetIndex());
		}
		return {token, max_seen, unsafe_reuse, protection_compatible, column_ids};
	}

private:
	//! Fold one appended rowid range into the guard. A range at or below
	//! max_seen has two histories: a checkpoint vacuumed fully deleted trailing
	//! row groups and the table reissued their rowids, which must latch, or an
	//! earlier commit advanced max_seen and then failed in a later index (a
	//! UNIQUE ART rejecting a key), so the same range is reissued, which must
	//! not. Every inserting transaction holds the vacuum lock shared from its
	//! first insert to the end of its commit and every file checkpoint bumps
	//! the header iteration, so the range latches only when the iteration
	//! differs from the one recorded at the last advance or either is unknown
	//! (in-memory database, checkpoint in progress, guard bound from disk/WAL).
	void Observe(int64_t first, int64_t last) {
		if (first <= max_seen) {
			auto current = ObservableCheckpointIteration(db);
			if (!advance_iteration.IsValid() || !current.IsValid() ||
			    current.GetIndex() != advance_iteration.GetIndex()) {
				unsafe_reuse = true;
			}
		}
		if (last > max_seen) {
			max_seen = last;
			advance_iteration = ObservableCheckpointIteration(db);
		}
	}

	void ApplyCheckpointSeal(uint64_t current_iteration) {
		if (!checkpoint_iteration.IsValid()) {
			return;
		}
		unsafe_reuse = unsafe_reuse || checkpoint_iteration.GetIndex() != current_iteration;
		checkpoint_iteration.SetInvalid();
	}

	IndexStorageInfo Serialize(int64_t serialized_max_seen, bool serialized_unsafe_reuse, uint64_t checkpoint_iteration,
	                           bool serialized_protection) const {
		IndexStorageInfo result(name);
		result.root = 0;
		result.options[OPTION_VERSION] = Value::BIGINT(GUARD_VERSION);
		result.options[OPTION_SOURCE] = Value(DUCKDB_SOURCE_ID);
		result.options[NGRAM_GUARD_TOKEN_OPTION] = Value(token);
		result.options[OPTION_MAX_SEEN] = Value::BIGINT(serialized_max_seen);
		result.options[OPTION_UNSAFE] = Value::BOOLEAN(serialized_unsafe_reuse);
		result.options[OPTION_CHECKPOINT] = Value::UBIGINT(checkpoint_iteration);
		result.options[OPTION_PROTECTION] = Value::BOOLEAN(serialized_protection);
		FixedSizeAllocatorInfo empty_allocator;
		empty_allocator.segment_size = 1;
		result.allocator_infos.push_back(std::move(empty_allocator));
		return result;
	}

	string token;
	int64_t max_seen;
	bool unsafe_reuse;
	bool protection_compatible;
	//! Persisted seal, compared once against the current iteration after bind.
	optional_idx checkpoint_iteration;
	//! Header iteration observed when max_seen last advanced; invalid until an
	//! append in this process advances it.
	optional_idx advance_iteration;
};

void RequirePinnedRuntime() {
	if (!RowIdGuardRuntimeCompatible()) {
		throw InvalidInputException("ngram rowid guard requires host DuckDB %s built from commit %s", DUCKDB_VERSION,
		                            DUCKDB_SOURCE_COMMIT);
	}
}

//! CREATE INDEX for the guard. The table's append lock is held from the rowid
//! baseline read until the guard is in the table's index list, so no committed
//! insert falls between the two; one call of this source takes and releases it
//! on one thread. The catalog steps are PhysicalCreateIndex::Finalize's. Both
//! transactions are looked up before the lock: a lookup can start a
//! transaction under the host's transaction locks, which a commit holds while
//! it takes the append lock.
class PhysicalCreateRowIdGuard final : public PhysicalOperator {
public:
	PhysicalCreateRowIdGuard(PhysicalPlan &physical_plan, LogicalCreateIndex &op)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, op.types, 0),
	      table(op.table.Cast<DuckTableEntry>()), info(std::move(op.info)),
	      expressions(std::move(op.unbound_expressions)) {
	}

	DuckTableEntry &table;
	unique_ptr<CreateIndexInfo> info;
	vector<unique_ptr<Expression>> expressions;

	bool IsSource() const override {
		return true;
	}

	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &, OperatorSourceInput &) const override {
		RequirePinnedRuntime();
		auto &storage_ids = info->column_ids;
		if (info->constraint_type != IndexConstraintType::NONE || storage_ids.empty() ||
		    storage_ids.size() != expressions.size()) {
			throw InvalidInputException("ngram rowid guard requires one or more non-unique physical columns");
		}
		if (!info->options.empty()) {
			throw InvalidInputException("ngram rowid guard does not accept caller-supplied options");
		}
		auto &storage = table.GetStorage();
		auto &schema = table.schema;
		auto &transaction = DuckTransaction::Get(context.client, table.ParentCatalog());
		auto catalog_transaction = schema.GetCatalogTransaction(context.client);
		TableAppendState append_state;
		storage.AppendLock(transaction, append_state);
		auto next_row_id = storage.GetNextRowId();
		auto max_seen = next_row_id == 0 ? int64_t(-1) : NumericCast<int64_t>(next_row_id - 1);
		unique_ptr<BoundIndex> guard =
		    make_uniq<RowIdGuard>(info->GetIndexName(), storage_ids, TableIOManager::Get(storage), expressions,
		                          storage.db, UUID::ToString(UUID::GenerateRandomUUID()), max_seen, false, true,
		                          optional_idx(), ObservableCheckpointIteration(storage.db));

		if (schema.GetEntry(catalog_transaction, CatalogType::INDEX_ENTRY, info->GetIndexName())) {
			if (info->on_conflict != OnCreateConflict::IGNORE_ON_CONFLICT) {
				throw CatalogException("Index with name %s already exists!", info->GetIndexName());
			}
			return SourceResultType::FINISHED;
		}
		auto &index = schema.CreateIndex(catalog_transaction, *info, table)->Cast<DuckIndexEntry>();
		index.initial_index_size = guard->GetInMemorySize();
		storage.AddIndex(std::move(guard), index.oid);
		return SourceResultType::FINISHED;
	}
};

static unique_ptr<BoundIndex> GuardCreateInstance(CreateIndexInput &input) {
	if (input.column_ids.empty() || input.column_ids.size() != input.unbound_expressions.size()) {
		throw InvalidInputException("ngram rowid guard has malformed storage metadata");
	}
	auto stored = ReadStoredGuardState(input.storage_info);
	return make_uniq<RowIdGuard>(input.name, input.column_ids, input.table_io_manager, input.unbound_expressions,
	                             input.db, std::move(stored.token), stored.max_seen, stored.unsafe_reuse,
	                             stored.protection_compatible, stored.checkpoint_iteration, optional_idx());
}

static PhysicalOperator &GuardCreatePlan(PlanIndexInput &input) {
	return input.planner.Make<PhysicalCreateRowIdGuard>(input.op);
}

static IndexType GuardIndexType() {
	IndexType result;
	result.name = NGRAM_ROWID_GUARD_TYPE;
	result.create_plan = GuardCreatePlan;
	result.create_instance = GuardCreateInstance;
	return result;
}

struct GuardTableName {
	string catalog;
	string schema;
	string table;
};

bool CanBindRowIdGuards(DuckTableEntry &table, bool require_compatible) {
	if (require_compatible && !RowIdGuardRuntimeCompatible()) {
		return false;
	}
	for (auto entry : table.GetStorage().GetDataTableInfo()->GetIndexes().IndexEntries()) {
		if (entry->GetIndexType() != NGRAM_ROWID_GUARD_TYPE || entry->GetBindState() == IndexBindState::BOUND) {
			continue;
		}
		if (entry->GetBindState() != IndexBindState::UNBOUND) {
			return false;
		}
		auto unbound = entry->GetReadHandle<UnboundIndex>();
		if (unbound->GetConstraintType() != IndexConstraintType::NONE) {
			return false;
		}
		auto &expressions = unbound->GetParsedExpressions();
		auto &column_ids = unbound->GetColumnIds();
		if (column_ids.empty() || expressions.size() != column_ids.size()) {
			return false;
		}
		for (idx_t i = 0; i < expressions.size(); i++) {
			if (expressions[i]->GetExpressionClass() != ExpressionClass::COLUMN_REF) {
				return false;
			}
			auto &column_name = expressions[i]->Cast<ColumnRefExpression>().GetColumnName();
			if (!table.ColumnExists(column_name)) {
				return false;
			}
			auto &column = table.GetColumn(column_name);
			if (column.Generated() || column.Type().id() != LogicalTypeId::VARCHAR ||
			    table.GetStorageIndex(ColumnIndex(column.Logical().index)).GetPrimaryIndex() != column_ids[i]) {
				return false;
			}
		}
		if (require_compatible && !ReadStoredGuardState(unbound->GetStorageInfo()).protection_compatible) {
			return false;
		}
	}
	return true;
}

static void BindAllRowIdGuards(ClientContext &context) {
	vector<GuardTableName> tables;
	unordered_set<string> seen;
	for (auto &db : DatabaseManager::Get(context).GetDatabases(context)) {
		if (!db->HasStorageManager() || !db->GetCatalog().IsDuckCatalog()) {
			continue;
		}
		// Each scan holds its CatalogSet lock while its callback runs, and
		// Scan(context) first looks the transaction up, which can start one under
		// the host's transaction locks; a commit takes those locks before catalog
		// set locks. The index sets are therefore scanned after the schema scan
		// returns.
		vector<reference<SchemaCatalogEntry>> schemas;
		db->GetCatalog().ScanSchemas(context, [&](SchemaCatalogEntry &schema) { schemas.push_back(schema); });
		for (auto &schema : schemas) {
			schema.get().Scan(context, CatalogType::INDEX_ENTRY, [&](CatalogEntry &entry) {
				auto &index = entry.Cast<DuckIndexEntry>();
				if (index.index_type != NGRAM_ROWID_GUARD_TYPE) {
					return;
				}
				auto table_schema = index.GetSchemaName().GetIdentifierName();
				auto table_name = index.GetTableName().GetIdentifierName();
				auto &catalog_name = db->GetName().GetIdentifierName();
				auto key = catalog_name + '\0' + table_schema + '\0' + table_name;
				if (seen.insert(std::move(key)).second) {
					tables.push_back({catalog_name, std::move(table_schema), std::move(table_name)});
				}
			});
		}
	}
	// Resolve and bind only after every scan callback has returned: BindIndexes
	// performs another catalog lookup and would otherwise self-deadlock.
	for (auto &name : tables) {
		try {
			EntryLookupInfo lookup(
			    CatalogType::TABLE_ENTRY,
			    QualifiedName(Identifier(name.catalog), Identifier(name.schema), Identifier(name.table)));
			auto entry = Catalog::GetEntry(context, lookup, OnEntryNotFound::RETURN_NULL);
			if (!entry || entry->type != CatalogType::TABLE_ENTRY || !entry->Cast<TableCatalogEntry>().IsDuckTable()) {
				continue;
			}
			auto &table = entry->Cast<DuckTableEntry>();
			if (CanBindRowIdGuards(table, true)) {
				table.GetStorage().GetDataTableInfo()->BindIndexes(context, NGRAM_ROWID_GUARD_TYPE);
			}
		} catch (std::exception &) {
			// Best effort only. Search and maintenance inspect unbound metadata and
			// fail closed; public drop must remain available for incompatible guards.
		}
	}
}

class RowIdGuardBindCallback final : public ExtensionCallback {
public:
	void OnExtensionLoaded(DatabaseInstance &db, const string &name) override {
		if (!StringUtil::CIEquals(name, "ngram") || ConnectionManager::Get(db).GetConnectionCount() != 0) {
			return;
		}
		try {
			Connection connection(db);
			if (ConnectionManager::Get(db).GetConnectionCount() != 1) {
				return;
			}
			connection.context->RunFunctionInTransaction([&]() { BindAllRowIdGuards(*connection.context); });
		} catch (std::exception &) {
		}
	}

	void OnConnectionClosed(ClientContext &context) override {
		if (ConnectionManager::Get(context).GetConnectionCount() != 1 || context.transaction.HasActiveTransaction()) {
			return;
		}
		try {
			context.RunFunctionInTransaction([&]() { BindAllRowIdGuards(context); });
		} catch (std::exception &) {
		}
	}
};

RowIdGuardState ReadBoundGuardState(IndexEntry &entry, optional_idx current_iteration) {
	auto handle = entry.GetWriteHandle<RowIdGuard>();
	return handle->GetState(current_iteration);
}

void RegisterRowIdGuard(ExtensionLoader &loader) {
	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	auto &types = config.GetIndexTypes();
	types.RegisterIndexType(GuardIndexType());
	ExtensionCallback::Register(config, make_shared_ptr<RowIdGuardBindCallback>());
}

} // namespace ngram
} // namespace duckdb
