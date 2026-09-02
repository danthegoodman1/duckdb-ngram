#include "ngram/rowid_guard.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_set.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/execution/index/bound_index.hpp"
#include "duckdb/execution/index/index_type.hpp"
#include "duckdb/execution/index/unbound_index.hpp"
#include "duckdb/execution/operator/scan/physical_empty_result.hpp"
#include "duckdb/execution/operator/schema/physical_create_index.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/connection_manager.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/planner/operator/logical_create_index.hpp"
#include "duckdb/planner/extension_callback.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/single_file_block_manager.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/table_io_manager.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/data_table_info.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "ngram/index_pragmas.hpp"
#include "ngram/maintenance.hpp"
#include "ngram/search_core.hpp"

#include <algorithm>
#include <atomic>

namespace duckdb {
namespace ngram {

static constexpr const char *OPTION_MAX_SEEN = "ngram_guard_max_seen";
static constexpr const char *OPTION_UNSAFE = "ngram_guard_unsafe_reuse";
static constexpr int64_t GUARD_VERSION = 1;
static constexpr const char *DUCKDB_VERSION = "v1.5.5";
//! The DuckDB commit the guard is pinned to. A host reports an abbreviation of
//! it as pragma_version().source_id whose length follows the build's git
//! configuration: eight characters from a full clone, ten in the official binary.
static constexpr const char *DUCKDB_SOURCE_COMMIT = "d8cdaa33fda8df955cc76ef58a280f68f4cd43fa";
static constexpr idx_t MIN_SOURCE_ID_LENGTH = 7;
//! The source tag persisted in every guard's storage options and compared
//! exactly on read. Guards already on disk carry this literal, so it stays
//! fixed independently of how the host abbreviates the commit.
static constexpr const char *DUCKDB_SOURCE_ID = "d8cdaa33";
static constexpr const char *OPTION_VERSION = "ngram_guard_version";
static constexpr const char *OPTION_SOURCE = "ngram_duckdb_source_id";
static constexpr const char *OPTION_TOKEN = "ngram_guard_token";
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

static uint64_t CheckpointIteration(AttachedDatabase &db) {
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
	if (manager.GetActiveCheckpoint() != MAX_TRANSACTION_ID) {
		return optional_idx();
	}
	return storage.GetBlockManager().Cast<SingleFileBlockManager>().GetCheckpointIteration();
}

static bool RowIdGuardRuntimeCompatible() {
	return host_runtime_state.load() == HostRuntimeState::COMPATIBLE;
}

static const Value &RequireOption(const case_insensitive_map_t<Value> &options, const char *name) {
	auto entry = options.find(name);
	if (entry == options.end() || entry->second.IsNull()) {
		throw InvalidInputException("ngram rowid guard is missing required option %s", name);
	}
	return entry->second;
}

struct StoredGuardState {
	string token;
	int64_t max_seen = -1;
	bool unsafe_reuse = true;
	bool protection_compatible = false;
	optional_idx checkpoint_iteration;
};

static StoredGuardState ReadStoredGuardState(const IndexStorageInfo &storage) {
	StoredGuardState result;
	try {
		if (!storage.IsValid()) {
			return result;
		}
		result.token = StringValue::Get(RequireOption(storage.options, OPTION_TOKEN));
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
	RowIdGuard(const string &name, const vector<column_t> &column_ids, TableIOManager &io_manager,
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
		row_ids.Flatten(chunk.size());
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

	idx_t GetInMemorySize(IndexLock &) override {
		return sizeof(*this) + token.size();
	}

	void Verify(IndexLock &) override {
	}

	string ToString(IndexLock &, bool) override {
		return NGRAM_ROWID_GUARD_TYPE;
	}

	void VerifyAllocations(IndexLock &) override {
	}

	void VerifyBuffers(IndexLock &) override {
	}

	void ResetStorage(IndexLock &) override {
		unsafe_reuse = true;
	}

	string GetConstraintViolationMessage(VerifyExistenceType, idx_t, DataChunk &) override {
		return "ngram rowid guard has no constraints";
	}

	IndexStorageInfo SerializeToDisk(QueryContext, const case_insensitive_map_t<Value> &) override {
		lock_guard<mutex> guard(lock);
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
		lock_guard<mutex> guard(lock);
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

	struct State {
		string token;
		int64_t max_seen;
		bool unsafe_reuse;
		bool protection_compatible;
		vector<column_t> column_ids;
	};

	State GetState(optional_idx current_iteration = optional_idx()) {
		lock_guard<mutex> guard(lock);
		if (current_iteration.IsValid()) {
			ApplyCheckpointSeal(current_iteration.GetIndex());
		}
		return {token, max_seen, unsafe_reuse, protection_compatible, column_ids};
	}

private:
	//! Fold one appended rowid range into the guard. A range at or below
	//! max_seen has two possible histories. Either a checkpoint vacuumed fully
	//! deleted trailing row groups and the table handed their rowids out again,
	//! which must latch, or an earlier commit advanced max_seen and then failed
	//! in a later index (a UNIQUE ART rejecting a duplicate key), so no row
	//! landed and the same range is handed out again, which must not. A vacuum
	//! needs the exclusive vacuum lock, which every inserting transaction holds
	//! shared from its first insert to the end of its commit, so a vacuum can
	//! only fall strictly between two appends and every file checkpoint bumps
	//! the header iteration. The range therefore latches only when the
	//! iteration differs from the one recorded at the last advance, or when
	//! either value is unknown (in-memory database, a checkpoint in progress, or
	//! a guard bound from disk or WAL).
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
		result.options[OPTION_TOKEN] = Value(token);
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

class GuardBuildGlobalState final : public IndexBuildGlobalState {
public:
	// PhysicalCreateIndex retains this state until after AddIndex. Holding the
	// table append lock here makes the baseline read and physical installation
	// one atomic window with respect to committed inserts.
	TableAppendState append_state;
	unique_ptr<BoundIndex> index;
};

class GuardBuildLocalState final : public IndexBuildLocalState {};

static void RequirePinnedRuntime() {
	if (!RowIdGuardRuntimeCompatible()) {
		throw InvalidInputException("ngram rowid guard requires host DuckDB %s built from commit %s", DUCKDB_VERSION,
		                            DUCKDB_SOURCE_COMMIT);
	}
}

static unique_ptr<IndexBuildBindData> GuardBuildBind(IndexBuildBindInput &) {
	return nullptr;
}

static unique_ptr<IndexBuildGlobalState> GuardBuildGlobalInit(IndexBuildInitGlobalStateInput &input) {
	RequirePinnedRuntime();
	if (input.info.constraint_type != IndexConstraintType::NONE || input.storage_ids.empty() ||
	    input.storage_ids.size() != input.expressions.size()) {
		throw InvalidInputException("ngram rowid guard requires one or more non-unique physical columns");
	}
	if (!input.info.options.empty()) {
		throw InvalidInputException("ngram rowid guard does not accept caller-supplied options");
	}
	auto &storage = input.table.GetStorage();
	auto state = make_uniq<GuardBuildGlobalState>();
	storage.AppendLock(DuckTransaction::Get(input.context, input.table.ParentCatalog()), state->append_state);
	auto total_rows = storage.GetTotalRows();
	auto max_seen = total_rows == 0 ? int64_t(-1) : NumericCast<int64_t>(total_rows - 1);
	state->index =
	    make_uniq<RowIdGuard>(input.info.index_name, input.storage_ids, TableIOManager::Get(storage), input.expressions,
	                          storage.db, UUID::ToString(UUID::GenerateRandomUUID()), max_seen, false, true,
	                          optional_idx(), ObservableCheckpointIteration(storage.db));
	return std::move(state);
}

static unique_ptr<IndexBuildLocalState> GuardBuildLocalInit(IndexBuildInitLocalStateInput &) {
	return make_uniq<GuardBuildLocalState>();
}

static void GuardBuildSink(IndexBuildSinkInput &, DataChunk &, DataChunk &) {
}

static void GuardBuildCombine(IndexBuildCombineInput &) {
}

static unique_ptr<BoundIndex> GuardBuildFinalize(IndexBuildFinalizeInput &input) {
	return std::move(input.global_state.Cast<GuardBuildGlobalState>().index);
}

static unique_ptr<BoundIndex> GuardCreateInstance(CreateIndexInput &input) {
	if (input.column_ids.empty() || input.column_ids.size() != input.unbound_expressions.size()) {
		throw InvalidInputException("ngram rowid guard has malformed storage metadata");
	}
	auto stored = ReadStoredGuardState(input.storage_info);
	return make_uniq<RowIdGuard>(input.name, input.column_ids, input.table_io_manager, input.unbound_expressions,
	                             input.db, std::move(stored.token), stored.max_seen, stored.unsafe_reuse,
	                             stored.protection_compatible, stored.checkpoint_iteration, optional_idx());
}

static IndexType GuardIndexType();

static PhysicalOperator &GuardCreatePlan(PlanIndexInput &input) {
	vector<LogicalType> empty_types;
	for (auto &expression : input.op.expressions) {
		empty_types.push_back(expression->return_type);
	}
	empty_types.push_back(LogicalType::ROW_TYPE);
	auto &empty = input.planner.Make<PhysicalEmptyResult>(std::move(empty_types), 0);
	auto &create = input.planner.Make<PhysicalCreateIndex>(
	    input.op, input.op.table, input.op.info->column_ids, std::move(input.op.info),
	    std::move(input.op.unbound_expressions), 0, GuardIndexType(), nullptr, std::move(input.op.alter_table_info));
	create.children.push_back(empty);
	return create;
}

static IndexType GuardIndexType() {
	IndexType result;
	result.name = NGRAM_ROWID_GUARD_TYPE;
	result.build_bind = GuardBuildBind;
	result.build_global_init = GuardBuildGlobalInit;
	result.build_local_init = GuardBuildLocalInit;
	result.build_sink = GuardBuildSink;
	result.build_combine = GuardBuildCombine;
	result.build_finalize = GuardBuildFinalize;
	result.create_plan = GuardCreatePlan;
	result.create_instance = GuardCreateInstance;
	return result;
}

static unique_ptr<RowIdGuard::State> ReadGuardState(DuckTableEntry &table, const string &column_name,
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
			return make_uniq<RowIdGuard::State>(index.Cast<RowIdGuard>().GetState(checkpoint_iteration));
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
		auto result = make_uniq<RowIdGuard::State>();
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

static bool CanBindRowIdGuards(DuckTableEntry &table, bool require_compatible);

static string RowIdGuardDropReason(ClientContext &context, DuckTableEntry &table, const MetaInfo &meta,
                                   bool bind_unbound = true) {
	if (meta.guard_name.empty() || meta.guard_token.empty()) {
		return "the index does not record a valid rowid guard name and token";
	}

	auto &storage = table.GetStorage();
	auto has_column = table.ColumnExists(meta.column_name);
	StorageIndex expected_column;
	if (has_column) {
		expected_column = table.GetStorageIndex(ColumnIndex(table.GetColumn(meta.column_name).Logical().index));
	}

	EntryLookupInfo lookup(CatalogType::INDEX_ENTRY, meta.guard_name);
	auto catalog_entry = Catalog::GetEntry(context, table.ParentCatalog().GetName(), table.ParentSchema().name, lookup,
	                                       OnEntryNotFound::RETURN_NULL);
	if (catalog_entry) {
		auto &index_entry = catalog_entry->Cast<DuckIndexEntry>();
		if (!has_column || index_entry.index_type != NGRAM_ROWID_GUARD_TYPE ||
		    std::find(index_entry.column_ids.begin(), index_entry.column_ids.end(),
		              expected_column.GetPrimaryIndex()) == index_entry.column_ids.end() ||
		    &index_entry.GetDataTableInfo() != storage.GetDataTableInfo().get()) {
			return "an index with the recorded guard name belongs to a different table, type, or column";
		}
	}

	bool needs_bind = false;
	{
		for (auto &entry : storage.GetDataTableInfo()->GetIndexes().IndexEntries()) {
			auto &index = *entry.index;
			if (index.GetIndexName() != meta.guard_name) {
				continue;
			}
			if (!catalog_entry) {
				return "the recorded rowid guard exists in table storage but not in the index catalog";
			}
			if (entry.bind_state.load() == IndexBindState::BINDING) {
				return "the recorded rowid guard is being bound; retry the drop";
			}
			if (!has_column || index.GetIndexType() != NGRAM_ROWID_GUARD_TYPE ||
			    std::find(index.GetColumnIds().begin(), index.GetColumnIds().end(),
			              expected_column.GetPrimaryIndex()) == index.GetColumnIds().end()) {
				return "the recorded rowid guard has the wrong type or column dependency";
			}
			string token;
			if (index.IsBound()) {
				token = index.Cast<RowIdGuard>().GetState().token;
			} else {
				auto &options = index.Cast<UnboundIndex>().GetStorageInfo().options;
				auto token_entry = options.find(OPTION_TOKEN);
				if (token_entry == options.end() || token_entry->second.IsNull()) {
					return "the unbound rowid guard does not persist an incarnation token";
				}
				token = StringValue::Get(token_entry->second);
				needs_bind = true;
			}
			if (token != meta.guard_token) {
				return "the recorded rowid guard was dropped and re-created";
			}
			if (!needs_bind) {
				return string();
			}
			break;
		}
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
		return RowIdGuardDropReason(context, table, meta, false);
	}
	if (catalog_entry) {
		return "the recorded rowid guard is cataloged but missing from table storage";
	}
	return string();
}

static string LegacyRowIdGuardReason(DuckTableEntry &table, const string &column_name) {
	auto has_column = table.ColumnExists(column_name);
	StorageIndex expected_column;
	if (has_column) {
		expected_column = table.GetStorageIndex(ColumnIndex(table.GetColumn(column_name).Logical().index));
	}
	for (auto &entry : table.GetStorage().GetDataTableInfo()->GetIndexes().IndexEntries()) {
		auto &index = *entry.index;
		if (index.GetIndexType() != NGRAM_ROWID_GUARD_TYPE) {
			continue;
		}
		if (!has_column || std::find(index.GetColumnIds().begin(), index.GetColumnIds().end(),
		                             expected_column.GetPrimaryIndex()) != index.GetColumnIds().end()) {
			return "a rowid guard exists on the base table, so this is not a guard-less v2 index";
		}
	}
	return string();
}

struct GuardTableName {
	string catalog;
	string schema;
	string table;
};

static bool CanBindRowIdGuards(DuckTableEntry &table, bool require_compatible) {
	if (require_compatible && !RowIdGuardRuntimeCompatible()) {
		return false;
	}
	for (auto &entry : table.GetStorage().GetDataTableInfo()->GetIndexes().IndexEntries()) {
		auto &index = *entry.index;
		if (index.GetIndexType() != NGRAM_ROWID_GUARD_TYPE || index.IsBound()) {
			continue;
		}
		if (entry.bind_state.load() != IndexBindState::UNBOUND ||
		    index.GetConstraintType() != IndexConstraintType::NONE) {
			return false;
		}
		auto &unbound = index.Cast<UnboundIndex>();
		auto &expressions = unbound.GetParsedExpressions();
		auto &column_ids = index.GetColumnIds();
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
		if (require_compatible && !ReadStoredGuardState(unbound.GetStorageInfo()).protection_compatible) {
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
		auto &catalog = db->GetCatalog();
		catalog.ScanSchemas(context, [&](SchemaCatalogEntry &schema) {
			schema.Scan(context, CatalogType::INDEX_ENTRY, [&](CatalogEntry &entry) {
				auto &index = entry.Cast<DuckIndexEntry>();
				if (index.index_type != NGRAM_ROWID_GUARD_TYPE) {
					return;
				}
				auto table_schema = index.GetSchemaName();
				auto table_name = index.GetTableName();
				auto key = db->GetName() + '\0' + table_schema + '\0' + table_name;
				if (seen.insert(std::move(key)).second) {
					tables.push_back({db->GetName(), std::move(table_schema), std::move(table_name)});
				}
			});
		});
	}
	// Schema::Scan holds its CatalogSet lock for the callback lifetime. Resolve
	// and bind only after every scan callback has returned: BindIndexes performs
	// another catalog lookup and would otherwise self-deadlock.
	for (auto &name : tables) {
		try {
			EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, name.table);
			auto entry = Catalog::GetEntry(context, name.catalog, name.schema, lookup, OnEntryNotFound::RETURN_NULL);
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

static void RowIdGuardValidateFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto output = FlatVector::GetData<bool>(result);
	for (idx_t row = 0; row < args.size(); row++) {
		auto &table = ResolveExistingTable(context, args.GetValue(0, row).ToString(), args.GetValue(1, row).ToString(),
		                                   args.GetValue(2, row).ToString(), "ngram index base table");
		auto column_name = args.GetValue(3, row).ToString();
		auto shadow_schema = args.GetValue(4, row).ToString();
		auto meta_name = args.GetValue(9, row).ToString();
		auto &meta_table = ResolveExistingTable(context, args.GetValue(0, row).ToString(), shadow_schema, meta_name,
		                                        "ngram index meta table");
		auto expected_format = args.GetValue(5, row).GetValue<int64_t>();
		if (NumericCast<int64_t>(meta_table.oid) != args.GetValue(6, row).GetValue<int64_t>()) {
			throw InvalidInputException("ngram meta table changed while drop_ngram_index was prepared");
		}
		ShadowTarget target {args.GetValue(1, row).ToString(), args.GetValue(2, row).ToString(), column_name,
		                     shadow_schema};
		auto &transaction = DuckTransaction::Get(context, table.ParentCatalog());
		auto actual_format = ReadMetaFormatVersion(context, transaction, meta_table, target);
		if (actual_format != expected_format) {
			throw InvalidInputException("ngram meta format changed while drop_ngram_index was prepared");
		}
		if (expected_format == 2) {
			if (meta_table.ColumnExists("guard_name") || meta_table.ColumnExists("guard_token")) {
				throw InvalidInputException("format_version 2 meta table unexpectedly contains rowid guard columns");
			}
			auto reason = LegacyRowIdGuardReason(table, column_name);
			if (!reason.empty()) {
				throw InvalidInputException("ngram v2 drop validation failed: %s", reason);
			}
			output[row] = true;
			continue;
		}
		if (expected_format != NGRAM_FORMAT_VERSION) {
			throw InvalidInputException("unsupported ngram meta format_version %lld", expected_format);
		}
		auto meta = ReadMeta(context, transaction, meta_table, target);
		if (meta.guard_name != args.GetValue(7, row).ToString() ||
		    meta.guard_token != args.GetValue(8, row).ToString()) {
			throw InvalidInputException("ngram rowid guard changed while drop_ngram_index was prepared");
		}
		auto reason = RowIdGuardDropReason(context, table, meta);
		if (!reason.empty()) {
			throw InvalidInputException("ngram rowid guard validation failed: %s", reason);
		}
		output[row] = true;
	}
}

void RegisterRowIdGuard(ExtensionLoader &loader) {
	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	auto &types = config.GetIndexTypes();
	types.RegisterIndexType(GuardIndexType());
	ExtensionCallback::Register(config, make_shared_ptr<RowIdGuardBindCallback>());

	auto validate = ScalarFunction(
	    NGRAM_ROWID_GUARD_VALIDATE,
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	     LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	    LogicalType::BOOLEAN, RowIdGuardValidateFunction);
	validate.stability = FunctionStability::VOLATILE;
	validate.SetFallible();
	loader.RegisterFunction(validate);
}

static unique_ptr<RowIdGuard::State> ReadExactGuard(DuckTableEntry &table, const MetaInfo &meta, string &reason,
                                                    optional_idx checkpoint_iteration = optional_idx()) {
	if (!RowIdGuardRuntimeCompatible()) {
		reason = StringUtil::Format("the host runtime does not match pinned DuckDB %s commit %s", DUCKDB_VERSION,
		                            DUCKDB_SOURCE_COMMIT);
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
		return StringUtil::Format("the host runtime does not match pinned DuckDB %s commit %s", DUCKDB_VERSION,
		                          DUCKDB_SOURCE_COMMIT);
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

string RowIdGuardProtectionReason(DuckTableEntry &table, const MetaInfo &meta, const string &column_name,
                                  vector<string> &protected_columns) {
	protected_columns.clear();
	string reason;
	auto state = ReadExactGuard(table, meta, reason);
	if (!state) {
		return reason;
	}
	if (!table.ColumnExists(column_name)) {
		return "the new indexed column no longer exists";
	}
	auto expected = table.GetStorageIndex(ColumnIndex(table.GetColumn(column_name).Logical().index)).GetPrimaryIndex();
	if (std::find(state->column_ids.begin(), state->column_ids.end(), expected) == state->column_ids.end()) {
		return "existing rowid guards do not cover the new indexed column";
	}
	for (auto &definition : table.GetColumns().Physical()) {
		auto physical = table.GetStorageIndex(ColumnIndex(definition.Logical().index)).GetPrimaryIndex();
		if (std::find(state->column_ids.begin(), state->column_ids.end(), physical) != state->column_ids.end()) {
			protected_columns.push_back(definition.Name());
		}
	}
	if (protected_columns.size() != state->column_ids.size()) {
		return "the rowid guard's physical column dependency no longer matches the table";
	}
	return string();
}

static bool CoversColumn(const vector<column_t> &column_ids, column_t expected) {
	return std::find(column_ids.begin(), column_ids.end(), expected) != column_ids.end();
}

bool FindNativeUpdateProtector(ClientContext &context, DuckTableEntry &table, const string &column_name,
                               NativeUpdateProtector &result) {
	if (!table.ColumnExists(column_name)) {
		return false;
	}
	auto expected = table.GetStorageIndex(ColumnIndex(table.GetColumn(column_name).Logical().index)).GetPrimaryIndex();
	auto &storage = table.GetStorage();
	try {
		storage.GetDataTableInfo()->BindIndexes(context, ART::TYPE_NAME);
	} catch (std::exception &ex) {
		throw InvalidInputException("create_ngram_index cannot bind an existing ART update protector: %s",
		                            ErrorData(ex).RawMessage());
	}
	vector<string> candidates;
	{
		auto entries = storage.GetDataTableInfo()->GetIndexes().IndexEntries();
		for (auto &item : entries) {
			auto &index = *item.index;
			if (index.IsBound() && index.GetIndexType() == ART::TYPE_NAME &&
			    CoversColumn(index.GetColumnIds(), expected)) {
				candidates.push_back(index.GetIndexName());
			}
		}
	}
	for (auto &candidate : candidates) {
		EntryLookupInfo lookup(CatalogType::INDEX_ENTRY, candidate);
		auto catalog_entry = Catalog::GetEntry(context, table.ParentCatalog().GetName(), table.ParentSchema().name,
		                                       lookup, OnEntryNotFound::RETURN_NULL);
		if (!catalog_entry) {
			continue;
		}
		auto &entry = catalog_entry->Cast<DuckIndexEntry>();
		if (entry.internal || entry.index_type != ART::TYPE_NAME ||
		    &entry.GetDataTableInfo() != storage.GetDataTableInfo().get() ||
		    !CoversColumn(entry.column_ids, expected)) {
			continue;
		}
		auto timestamp = entry.timestamp.load();
		if (!CatalogSet::IsCommitted(timestamp)) {
			continue;
		}
		result = NativeUpdateProtector(entry.name, entry.oid, timestamp);
		return true;
	}
	return false;
}

string NativeUpdateProtectorReason(ClientContext &context, DuckTableEntry &table, const string &column_name,
                                   const NativeUpdateProtector &expected_protector) {
	if (expected_protector.name.empty() || !CatalogSet::IsCommitted(expected_protector.timestamp)) {
		return "the recorded native update protector is malformed or uncommitted";
	}
	if (!table.ColumnExists(column_name)) {
		return "the protected column no longer exists";
	}
	auto expected_column =
	    table.GetStorageIndex(ColumnIndex(table.GetColumn(column_name).Logical().index)).GetPrimaryIndex();
	auto &storage = table.GetStorage();
	EntryLookupInfo lookup(CatalogType::INDEX_ENTRY, expected_protector.name);
	auto catalog_entry = Catalog::GetEntry(context, table.ParentCatalog().GetName(), table.ParentSchema().name, lookup,
	                                       OnEntryNotFound::RETURN_NULL);
	if (!catalog_entry) {
		return "the native update protector is missing from the index catalog";
	}
	auto &entry = catalog_entry->Cast<DuckIndexEntry>();
	if (entry.internal || entry.oid != expected_protector.oid || entry.index_type != ART::TYPE_NAME ||
	    entry.timestamp.load() != expected_protector.timestamp ||
	    &entry.GetDataTableInfo() != storage.GetDataTableInfo().get() ||
	    !CoversColumn(entry.column_ids, expected_column)) {
		return "the native update protector's catalog identity, table, type, column dependency, or timestamp changed";
	}
	for (auto &item : storage.GetDataTableInfo()->GetIndexes().IndexEntries()) {
		auto &index = *item.index;
		if (index.GetIndexName() != expected_protector.name) {
			continue;
		}
		if (!index.IsBound() || index.GetIndexType() != ART::TYPE_NAME ||
		    !CoversColumn(index.GetColumnIds(), expected_column)) {
			return "the native update protector is no longer bound to the indexed column";
		}
		return string();
	}
	return "the native update protector is missing from table storage";
}

} // namespace ngram
} // namespace duckdb
