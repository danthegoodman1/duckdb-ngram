#include "ngram/search_core.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/parallel/task_executor.hpp"
#include "ngram/catalog.hpp"

namespace duckdb {
namespace ngram {

//! Batch indexes: every fetch batch of an admitted segment precedes every
//! storage batch, so ordered sinks restore candidate-then-tail order.
static constexpr idx_t FETCH_BATCHES_PER_SEGMENT = (idx_t(1) << SEGMENT_SHIFT) / STANDARD_VECTOR_SIZE;

DuckTableEntry &ResolveExistingTable(ClientContext &context, const string &catalog, const string &schema,
                                     const string &name, const char *what) {
	auto entry = TryResolveExistingTable(context, catalog, schema, name, what);
	if (!entry) {
		throw CatalogException("ngram: %s %s.%s no longer exists; was the table or index dropped after binding?", what,
		                       schema, name);
	}
	return *entry;
}

optional_ptr<DuckTableEntry> TryResolveExistingTable(ClientContext &context, const string &catalog,
                                                     const string &schema, const string &name, const char *what) {
	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, name);
	auto entry = Catalog::GetEntry(context, catalog, schema, lookup, OnEntryNotFound::RETURN_NULL);
	if (!entry) {
		return nullptr;
	}
	if (entry->type != CatalogType::TABLE_ENTRY || !entry->Cast<TableCatalogEntry>().IsDuckTable()) {
		throw InvalidInputException("ngram: %s %s.%s has the wrong catalog type; the index is malformed", what, schema,
		                            name);
	}
	return entry->Cast<DuckTableEntry>();
}

DuckTableEntry &ResolveBoundBase(ClientContext &context, const string &catalog, const string &schema,
                                 const string &table, const vector<string> &names, const vector<LogicalType> &types,
                                 const char *changed_message) {
	auto &base = ResolveExistingTable(context, catalog, schema, table, "table");
	idx_t position = 0;
	for (auto &col : base.GetColumns().Logical()) {
		if (col.Generated() || position >= types.size() || col.Name() != names[position] ||
		    col.Type() != types[position]) {
			throw InvalidInputException(changed_message, table);
		}
		position++;
	}
	if (position != types.size()) {
		throw InvalidInputException(changed_message, table);
	}
	return base;
}

void AddShadowColumn(DuckTableEntry &entry, const string &column_name, LogicalTypeId expected,
                     vector<StorageIndex> &column_ids, vector<LogicalType> &types) {
	if (!entry.ColumnExists(column_name)) {
		throw InvalidInputException("ngram: table %s is missing column %s; the index tables are malformed", entry.name,
		                            column_name);
	}
	auto &col = entry.GetColumn(column_name);
	if (col.Type().id() != expected) {
		throw InvalidInputException("ngram: column %s of %s has type %s; the index tables are malformed", column_name,
		                            entry.name, col.Type().ToString());
	}
	column_ids.push_back(entry.GetStorageIndex(ColumnIndex(col.Logical().index)));
	types.push_back(col.Type());
}

void InitializeExhaustiveScan(ClientContext &context, DuckTransaction &tx, DataTable &storage, TableScanState &state,
                              const vector<StorageIndex> &column_ids, optional_ptr<TableFilterSet> filters) {
	if (storage.GetTotalRows() == 0) {
		// no committed row groups (a table created inside this transaction):
		// initialize only the transaction-local phase; the committed phase
		// then scans nothing. Going through DataTable::InitializeScan instead
		// trips a DEBUG-build assertion on the empty row-group collection.
		state.Initialize(column_ids, context, filters);
		LocalStorage::Get(tx).InitializeScan(storage, state.local_state, filters);
		return;
	}
	storage.InitializeScan(context, tx, state, column_ids, filters);
}

void ThrowIfInterrupted(ClientContext &context) {
	if (context.interrupted.load(std::memory_order_relaxed)) {
		throw InterruptException();
	}
}

void ScanShadowTable(ClientContext &context, DuckTransaction &tx, DataTable &storage,
                     const vector<StorageIndex> &column_ids, const vector<LogicalType> &types,
                     optional_ptr<TableFilterSet> filters, const std::function<void(DataChunk &)> &fn) {
	TableScanState state;
	InitializeExhaustiveScan(context, tx, storage, state, column_ids, filters);
	DataChunk chunk;
	chunk.Initialize(Allocator::Get(context), types);
	while (true) {
		ThrowIfInterrupted(context);
		chunk.Reset();
		storage.Scan(tx, chunk, state);
		if (chunk.size() == 0) {
			break;
		}
		fn(chunk);
	}
}

//! Run `body(unit)` for every unit in [0, units) across the scheduler's
//! threads, or inline when there is only one of either. The stats scan is a
//! read-only pass whose atomic results are order-independent, so it needs no
//! operator pipeline of its own.
namespace {

class IndexedTask : public BaseExecutorTask {
public:
	IndexedTask(TaskExecutor &executor, atomic<idx_t> &cursor, idx_t units, const std::function<void(idx_t)> &body)
	    : BaseExecutorTask(executor), cursor(cursor), units(units), body(body) {
	}

	void ExecuteTask() override {
		while (true) {
			auto unit = cursor.fetch_add(1);
			if (unit >= units) {
				return;
			}
			body(unit);
		}
	}

private:
	atomic<idx_t> &cursor;
	idx_t units;
	const std::function<void(idx_t)> &body;
};

} // namespace

//! InitializeParallelScan takes the columns a scan will project, which the
//! per-thread TableScanState already carries here.
static const vector<ColumnIndex> NO_COLUMN_INDEXES;

static void ParallelForEachUnit(ClientContext &context, idx_t units, idx_t workers,
                                const std::function<void(idx_t)> &body) {
	if (units == 0) {
		return;
	}
	if (workers <= 1 || units == 1) {
		for (idx_t unit = 0; unit < units; unit++) {
			body(unit);
		}
		return;
	}
	TaskExecutor executor(context);
	atomic<idx_t> cursor {0};
	for (idx_t worker = 0; worker < MinValue<idx_t>(workers, units); worker++) {
		executor.ScheduleTask(make_uniq<IndexedTask>(executor, cursor, units, body));
	}
	executor.WorkOnTasks();
}

void ParallelScanShadowTable(ClientContext &context, DuckTransaction &tx, DataTable &storage,
                             const vector<StorageIndex> &column_ids, const vector<LogicalType> &types,
                             optional_ptr<TableFilterSet> filters, idx_t workers,
                             const std::function<void(DataChunk &, idx_t)> &body) {
	ParallelTableScanState parallel_state;
	storage.InitializeParallelScan(context, parallel_state, NO_COLUMN_INDEXES);
	ParallelForEachUnit(context, workers, workers, [&](idx_t worker) {
		TableScanState scan;
		scan.Initialize(column_ids, &context, filters);
		DataChunk chunk;
		chunk.Initialize(Allocator::Get(context), types);
		while (storage.NextParallelScan(context, parallel_state, scan) != 0) {
			while (true) {
				ThrowIfInterrupted(context);
				chunk.Reset();
				storage.Scan(tx, chunk, scan);
				if (chunk.size() == 0) {
					break;
				}
				body(chunk, worker);
			}
		}
	});
}

static idx_t SearchCoreScanUnits(ClientContext &context, DataTable &storage, const SearchCoreGlobal &state) {
	if (!state.probe || state.hwm < 0) {
		return storage.MaxThreads(context);
	}
	idx_t units = 1;
	auto total_rows = storage.GetTotalRows();
	auto indexed = NumericCast<idx_t>(state.hwm) + 1;
	if (total_rows > indexed) {
		units += (total_rows - indexed) / storage.GetRowGroupSize() + 1;
	}
	return units;
}

void FinalizeSearchCore(ClientContext &context, SearchCoreGlobal &state) {
	D_ASSERT(state.storage && state.tx);
	state.scan_column_ids = state.fetch_column_ids;
	state.scan_types = state.fetch_types;
	if (!state.scan_filters) {
		state.scan_filters = make_uniq<TableFilterSet>();
	}
	if (state.probe && state.hwm >= 0) {
		optional_idx rowid_position;
		for (idx_t i = 0; i < state.scan_column_ids.size(); i++) {
			if (state.scan_column_ids[i].IsRowIdColumn()) {
				rowid_position = i;
				break;
			}
		}
		if (!rowid_position.IsValid()) {
			rowid_position = state.scan_column_ids.size();
			state.scan_column_ids.emplace_back(StorageIndex(COLUMN_IDENTIFIER_ROW_ID));
			state.scan_types.emplace_back(LogicalType::ROW_TYPE);
		}
		state.scan_filters->PushFilter(
		    ColumnIndex(rowid_position.GetIndex()),
		    make_uniq<ConstantFilter>(ExpressionType::COMPARE_GREATERTHAN, Value::BIGINT(state.hwm)));
	}
	state.storage->InitializeParallelScan(context, state.parallel_scan, NO_COLUMN_INDEXES);
	state.fetch_batch_base = state.probe ? state.probe->segments.size() * FETCH_BATCHES_PER_SEGMENT : 0;
	state.max_threads =
	    (state.probe ? state.probe->max_threads : 0) + SearchCoreScanUnits(context, *state.storage, state);
	state.max_threads = MaxValue<idx_t>(state.max_threads, 1);
}

void InitializeSearchCoreLocal(ExecutionContext &context, SearchCoreGlobal &global, SearchCoreLocal &local) {
	local.phase = global.probe && global.next_probe_thread.fetch_add(1) < global.probe->max_threads
	                  ? SearchCorePhase::FETCH
	                  : SearchCorePhase::SCAN;
	local.fetch_chunk.Initialize(Allocator::Get(context.client), global.fetch_types);
	local.scan_state.Initialize(global.scan_column_ids, &context.client,
	                            global.scan_filters->filters.empty() ? nullptr : global.scan_filters.get());
	local.scan_chunk.Initialize(Allocator::Get(context.client), global.scan_types);
	local.sel.Initialize(STANDARD_VECTOR_SIZE);
}

static bool SearchCoreYieldEmpty(TableFunctionInput &data) {
	if (data.results_execution_mode != AsyncResultsExecutionMode::TASK_EXECUTOR) {
		return false;
	}
	data.async_result = AsyncResultType::HAVE_MORE_OUTPUT;
	return true;
}

static void SearchCoreEmit(SearchCoreGlobal &global, SearchCoreLocal &local, DataChunk &source, idx_t count,
                           DataChunk &output) {
	D_ASSERT(output.ColumnCount() == global.output_ids.size());
	for (idx_t column = 0; column < global.output_ids.size(); column++) {
		auto source_id = global.output_ids[column];
		if (source_id == DConstants::INVALID_INDEX) {
			output.data[column].Reference(Value::BOOLEAN(true));
		} else {
			output.data[column].Slice(source.data[source_id], local.sel, count);
		}
	}
	output.SetCardinality(count);
}

void ExecuteSearchCore(ClientContext &context, TableFunctionInput &data, SearchCoreGlobal &global,
                       SearchCoreLocal &local, const std::function<idx_t(DataChunk &, SelectionVector &)> &recheck,
                       DataChunk &output) {
	while (true) {
		switch (local.phase) {
		case SearchCorePhase::FETCH: {
			// The prior output is consumed before re-entry. Release its block pins
			// before decoding another segment or transitioning to the tail scan.
			local.fetch_chunk.Reset();
			local.fetch_state = ColumnFetchState();
			if (local.candidate_offset >= local.candidates.size()) {
				local.candidates.clear();
				local.candidate_offset = 0;
				if (!NextCandidateSegment(context, *global.tx, *global.probe, local.candidates,
				                          local.segment_ordinal)) {
					local.phase = SearchCorePhase::SCAN;
					continue;
				}
				if (local.candidates.empty()) {
					continue;
				}
			}
			auto offset = local.candidate_offset;
			auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, local.candidates.size() - offset);
			local.batch_index = local.segment_ordinal * FETCH_BATCHES_PER_SEGMENT + offset / STANDARD_VECTOR_SIZE;
			Vector rowids(LogicalType::ROW_TYPE,
			              reinterpret_cast<data_ptr_t>(local.candidates.data() + local.candidate_offset));
			local.candidate_offset += count;
			global.storage->Fetch(*global.tx, local.fetch_chunk, global.fetch_column_ids, rowids, count,
			                      local.fetch_state);
			auto hits = local.fetch_chunk.size() == 0 ? 0 : recheck(local.fetch_chunk, local.sel);
			if (hits == 0) {
				if (SearchCoreYieldEmpty(data)) {
					return;
				}
				continue;
			}
			SearchCoreEmit(global, local, local.fetch_chunk, hits, output);
			return;
		}
		case SearchCorePhase::SCAN: {
			if (!local.scan_unit_active) {
				if (global.storage->NextParallelScan(context, global.parallel_scan, local.scan_state) == 0) {
					local.phase = SearchCorePhase::DONE;
					continue;
				}
				local.scan_unit_active = true;
			}
			local.scan_chunk.Reset();
			global.storage->Scan(*global.tx, local.scan_chunk, local.scan_state);
			if (local.scan_chunk.size() == 0) {
				local.scan_unit_active = false;
				if (SearchCoreYieldEmpty(data)) {
					return;
				}
				continue;
			}
			local.batch_index = global.fetch_batch_base + local.scan_state.table_state.batch_index +
			                    local.scan_state.local_state.batch_index;
			auto hits = recheck(local.scan_chunk, local.sel);
			if (hits == 0) {
				if (SearchCoreYieldEmpty(data)) {
					return;
				}
				continue;
			}
			SearchCoreEmit(global, local, local.scan_chunk, hits, output);
			return;
		}
		case SearchCorePhase::DONE:
			return;
		}
	}
}

} // namespace ngram
} // namespace duckdb
