#include "ngram/search_core.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/parallel/task_executor.hpp"
#include "ngram/catalog.hpp"

namespace duckdb {
namespace ngram {

//! Batch indexes: every fetch chunk of an admitted segment precedes every
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

//! The manifest scans are read-only passes whose per-unit results live in
//! slots their unit owns, so they need no operator pipeline of their own.
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

void ParallelForEachUnit(ClientContext &context, idx_t units, idx_t workers, const std::function<void(idx_t)> &body) {
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
		// the fetch projection carries the rowid for range-scanned batches and
		// the tail scan filters on it; an extra trailing column leaves every
		// output and recheck position unchanged
		optional_idx rowid_position;
		for (idx_t i = 0; i < state.fetch_column_ids.size(); i++) {
			if (state.fetch_column_ids[i].IsRowIdColumn()) {
				rowid_position = i;
				break;
			}
		}
		if (!rowid_position.IsValid()) {
			rowid_position = state.fetch_column_ids.size();
			state.fetch_column_ids.emplace_back(StorageIndex(COLUMN_IDENTIFIER_ROW_ID));
			state.fetch_types.emplace_back(LogicalType::ROW_TYPE);
			state.scan_column_ids.emplace_back(StorageIndex(COLUMN_IDENTIFIER_ROW_ID));
			state.scan_types.emplace_back(LogicalType::ROW_TYPE);
		}
		state.fetch_rowid_position = rowid_position.GetIndex();
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

enum class BatchClaim : uint8_t { CLAIMED, EXHAUSTED, WAITING };

//! Take the next candidate batch for `local`: from a published segment when
//! one has batches left, otherwise by claiming the next admitted segment,
//! decoding it and publishing it. EXHAUSTED once every segment is decoded and
//! every batch handed out; WAITING when another worker is still decoding and
//! the async protocol asks this one to yield rather than block.
static BatchClaim ClaimCandidateBatch(ClientContext &context, TableFunctionInput &data, SearchCoreGlobal &global,
                                      SearchCoreLocal &local) {
	auto &plan = *global.probe;
	auto &queue = global.candidates;
	while (true) {
		ThrowIfInterrupted(context);
		idx_t ordinal;
		{
			std::unique_lock<mutex> guard(queue.lock);
			if (!queue.ready.empty()) {
				auto &front = queue.ready.front();
				local.candidates = front.rowids;
				local.segment_ordinal = front.segment_ordinal;
				local.candidate_offset = front.next_offset;
				local.candidate_end = MinValue<idx_t>(front.next_offset + FETCH_BATCH_ROWS, front.rowids->size());
				front.next_offset = local.candidate_end;
				if (front.next_offset >= front.rowids->size()) {
					queue.ready.pop_front();
				}
				return BatchClaim::CLAIMED;
			}
			// the claim and the decoding count change together under the lock,
			// so a worker that sees no segment left and nobody decoding is done
			ordinal = plan.next_segment.fetch_add(1);
			if (ordinal >= plan.segments.size()) {
				if (queue.decoding == 0) {
					return BatchClaim::EXHAUSTED;
				}
				queue.published.wait_for(guard, std::chrono::milliseconds(1));
				if (queue.ready.empty() && queue.decoding > 0 &&
				    data.results_execution_mode == AsyncResultsExecutionMode::TASK_EXECUTOR) {
					return BatchClaim::WAITING;
				}
				continue;
			}
			queue.decoding++;
		}
		auto rowids = make_shared_ptr<vector<row_t>>();
		try {
			DecodeCandidateSegment(context, *global.tx, plan, ordinal, local.decode, *rowids);
		} catch (...) {
			std::lock_guard<mutex> guard(queue.lock);
			queue.decoding--;
			queue.published.notify_all();
			throw;
		}
		std::lock_guard<mutex> guard(queue.lock);
		queue.decoding--;
		if (!rowids->empty()) {
			queue.ready.emplace_back(ordinal, std::move(rowids));
		}
		queue.published.notify_all();
	}
}

//! A claimed batch whose rowids fill at least half of their span is read with
//! one rowid-range scan instead of a fetch per row: the scan decompresses
//! whole vectors and takes no per-row lock, while DataTable::Fetch locks the
//! row-group tree for every row and decodes FSST strings one at a time
//! (measured 3.8 us per fetched row against 0.13 us for uncompressed strings).
//! Rows of the span that are not candidates cannot match, so rechecking them
//! changes no result. The rowid filters prune every other row group by zone
//! map and are exact within the range, so no row of another batch is read.
static constexpr idx_t RANGE_SCAN_MIN_ROWS = 256;

static bool StartRangeScan(ClientContext &context, SearchCoreGlobal &global, SearchCoreLocal &local) {
	auto &rowids = *local.candidates;
	auto batch_rows = local.candidate_end - local.candidate_offset;
	auto first = rowids[local.candidate_offset];
	auto last = rowids[local.candidate_end - 1];
	if (batch_rows < RANGE_SCAN_MIN_ROWS || NumericCast<idx_t>(last - first) + 1 > 2 * batch_rows) {
		return false;
	}
	local.range_filters = make_uniq<TableFilterSet>();
	local.range_filters->PushFilter(
	    ColumnIndex(global.fetch_rowid_position),
	    make_uniq<ConstantFilter>(ExpressionType::COMPARE_GREATERTHANOREQUALTO, Value::BIGINT(first)));
	local.range_filters->PushFilter(
	    ColumnIndex(global.fetch_rowid_position),
	    make_uniq<ConstantFilter>(ExpressionType::COMPARE_LESSTHANOREQUALTO, Value::BIGINT(last)));
	local.range_state = make_uniq<TableScanState>();
	InitializeExhaustiveScan(context, *global.tx, *global.storage, *local.range_state, global.fetch_column_ids,
	                         local.range_filters.get());
	local.candidate_offset = local.candidate_end;
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
			if (local.range_state) {
				global.storage->Scan(*global.tx, local.fetch_chunk, *local.range_state);
				if (local.fetch_chunk.size() == 0) {
					local.range_state.reset();
					local.range_filters.reset();
					if (SearchCoreYieldEmpty(data)) {
						return;
					}
					continue;
				}
			} else {
				if (local.candidate_offset >= local.candidate_end) {
					local.candidates.reset();
					auto claim = ClaimCandidateBatch(context, data, global, local);
					if (claim == BatchClaim::WAITING) {
						data.async_result = AsyncResultType::HAVE_MORE_OUTPUT;
						return;
					}
					if (claim == BatchClaim::EXHAUSTED) {
						local.phase = SearchCorePhase::SCAN;
						continue;
					}
					// every chunk of the batch, fetched or range-scanned, carries
					// the batch's index; one thread emits them in order
					local.batch_index = local.segment_ordinal * FETCH_BATCHES_PER_SEGMENT +
					                    local.candidate_offset / STANDARD_VECTOR_SIZE;
					if (StartRangeScan(context, global, local)) {
						continue;
					}
				}
				auto offset = local.candidate_offset;
				auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, local.candidate_end - offset);
				Vector rowids(LogicalType::ROW_TYPE, reinterpret_cast<data_ptr_t>(local.candidates->data() + offset));
				local.candidate_offset += count;
				global.storage->Fetch(*global.tx, local.fetch_chunk, global.fetch_column_ids, rowids, count,
				                      local.fetch_state);
			}
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
