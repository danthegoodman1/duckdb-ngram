#include "ngram/search_core.hpp"
#include "ngram/test_hooks.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parallel/task_executor.hpp"
#include "duckdb/storage/optimistic_data_writer.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/row_group_segment_tree.hpp"
#include "duckdb/transaction/local_storage.hpp"
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

idx_t ExtraFetchColumns(const SearchCoreGlobal &state) {
	idx_t extra = 0;
	for (idx_t i = 0; i < state.fetch_column_ids.size(); i++) {
		if (state.fetch_column_ids[i].IsRowIdColumn()) {
			continue;
		}
		if (std::find(state.recheck_positions.begin(), state.recheck_positions.end(), i) ==
		    state.recheck_positions.end()) {
			extra++;
		}
	}
	return extra;
}

void FinalizeSearchCore(ClientContext &context, SearchCoreGlobal &state) {
	D_ASSERT(state.storage && state.tx);
	if (state.scan_filters && state.scan_filters->filters.empty()) {
		state.scan_filters.reset();
	}
	state.tail_start = state.probe && state.hwm >= 0 ? NumericCast<idx_t>(state.hwm) + 1 : 0;
	// every bounded scan filters on the rowid; an extra trailing column
	// leaves every output and recheck position unchanged
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
	}
	state.fetch_rowid_position = rowid_position.GetIndex();
	// the probe layout: the recheck's columns in its order, then the rowid
	state.probe_positions = state.recheck_positions;
	if (std::find(state.probe_positions.begin(), state.probe_positions.end(), state.fetch_rowid_position) ==
	    state.probe_positions.end()) {
		state.probe_positions.push_back(state.fetch_rowid_position);
	}
	for (idx_t i = 0; i < state.fetch_column_ids.size(); i++) {
		if (std::find(state.probe_positions.begin(), state.probe_positions.end(), i) == state.probe_positions.end()) {
			state.extra_positions.push_back(i);
		}
	}
	for (auto position : state.probe_positions) {
		if (position == state.fetch_rowid_position) {
			state.probe_rowid_position = state.probe_column_ids.size();
		}
		state.probe_column_ids.push_back(state.fetch_column_ids[position]);
		state.probe_types.push_back(state.fetch_types[position]);
	}
	for (auto position : state.extra_positions) {
		state.extra_column_ids.push_back(state.fetch_column_ids[position]);
		state.extra_types.push_back(state.fetch_types[position]);
	}
	for (auto source_id : state.output_ids) {
		if (source_id == DConstants::INVALID_INDEX) {
			state.output_sources.emplace_back(true, DConstants::INVALID_INDEX);
			continue;
		}
		auto probe = std::find(state.probe_positions.begin(), state.probe_positions.end(), source_id);
		if (probe != state.probe_positions.end()) {
			state.output_sources.emplace_back(true, NumericCast<idx_t>(probe - state.probe_positions.begin()));
		} else {
			auto extra = std::find(state.extra_positions.begin(), state.extra_positions.end(), source_id);
			D_ASSERT(extra != state.extra_positions.end());
			state.output_sources.emplace_back(false, NumericCast<idx_t>(extra - state.extra_positions.begin()));
		}
	}
	state.tail_end = state.storage->GetTotalRows();
	state.tail_unit_rows = MaxValue<idx_t>(state.storage->GetRowGroupSize(), 1);
	state.tail_units = state.tail_start < state.tail_end
	                       ? (state.tail_end - state.tail_start + state.tail_unit_rows - 1) / state.tail_unit_rows
	                       : 0;
	state.fetch_batch_base = state.probe ? state.probe->segments.size() * FETCH_BATCHES_PER_SEGMENT : 0;
	state.max_threads = (state.probe ? state.probe->max_threads : 0) + state.tail_units + 1;
}

void InitializeSearchCoreLocal(ExecutionContext &context, SearchCoreGlobal &global, SearchCoreLocal &local) {
	local.phase = global.probe && global.next_probe_thread.fetch_add(1) < global.probe->max_threads
	                  ? SearchCorePhase::FETCH
	                  : SearchCorePhase::SCAN;
	local.fetch_chunk.Initialize(Allocator::Get(context.client), global.fetch_types);
	local.probe_chunk.Initialize(Allocator::Get(context.client), global.probe_types);
	if (!global.extra_types.empty()) {
		local.extra_chunk.Initialize(Allocator::Get(context.client), global.extra_types);
	}
	local.scan_chunk.Initialize(Allocator::Get(context.client), global.fetch_types);
	local.sel.Initialize(STANDARD_VECTOR_SIZE);
}

idx_t SelectRechecked(ExpressionExecutor *executor, DataChunk &chunk, SelectionVector &sel, bool natively_filtered) {
	if (!executor || natively_filtered) {
		for (idx_t r = 0; r < chunk.size(); r++) {
			sel.set_index(r, r);
		}
		return chunk.size();
	}
	return executor->SelectExpression(chunk, sel);
}

static bool SearchCoreYieldEmpty(TableFunctionInput &data) {
	if (data.results_execution_mode != AsyncResultsExecutionMode::TASK_EXECUTOR) {
		return false;
	}
	data.async_result = AsyncResultType::HAVE_MORE_OUTPUT;
	return true;
}

enum class BatchClaim : uint8_t { CLAIMED, EXHAUSTED, WAITING };

NgramTestHooks &GetNgramTestHooks() {
	static NgramTestHooks hooks;
	return hooks;
}

//! Move every decoded segment that is next in ordinal order from `pending`
//! to `ready`. Caller holds the queue lock.
static void PublishInOrder(CandidateQueue &queue) {
	while (!queue.pending.empty() && queue.pending.begin()->first == queue.next_publish) {
		auto &decoded = queue.pending.begin()->second;
		if (!decoded->empty()) {
			queue.ready.emplace_back(queue.next_publish, std::move(decoded));
		}
		queue.pending.erase(queue.pending.begin());
		queue.next_publish++;
	}
	queue.published.notify_all();
}

//! Take the next candidate batch for `local`: from the oldest published
//! segment when one has batches left, otherwise by claiming the next admitted
//! segment, decoding it and publishing it in ordinal order. At most
//! max_threads segments are decoding, pending or published at once, so with
//! the segments fetching workers still hold at most 2 * max_threads - 2
//! candidate vectors are alive, which the plan's reservation charges.
//! EXHAUSTED once every segment is published and every batch handed out;
//! WAITING when another worker is still decoding and the async protocol asks
//! this one to yield rather than block.
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
				auto &rowids = *front.rowids;
				local.candidates = front.rowids;
				local.segment_ordinal = front.segment_ordinal;
				local.candidate_offset = front.next_offset;
				local.candidate_end = MinValue<idx_t>(front.next_offset + FETCH_BATCH_ROWS, rowids.size());
				// A batch runs on through the candidates that share the last
				// one's vector-aligned rowid block, so the next batch starts at
				// a vector boundary and a range scan never reads a vector an
				// earlier batch already decompressed. Batch indexes stay unique
				// because consecutive batches start at least FETCH_BATCH_ROWS
				// apart.
				auto block = rowids[local.candidate_end - 1] & ~row_t(STANDARD_VECTOR_SIZE - 1);
				while (local.candidate_end < rowids.size() &&
				       (rowids[local.candidate_end] & ~row_t(STANDARD_VECTOR_SIZE - 1)) == block) {
					local.candidate_end++;
				}
				front.next_offset = local.candidate_end;
				if (front.next_offset >= front.rowids->size()) {
					queue.ready.pop_front();
				}
				return BatchClaim::CLAIMED;
			}
			// the claim and the in-flight count change together under the lock,
			// so a worker that sees no segment left and nothing in flight is done
			auto in_flight = queue.decoding + queue.pending.size();
			auto unclaimed = plan.next_segment.load() < plan.segments.size();
			if (queue.failed || (in_flight == 0 && !unclaimed)) {
				return BatchClaim::EXHAUSTED;
			}
			if (!unclaimed || in_flight >= plan.max_threads) {
				queue.published.wait_for(guard, std::chrono::milliseconds(1));
				if (queue.ready.empty() && data.results_execution_mode == AsyncResultsExecutionMode::TASK_EXECUTOR) {
					return BatchClaim::WAITING;
				}
				continue;
			}
			ordinal = plan.next_segment.fetch_add(1);
			queue.decoding++;
		}
		shared_ptr<vector<row_t>> rowids;
		try {
			vector<row_t> decoded;
			DecodeCandidateSegment(context, *global.tx, plan, ordinal, local.decode, decoded);
			rowids = TrackPublishedCandidates(plan, local.decode, std::move(decoded));
			auto &hooks = GetNgramTestHooks();
			if (hooks.before_segment_publish) {
				hooks.before_segment_publish(ordinal);
			}
		} catch (...) {
			std::lock_guard<mutex> guard(queue.lock);
			queue.decoding--;
			queue.failed = true;
			queue.published.notify_all();
			throw;
		}
		std::lock_guard<mutex> guard(queue.lock);
		queue.decoding--;
		queue.pending.emplace(ordinal, std::move(rowids));
		PublishInOrder(queue);
	}
}

//! A claimed batch whose rowids fill at least one part in RANGE_ROWS_PER_FETCH
//! of their span is read with one committed scan bounded to that span instead
//! of a fetch per row: the scan decompresses whole vectors and evaluates the
//! scan filters natively, while DataTable::Fetch locks the row-group tree for
//! every row and decodes compressed strings one at a time (measured on enwik9
//! at one thread, docs/review/2026-09-09: a scattered fetch 1.35 us, a span
//! row 0.29 us). The same ratio prices spans in the admission gate. The scan
//! starts at the vector holding the first candidate and stops after the last,
//! so it visits the batch's row groups only; the rowid filter excludes the
//! rows of that vector before the first candidate, which an earlier batch
//! owns. Every row the span holds lies below the high-water mark, and the
//! ones that are not candidates lack a gram of the needle, so the filters or
//! the recheck reject them. The scan carries the global scan filters, so its
//! chunks come out natively filtered, like the tail's.
static constexpr idx_t RANGE_SCAN_MIN_ROWS = 256;

idx_t InitializeBoundedScan(ClientContext &context, DataTable &storage, TableScanState &state,
                            const vector<StorageIndex> &column_ids, optional_ptr<TableFilterSet> filters,
                            idx_t start_row, idx_t end_row) {
	D_ASSERT(start_row < end_row && end_row <= storage.GetTotalRows());
	state.Initialize(column_ids, &context, filters);
	auto &collection = *storage.GetRowGroupCollection();
	auto row_groups = collection.GetRowGroups();
	auto &scan = state.table_state;
	auto row_group = row_groups->GetSegment(start_row);
	D_ASSERT(row_group);
	auto vector_index = (start_row - row_group->GetRowStart()) / STANDARD_VECTOR_SIZE;
	auto span_start = row_group->GetRowStart() + vector_index * STANDARD_VECTOR_SIZE;
	while (row_group && row_group->GetRowStart() < end_row) {
		if (RowGroupCollection::InitializeScanInRowGroup(context, scan, collection, *row_group, vector_index,
		                                                 end_row)) {
			return end_row - span_start;
		}
		row_group = row_groups->GetNextSegment(*row_group);
		vector_index = 0;
	}
	scan.row_group = nullptr;
	return 0;
}

static bool StartRangeScan(ClientContext &context, SearchCoreGlobal &global, SearchCoreLocal &local) {
	auto &rowids = *local.candidates;
	auto batch_rows = local.candidate_end - local.candidate_offset;
	auto first = rowids[local.candidate_offset];
	auto last = rowids[local.candidate_end - 1];
	if (batch_rows < RANGE_SCAN_MIN_ROWS || NumericCast<idx_t>(last - first) + 1 > RANGE_ROWS_PER_FETCH * batch_rows) {
		return false;
	}
	local.range_filters = make_uniq<TableFilterSet>();
	if (global.scan_filters) {
		for (auto &entry : global.scan_filters->filters) {
			local.range_filters->PushFilter(ColumnIndex(entry.first), entry.second->Copy());
		}
	}
	local.range_filters->PushFilter(
	    ColumnIndex(global.fetch_rowid_position),
	    make_uniq<ConstantFilter>(ExpressionType::COMPARE_GREATERTHANOREQUALTO, Value::BIGINT(first)));
	local.range_state = make_uniq<TableScanState>();
	auto span =
	    InitializeBoundedScan(context, *global.storage, *local.range_state, global.fetch_column_ids,
	                          local.range_filters.get(), NumericCast<idx_t>(first), NumericCast<idx_t>(last) + 1);
	global.range_rows.fetch_add(span, std::memory_order_relaxed);
	local.rows_scanned += span;
	local.candidate_offset = local.candidate_end;
	return true;
}

//! Claim the next tail unit for `local`: false when none is left.
static bool StartTailUnit(ClientContext &context, SearchCoreGlobal &global, SearchCoreLocal &local) {
	auto unit = global.next_tail_unit.fetch_add(1);
	if (unit > global.tail_units) {
		return false;
	}
	local.scan_state = make_uniq<TableScanState>();
	local.scan_local_storage = unit == global.tail_units;
	if (local.scan_local_storage) {
		// every local row lies past the committed tail, so the pushed filters
		// are the only ones; the unit visits every local row
		auto filters = global.scan_filters.get();
		auto &local_storage = LocalStorage::Get(*global.tx);
		local.scan_state->Initialize(global.fetch_column_ids, &context, filters);
		local_storage.InitializeScan(*global.storage, local.scan_state->local_state, filters);
		auto storage = local_storage.GetStorage(*global.storage);
		auto rows = storage && storage->row_groups && storage->row_groups->collection
		                ? storage->row_groups->collection->GetTotalRows()
		                : 0;
		global.local_rows.fetch_add(rows, std::memory_order_relaxed);
		local.rows_scanned += rows;
	} else {
		// the unit's rowid lower bound excludes the rows of its first vector
		// that the previous unit owns, or the indexed rows before the tail
		auto start = global.tail_start + unit * global.tail_unit_rows;
		auto end = MinValue<idx_t>(start + global.tail_unit_rows, global.tail_end);
		local.scan_filters = make_uniq<TableFilterSet>();
		if (global.scan_filters) {
			for (auto &entry : global.scan_filters->filters) {
				local.scan_filters->PushFilter(ColumnIndex(entry.first), entry.second->Copy());
			}
		}
		local.scan_filters->PushFilter(
		    ColumnIndex(global.fetch_rowid_position),
		    make_uniq<ConstantFilter>(ExpressionType::COMPARE_GREATERTHANOREQUALTO, Value::BIGINT(start)));
		auto span = InitializeBoundedScan(context, *global.storage, *local.scan_state, global.fetch_column_ids,
		                                  local.scan_filters.get(), start, end);
		global.tail_rows.fetch_add(span, std::memory_order_relaxed);
		local.rows_scanned += span;
	}
	// every chunk of the unit carries the unit's index, past every fetch batch
	local.batch_index = global.fetch_batch_base + unit;
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

//! Emit the `count` rows the recheck kept from a per-row fetch: probe
//! columns through the selection, extra columns as fetched for those rows.
static void SearchCoreEmitFetched(SearchCoreGlobal &global, SearchCoreLocal &local, idx_t count, DataChunk &output) {
	D_ASSERT(output.ColumnCount() == global.output_sources.size());
	D_ASSERT(global.extra_column_ids.empty() || local.extra_chunk.size() == count);
	for (idx_t column = 0; column < global.output_sources.size(); column++) {
		auto &source = global.output_sources[column];
		if (source.second == DConstants::INVALID_INDEX) {
			output.data[column].Reference(Value::BOOLEAN(true));
		} else if (source.first) {
			output.data[column].Slice(local.probe_chunk.data[source.second], local.sel, count);
		} else {
			output.data[column].Reference(local.extra_chunk.data[source.second]);
		}
	}
	output.SetCardinality(count);
}

void ExecuteSearchCore(ClientContext &context, TableFunctionInput &data, SearchCoreGlobal &global,
                       SearchCoreLocal &local,
                       const std::function<idx_t(DataChunk &, SelectionVector &, bool)> &recheck, DataChunk &output) {
	while (true) {
		switch (local.phase) {
		case SearchCorePhase::FETCH: {
			// The prior output is consumed before re-entry. Release its block pins
			// before decoding another segment or transitioning to the tail scan.
			local.fetch_chunk.Reset();
			local.probe_chunk.Reset();
			local.extra_chunk.Reset();
			local.fetch_state = ColumnFetchState();
			local.extra_state = ColumnFetchState();
			if (local.range_state) {
				local.range_state->table_state.Scan(*global.tx, local.fetch_chunk);
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
						// every decode is done: the tracked peak is final
						D_ASSERT(global.probe->tracker->peak.load() <= global.probe->workspace_bytes);
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
				global.storage->Fetch(*global.tx, local.probe_chunk, global.probe_column_ids, rowids, count,
				                      local.fetch_state);
				global.fetched_rows.fetch_add(count, std::memory_order_relaxed);
				local.rows_scanned += count;
				auto hits = local.probe_chunk.size() == 0 ? 0 : recheck(local.probe_chunk, local.sel, false);
				if (hits == 0) {
					if (SearchCoreYieldEmpty(data)) {
						return;
					}
					continue;
				}
				if (!global.extra_column_ids.empty()) {
					// Fetch returns the visible rows in the order asked, with
					// their rowids, so the kept rows are addressed exactly
					auto fetched_rowids =
					    FlatVector::GetData<row_t>(local.probe_chunk.data[global.probe_rowid_position]);
					auto hit_rowids = FlatVector::GetData<row_t>(local.hit_rowids);
					for (idx_t i = 0; i < hits; i++) {
						hit_rowids[i] = fetched_rowids[local.sel.get_index(i)];
					}
					global.storage->Fetch(*global.tx, local.extra_chunk, global.extra_column_ids, local.hit_rowids,
					                      hits, local.extra_state);
					if (local.extra_chunk.size() != hits) {
						throw InvalidInputException("ngram: a kept candidate row vanished between two fetches");
					}
				}
				SearchCoreEmitFetched(global, local, hits, output);
				return;
			}
			// a range scan's chunk, produced under the scan filters
			auto hits = local.fetch_chunk.size() == 0 ? 0 : recheck(local.fetch_chunk, local.sel, true);
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
			if (!local.scan_state) {
				if (!StartTailUnit(context, global, local)) {
					local.phase = SearchCorePhase::DONE;
					continue;
				}
			}
			local.scan_chunk.Reset();
			if (local.scan_local_storage) {
				LocalStorage::Get(*global.tx)
				    .Scan(local.scan_state->local_state, global.fetch_column_ids, local.scan_chunk);
			} else {
				local.scan_state->table_state.Scan(*global.tx, local.scan_chunk);
			}
			if (local.scan_chunk.size() == 0) {
				local.scan_state.reset();
				local.scan_filters.reset();
				if (SearchCoreYieldEmpty(data)) {
					return;
				}
				continue;
			}
			auto hits = recheck(local.scan_chunk, local.sel, true);
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
