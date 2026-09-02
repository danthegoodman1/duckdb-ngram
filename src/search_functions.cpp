#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "ngram/catalog.hpp"
#include "ngram/index_state.hpp"
#include "ngram/search_core.hpp"
#include "ngram/settings.hpp"
#include "ngram_extension.hpp"

#include <algorithm>
#include <cstring>

namespace duckdb {
namespace ngram {

//===----------------------------------------------------------------------===//
// The explicit query path.
//
// ngram_candidates(table, column, needle) emits the rowids of every indexed
// row that might contain the needle: the posting lists of its rarest grams (at
// most ngram_max_grams_per_query) intersected per segment, a superset of the
// matches at or below the high-water mark; callers recheck and cover the tail
// themselves. ngram_search(table, needle[, col := ...]) returns exactly what a
// brute-force scan would: candidates fetched through DataTable::Fetch and
// rechecked, then a tail scan past the mark that also covers transaction-local
// rows, both scheduled by the search core under a shared vacuum lock that an
// unfiltered full scan releases first.
//===----------------------------------------------------------------------===//

//===----------------------------------------------------------------------===//
// Bind
//===----------------------------------------------------------------------===//

struct QueryBindData : public TableFunctionData {
	//! Resolved base table (names, not pointers: execution re-resolves so a
	//! prepared statement outliving a DROP fails cleanly instead of dangling).
	string catalog_name;
	string schema_name;
	string table_name;
	IndexLocation location;
	//! The indexed column being searched.
	string column_name;
	string needle;
	//! ngram_search only: bind-time snapshot of the output schema.
	vector<string> names;
	vector<LogicalType> types;
	idx_t search_column_idx = 0;
	//! ngram_search semantics if a prepared query outlives its index.
	GramOptions bound_options;

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<QueryBindData>(*this);
	}
	bool Equals(const FunctionData &other) const override {
		return false;
	}
};

static string RequireStringArg(const Value &value, const char *fn, const char *arg) {
	if (value.IsNull()) {
		throw BinderException("%s: %s cannot be NULL", fn, arg);
	}
	return StringValue::Get(value);
}

//! Resolve the base table plus the index for `column` (or the only indexed
//! column when none is given), filling everything but the search-specific
//! members of the bind data.
static void BindQueryTarget(ClientContext &context, const char *fn, const string &table_input, string column,
                            QueryBindData &result) {
	auto target = ResolveTarget(context, table_input, string(), false);
	if (!target.entry->IsDuckTable()) {
		throw BinderException("%s: %s is not a DuckDB base table", fn, table_input);
	}
	if (column.empty()) {
		auto indexed = ExistingIndexes(context, target);
		if (indexed.empty()) {
			throw BinderException("%s: no ngram index exists on %s; build one with PRAGMA create_ngram_index", fn,
			                      table_input);
		}
		if (indexed.size() > 1) {
			vector<string> columns;
			for (auto &location : indexed) {
				columns.push_back(location.column_name);
			}
			throw BinderException("%s: %s has ngram indexes on multiple columns (%s); pass col := '...' to choose", fn,
			                      table_input, StringUtil::Join(columns, ", "));
		}
		result.location = indexed[0];
		column = indexed[0].column_name;
	} else {
		target.column_name = column;
		auto indexed = ExistingIndexes(context, target);
		if (indexed.empty()) {
			throw BinderException("%s: no ngram index exists on %s.%s; build one with PRAGMA create_ngram_index", fn,
			                      table_input, column);
		}
		if (indexed.size() != 1) {
			throw InvalidInputException("ngram: multiple allocations claim %s.%s", table_input, column);
		}
		result.location = indexed[0];
	}
	auto &table_entry = *target.entry;
	if (!table_entry.ColumnExists(column)) {
		throw BinderException("%s: the ngram index on %s references column %s, which no longer exists", fn, table_input,
		                      column);
	}
	auto &col = table_entry.GetColumn(column);
	if (col.Type().id() != LogicalTypeId::VARCHAR) {
		throw BinderException("%s: column %s of %s is %s, not VARCHAR", fn, column, table_input, col.Type().ToString());
	}
	result.catalog_name = target.catalog_name;
	result.schema_name = target.schema_name;
	result.table_name = target.table_name;
	// the catalog's spelling: `column` may carry the user's casing (from col :=),
	// and later name comparisons and owner keys must be casing-stable
	result.column_name = col.Name();
}

//! ngram_search(table, needle[, col := ...]) returns every row of `table`
//! whose indexed column contains `needle`, folded like the index build over a
//! case-insensitive index and compared byte-wise over a case-sensitive one.
//! Results are exhaustive in every state: rows past the high-water mark and
//! transaction-local rows come from the tail scan, deleted rows are hidden by
//! visibility and recheck, and when the rowid guard is missing, incompatible,
//! replaced, or cannot exclude reuse of a discarded trailing range the whole
//! table is scanned instead. Malformed present index objects still raise.
static unique_ptr<FunctionData> SearchBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<QueryBindData>();
	auto table_input = RequireStringArg(input.inputs[0], "ngram_search", "table");
	result->needle = RequireStringArg(input.inputs[1], "ngram_search", "needle");
	string column;
	// named parameter col (not "column": that is a reserved keyword and would
	// force users to quote it)
	auto column_entry = input.named_parameters.find("col");
	if (column_entry != input.named_parameters.end()) {
		column = RequireStringArg(column_entry->second, "ngram_search", "col");
		if (column.empty()) {
			throw BinderException("ngram_search: col cannot be empty");
		}
	}
	BindQueryTarget(context, "ngram_search", table_input, column, *result);

	// snapshot the output schema; execution re-validates against it
	auto target = ResolveTarget(context, table_input, string(), false);
	auto &table_entry = *target.entry;
	idx_t position = 0;
	bool found = false;
	for (auto &col : table_entry.GetColumns().Logical()) {
		if (col.Generated()) {
			throw BinderException("ngram_search: table %s has generated column %s; tables with generated columns "
			                      "are not supported yet",
			                      table_input, col.Name());
		}
		if (col.Name() == result->column_name) {
			result->search_column_idx = position;
			found = true;
		}
		result->names.push_back(col.Name());
		result->types.push_back(col.Type());
		position++;
	}
	if (!found) {
		throw InvalidInputException("ngram_search: indexed column vanished during binding");
	}
	auto located = LocateIndex(context, target, result->location);
	if (located.availability == IndexAvailability::CHANGED) {
		throw InvalidInputException(located.reason);
	}
	if (located.availability == IndexAvailability::ABSENT) {
		throw CatalogException("ngram_search: index storage is unavailable");
	}
	result->bound_options = located.meta.options;
	return_types = result->types;
	names = result->names;
	return std::move(result);
}

//! ngram_candidates(table, column, needle) emits candidate rowids for the
//! indexed rows (rowid <= the index high-water mark). Output is a superset of
//! the true matches among indexed rows; callers must recheck and must cover
//! rows past the high-water mark themselves. A needle with fewer than
//! gram_size codepoints cannot be probed: candidates degrade to every indexed
//! rowid ("all rowids" semantics), i.e. callers fall back to a full scan.
static unique_ptr<FunctionData> CandidatesBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<QueryBindData>();
	auto table_input = RequireStringArg(input.inputs[0], "ngram_candidates", "table");
	auto column = RequireStringArg(input.inputs[1], "ngram_candidates", "column");
	result->needle = RequireStringArg(input.inputs[2], "ngram_candidates", "needle");
	if (column.empty()) {
		throw BinderException("ngram_candidates: column cannot be empty");
	}
	BindQueryTarget(context, "ngram_candidates", table_input, column, *result);
	return_types = {LogicalType::BIGINT};
	names = {"rowid"};
	return std::move(result);
}

//===----------------------------------------------------------------------===//
// Recheck
//===----------------------------------------------------------------------===//

static bool BytesContain(const char *haystack, idx_t haystack_len, const string &needle) {
	if (needle.empty()) {
		return true;
	}
	if (needle.size() > haystack_len) {
		return false;
	}
	auto end = haystack + haystack_len;
	return std::search(haystack, end, needle.begin(), needle.end()) != end;
}

struct RecheckState {
	//! The needle in comparison form: normalized through the index's fold for
	//! case-insensitive indexes, raw bytes otherwise.
	string needle_cmp;
	GramOptions options;
	string scratch;
	vector<idx_t> scratch_offsets;

	//! Selects the rows of chunk.data[column_idx] that truly contain the
	//! needle into sel; returns the match count. NULLs never match.
	idx_t Recheck(DataChunk &chunk, idx_t column_idx, SelectionVector &sel) {
		UnifiedVectorFormat format;
		chunk.data[column_idx].ToUnifiedFormat(chunk.size(), format);
		auto strings = UnifiedVectorFormat::GetData<string_t>(format);
		idx_t hits = 0;
		for (idx_t r = 0; r < chunk.size(); r++) {
			auto idx = format.sel->get_index(r);
			if (!format.validity.RowIsValid(idx)) {
				continue;
			}
			auto &value = strings[idx];
			bool match;
			if (options.case_insensitive) {
				NormalizeString(value.GetData(), value.GetSize(), options, scratch, scratch_offsets);
				match = BytesContain(scratch.data(), scratch.size(), needle_cmp);
			} else {
				match = BytesContain(value.GetData(), value.GetSize(), needle_cmp);
			}
			if (match) {
				sel.set_index(hits++, r);
			}
		}
		return hits;
	}
};

//===----------------------------------------------------------------------===//
// ngram_search execution
//===----------------------------------------------------------------------===//

struct SearchGlobalState : public GlobalTableFunctionState {
	SearchCoreGlobal core;
	//! Held through indexed candidate fetch and the rowid-filtered tail so
	//! checkpoint vacuum cannot move rowids; released before unfiltered full
	//! scans. A shared lock has no thread affinity, so one key covers every
	//! thread.
	unique_ptr<StorageLockKey> vacuum_lock;
	//! The needle in comparison form plus the index's options; each thread
	//! copies these into its own RecheckState, which carries fold scratch.
	string needle_cmp;
	GramOptions options;
	idx_t search_column_idx = 0;
	string fallback_reason;
	vector<string> fetched_columns;

	idx_t MaxThreads() const override {
		return core.max_threads;
	}
};

//! Per-thread scan state. Nothing here may be shared: DataTable::Fetch writes
//! through its ColumnFetchState, a TableScanState owns per-thread filter
//! state, and the recheck fold reuses a scratch buffer.
struct SearchLocalState : public LocalTableFunctionState {
	SearchCoreLocal core;
	RecheckState recheck;
};

//! An upper bound on the row groups the tail scan can hand out, so the
//! executor spawns threads in proportion to the work. A negative high-water
//! mark means the tail scan is a full scan (short needle, or an index built on
//! an empty table). Always at least one, for the transaction-local rows.
static unique_ptr<GlobalTableFunctionState> SearchInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind = input.bind_data->Cast<QueryBindData>();
	auto state = make_uniq<SearchGlobalState>();

	auto &base = ResolveBoundBase(context, bind.catalog_name, bind.schema_name, bind.table_name, bind.names, bind.types,
	                              "ngram_search: table %s changed since the query was bound; re-prepare it");
	auto &storage = base.GetStorage();
	state->core.storage = &storage;
	state->core.tx = &DuckTransaction::Get(context, base.ParentCatalog());
	state->vacuum_lock = DuckTransactionManager::Get(storage.GetAttached()).SharedVacuumLock();

	state->options = bind.bound_options;
	ResolvedTarget target {bind.catalog_name, bind.schema_name, bind.table_name, bind.column_name, &base};
	auto verdict = ValidateIndex(context, target, bind.location);
	if (verdict.availability != IndexAvailability::AVAILABLE) {
		state->fallback_reason = "index unavailable";
	} else {
		state->options = verdict.meta.options;
		if (!verdict.reason.empty()) {
			state->fallback_reason = "rowid guard: " + verdict.reason;
		} else {
			state->core.hwm = verdict.meta.hwm_rowid;
		}
	}
	vector<idx_t> requested_to_fetch;
	requested_to_fetch.reserve(input.column_indexes.size());
	optional_idx search_column_idx;
	auto &columns = base.GetColumns();
	for (auto &col_idx : input.column_indexes) {
		if (col_idx.IsEmptyColumn()) {
			requested_to_fetch.push_back(DConstants::INVALID_INDEX);
			continue;
		}
		if (col_idx.IsRowIdColumn()) {
			requested_to_fetch.push_back(state->core.fetch_types.size());
			state->core.fetch_column_ids.emplace_back(StorageIndex(COLUMN_IDENTIFIER_ROW_ID));
			state->core.fetch_types.emplace_back(LogicalType::ROW_TYPE);
			state->fetched_columns.push_back("rowid");
			continue;
		}
		if (col_idx.IsVirtualColumn()) {
			throw InvalidInputException("ngram_search: unsupported virtual column");
		}
		requested_to_fetch.push_back(state->core.fetch_types.size());
		state->core.fetch_column_ids.push_back(base.GetStorageIndex(col_idx));
		state->core.fetch_types.push_back(col_idx.HasType() ? col_idx.GetScanType()
		                                                    : columns.GetColumn(col_idx.ToLogical()).Type());
		state->fetched_columns.push_back(columns.GetColumn(col_idx.ToLogical()).Name() +
		                                 (col_idx.IsPushdownExtract() ? " (extract)" : ""));
		if (col_idx.GetPrimaryIndex() == bind.search_column_idx && !col_idx.HasChildren()) {
			search_column_idx = state->core.fetch_types.size() - 1;
		}
	}
	if (!search_column_idx.IsValid()) {
		search_column_idx = state->core.fetch_types.size();
		auto search_index = ColumnIndex(bind.search_column_idx);
		state->core.fetch_column_ids.push_back(base.GetStorageIndex(search_index));
		state->core.fetch_types.push_back(columns.GetColumn(search_index.ToLogical()).Type());
		state->fetched_columns.push_back(bind.column_name);
	}
	state->search_column_idx = search_column_idx.GetIndex();
	if (input.CanRemoveFilterColumns()) {
		for (auto projection_id : input.projection_ids) {
			state->core.output_ids.push_back(requested_to_fetch[projection_id]);
		}
	} else {
		state->core.output_ids = std::move(requested_to_fetch);
	}

	if (state->options.case_insensitive) {
		vector<idx_t> offsets;
		NormalizeString(bind.needle.data(), bind.needle.size(), state->options, state->needle_cmp, offsets);
	} else {
		state->needle_cmp = bind.needle;
	}

	auto decomposition = DecomposeNeedle(bind.needle.data(), bind.needle.size(), state->options);
	if (!state->fallback_reason.empty()) {
		state->core.hwm = -1;
	} else if (decomposition.too_short) {
		// the index cannot be probed; the tail scan becomes a full scan, which
		// is still exhaustive
		state->fallback_reason = "needle shorter than gram size";
		state->core.hwm = -1;
	} else {
		auto segments = TryResolveExistingTable(context, bind.catalog_name, NGRAM_SCHEMA, bind.location.SegmentsTable(),
		                                        "ngram index segments table");
		auto stats = TryResolveExistingTable(context, bind.catalog_name, NGRAM_SCHEMA, bind.location.StatsTable(),
		                                     "ngram index stats table");
		if (!segments || !stats) {
			state->fallback_reason = "index unavailable";
			state->core.hwm = -1;
		} else {
			state->core.probe = PlanIndexProbe(context, *state->core.tx, *segments, *stats, decomposition.grams,
			                                   MaxGramsPerQuery(context), state->core.hwm, storage.GetTotalRows(),
			                                   MaxCandidateFraction(context), DConstants::INVALID_INDEX);
		}
		if (state->core.probe && !state->core.probe->admitted) {
			state->fallback_reason = state->core.probe->decline_reason;
			state->core.probe.reset();
			state->core.hwm = -1;
		}
	}
	if (state->core.hwm < 0) {
		// A fallback scan does not retain candidate rowids, so vacuum no longer
		// has to wait for this query.
		state->vacuum_lock.reset();
	}
	FinalizeSearchCore(context, state->core);
	return std::move(state);
}

static unique_ptr<LocalTableFunctionState> SearchInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                           GlobalTableFunctionState *global_state) {
	auto &gstate = global_state->Cast<SearchGlobalState>();
	auto state = make_uniq<SearchLocalState>();
	state->recheck.options = gstate.options;
	state->recheck.needle_cmp = gstate.needle_cmp;
	InitializeSearchCoreLocal(context, gstate.core, state->core);
	return std::move(state);
}

static void SearchFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<SearchGlobalState>();
	auto &lstate = data.local_state->Cast<SearchLocalState>();
	ExecuteSearchCore(
	    context, data, state.core, lstate.core,
	    [&](DataChunk &chunk, SelectionVector &sel) {
		    return lstate.recheck.Recheck(chunk, state.search_column_idx, sel);
	    },
	    output);
}

//! Ordered sinks reassemble a parallel scan's output by batch index. Fetch
//! blocks carry their block number and tail batches follow them, so the
//! reassembled order is the one the single-threaded scan produced: candidate
//! rowids ascending, then the tail in storage order.
static OperatorPartitionData SearchGetPartitionData(ClientContext &context, TableFunctionGetPartitionInput &input) {
	auto &lstate = input.local_state->Cast<SearchLocalState>();
	return OperatorPartitionData(lstate.core.batch_index);
}

static InsertionOrderPreservingMap<string> SearchToString(TableFunctionToStringInput &input) {
	InsertionOrderPreservingMap<string> result;
	auto &bind = input.bind_data->Cast<QueryBindData>();
	result["Table"] = bind.table_name;
	result["Ngram Column"] = bind.column_name;
	result["Ngram Needle"] = bind.needle;
	return result;
}

static InsertionOrderPreservingMap<string> SearchDynamicToString(TableFunctionDynamicToStringInput &input) {
	InsertionOrderPreservingMap<string> result;
	if (!input.global_state) {
		return result;
	}
	auto &state = input.global_state->Cast<SearchGlobalState>();
	result["Ngram Storage Columns"] = StringUtil::Join(state.fetched_columns, ", ");
	if (state.core.probe) {
		result["Ngram Mode"] =
		    StringUtil::Format("index (<= %llu candidates, %llu decoded rowids)",
		                       state.core.probe->candidate_upper_bound, state.core.probe->decoded_rowids.load());
		result["Ngram Stats Rows Scanned"] = to_string(state.core.probe->stats_rows_scanned);
		result["Ngram Stats Chunks Scanned"] = to_string(state.core.probe->stats_chunks_scanned);
	} else {
		result["Ngram Mode"] = "full scan fallback: " + state.fallback_reason;
	}
	return result;
}

static bool SearchSupportsPushdownExtract(const FunctionData &bind_data, const LogicalIndex &column_idx) {
	auto &bind = bind_data.Cast<QueryBindData>();
	if (column_idx.index >= bind.types.size()) {
		return false;
	}
	auto type = bind.types[column_idx.index].id();
	return type == LogicalTypeId::STRUCT || type == LogicalTypeId::VARIANT;
}

//===----------------------------------------------------------------------===//
// ngram_candidates execution
//===----------------------------------------------------------------------===//

struct CandidatesGlobalState : public GlobalTableFunctionState {
	DataTable *storage = nullptr;
	DuckTransaction *tx = nullptr;
	unique_ptr<StorageLockKey> vacuum_lock;
	int64_t hwm = -1;

	//! Probed mode: decode and emit one admitted rowid segment at a time.
	unique_ptr<ProbePlan> probe;
	vector<row_t> candidates;
	idx_t offset = 0;
	idx_t segment_ordinal = 0;

	//! Short-needle mode: emit every indexed rowid (rowid <= hwm) instead.
	bool all_rowids = false;
	bool scan_initialized = false;
	unique_ptr<TableFilterSet> scan_filters;
	TableScanState scan_state;
	DataChunk scan_chunk;
};

static unique_ptr<GlobalTableFunctionState> CandidatesInitGlobal(ClientContext &context,
                                                                 TableFunctionInitInput &input) {
	auto &bind = input.bind_data->Cast<QueryBindData>();
	auto state = make_uniq<CandidatesGlobalState>();

	auto &base = ResolveExistingTable(context, bind.catalog_name, bind.schema_name, bind.table_name, "table");
	auto &storage = base.GetStorage();
	state->storage = &storage;
	state->tx = &DuckTransaction::Get(context, base.ParentCatalog());
	state->vacuum_lock = DuckTransactionManager::Get(storage.GetAttached()).SharedVacuumLock();

	ResolvedTarget target {bind.catalog_name, bind.schema_name, bind.table_name, bind.column_name, &base};
	auto verdict = ValidateIndex(context, target, bind.location);
	if (verdict.availability == IndexAvailability::CHANGED) {
		throw InvalidInputException(verdict.reason);
	}
	if (verdict.availability == IndexAvailability::ABSENT) {
		throw InvalidInputException("ngram_candidates: index storage was removed after binding");
	}
	state->hwm = verdict.meta.hwm_rowid;

	auto decomposition = DecomposeNeedle(bind.needle.data(), bind.needle.size(), verdict.meta.options);
	if (!verdict.reason.empty()) {
		state->all_rowids = true;
	} else if (decomposition.too_short) {
		state->all_rowids = true;
	} else {
		auto &segments = ResolveExistingTable(context, bind.catalog_name, NGRAM_SCHEMA, bind.location.SegmentsTable(),
		                                      "ngram index segments table");
		auto &stats = ResolveExistingTable(context, bind.catalog_name, NGRAM_SCHEMA, bind.location.StatsTable(),
		                                   "ngram index stats table");
		state->probe = PlanIndexProbe(context, *state->tx, segments, stats, decomposition.grams,
		                              MaxGramsPerQuery(context), state->hwm, storage.GetTotalRows(), -1, 1);
		if (!state->probe->admitted) {
			throw InvalidInputException("ngram_candidates: %s", state->probe->decline_reason);
		}
	}
	return std::move(state);
}

static void CandidatesFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<CandidatesGlobalState>();
	if (state.all_rowids) {
		if (state.hwm < 0) {
			// the index was built on an empty table: no indexed rows
			return;
		}
		if (!state.scan_initialized) {
			vector<StorageIndex> column_ids;
			column_ids.push_back(StorageIndex(COLUMN_IDENTIFIER_ROW_ID));
			state.scan_filters = make_uniq<TableFilterSet>();
			state.scan_filters->PushFilter(
			    ColumnIndex(0),
			    make_uniq<ConstantFilter>(ExpressionType::COMPARE_LESSTHANOREQUALTO, Value::BIGINT(state.hwm)));
			InitializeExhaustiveScan(context, *state.tx, *state.storage, state.scan_state, column_ids,
			                         state.scan_filters.get());
			state.scan_chunk.Initialize(Allocator::Get(context), {LogicalType::ROW_TYPE});
			state.scan_initialized = true;
		}
		state.scan_chunk.Reset();
		state.storage->Scan(*state.tx, state.scan_chunk, state.scan_state);
		if (state.scan_chunk.size() == 0) {
			return;
		}
		output.Reference(state.scan_chunk);
		return;
	}
	while (state.offset >= state.candidates.size()) {
		state.candidates.clear();
		state.offset = 0;
		if (!NextCandidateSegment(context, *state.tx, *state.probe, state.candidates, state.segment_ordinal)) {
			return;
		}
	}
	auto remaining = state.candidates.size() - state.offset;
	idx_t count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, remaining);
	auto result = FlatVector::GetData<int64_t>(output.data[0]);
	memcpy(result, state.candidates.data() + state.offset, count * sizeof(int64_t));
	state.offset += count;
	output.SetCardinality(count);
}

static InsertionOrderPreservingMap<string> CandidatesDynamicToString(TableFunctionDynamicToStringInput &input) {
	InsertionOrderPreservingMap<string> result;
	if (!input.global_state) {
		return result;
	}
	auto &state = input.global_state->Cast<CandidatesGlobalState>();
	if (state.probe) {
		result["Ngram Probe Workers"] = to_string(state.probe->max_threads);
		result["Ngram Stats Rows Scanned"] = to_string(state.probe->stats_rows_scanned);
		result["Ngram Stats Chunks Scanned"] = to_string(state.probe->stats_chunks_scanned);
		result["Ngram Decoded Rowids"] = to_string(state.probe->decoded_rowids.load());
	}
	return result;
}

//===----------------------------------------------------------------------===//
// Registration
//===----------------------------------------------------------------------===//

void RegisterSearchFunctions(ExtensionLoader &loader) {
	TableFunction candidates("ngram_candidates", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                         CandidatesFunction, CandidatesBind, CandidatesInitGlobal);
	candidates.dynamic_to_string = CandidatesDynamicToString;
	loader.RegisterFunction(candidates);

	TableFunction search("ngram_search", {LogicalType::VARCHAR, LogicalType::VARCHAR}, SearchFunction, SearchBind,
	                     SearchInitGlobal, SearchInitLocal);
	search.get_partition_data = SearchGetPartitionData;
	search.projection_pushdown = true;
	search.supports_pushdown_extract = SearchSupportsPushdownExtract;
	search.to_string = SearchToString;
	search.dynamic_to_string = SearchDynamicToString;
	search.get_virtual_columns = [](ClientContext &, optional_ptr<FunctionData>) {
		virtual_column_map_t result;
		result.emplace(COLUMN_IDENTIFIER_EMPTY, TableColumn("", LogicalType::BOOLEAN));
		return result;
	};
	search.named_parameters["col"] = LogicalType::VARCHAR;
	loader.RegisterFunction(search);
}

} // namespace ngram
} // namespace duckdb
