#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/insertion_order_preserving_map.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "ngram/index_pragmas.hpp"
#include "ngram/maintenance.hpp"
#include "ngram/ngram_rewrite.hpp"
#include "ngram/rowid_guard.hpp"
#include "ngram/search_core.hpp"
#include "ngram/trigram.hpp"

namespace duckdb {
namespace ngram {

//===----------------------------------------------------------------------===//
// The transparent query path.
//
// A post-optimize OptimizerExtension walks the plan for LogicalGet(seq_scan)
// nodes whose pushed-down table_filters contain contains(col, 'needle'),
// LIKE ('~~'), or ILIKE ('~~*') over a column with an ngram index, and swaps
// get.function / get.bind_data for the NGRAM_INDEX_SCAN table function.
// Everything else on the node (table_index, returned types/names, column ids,
// projection ids, the filters themselves) is left untouched, so column
// bindings and EXPLAIN filter rendering are unchanged.
//
// NGRAM_INDEX_SCAN reuses the Phase 3 pipeline: probe the index for candidate
// rowids, fetch them through DataTable::Fetch, recheck every fetched row
// against ALL pushed filters (evaluated exactly, via
// TableFilter::ToExpression — the original query predicate, so query
// semantics never depend on index normalization), then run a storage tail
// scan (rowid > high-water mark, covering unindexed and transaction-local
// rows) with the same filters applied natively by the storage scan. Indexed
// execution holds a shared vacuum lock through candidate fetch and the
// rowid-filtered tail; an unfiltered full scan releases it first.
//
// Both phases use the shared search core. Probe workers claim one bounded
// rowid segment at a time, fetch/recheck it, then claim another; storage scan
// workers use DuckDB's row-group cursor for the tail. Nothing about
// exhaustiveness changes, and ordered sinks use the reported batch indexes to
// restore deterministic segment-then-tail order.
//
// Fallbacks never re-plan: when the index cannot be used at execution time
// (dropped index, changed options, selectivity gate) the same table function
// degrades to a full storage scan with the filters applied natively — a
// parallel scan over every row group, i.e. what the seq scan it replaced
// would have cost. The rewrite itself is skipped (leaving the native seq
// scan) for every non-qualifying shape; see TryRewriteGet.
//
// Case semantics (superset invariant): contains/LIKE are case-sensitive and
// may probe both case-sensitive and case-insensitive indexes (folding merges
// classes, so a CI index's candidates are a superset; recheck applies the
// case-sensitive predicate). ILIKE is case-insensitive and may only probe
// case-insensitive indexes; probing a CS index with an ILIKE needle would
// miss case-variant matches and is never done.
//
// Staleness: the same rowid-guard verdict as the explicit path makes this
// path decline instead. At plan time it leaves the sequential scan intact; at
// execution time the replacement performs one full scan. Updates, vacuum,
// reopen, and uncertain guard state therefore cannot make a rewritten
// predicate omit rows.
//===----------------------------------------------------------------------===//

//! The density gate runs after metadata planning but before postings decode;
//! its job is choosing between fetching candidates and scanning the table.
//! Measured fetch cost is ~250-300 ns per candidate at
//! every scale, and a parallel scan of the whole table costs ~0.04 s at 1 GB,
//! ~0.35 s at 10 GB and ~3.5 s at 100 GB, which puts the break-even at 1.6%,
//! 1.3% and 1.1% of rows respectively. One percent is that crossover, rounded
//! toward scanning. The previous 0.05 was set before fetch was parallel and is
//! four to five times too permissive: at 100 GB it let a 6.3%-selectivity
//! needle spend 34.4 s on the index path where the fallback needs 20.1 s and a
//! plain scan 4.6 s (benchmarks/RESULTS.md).
static bool AutoAccelerateEnabled(ClientContext &context) {
	Value value;
	if (context.TryGetCurrentSetting("ngram_auto_accelerate", value) && !value.IsNull()) {
		return value.GetValue<bool>();
	}
	return false;
}

//===----------------------------------------------------------------------===//
// Bind data
//===----------------------------------------------------------------------===//

//! One literal the scan probes the index for. contains/LIKE literals may be
//! probed against either index flavor; ILIKE literals only against a
//! case-insensitive index (requires_ci).
struct RewriteNeedle {
	RewriteNeedle() = default;
	RewriteNeedle(string text_p, bool requires_ci_p) : text(std::move(text_p)), requires_ci(requires_ci_p) {
	}

	string text;
	bool requires_ci = false;
};

struct NgramScanBindData : public TableFunctionData {
	//! Resolved base table (names, not pointers: execution re-resolves so a
	//! plan outliving catalog changes fails cleanly instead of dangling).
	string catalog_name;
	string schema_name;
	string table_name;
	IndexLocation location;
	//! The indexed column the needles probe.
	string column_name;
	//! Literals to probe; every pushed filter is still applied in full to each
	//! row, so needles only ever narrow the candidate set.
	vector<RewriteNeedle> needles;
	//! Rewrite-time snapshot of the table's logical schema; execution
	//! re-validates against it.
	vector<string> base_names;
	vector<LogicalType> base_types;
	//! For the dependency callback only; execution goes through the names.
	optional_ptr<TableCatalogEntry> table;

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<NgramScanBindData>(*this);
	}
	bool Equals(const FunctionData &other) const override {
		return false;
	}
};

//===----------------------------------------------------------------------===//
// Needle extraction from pushed-down filters
//===----------------------------------------------------------------------===//

//! Split a LIKE/ILIKE pattern into its literal segments. Conservative v1:
//! patterns containing the single-character wildcard '_' are not decomposed
//! (returns false). Escaped patterns never reach here — LIKE ... ESCAPE binds
//! to the separate like_escape functions, which are not matched.
static bool CollectLikeSegments(const string &pattern, bool requires_ci, vector<RewriteNeedle> &needles) {
	if (pattern.find('_') != string::npos) {
		return false;
	}
	string segment;
	for (auto c : pattern) {
		if (c == '%') {
			if (!segment.empty()) {
				needles.push_back(RewriteNeedle {segment, requires_ci});
				segment.clear();
			}
		} else {
			segment += c;
		}
	}
	if (!segment.empty()) {
		needles.push_back(RewriteNeedle {segment, requires_ci});
	}
	return true;
}

//! Harvest needles from one pushed filter expression. Pushed expressions have
//! their column refs rebound to BoundReferenceExpression(..., 0) over a
//! one-column chunk, so a qualifying shape is exactly
//! fn(BOUND_REF, VARCHAR constant) for fn in {contains, ~~, ~~*}.
static void CollectExprNeedles(const Expression &expr, vector<RewriteNeedle> &needles) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return;
	}
	auto &func = expr.Cast<BoundFunctionExpression>();
	if (func.children.size() != 2) {
		return;
	}
	auto &name = func.function.name;
	bool is_contains = name == "contains";
	bool is_like = name == "~~";
	bool is_ilike = name == "~~*";
	if (!is_contains && !is_like && !is_ilike) {
		return;
	}
	// column on the left, literal on the right: contains('lit', col) probes
	// nothing
	if (func.children[0]->GetExpressionType() != ExpressionType::BOUND_REF ||
	    func.children[0]->return_type.id() != LogicalTypeId::VARCHAR) {
		return;
	}
	if (func.children[1]->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
		return;
	}
	auto &value = func.children[1]->Cast<BoundConstantExpression>().value;
	if (value.IsNull() || value.type().id() != LogicalTypeId::VARCHAR) {
		return;
	}
	auto text = StringValue::Get(value);
	if (is_contains) {
		needles.push_back(RewriteNeedle {text, false});
		return;
	}
	CollectLikeSegments(text, is_ilike, needles);
}

//! Harvest needles from a table filter tree. Only AND-connected filters can
//! contribute (every conjunct must hold, so each needle independently narrows
//! the candidate set); anything under an OR is ignored.
static void CollectFilterNeedles(const TableFilter &filter, vector<RewriteNeedle> &needles) {
	switch (filter.filter_type) {
	case TableFilterType::CONJUNCTION_AND:
		for (auto &child : filter.Cast<ConjunctionAndFilter>().child_filters) {
			CollectFilterNeedles(*child, needles);
		}
		break;
	case TableFilterType::EXPRESSION_FILTER:
		CollectExprNeedles(*filter.Cast<ExpressionFilter>().expr, needles);
		break;
	default:
		break;
	}
}

//! The needles that may probe this index: ILIKE needles require a
//! case-insensitive index, and a needle must decompose into at least one gram
//! under the index's options to contribute to the probe.
static vector<RewriteNeedle> UsableNeedles(const vector<RewriteNeedle> &needles, const GramOptions &options) {
	vector<RewriteNeedle> usable;
	for (auto &needle : needles) {
		if (needle.requires_ci && !options.case_insensitive) {
			continue;
		}
		auto decomposition = DecomposeNeedle(needle.text.data(), needle.text.size(), options);
		if (!decomposition.too_short && !decomposition.grams.empty()) {
			usable.push_back(needle);
		}
	}
	return usable;
}

//===----------------------------------------------------------------------===//
// Execution: global state
//===----------------------------------------------------------------------===//

enum class NgramScanMode : uint8_t {
	//! Candidate fetch + recheck, then tail scan past the high-water mark.
	INDEX,
	//! Full storage scan with the pushed filters applied natively — the
	//! in-function fallback (missing/unusable index, selectivity gate). Still
	//! exhaustive, no re-planning.
	FULL_SCAN
};

struct NgramScanGlobalState final : public GlobalTableFunctionState {
	SearchCoreGlobal core;
	//! Held through indexed candidate fetch and the rowid-filtered tail so
	//! checkpoint vacuum cannot move rowids; released before unfiltered full
	//! scans. A shared lock has no thread affinity, so one key covers every
	//! scanning thread (the pattern v1.5.5's own index scan uses,
	//! table_scan.cpp:127).
	unique_ptr<StorageLockKey> vacuum_lock;

	NgramScanMode mode = NgramScanMode::FULL_SCAN;
	//! Why mode is FULL_SCAN; rendered by dynamic_to_string (EXPLAIN ANALYZE).
	string fallback_reason;
	idx_t candidate_count = 0;
	vector<string> fetched_columns;

	//! The conjunction of every non-optional pushed filter, evaluated on
	//! fetched candidate chunks (DataTable::Fetch applies no filters). Shared
	//! read-only; each thread builds its own executor over it, because an
	//! ExpressionExecutor carries per-evaluation state.
	unique_ptr<Expression> recheck_expr;

	idx_t MaxThreads() const override {
		return core.max_threads;
	}
};

//! Per-thread scan state: output buffers, fetch/scan cursors and the recheck
//! executor. Nothing here may be shared — DataTable::Fetch writes through its
//! ColumnFetchState, and a TableScanState owns per-thread filter state.
struct NgramScanLocalState final : public LocalTableFunctionState {
	SearchCoreLocal core;
	unique_ptr<ExpressionExecutor> recheck_executor;
};

//===----------------------------------------------------------------------===//
// Execution: init
//===----------------------------------------------------------------------===//

static DuckTableEntry &ResolveRewriteBase(ClientContext &context, const NgramScanBindData &bind) {
	auto &base = ResolveExistingTable(context, bind.catalog_name, bind.schema_name, bind.table_name, "table");
	// a cached plan can outlive DROP+CREATE; re-validate the schema the query
	// was planned against
	idx_t position = 0;
	for (auto &col : base.GetColumns().Logical()) {
		if (col.Generated() || position >= bind.base_types.size() || col.Name() != bind.base_names[position] ||
		    col.Type() != bind.base_types[position]) {
			throw InvalidInputException("ngram accelerated scan: table %s changed since the query was planned; "
			                            "re-prepare the statement",
			                            bind.table_name);
		}
		position++;
	}
	if (position != bind.base_types.size()) {
		throw InvalidInputException(
		    "ngram accelerated scan: table %s changed since the query was planned; re-prepare the statement",
		    bind.table_name);
	}
	return base;
}

//! The conjunction of every non-optional pushed filter over the scanned
//! chunk. Optional filters (zone-map hints, dynamic TopN/join filters) are
//! skipped: their contract says executing them is not required for
//! correctness, and their state can change between init and evaluation.
static unique_ptr<Expression> BuildRecheckExpression(optional_ptr<TableFilterSet> filters,
                                                     const vector<LogicalType> &scanned_types) {
	unique_ptr<Expression> result;
	if (!filters) {
		return result;
	}
	for (auto &entry : filters->filters) {
		if (entry.second->filter_type == TableFilterType::OPTIONAL_FILTER) {
			continue;
		}
		if (entry.first >= scanned_types.size()) {
			throw InvalidInputException("ngram accelerated scan: table filter references column %llu of %llu scanned",
			                            entry.first, scanned_types.size());
		}
		BoundReferenceExpression column(scanned_types[entry.first], entry.first);
		auto expr = entry.second->ToExpression(column);
		if (result) {
			result = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(result),
			                                               std::move(expr));
		} else {
			result = std::move(expr);
		}
	}
	return result;
}

//! Probe the index for the bind data's needles. Returns false (with a reason)
//! when the index cannot be used, in which case the caller falls back to a
//! full scan — the index is an optimization, never a correctness dependency.
static bool TryProbeIndex(ClientContext &context, const NgramScanBindData &bind, NgramScanGlobalState &state) {
	auto &base = ResolveExistingTable(context, bind.catalog_name, bind.schema_name, bind.table_name, "table");
	ResolvedTarget target {bind.catalog_name, bind.schema_name, bind.table_name, bind.column_name, &base};
	if (!IndexLocationAvailable(context, target, bind.location, true)) {
		state.fallback_reason = "index unavailable";
		return false;
	}
	auto info = ReadMeta(context, bind.catalog_name, bind.location);
	auto guard_reason = RowIdGuardReason(context, base, info);
	if (!guard_reason.empty()) {
		state.fallback_reason = "rowid guard: " + guard_reason;
		return false;
	}
	auto usable = UsableNeedles(bind.needles, info.options);
	if (usable.empty()) {
		// only possible when the index was rebuilt with different options
		// after planning; the rewrite never fires without a usable needle
		state.fallback_reason = "no probeable needle";
		return false;
	}
	// a matching row must contain every needle, hence every gram of every
	// needle: one intersection over the union of gram sets is exactly the
	// per-needle candidate-set intersection
	vector<string> grams;
	unordered_set<string> seen;
	for (auto &needle : usable) {
		auto decomposition = DecomposeNeedle(needle.text.data(), needle.text.size(), info.options);
		for (auto &gram : decomposition.grams) {
			if (seen.insert(gram).second) {
				grams.push_back(gram);
			}
		}
	}
	auto segments = TryResolveExistingTable(context, bind.catalog_name, NGRAM_SCHEMA, bind.location.SegmentsTable(),
	                                        "ngram index segments table");
	auto stats = TryResolveExistingTable(context, bind.catalog_name, NGRAM_SCHEMA, bind.location.StatsTable(),
	                                     "ngram index stats table");
	if (!segments || !stats) {
		state.fallback_reason = "index unavailable";
		return false;
	}
	auto probe =
	    PlanIndexProbe(context, *state.core.tx, *segments, *stats, grams, MaxGramsPerQuery(context), info.hwm_rowid,
	                   state.core.storage->GetTotalRows(), MaxCandidateFraction(context), DConstants::INVALID_INDEX);
	state.candidate_count = probe->candidate_upper_bound;
	if (!probe->admitted) {
		state.fallback_reason = probe->decline_reason;
		return false;
	}
	state.core.hwm = info.hwm_rowid;
	state.core.probe = std::move(probe);
	return true;
}

//! An upper bound on the row groups the storage scan can hand out, so the
//! executor spawns threads in proportion to the work: the whole table in
//! FULL_SCAN mode, the rows past the high-water mark in INDEX mode. Always at
//! least one, for the transaction-local rows the tail scan must still visit.
static unique_ptr<GlobalTableFunctionState> NgramScanInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind = input.bind_data->Cast<NgramScanBindData>();
	auto state = make_uniq<NgramScanGlobalState>();

	auto &base = ResolveRewriteBase(context, bind);
	auto &storage = base.GetStorage();
	state->core.storage = &storage;
	state->core.tx = &DuckTransaction::Get(context, base.ParentCatalog());
	state->vacuum_lock = DuckTransactionManager::Get(storage.GetAttached()).SharedVacuumLock();

	// scanned column layout, mirroring DuckTableScanInitGlobal
	auto &columns = base.GetColumns();
	for (auto &col_idx : input.column_indexes) {
		if (col_idx.IsRowIdColumn()) {
			state->core.fetch_types.emplace_back(LogicalType::ROW_TYPE);
			state->fetched_columns.push_back("rowid");
		} else if (col_idx.IsVirtualColumn()) {
			throw InvalidInputException("ngram accelerated scan: unsupported column reference in scan");
		} else if (col_idx.HasType()) {
			state->core.fetch_types.push_back(col_idx.GetScanType());
			state->fetched_columns.push_back(columns.GetColumn(col_idx.ToLogical()).Name() + " (extract)");
		} else {
			state->core.fetch_types.push_back(columns.GetColumn(col_idx.ToLogical()).Type());
			state->fetched_columns.push_back(columns.GetColumn(col_idx.ToLogical()).Name());
		}
		state->core.fetch_column_ids.push_back(base.GetStorageIndex(col_idx));
	}
	if (input.CanRemoveFilterColumns()) {
		state->core.output_ids = input.projection_ids;
	} else {
		for (idx_t i = 0; i < input.column_indexes.size(); i++) {
			state->core.output_ids.push_back(i);
		}
	}

	state->recheck_expr = BuildRecheckExpression(input.filters, state->core.fetch_types);

	if (TryProbeIndex(context, bind, *state)) {
		state->mode = NgramScanMode::INDEX;
	} else {
		state->mode = NgramScanMode::FULL_SCAN;
	}
	if (state->core.hwm < 0) {
		state->vacuum_lock.reset();
	}

	// the storage scan: tail (rowid > hwm) in INDEX mode, whole table in
	// FULL_SCAN mode; the pushed filters are applied natively either way
	state->core.scan_filters = make_uniq<TableFilterSet>();
	if (input.filters) {
		for (auto &entry : input.filters->filters) {
			state->core.scan_filters->PushFilter(ColumnIndex(entry.first), entry.second->Copy());
		}
	}
	FinalizeSearchCore(context, state->core);

	return std::move(state);
}

static unique_ptr<LocalTableFunctionState> NgramScanInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                              GlobalTableFunctionState *global_state) {
	auto &gstate = global_state->Cast<NgramScanGlobalState>();
	auto state = make_uniq<NgramScanLocalState>();
	if (gstate.recheck_expr) {
		state->recheck_executor = make_uniq<ExpressionExecutor>(context.client, *gstate.recheck_expr);
	}
	InitializeSearchCoreLocal(context, gstate.core, state->core);
	return std::move(state);
}

//===----------------------------------------------------------------------===//
// Execution: scan
//===----------------------------------------------------------------------===//

//! Rows of `chunk` passing every non-optional pushed filter, selected into
//! the thread's selection vector. Storage-scan chunks are already filtered
//! natively; re-running the executor there is an idempotent belt-and-braces
//! pass over survivors.
static idx_t RecheckChunk(NgramScanLocalState &lstate, DataChunk &chunk, SelectionVector &sel) {
	if (!lstate.recheck_executor) {
		for (idx_t r = 0; r < chunk.size(); r++) {
			sel.set_index(r, r);
		}
		return chunk.size();
	}
	return lstate.recheck_executor->SelectExpression(chunk, sel);
}

static void NgramScanFunc(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<NgramScanGlobalState>();
	auto &lstate = data.local_state->Cast<NgramScanLocalState>();
	ExecuteSearchCore(
	    context, data, state.core, lstate.core,
	    [&](DataChunk &chunk, SelectionVector &sel) { return RecheckChunk(lstate, chunk, sel); }, output);
}

//! Ordered sinks reassemble a parallel scan's output by batch index. Fetch
//! blocks carry their block number and storage batches follow them, so the
//! reassembled order is the one the single-threaded scan produced: candidate
//! rowids ascending, then the tail in storage order.
static OperatorPartitionData NgramScanGetPartitionData(ClientContext &context, TableFunctionGetPartitionInput &input) {
	auto &lstate = input.local_state->Cast<NgramScanLocalState>();
	return OperatorPartitionData(lstate.core.batch_index);
}

//===----------------------------------------------------------------------===//
// EXPLAIN / profiling rendering, dependencies
//===----------------------------------------------------------------------===//

static InsertionOrderPreservingMap<string> NgramScanToString(TableFunctionToStringInput &input) {
	InsertionOrderPreservingMap<string> result;
	auto &bind = input.bind_data->Cast<NgramScanBindData>();
	result["Table"] = bind.table_name;
	result["Ngram Column"] = bind.column_name;
	vector<string> needles;
	for (auto &needle : bind.needles) {
		needles.push_back(needle.text);
	}
	result["Ngram Needles"] = StringUtil::Join(needles, ", ");
	return result;
}

static InsertionOrderPreservingMap<string> NgramScanDynamicToString(TableFunctionDynamicToStringInput &input) {
	InsertionOrderPreservingMap<string> result;
	if (!input.global_state) {
		return result;
	}
	auto &state = input.global_state->Cast<NgramScanGlobalState>();
	result["Ngram Storage Columns"] = StringUtil::Join(state.fetched_columns, ", ");
	if (state.mode == NgramScanMode::INDEX) {
		result["Ngram Mode"] = StringUtil::Format("index (<= %llu candidates, %llu decoded rowids)",
		                                          state.candidate_count, state.core.probe->decoded_rowids.load());
	} else {
		result["Ngram Mode"] = "full scan fallback: " + state.fallback_reason;
	}
	return result;
}

static void NgramScanDependency(LogicalDependencyList &dependencies, const FunctionData *bind_data) {
	auto &bind = bind_data->Cast<NgramScanBindData>();
	auto table = bind.table;
	if (table) {
		dependencies.AddDependency(*table);
	}
}

static TableFunction NgramIndexScanFunction() {
	TableFunction function("ngram_index_scan", {}, NgramScanFunc);
	function.init_global = NgramScanInitGlobal;
	function.init_local = NgramScanInitLocal;
	function.get_partition_data = NgramScanGetPartitionData;
	function.projection_pushdown = true;
	function.filter_pushdown = true;
	function.filter_prune = true;
	function.dependency = NgramScanDependency;
	function.to_string = NgramScanToString;
	function.dynamic_to_string = NgramScanDynamicToString;
	// injected post-optimize, never serialized; skips the DEBUG-build plan
	// serialization verification
	function.verify_serialization = false;
	return function;
}

//===----------------------------------------------------------------------===//
// The optimizer hook
//===----------------------------------------------------------------------===//

//! Swap a qualifying seq_scan LogicalGet for NGRAM_INDEX_SCAN. Ordinary
//! availability/shape checks decline to the native scan; a present malformed
//! index object is corruption and propagates.
static void TryRewriteGet(ClientContext &context, LogicalGet &get) {
	if (get.function.name != "seq_scan") {
		return;
	}
	if (get.table_filters.filters.empty()) {
		return;
	}
	// shapes the swapped scan does not reproduce
	if (get.extra_info.sample_options || get.ordinality_idx.IsValid()) {
		return;
	}
	auto table = get.GetTable();
	if (!table || !table->IsDuckTable() || table->HasGeneratedColumns()) {
		return;
	}
	for (auto &col_idx : get.GetColumnIds()) {
		if (col_idx.IsVirtualColumn() && !col_idx.IsRowIdColumn()) {
			return;
		}
	}
	auto &columns = table->GetColumns();
	auto catalog_name = table->ParentCatalog().GetName();
	auto schema_name = table->ParentSchema().name;
	ResolvedTarget resolved {catalog_name, schema_name, table->name, string(), table};
	auto locations = ExistingIndexes(context, resolved, true);

	for (auto &entry : get.table_filters.filters) {
		if (entry.first >= columns.LogicalColumnCount()) {
			continue;
		}
		auto &column = columns.GetColumn(LogicalIndex(entry.first));
		if (column.Type().id() != LogicalTypeId::VARCHAR) {
			continue;
		}
		vector<RewriteNeedle> needles;
		CollectFilterNeedles(*entry.second, needles);
		if (needles.empty()) {
			continue;
		}
		IndexLocation location;
		bool found = false;
		for (auto &candidate : locations) {
			if (StringUtil::CIEquals(candidate.column_name, column.Name())) {
				if (found) {
					return;
				}
				location = candidate;
				found = true;
			}
		}
		if (!found) {
			continue;
		}
		if (!IndexLocationAvailable(context, resolved, location)) {
			continue;
		}
		auto info = ReadMeta(context, catalog_name, location);
		if (!RowIdGuardReason(context, table->Cast<DuckTableEntry>(), info).empty()) {
			continue;
		}
		auto usable = UsableNeedles(needles, info.options);
		if (usable.empty()) {
			// short needles, or ILIKE against a case-sensitive index
			continue;
		}

		auto bind = make_uniq<NgramScanBindData>();
		bind->catalog_name = catalog_name;
		bind->schema_name = schema_name;
		bind->table_name = table->name;
		bind->location = location;
		bind->column_name = column.Name();
		bind->needles = std::move(usable);
		for (auto &col : columns.Logical()) {
			bind->base_names.push_back(col.Name());
			bind->base_types.push_back(col.Type());
		}
		bind->table = table;

		get.function = NgramIndexScanFunction();
		get.bind_data = std::move(bind);
		// row-group ordering hints (RowGroupPruner, ORDER BY ... LIMIT shapes)
		// are performance-only whenever the scan carries filters — the pruner
		// never prunes rows through a filtered get — and the swapped scan does
		// not implement them
		get.row_group_order_options.reset();
		return;
	}
}

static void RewriteOperator(ClientContext &context, LogicalOperator &op) {
	for (auto &child : op.children) {
		RewriteOperator(context, *child);
	}
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		TryRewriteGet(context, op.Cast<LogicalGet>());
	}
}

static void NgramOptimizeFunction(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	if (!AutoAccelerateEnabled(input.context)) {
		return;
	}
	RewriteOperator(input.context, *plan);
}

//===----------------------------------------------------------------------===//
// Registration
//===----------------------------------------------------------------------===//

void RegisterRewrite(ExtensionLoader &loader) {
	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	config.AddExtensionOption("ngram_auto_accelerate",
	                          "rewrite contains/LIKE/ILIKE filters over ngram-indexed columns into index scans "
	                          "(opt-in; exact queries fall back when probe resources or density are too high)",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false));
	OptimizerExtension extension;
	extension.optimize_function = NgramOptimizeFunction;
	OptimizerExtension::Register(config, std::move(extension));
}

} // namespace ngram
} // namespace duckdb
