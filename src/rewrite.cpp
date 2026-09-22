#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/profiler/profiling_node.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "ngram/catalog.hpp"
#include "ngram/index_state.hpp"
#include "ngram/search_core.hpp"
#include "ngram/settings.hpp"
#include "ngram_extension.hpp"

namespace duckdb {
namespace ngram {

//===----------------------------------------------------------------------===//
// The transparent query path.
//
// A post-optimize OptimizerExtension swaps get.function / get.bind_data of a
// seq_scan LogicalGet whose pushed-down table_filters contain
// contains(col, 'needle'), LIKE ('~~') or ILIKE ('~~*') over an indexed column
// for the ngram_index_scan table function; everything else on the node stays,
// so column bindings and EXPLAIN filter rendering are unchanged.
//
// ngram_index_scan runs the search core: candidates from the probe, fetched
// and rechecked against ALL pushed filters (TableFilter::ToExpression, so
// semantics never depend on index normalization), then the tail scan with the
// same filters applied natively. Fallbacks never re-plan: a dropped index,
// changed options, a failed guard verdict or the selectivity gate degrade the
// same table function to a full storage scan, what the seq scan would have
// cost. Non-qualifying shapes and a failed plan-time guard verdict skip the
// rewrite (TryRewriteGet).
//
// Case semantics: contains/LIKE are case-sensitive and may probe either index
// flavor (folding merges classes, so a CI index's candidates are a superset,
// and recheck applies the exact predicate); ILIKE may only probe a
// case-insensitive index, since a CS index would miss case variants.
//===----------------------------------------------------------------------===//

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
	// several filters on one column arrive ANDed into a single pushed
	// expression, and every conjunct independently narrows the candidate set
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION) {
		auto &conjunction = expr.Cast<BoundConjunctionExpression>();
		if (conjunction.GetExpressionType() != ExpressionType::CONJUNCTION_AND) {
			return;
		}
		for (auto &child : conjunction.GetChildren()) {
			CollectExprNeedles(*child, needles);
		}
		return;
	}
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return;
	}
	auto &func = expr.Cast<BoundFunctionExpression>();
	if (func.GetChildren().size() != 2) {
		return;
	}
	auto &name = func.Function().GetName();
	bool is_contains = name == "contains";
	bool is_like = name == "~~";
	bool is_ilike = name == "~~*";
	if (!is_contains && !is_like && !is_ilike) {
		return;
	}
	// column on the left, literal on the right: contains('lit', col) probes
	// nothing
	if (func.GetChildren()[0]->GetExpressionType() != ExpressionType::BOUND_REF ||
	    func.GetChildren()[0]->GetReturnType().id() != LogicalTypeId::VARCHAR) {
		return;
	}
	if (func.GetChildren()[1]->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
		return;
	}
	auto &value = func.GetChildren()[1]->Cast<BoundConstantExpression>().GetValue();
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
	case TableFilterType::LEGACY_CONJUNCTION_AND:
		for (auto &child : filter.Cast<LegacyConjunctionAndFilter>().child_filters) {
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
//! within the query memory budget under the index's options to contribute to
//! the probe.
static vector<RewriteNeedle> UsableNeedles(ClientContext &context, const vector<RewriteNeedle> &needles,
                                           const GramOptions &options) {
	vector<RewriteNeedle> usable;
	auto max_keys = MaxProbeKeys(context);
	for (auto &needle : needles) {
		if (needle.requires_ci && !options.case_insensitive) {
			continue;
		}
		NeedleKeys keys;
		if (DecomposeNeedle(context, needle.text.data(), needle.text.size(), options, max_keys, keys) ==
		    NeedleShape::PROBEABLE) {
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

	//! The conjunction of every non-optional pushed filter over the probe
	//! layout, evaluated on fetched candidate chunks (DataTable::Fetch applies
	//! no filters). Shared read-only; each thread builds its own executor over
	//! it, because an ExpressionExecutor carries per-evaluation state.
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

//! The conjunction of every non-optional pushed filter over the probe
//! layout, whose columns `recheck_positions` lists in the expression's
//! reference order. Optional filters (zone-map hints, dynamic TopN/join
//! filters) are skipped: their contract says executing them is not required
//! for correctness, and their state can change between init and evaluation.
static unique_ptr<Expression> BuildRecheckExpression(optional_ptr<TableFilterSet> filters,
                                                     const vector<LogicalType> &scanned_types,
                                                     vector<idx_t> &recheck_positions) {
	unique_ptr<Expression> result;
	if (!filters) {
		return result;
	}
	for (auto &entry : *filters) {
		auto column_index = entry.GetIndex();
		if (entry.Filter().filter_type == TableFilterType::LEGACY_OPTIONAL_FILTER) {
			continue;
		}
		if (column_index >= scanned_types.size()) {
			throw InvalidInputException("ngram accelerated scan: table filter references column %llu of %llu scanned",
			                            column_index, scanned_types.size());
		}
		auto position = std::find(recheck_positions.begin(), recheck_positions.end(), column_index);
		idx_t reference = NumericCast<idx_t>(position - recheck_positions.begin());
		if (position == recheck_positions.end()) {
			recheck_positions.push_back(column_index);
		}
		BoundReferenceExpression column(scanned_types[column_index], reference);
		auto expr = entry.Filter().ToExpression(column);
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
	auto verdict = ValidateIndex(context, target, bind.location);
	if (verdict.availability != IndexAvailability::AVAILABLE) {
		state.fallback_reason = "index unavailable";
		return false;
	}
	if (!verdict.reason.empty()) {
		state.fallback_reason = "rowid guard: " + verdict.reason;
		return false;
	}
	auto &info = verdict.meta;
	// a matching row must contain every needle, hence every gram of every
	// needle: one intersection over the union of gram sets is exactly the
	// per-needle candidate-set intersection, and any subset of that union
	// still yields a superset of the matches, so needles or grams past the
	// key budget are left out rather than declining the probe
	NeedleKeys keys;
	auto max_keys = MaxProbeKeys(context);
	idx_t usable = 0;
	for (auto &needle : bind.needles) {
		if (needle.requires_ci && !info.options.case_insensitive) {
			continue;
		}
		NeedleKeys own;
		if (DecomposeNeedle(context, needle.text.data(), needle.text.size(), info.options, max_keys, own) !=
		    NeedleShape::PROBEABLE) {
			continue;
		}
		usable++;
		for (auto &key : own.keys) {
			if (!keys.Add(key, max_keys)) {
				break;
			}
		}
	}
	if (usable == 0) {
		// only possible when the index was rebuilt with different options
		// after planning; the rewrite never fires without a usable needle
		state.fallback_reason = "no probeable needle";
		return false;
	}
	auto segments = TryResolveExistingTable(context, bind.catalog_name, NGRAM_SCHEMA, bind.location.SegmentsTable(),
	                                        "ngram index segments table");
	if (!segments) {
		state.fallback_reason = "index unavailable";
		return false;
	}
	auto probe =
	    PlanIndexProbe(context, *state.core.tx, *segments, keys.keys, MaxGramsPerQuery(context), info.hwm_rowid,
	                   MaxCandidateFraction(context), DConstants::INVALID_INDEX, ExtraFetchColumns(state.core));
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

	auto &base = ResolveBoundBase(
	    context, bind.catalog_name, bind.schema_name, bind.table_name, bind.base_names, bind.base_types,
	    "ngram accelerated scan: table %s changed since the query was planned; re-prepare the statement");
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
			state->fetched_columns.push_back(columns.GetColumn(col_idx.ToLogical()).Name().GetIdentifierName() +
			                                 " (extract)");
		} else {
			state->core.fetch_types.push_back(columns.GetColumn(col_idx.ToLogical()).Type());
			state->fetched_columns.push_back(columns.GetColumn(col_idx.ToLogical()).Name().GetIdentifierName());
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

	state->recheck_expr = BuildRecheckExpression(input.filters, state->core.fetch_types, state->core.recheck_positions);

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
	state->core.scan_filters = input.filters ? input.filters->Copy() : make_uniq<TableFilterSet>();
	FinalizeSearchCore(context, state->core);

	return state;
}

static unique_ptr<LocalTableFunctionState> NgramScanInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                              GlobalTableFunctionState *global_state) {
	auto &gstate = global_state->Cast<NgramScanGlobalState>();
	auto state = make_uniq<NgramScanLocalState>();
	if (gstate.recheck_expr) {
		state->recheck_executor = make_uniq<ExpressionExecutor>(context.client, *gstate.recheck_expr);
	}
	InitializeSearchCoreLocal(context, gstate.core, state->core);
	return state;
}

//===----------------------------------------------------------------------===//
// Execution: scan
//===----------------------------------------------------------------------===//

//! A chunk a storage scan produced under the pushed filters holds only
//! passing rows, the same native evaluation the host's seq scan relies on,
//! so the executor runs only on fetched candidates, which DataTable::Fetch
//! does not filter.
static void NgramScanFunc(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<NgramScanGlobalState>();
	auto &lstate = data.local_state->Cast<NgramScanLocalState>();
	ExecuteSearchCore(
	    context, data, state.core, lstate.core,
	    [&](DataChunk &chunk, SelectionVector &sel, bool natively_filtered) {
		    return SelectRechecked(lstate.recheck_executor.get(), chunk, sel, natively_filtered);
	    },
	    output);
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

//! Invoked once per (global, local) state pair, as the core table scan's own
//! hook is: the scanned-row count is the calling thread's, while the rendered
//! counters describe the shared global state and so are keyed-insert idempotent.
static void NgramScanGetMetrics(TableFunctionGetMetricsInput &input) {
	auto &metrics = input.operator_metrics;
	if (input.local_state) {
		metrics.rows_scanned = input.local_state->Cast<NgramScanLocalState>().core.rows_scanned;
	}
	if (!input.global_state) {
		return;
	}
	auto &state = input.global_state->Cast<NgramScanGlobalState>();
	metrics.AddExtraInfo("Ngram Storage Columns", StringUtil::Join(state.fetched_columns, ", "));
	if (state.mode == NgramScanMode::INDEX) {
		metrics.AddExtraInfo("Ngram Mode",
		                     StringUtil::Format("index (<= %llu candidates, %llu decoded rowids)",
		                                        state.candidate_count, state.core.probe->decoded_rowids.load()));
		metrics.AddExtraInfo("Ngram Manifest Rows Scanned", to_string(state.core.probe->manifest_rows_scanned));
		metrics.AddExtraInfo("Ngram Manifest Rows Visited", to_string(state.core.probe->manifest_rows_visited));
		metrics.AddExtraInfo("Ngram Admission Rows", to_string(state.core.probe->admission_rows));
		metrics.AddExtraInfo("Ngram Probe Workers", to_string(state.core.probe->max_threads));
		metrics.AddExtraInfo("Ngram Decode Workspace Bytes", to_string(state.core.probe->workspace_bytes));
		metrics.AddExtraInfo("Ngram Decode Peak Bytes", to_string(state.core.probe->tracker->peak.load()));
	} else {
		metrics.AddExtraInfo("Ngram Mode", "full scan fallback: " + state.fallback_reason);
	}
	metrics.AddExtraInfo("Ngram Fetched Rows", to_string(state.core.fetched_rows.load()));
	metrics.AddExtraInfo("Ngram Range Rows", to_string(state.core.range_rows.load()));
	metrics.AddExtraInfo("Ngram Tail Rows", to_string(state.core.tail_rows.load()));
	metrics.AddExtraInfo("Ngram Local Rows", to_string(state.core.local_rows.load()));
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
	function.get_metrics = NgramScanGetMetrics;
	// injected post-optimize, never serialized; skips the DEBUG-build plan
	// serialization verification
	function.verify_serialization = false;
	return function;
}

//===----------------------------------------------------------------------===//
// The optimizer hook
//===----------------------------------------------------------------------===//

//! Swap a qualifying seq_scan LogicalGet for ngram_index_scan. Ordinary
//! availability/shape checks decline to the native scan; a present malformed
//! index object is corruption and propagates.
static void TryRewriteGet(ClientContext &context, LogicalGet &get) {
	if (get.function.name != "seq_scan") {
		return;
	}
	if (!get.table_filters.HasFilters()) {
		return;
	}
	// shapes the swapped scan does not reproduce; a partition subset means the
	// optimizer answered the other partitions from statistics
	if (get.extra_info.sample_options || get.ordinality_idx.IsValid() || !get.scan_partition_indices.empty()) {
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
	// filter shapes first: a scan without a substring filter on a VARCHAR
	// column never reads the registry
	vector<std::pair<const ColumnDefinition *, vector<RewriteNeedle>>> probeable;
	auto &scanned_columns = get.GetColumnIds();
	for (auto &entry : get.table_filters) {
		// a pushed filter is keyed by its position in the scan's projection
		auto projection_index = entry.GetIndex();
		if (projection_index >= scanned_columns.size()) {
			continue;
		}
		auto &scanned = scanned_columns[projection_index];
		if (scanned.IsVirtualColumn()) {
			continue;
		}
		auto &column = columns.GetColumn(scanned.ToLogical());
		if (column.Type().id() != LogicalTypeId::VARCHAR) {
			continue;
		}
		vector<RewriteNeedle> needles;
		CollectFilterNeedles(entry.Filter(), needles);
		if (!needles.empty()) {
			probeable.emplace_back(&column, std::move(needles));
		}
	}
	if (probeable.empty()) {
		return;
	}
	auto catalog_name = table->ParentCatalog().GetName().GetIdentifierName();
	auto schema_name = table->ParentSchema().name.GetIdentifierName();
	for (auto &candidate : probeable) {
		auto &column = *candidate.first;
		auto &needles = candidate.second;
		// one owner-keyed registry read per filtered column
		ResolvedTarget resolved {catalog_name, schema_name, table->name.GetIdentifierName(),
		                         column.Name().GetIdentifierName(), table};
		auto owned = OwnedIndexes(context, resolved, true);
		if (owned.size() > 1) {
			return;
		}
		if (owned.empty()) {
			continue;
		}
		auto index = &owned[0];
		// the row was read in this statement's snapshot, so the plan-time
		// verdict is the guard's alone; execution revalidates row and guard
		if (!RowIdGuardReason(context, table->Cast<DuckTableEntry>(), index->meta).empty()) {
			continue;
		}
		auto usable = UsableNeedles(context, needles, index->meta.options);
		if (usable.empty()) {
			// short needles, or ILIKE against a case-sensitive index
			continue;
		}

		auto bind = make_uniq<NgramScanBindData>();
		bind->catalog_name = catalog_name;
		bind->schema_name = schema_name;
		bind->table_name = table->name.GetIdentifierName();
		bind->location = index->location;
		bind->column_name = column.Name().GetIdentifierName();
		bind->needles = std::move(usable);
		for (auto &col : columns.Logical()) {
			bind->base_names.push_back(col.Name().GetIdentifierName());
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
	OptimizerExtension extension;
	extension.optimize_function = NgramOptimizeFunction;
	OptimizerExtension::Register(config, std::move(extension));
}

} // namespace ngram
} // namespace duckdb
