#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/atomic.hpp"
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
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "ngram/index_pragmas.hpp"
#include "ngram/maintenance.hpp"
#include "ngram/ngram_rewrite.hpp"
#include "ngram/rowid_guard.hpp"
#include "ngram/search_core.hpp"
#include "ngram/trigram.hpp"

#include <cmath>

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
// rows) with the same filters applied natively by the storage scan. A shared
// vacuum lock is held from before the probe until the scan state dies.
//
// Both phases run on as many threads as there is work for. The probe happens
// once, in init_global; after it the candidate list is immutable and threads
// claim disjoint STANDARD_VECTOR_SIZE blocks of it through an atomic counter,
// while the storage scan hands out one row group at a time through DuckDB's
// own parallel cursor (which covers the transaction-local rows after the
// committed ones). Nothing about exhaustiveness changes: the same rows are
// visited, once each. Output order is preserved for ordered sinks by the
// batch index the scan reports, so a parallel run returns rows in the order
// the single-threaded one did.
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
// Staleness: the same identity checks and rowid-guard validation as the
// explicit path make this path decline instead. At plan time it leaves the
// sequential scan intact; at execution time the replacement performs one full
// scan. Updates, vacuum, reopen, and uncertain guard state therefore cannot
// make a rewritten predicate omit rows. Auto acceleration remains opt-in until
// Phase 12 bounds candidate materialization and consolidates the two engines.
//===----------------------------------------------------------------------===//

//! The gate runs after the probe, so it cannot recover probe cost; its only
//! job is choosing between fetching candidates and scanning the table for the
//! work that remains. Measured fetch cost is ~250-300 ns per candidate at
//! every scale, and a parallel scan of the whole table costs ~0.04 s at 1 GB,
//! ~0.35 s at 10 GB and ~3.5 s at 100 GB, which puts the break-even at 1.6%,
//! 1.3% and 1.1% of rows respectively. One percent is that crossover, rounded
//! toward scanning. The previous 0.05 was set before fetch was parallel and is
//! four to five times too permissive: at 100 GB it let a 6.3%-selectivity
//! needle spend 34.4 s on the index path where the fallback needs 20.1 s and a
//! plain scan 4.6 s (benchmarks/RESULTS.md).
static constexpr double DEFAULT_MAX_CANDIDATE_FRACTION = 0.01;

static bool AutoAccelerateEnabled(ClientContext &context) {
	Value value;
	if (context.TryGetCurrentSetting("ngram_auto_accelerate", value) && !value.IsNull()) {
		return value.GetValue<bool>();
	}
	return false;
}

static double MaxCandidateFraction(ClientContext &context) {
	Value value;
	if (context.TryGetCurrentSetting("ngram_max_candidate_fraction", value) && !value.IsNull()) {
		auto fraction = value.GetValue<double>();
		if (std::isnan(fraction) || fraction < 0) {
			throw InvalidInputException("ngram_max_candidate_fraction must be a non-negative fraction, got %s",
			                            to_string(fraction));
		}
		return fraction;
	}
	return DEFAULT_MAX_CANDIDATE_FRACTION;
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
	string shadow_schema;
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

	ShadowTarget Target() const {
		return ShadowTarget {schema_name, table_name, column_name, shadow_schema};
	}

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

enum class NgramScanPhase : uint8_t { FETCH, SCAN, DONE };

//! Everything shared by the scan's threads. Immutable once init_global
//! returns, except `next_fetch_block` (atomic) and `parallel_scan` (which
//! hands out row groups under its own mutex).
struct NgramScanGlobalState final : public GlobalTableFunctionState {
	DataTable *storage = nullptr;
	DuckTransaction *tx = nullptr;
	//! Held from before the index probe until this state dies: checkpoint
	//! vacuum must not move rowids while we hold candidate rowids. A shared
	//! lock has no thread affinity, so one key covers every scanning thread
	//! (the pattern v1.5.5's own index scan uses, table_scan.cpp:127).
	unique_ptr<StorageLockKey> vacuum_lock;
	int64_t hwm = -1;

	NgramScanMode mode = NgramScanMode::FULL_SCAN;
	//! Why mode is FULL_SCAN; rendered by dynamic_to_string (EXPLAIN ANALYZE).
	string fallback_reason;
	idx_t candidate_count = 0;

	//! Scanned column layout (mirrors the physical scan's column_ids order).
	vector<StorageIndex> column_ids;
	vector<LogicalType> scanned_types;
	vector<idx_t> projection_ids;
	bool remove_columns = false;

	//! The conjunction of every non-optional pushed filter, evaluated on
	//! fetched candidate chunks (DataTable::Fetch applies no filters). Shared
	//! read-only; each thread builds its own executor over it, because an
	//! ExpressionExecutor carries per-evaluation state.
	unique_ptr<Expression> recheck_expr;

	//! Candidate rowids (sorted, clamped to hwm); INDEX mode only. Threads
	//! claim STANDARD_VECTOR_SIZE-sized blocks of it by index.
	vector<row_t> candidates;
	atomic<idx_t> next_fetch_block {0};
	idx_t fetch_block_count = 0;

	//! The storage scan: the tail (rowid > hwm) in INDEX mode, the whole
	//! table in FULL_SCAN mode. Filters are applied by the storage scan; the
	//! parallel cursor hands each thread one row group at a time and covers
	//! the transaction-local rows after the committed ones, so the scan stays
	//! exactly as exhaustive as the single-threaded version was.
	vector<StorageIndex> scan_column_ids;
	vector<LogicalType> scan_types;
	unique_ptr<TableFilterSet> scan_filters;
	ParallelTableScanState parallel_scan;
	//! Rowid appended past the projected columns for the tail filter.
	bool scan_has_extra_rowid = false;

	idx_t max_threads = 1;

	idx_t MaxThreads() const override {
		return max_threads;
	}
};

//! Per-thread scan state: output buffers, fetch/scan cursors and the recheck
//! executor. Nothing here may be shared — DataTable::Fetch writes through its
//! ColumnFetchState, and a TableScanState owns per-thread filter state.
struct NgramScanLocalState final : public LocalTableFunctionState {
	NgramScanPhase phase = NgramScanPhase::FETCH;

	DataChunk fetch_chunk;
	ColumnFetchState fetch_state;

	TableScanState scan_state;
	DataChunk scan_chunk;
	//! Whether scan_state currently holds a row group claimed from the
	//! parallel cursor.
	bool scan_unit_active = false;

	unique_ptr<ExpressionExecutor> recheck_executor;
	SelectionVector sel;

	//! Batch index of the chunk this thread emitted last; ordered sinks
	//! reassemble the output by it (see NgramScanGetPartitionData).
	idx_t batch_index = 0;
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
			throw InternalException("ngram accelerated scan: table filter references column %llu of %llu scanned",
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
	MetaInfo info;
	try {
		auto &meta = ResolveExistingTable(context, bind.catalog_name, bind.shadow_schema,
		                                  MetaTableName(bind.column_name), "ngram index meta table");
		info = ReadMeta(context, *state.tx, meta, bind.Target());
	} catch (std::exception &) {
		// dropped or malformed shadow tables: a plain LIKE must keep working,
		// so degrade to the full scan instead of surfacing an index error
		state.fallback_reason = "index unavailable";
		return false;
	}
	auto &base = ResolveExistingTable(context, bind.catalog_name, bind.schema_name, bind.table_name, "table");
	if (!CertainStaleReason(info, ComputeTableFingerprint(context, base)).empty()) {
		// a plain LIKE must return every matching row: a proven-stale index
		// cannot, so the scan degrades instead of erroring the user's query
		state.fallback_reason = "index stale";
		return false;
	}
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
	vector<row_t> candidates;
	try {
		auto &segments = ResolveExistingTable(context, bind.catalog_name, bind.shadow_schema,
		                                      SegmentsTableName(bind.column_name), "ngram index segments table");
		auto &stats = ResolveExistingTable(context, bind.catalog_name, bind.shadow_schema,
		                                   StatsTableName(bind.column_name), "ngram index stats table");
		candidates = ProbeIndex(context, *state.tx, segments, stats, grams, MaxGramsPerQuery(context));
	} catch (std::exception &) {
		state.fallback_reason = "index unavailable";
		return false;
	}
	// the index never legitimately references rowids past its own high-water
	// mark; dropping any prevents double-returning rows the tail scan visits
	candidates.erase(std::upper_bound(candidates.begin(), candidates.end(), static_cast<row_t>(info.hwm_rowid)),
	                 candidates.end());
	state.candidate_count = candidates.size();

	auto fraction = MaxCandidateFraction(context);
	auto approx_rows = static_cast<double>(state.storage->GetTotalRows());
	if (static_cast<double>(candidates.size()) > fraction * approx_rows) {
		state.fallback_reason = "candidate fraction exceeded";
		return false;
	}
	state.hwm = info.hwm_rowid;
	state.candidates = std::move(candidates);
	return true;
}

//! An upper bound on the row groups the storage scan can hand out, so the
//! executor spawns threads in proportion to the work: the whole table in
//! FULL_SCAN mode, the rows past the high-water mark in INDEX mode. Always at
//! least one, for the transaction-local rows the tail scan must still visit.
static idx_t TailScanUnits(ClientContext &context, DataTable &storage, const NgramScanGlobalState &state) {
	if (state.mode == NgramScanMode::FULL_SCAN || state.hwm < 0) {
		return storage.MaxThreads(context);
	}
	idx_t units = 1;
	auto total_rows = storage.GetTotalRows();
	auto indexed = static_cast<idx_t>(state.hwm) + 1;
	if (total_rows > indexed) {
		units += (total_rows - indexed) / storage.GetRowGroupSize() + 1;
	}
	return units;
}

static unique_ptr<GlobalTableFunctionState> NgramScanInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind = input.bind_data->Cast<NgramScanBindData>();
	auto state = make_uniq<NgramScanGlobalState>();

	auto &base = ResolveRewriteBase(context, bind);
	auto &storage = base.GetStorage();
	state->storage = &storage;
	state->tx = &DuckTransaction::Get(context, base.ParentCatalog());
	state->vacuum_lock = DuckTransactionManager::Get(storage.GetAttached()).SharedVacuumLock();

	// scanned column layout, mirroring DuckTableScanInitGlobal
	auto &columns = base.GetColumns();
	for (auto &col_idx : input.column_indexes) {
		if (col_idx.IsRowIdColumn()) {
			state->scanned_types.emplace_back(LogicalType::ROW_TYPE);
		} else if (col_idx.IsVirtualColumn() || col_idx.HasChildren()) {
			// the rewrite never fires on such scans
			throw InternalException("ngram accelerated scan: unsupported column reference in scan");
		} else {
			state->scanned_types.push_back(columns.GetColumn(col_idx.ToLogical()).Type());
		}
		state->column_ids.push_back(base.GetStorageIndex(col_idx));
	}
	state->projection_ids = input.projection_ids;
	state->remove_columns = input.CanRemoveFilterColumns();

	state->recheck_expr = BuildRecheckExpression(input.filters, state->scanned_types);

	if (TryProbeIndex(context, bind, *state)) {
		state->mode = NgramScanMode::INDEX;
		state->fetch_block_count = (state->candidates.size() + STANDARD_VECTOR_SIZE - 1) / STANDARD_VECTOR_SIZE;
	} else {
		state->mode = NgramScanMode::FULL_SCAN;
	}

	// the storage scan: tail (rowid > hwm) in INDEX mode, whole table in
	// FULL_SCAN mode; the pushed filters are applied natively either way
	state->scan_column_ids = state->column_ids;
	state->scan_types = state->scanned_types;
	state->scan_filters = make_uniq<TableFilterSet>();
	if (input.filters) {
		for (auto &entry : input.filters->filters) {
			state->scan_filters->PushFilter(ColumnIndex(entry.first), entry.second->Copy());
		}
	}
	if (state->mode == NgramScanMode::INDEX && state->hwm >= 0) {
		optional_idx rowid_position;
		for (idx_t i = 0; i < input.column_indexes.size(); i++) {
			if (input.column_indexes[i].IsRowIdColumn()) {
				rowid_position = i;
				break;
			}
		}
		if (!rowid_position.IsValid()) {
			rowid_position = state->scan_column_ids.size();
			state->scan_column_ids.emplace_back(StorageIndex(COLUMN_IDENTIFIER_ROW_ID));
			state->scan_types.emplace_back(LogicalType::ROW_TYPE);
			state->scan_has_extra_rowid = true;
		}
		// rowid > hwm: zone maps skip fully-indexed row groups, and
		// transaction-local rows (rowid >= MAX_ROW_ID) always pass
		state->scan_filters->PushFilter(
		    ColumnIndex(rowid_position.GetIndex()),
		    make_uniq<ConstantFilter>(ExpressionType::COMPARE_GREATERTHAN, Value::BIGINT(state->hwm)));
	}
	// The parallel cursor needs no empty-table special case: an empty row-group
	// collection leaves it with no current row group, and the transaction-local
	// side is null-guarded, so the first claim simply reports "nothing left".
	// (DataTable::InitializeScan asserts instead, which is why the sequential
	// helper exists.)
	static const vector<ColumnIndex> NO_COLUMN_INDEXES;
	storage.InitializeParallelScan(context, state->parallel_scan, NO_COLUMN_INDEXES);
	state->max_threads = state->fetch_block_count + TailScanUnits(context, storage, *state);

	return std::move(state);
}

static unique_ptr<LocalTableFunctionState> NgramScanInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                              GlobalTableFunctionState *global_state) {
	auto &gstate = global_state->Cast<NgramScanGlobalState>();
	auto state = make_uniq<NgramScanLocalState>();
	state->phase = gstate.mode == NgramScanMode::INDEX ? NgramScanPhase::FETCH : NgramScanPhase::SCAN;
	if (gstate.mode == NgramScanMode::INDEX) {
		state->fetch_chunk.Initialize(Allocator::Get(context.client), gstate.scanned_types);
	}
	state->scan_state.Initialize(gstate.scan_column_ids, &context.client,
	                             gstate.scan_filters->filters.empty() ? nullptr : gstate.scan_filters.get());
	state->scan_chunk.Initialize(Allocator::Get(context.client), gstate.scan_types);
	if (gstate.recheck_expr) {
		state->recheck_executor = make_uniq<ExpressionExecutor>(context.client, *gstate.recheck_expr);
	}
	state->sel.Initialize(STANDARD_VECTOR_SIZE);
	return std::move(state);
}

//===----------------------------------------------------------------------===//
// Execution: scan
//===----------------------------------------------------------------------===//

//! Rows of `chunk` passing every non-optional pushed filter, selected into
//! the thread's selection vector. Storage-scan chunks are already filtered
//! natively; re-running the executor there is an idempotent belt-and-braces
//! pass over survivors.
static idx_t RecheckChunk(NgramScanLocalState &lstate, DataChunk &chunk) {
	if (!lstate.recheck_executor) {
		for (idx_t r = 0; r < chunk.size(); r++) {
			lstate.sel.set_index(r, r);
		}
		return chunk.size();
	}
	return lstate.recheck_executor->SelectExpression(chunk, lstate.sel);
}

//! Emit the selected rows of `src` into `output`, honoring the scan's
//! projection. `src` may carry the extra tail rowid column at the end; both
//! projection_ids and output.ColumnCount() only reference columns before it.
static void EmitSelected(NgramScanGlobalState &state, NgramScanLocalState &lstate, DataChunk &src, idx_t hits,
                         DataChunk &output) {
	if (state.remove_columns) {
		for (idx_t i = 0; i < state.projection_ids.size(); i++) {
			output.data[i].Slice(src.data[state.projection_ids[i]], lstate.sel, hits);
		}
	} else {
		for (idx_t c = 0; c < output.ColumnCount(); c++) {
			output.data[c].Slice(src.data[c], lstate.sel, hits);
		}
	}
	output.SetCardinality(hits);
}

//! Signal "I made progress but produced nothing; call me again". Under the
//! synchronous debug strategy an empty HAVE_MORE_OUTPUT is rejected, so there
//! the caller loops instead.
static bool YieldEmpty(TableFunctionInput &data) {
	if (data.results_execution_mode != AsyncResultsExecutionMode::TASK_EXECUTOR) {
		return false;
	}
	data.async_result = AsyncResultType::HAVE_MORE_OUTPUT;
	return true;
}

static void NgramScanFunc(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<NgramScanGlobalState>();
	auto &lstate = data.local_state->Cast<NgramScanLocalState>();
	while (true) {
		switch (lstate.phase) {
		case NgramScanPhase::FETCH: {
			// claim the next block of candidate rowids; blocks are disjoint,
			// so the fetched rows partition the candidate set exactly once
			auto block = state.next_fetch_block++;
			if (block >= state.fetch_block_count) {
				lstate.phase = NgramScanPhase::SCAN;
				continue;
			}
			auto offset = block * STANDARD_VECTOR_SIZE;
			idx_t count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, state.candidates.size() - offset);
			lstate.batch_index = block;
			auto row_id_data = reinterpret_cast<data_ptr_t>(state.candidates.data() + offset);
			Vector row_ids(LogicalType::ROW_TYPE, row_id_data);

			lstate.fetch_chunk.Reset();
			state.storage->Fetch(*state.tx, lstate.fetch_chunk, state.column_ids, row_ids, count, lstate.fetch_state);
			idx_t hits = 0;
			if (lstate.fetch_chunk.size() != 0) {
				hits = RecheckChunk(lstate, lstate.fetch_chunk);
			}
			if (hits == 0) {
				if (YieldEmpty(data)) {
					return;
				}
				continue;
			}
			EmitSelected(state, lstate, lstate.fetch_chunk, hits, output);
			return;
		}
		case NgramScanPhase::SCAN: {
			if (!lstate.scan_unit_active) {
				if (state.storage->NextParallelScan(context, state.parallel_scan, lstate.scan_state) == 0) {
					lstate.phase = NgramScanPhase::DONE;
					continue;
				}
				lstate.scan_unit_active = true;
			}
			lstate.scan_chunk.Reset();
			state.storage->Scan(*state.tx, lstate.scan_chunk, lstate.scan_state);
			if (lstate.scan_chunk.size() == 0) {
				// this row group is done; the next call claims another
				lstate.scan_unit_active = false;
				if (YieldEmpty(data)) {
					return;
				}
				continue;
			}
			// committed row groups are numbered from 1 and the local-storage
			// numbering resumes past the committed total, so scan batches
			// always sort after every fetch block
			lstate.batch_index = state.fetch_block_count + lstate.scan_state.table_state.batch_index +
			                     lstate.scan_state.local_state.batch_index;
			idx_t hits = RecheckChunk(lstate, lstate.scan_chunk);
			if (hits == 0) {
				if (YieldEmpty(data)) {
					return;
				}
				continue;
			}
			EmitSelected(state, lstate, lstate.scan_chunk, hits, output);
			return;
		}
		case NgramScanPhase::DONE:
			// this thread is retired; the others carry on independently
			return;
		}
	}
}

//! Ordered sinks reassemble a parallel scan's output by batch index. Fetch
//! blocks carry their block number and storage batches follow them, so the
//! reassembled order is the one the single-threaded scan produced: candidate
//! rowids ascending, then the tail in storage order.
static OperatorPartitionData NgramScanGetPartitionData(ClientContext &context, TableFunctionGetPartitionInput &input) {
	auto &lstate = input.local_state->Cast<NgramScanLocalState>();
	return OperatorPartitionData(lstate.batch_index);
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
	if (state.mode == NgramScanMode::INDEX) {
		result["Ngram Mode"] = StringUtil::Format("index (%llu candidates)", state.candidate_count);
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

//! Swap a qualifying seq_scan LogicalGet for NGRAM_INDEX_SCAN. Every check
//! that fails leaves the node untouched — the native seq scan is always
//! correct, so this function only ever declines, never errors.
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
		if (col_idx.HasChildren()) {
			return;
		}
	}
	auto &columns = table->GetColumns();
	auto catalog_name = table->ParentCatalog().GetName();
	auto schema_name = table->ParentSchema().name;
	auto shadow_schema = ShadowSchemaName(schema_name, table->name);

	auto &transaction = DuckTransaction::Get(context, table->ParentCatalog());
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
		// an ngram index on this column? (EntryLookupInfo holds a reference,
		// so the name must outlive the lookup)
		auto meta_name = MetaTableName(column.Name());
		EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, meta_name);
		auto meta = Catalog::GetEntry(context, catalog_name, shadow_schema, lookup, OnEntryNotFound::RETURN_NULL);
		if (!meta || meta->type != CatalogType::TABLE_ENTRY || !meta->Cast<TableCatalogEntry>().IsDuckTable()) {
			continue;
		}
		MetaInfo info;
		try {
			ShadowTarget target {schema_name, table->name, column.Name(), shadow_schema};
			info = ReadMeta(context, transaction, meta->Cast<DuckTableEntry>(), target);
		} catch (std::exception &) {
			// unusable shadow tables (collision, malformed, unreadable): the
			// plain seq scan stands
			continue;
		}
		if (!CertainStaleReason(info, ComputeTableFingerprint(context, *table)).empty()) {
			// the index provably no longer describes this table; a LIKE must
			// still return every matching row, so leave the seq scan in place
			continue;
		}
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
		bind->shadow_schema = shadow_schema;
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
	                          "(opt-in while accelerated-query resource use remains unbounded)",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false));
	config.AddExtensionOption("ngram_max_candidate_fraction",
	                          "accelerated scans fall back to a full scan when the index returns more than this "
	                          "fraction of the table as candidates",
	                          LogicalType::DOUBLE, Value::DOUBLE(DEFAULT_MAX_CANDIDATE_FRACTION));

	OptimizerExtension extension;
	extension.optimize_function = NgramOptimizeFunction;
	OptimizerExtension::Register(config, std::move(extension));
}

} // namespace ngram
} // namespace duckdb
