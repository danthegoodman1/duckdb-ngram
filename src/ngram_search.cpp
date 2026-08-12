#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/insertion_order_preserving_map.hpp"
#include "duckdb/common/string_map_set.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parallel/task_executor.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/local_storage.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "ngram/index_pragmas.hpp"
#include "ngram/maintenance.hpp"
#include "ngram/ngram_search.hpp"
#include "ngram/postings_codec.hpp"
#include "ngram/rowid_guard.hpp"
#include "ngram/search_core.hpp"
#include "ngram/trigram.hpp"

#include <algorithm>
#include <cmath>
#include <functional>
#include <limits>

namespace duckdb {
namespace ngram {

//===----------------------------------------------------------------------===//
// The explicit query path.
//
// ngram_candidates(table, column, needle) emits the rowids of every INDEXED
// row that might contain the needle: the posting lists of the needle's rarest
// grams (at most ngram_max_grams_per_query of them) are intersected per
// segment. The result is a superset of the true matches among rows with
// rowid <= the index high-water mark; callers must recheck candidates and
// must handle rows past the high-water mark themselves.
//
// ngram_search(table, needle[, col := ...]) returns exactly the rows a
// brute-force scan would return: candidates are fetched through
// DataTable::Fetch and rechecked against the real predicate, then a tail scan
// (rowid > high-water mark, which also covers transaction-local rows) unions
// in every row the index has never seen. Indexed execution holds a shared
// vacuum lock through candidate fetch and the rowid-filtered tail; an
// unfiltered full scan releases it first.
//
// Both phases run on as many threads as there is work for. Probe workers claim
// one bounded rowid segment at a time, then fetch/recheck it before claiming
// another; the tail scan hands out row groups through DuckDB's parallel
// cursor. The same rows are visited, once each, without retaining a
// query-wide candidate list. Ordered sinks use the reported batch indexes to
// restore deterministic segment-then-tail order.
//===----------------------------------------------------------------------===//

//! Probing more of the needle's grams shrinks the candidate set but costs one
//! more posting-list decode each time, and rarest-first means every additional
//! gram is denser than the last. Measured across four indexes (1/10/100 GB of
//! natural language, plus a bigram index whose grams are deliberately dense),
//! total query time is a shallow U with its floor at 2-4 and a steep right
//! arm: at 100 GB a rare needle costs 0.465 s at K=2, 0.640 s at K=3 and
//! 1.750 s at K=8. Three sits at the floor everywhere while keeping a genuine
//! three-way intersection, which K=2 does not: on the dense-gram index K=2
//! leaves 0.89% of rows as candidates against K=3's 0.28%, close enough to
//! ngram_max_candidate_fraction to risk giving up the index entirely.
//! Lowering K is always safe for correctness — fewer grams can only widen the
//! candidate set, never drop a match (benchmarks/RESULTS.md).
static constexpr idx_t DEFAULT_MAX_GRAMS_PER_QUERY = 3;
static constexpr double DEFAULT_MAX_CANDIDATE_FRACTION = 0.01;
static constexpr int64_t DEFAULT_MAX_PROBE_ROWIDS = 100000000;
static constexpr idx_t MAX_PROBE_MEMORY_BYTES = 256ULL * 1024ULL * 1024ULL;

double MaxCandidateFraction(ClientContext &context) {
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

static idx_t MaxProbeRowids(ClientContext &context) {
	Value value;
	if (context.TryGetCurrentSetting("ngram_max_probe_rowids", value) && !value.IsNull()) {
		auto count = value.GetValue<int64_t>();
		if (count < 1) {
			throw InvalidInputException("ngram_max_probe_rowids must be at least 1, got %lld", count);
		}
		return NumericCast<idx_t>(count);
	}
	return NumericCast<idx_t>(DEFAULT_MAX_PROBE_ROWIDS);
}

static idx_t ProbeMemoryBudget(ClientContext &context) {
	auto limit = DBConfig::GetConfig(context).options.maximum_memory;
	if (limit == DConstants::INVALID_INDEX) {
		limit = 8ULL * 1024ULL * 1024ULL * 1024ULL;
	}
	return MinValue<idx_t>(limit / 4, MAX_PROBE_MEMORY_BYTES);
}

ProbeMemoryReservation::ProbeMemoryReservation(BufferManager &manager_p, idx_t size_p)
    : manager(manager_p), size(size_p) {
	manager.ReserveMemory(size);
}

ProbeMemoryReservation::~ProbeMemoryReservation() {
	manager.FreeReservedMemory(size);
}

void ProbeMemoryReservation::Grow(idx_t extra) {
	manager.ReserveMemory(extra);
	size += extra;
}

idx_t MaxGramsPerQuery(ClientContext &context) {
	Value value;
	if (context.TryGetCurrentSetting("ngram_max_grams_per_query", value) && !value.IsNull()) {
		auto k = value.GetValue<int64_t>();
		if (k < 1) {
			throw InvalidInputException("ngram_max_grams_per_query must be at least 1, got %lld", k);
		}
		return static_cast<idx_t>(k);
	}
	return DEFAULT_MAX_GRAMS_PER_QUERY;
}

//===----------------------------------------------------------------------===//
// Bind
//===----------------------------------------------------------------------===//

struct QueryBindData : public TableFunctionData {
	//! Resolved base table (names, not pointers: execution re-resolves so a
	//! prepared statement outliving a DROP fails cleanly instead of dangling).
	string catalog_name;
	string schema_name;
	string table_name;
	string shadow_schema;
	//! The indexed column being searched.
	string column_name;
	string needle;
	//! ngram_search only: bind-time snapshot of the output schema.
	vector<string> names;
	vector<LogicalType> types;
	idx_t search_column_idx = 0;
	//! ngram_search semantics if a prepared query outlives its index.
	GramOptions bound_options;

	ShadowTarget Target() const {
		return ShadowTarget {schema_name, table_name, column_name, shadow_schema};
	}

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
		vector<string> indexed;
		for (auto &meta_name : ExistingMetaTables(context, target)) {
			indexed.push_back(meta_name.substr(strlen("meta_")));
		}
		if (indexed.empty()) {
			throw BinderException("%s: no ngram index exists on %s; build one with PRAGMA create_ngram_index", fn,
			                      table_input);
		}
		if (indexed.size() > 1) {
			throw BinderException("%s: %s has ngram indexes on multiple columns (%s); pass col := '...' to choose", fn,
			                      table_input, StringUtil::Join(indexed, ", "));
		}
		column = indexed[0];
	} else if (!ShadowTableExists(context, target, MetaTableName(column))) {
		throw BinderException("%s: no ngram index exists on %s.%s; build one with PRAGMA create_ngram_index", fn,
		                      table_input, column);
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
	result.shadow_schema = target.shadow_schema;
	// the catalog's spelling: `column` may carry the user's casing (from col :=)
	// or the shadow table's casing (from meta_* discovery), and later name
	// comparisons and shadow lookups must be casing-stable
	result.column_name = col.Name();
}

//! ngram_search(table, needle[, col := ...]) returns every row of `table`
//! whose indexed column contains `needle`. Matching follows the index's
//! normalization: over a case-insensitive index the needle and the column are
//! both folded with the extension's simple per-codepoint lowercase (the same
//! fold the index build uses), so matching is case-insensitive; over a
//! case-sensitive index matching is exact byte-wise `contains`. Results are
//! exhaustive in every admitted state. Rows past the high-water mark and
//! transaction-local rows are found by a brute-force tail scan; deleted rows
//! are hidden by visibility and discarded by recheck. A per-index non-ART
//! rowid guard makes covered-column updates delete+insert and prevents vacuum
//! from moving live rowids. If that guard is missing, incompatible, replaced,
//! or cannot exclude reuse of a discarded trailing range, this function scans
//! the whole table instead of probing postings. Proven stale identity takes
//! the same exhaustive fallback; malformed present index objects still raise.
//! Recheck makes false positives impossible in every path.
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
		throw InternalException("ngram_search: indexed column vanished during binding");
	}
	auto &tx = DuckTransaction::Get(context, table_entry.ParentCatalog());
	auto &meta = ResolveExistingTable(context, result->catalog_name, result->shadow_schema,
	                                  MetaTableName(result->column_name), "ngram index meta table");
	result->bound_options = ReadMeta(context, tx, meta, result->Target()).options;
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
// Execution-time shadow-table access
//===----------------------------------------------------------------------===//

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

//! Append the storage index and type of `column_name` to a projection, or throw
//! if the shadow table does not look like this extension built it.
static void AddShadowColumn(DuckTableEntry &entry, const string &column_name, LogicalTypeId expected,
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

static void ThrowIfInterrupted(ClientContext &context) {
	if (context.interrupted.load(std::memory_order_relaxed)) {
		throw InterruptException();
	}
}

//! Scan an entire table (committed storage plus this transaction's local rows)
//! through the caller's transaction, invoking fn per non-empty chunk.
static void ScanShadowTable(ClientContext &context, DuckTransaction &tx, DataTable &storage,
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

//! Read the single meta row through `projection`, validating that there is
//! exactly one and that no value is NULL. Runs once per column group so that
//! the format version can be checked before columns that only exist in some
//! versions are touched.
static void ScanMetaRow(ClientContext &context, DuckTransaction &tx, DuckTableEntry &meta_entry,
                        const ShadowTarget &bind, const vector<StorageIndex> &column_ids,
                        const vector<LogicalType> &types, const std::function<void(DataChunk &, idx_t)> &read_row) {
	idx_t rows = 0;
	ScanShadowTable(context, tx, meta_entry.GetStorage(), column_ids, types, nullptr, [&](DataChunk &chunk) {
		for (idx_t r = 0; r < chunk.size(); r++) {
			if (++rows > 1) {
				throw InvalidInputException("ngram: meta table for %s.%s holds more than one row; the index is "
				                            "malformed",
				                            bind.table_name, bind.column_name);
			}
			for (idx_t c = 0; c < chunk.ColumnCount(); c++) {
				if (chunk.GetValue(c, r).IsNull()) {
					throw InvalidInputException("ngram: meta table for %s.%s contains NULLs; the index is malformed",
					                            bind.table_name, bind.column_name);
				}
			}
			read_row(chunk, r);
		}
	});
	if (rows == 0) {
		throw CatalogException("ngram: meta table for %s.%s is empty; the index is malformed", bind.table_name,
		                       bind.column_name);
	}
}

int64_t ReadMetaFormatVersion(ClientContext &context, DuckTransaction &tx, DuckTableEntry &meta_entry,
                              const ShadowTarget &bind) {
	int64_t format_version = -1;
	vector<StorageIndex> column_ids;
	vector<LogicalType> types;
	AddShadowColumn(meta_entry, "format_version", LogicalTypeId::INTEGER, column_ids, types);
	AddShadowColumn(meta_entry, "schema_name", LogicalTypeId::VARCHAR, column_ids, types);
	AddShadowColumn(meta_entry, "table_name", LogicalTypeId::VARCHAR, column_ids, types);
	ScanMetaRow(context, tx, meta_entry, bind, column_ids, types, [&](DataChunk &chunk, idx_t r) {
		auto owner_schema = StringValue::Get(chunk.GetValue(1, r));
		auto owner_table = StringValue::Get(chunk.GetValue(2, r));
		if (!StringUtil::CIEquals(owner_schema, bind.schema_name) ||
		    !StringUtil::CIEquals(owner_table, bind.table_name)) {
			throw CatalogException("ngram shadow schema collision: %s belongs to the index on %s.%s, not to "
			                       "%s.%s; no usable ngram index exists",
			                       bind.shadow_schema, owner_schema, owner_table, bind.schema_name, bind.table_name);
		}
		format_version = chunk.GetValue(0, r).GetValue<int64_t>();
	});
	return format_version;
}

MetaInfo ReadMeta(ClientContext &context, DuckTransaction &tx, DuckTableEntry &meta_entry, const ShadowTarget &bind) {
	MetaInfo info;
	auto format_version = ReadMetaFormatVersion(context, tx, meta_entry, bind);
	if (format_version != NGRAM_FORMAT_VERSION) {
		// never guess at another version's column layout
		throw InvalidInputException(
		    "ngram: the index on %s.%s uses meta format_version %lld, but this version of the extension writes and "
		    "reads format_version %lld; drop and rebuild the index",
		    bind.table_name, bind.column_name, format_version, NGRAM_FORMAT_VERSION);
	}
	vector<StorageIndex> column_ids;
	vector<LogicalType> types;
	AddShadowColumn(meta_entry, "gram_size", LogicalTypeId::INTEGER, column_ids, types);
	AddShadowColumn(meta_entry, "case_insensitive", LogicalTypeId::BOOLEAN, column_ids, types);
	AddShadowColumn(meta_entry, "hwm_rowid", LogicalTypeId::BIGINT, column_ids, types);
	AddShadowColumn(meta_entry, "schema_fingerprint", LogicalTypeId::VARCHAR, column_ids, types);
	AddShadowColumn(meta_entry, "table_oid", LogicalTypeId::BIGINT, column_ids, types);
	AddShadowColumn(meta_entry, "instance_id", LogicalTypeId::VARCHAR, column_ids, types);
	AddShadowColumn(meta_entry, "column_name", LogicalTypeId::VARCHAR, column_ids, types);
	AddShadowColumn(meta_entry, "catalog_oid", LogicalTypeId::BIGINT, column_ids, types);
	AddShadowColumn(meta_entry, "column_type", LogicalTypeId::VARCHAR, column_ids, types);
	AddShadowColumn(meta_entry, "guard_name", LogicalTypeId::VARCHAR, column_ids, types);
	AddShadowColumn(meta_entry, "guard_token", LogicalTypeId::VARCHAR, column_ids, types);
	ScanMetaRow(context, tx, meta_entry, bind, column_ids, types, [&](DataChunk &chunk, idx_t r) {
		auto gram_size = chunk.GetValue(0, r).GetValue<int64_t>();
		if (gram_size < 1) {
			throw InvalidInputException("ngram: meta table for %s.%s records gram_size %lld; the index is "
			                            "malformed",
			                            bind.table_name, bind.column_name, gram_size);
		}
		info.options.gram_size = static_cast<idx_t>(gram_size);
		info.options.case_insensitive = chunk.GetValue(1, r).GetValue<bool>();
		info.hwm_rowid = chunk.GetValue(2, r).GetValue<int64_t>();
		if (info.hwm_rowid < -1 || info.hwm_rowid >= MAX_ROW_ID) {
			throw InvalidInputException("ngram: meta table for %s.%s records hwm_rowid %lld; the index is malformed",
			                            bind.table_name, bind.column_name, info.hwm_rowid);
		}
		info.schema_fingerprint = StringValue::Get(chunk.GetValue(3, r));
		info.table_oid = chunk.GetValue(4, r).GetValue<int64_t>();
		info.instance_id = StringValue::Get(chunk.GetValue(5, r));
		info.column_name = StringValue::Get(chunk.GetValue(6, r));
		info.catalog_oid = chunk.GetValue(7, r).GetValue<int64_t>();
		info.column_type = StringValue::Get(chunk.GetValue(8, r));
		info.guard_name = StringValue::Get(chunk.GetValue(9, r));
		info.guard_token = StringValue::Get(chunk.GetValue(10, r));
	});
	return info;
}

//! The candidate-only API has no exhaustive-table substitute: when a detector
//! proves the index stale, returning its partial prefix would violate the
//! candidate contract, so it fails with the repair instead.
static void ThrowIfCandidatesCertainlyStale(ClientContext &context, DuckTableEntry &base, const MetaInfo &info,
                                            const QueryBindData &bind) {
	auto reason = CertainStaleReason(info, ComputeTableFingerprint(context, base));
	if (reason.empty()) {
		return;
	}
	throw InvalidInputException("ngram_candidates: the ngram index on %s.%s is stale and cannot be used: %s. Rebuild it: PRAGMA "
	                            "drop_ngram_index('%s', '%s') then PRAGMA create_ngram_index('%s', '%s')",
	                            bind.table_name, bind.column_name, reason, bind.table_name, bind.column_name,
	                            bind.table_name, bind.column_name);
}

//===----------------------------------------------------------------------===//
// Index probe: rarest-first selection + per-segment intersection
//===----------------------------------------------------------------------===//

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

static idx_t ProbeThreads(ClientContext &context) {
	return MaxValue<idx_t>(NumericCast<idx_t>(TaskScheduler::GetScheduler(context).NumberOfThreads()), 1);
}

static idx_t StatsWorkers(ClientContext &context, DuckTableEntry &stats_entry) {
	return MaxValue<idx_t>(MinValue(ProbeThreads(context), stats_entry.GetStorage().MaxThreads(context)), 1);
}

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

//! Scan a whole shadow table in parallel, one row group per claim. `body` is
//! called once per chunk with the worker's own index so it can accumulate into
//! a private slot.
static void ParallelScanShadowTable(ClientContext &context, DuckTransaction &tx, DataTable &storage,
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

//! Counts recorded for one gram. `segment_count` is the exact number of
//! visible segment-table rows (including refresh generations), so it also
//! bounds the manifest before that vector is allowed to grow.
struct GramStats {
	idx_t row_count = 0;
	idx_t segment_count = 0;
};

struct AtomicGramStats {
	atomic<idx_t> row_count {0};
	atomic<idx_t> segment_count {0};
};

static void CheckedAtomicAdd(atomic<idx_t> &target, idx_t value) {
	auto current = target.load();
	while (true) {
		if (value > std::numeric_limits<idx_t>::max() - current) {
			throw InvalidInputException("ngram: stats counts overflow; the index is malformed");
		}
		if (target.compare_exchange_weak(current, current + value)) {
			return;
		}
	}
}

static vector<GramStats> ReadGramStats(ClientContext &context, DuckTransaction &tx, DuckTableEntry &stats_entry,
                                      const vector<string> &grams, idx_t workers) {
	// string_t keys reference the immutable query grams for this scan. Looking
	// up a stats value therefore allocates no per-row scratch, even when a
	// malformed/unrelated stats gram is very large.
	string_map_t<idx_t> gram_index;
	for (idx_t i = 0; i < grams.size(); i++) {
		gram_index.emplace(string_t(grams[i].data(), NumericCast<uint32_t>(grams[i].size())), i);
	}
	unique_ptr<AtomicGramStats[]> totals(new AtomicGramStats[grams.size()]);

	vector<StorageIndex> column_ids;
	vector<LogicalType> types;
	AddShadowColumn(stats_entry, "gram", LogicalTypeId::VARCHAR, column_ids, types);
	AddShadowColumn(stats_entry, "row_count", LogicalTypeId::BIGINT, column_ids, types);
	AddShadowColumn(stats_entry, "segment_count", LogicalTypeId::BIGINT, column_ids, types);
	ParallelScanShadowTable(context, tx, stats_entry.GetStorage(), column_ids, types, nullptr, workers,
	                        [&](DataChunk &chunk, idx_t worker) {
		UnifiedVectorFormat gram_format, row_format, segment_format;
		chunk.data[0].ToUnifiedFormat(chunk.size(), gram_format);
		chunk.data[1].ToUnifiedFormat(chunk.size(), row_format);
		chunk.data[2].ToUnifiedFormat(chunk.size(), segment_format);
		auto gram_data = UnifiedVectorFormat::GetData<string_t>(gram_format);
		auto row_data = UnifiedVectorFormat::GetData<int64_t>(row_format);
		auto segment_data = UnifiedVectorFormat::GetData<int64_t>(segment_format);
		for (idx_t r = 0; r < chunk.size(); r++) {
			auto gram_idx = gram_format.sel->get_index(r);
			auto row_idx = row_format.sel->get_index(r);
			auto segment_idx = segment_format.sel->get_index(r);
			if (!gram_format.validity.RowIsValid(gram_idx) || !row_format.validity.RowIsValid(row_idx) ||
			    !segment_format.validity.RowIsValid(segment_idx)) {
				throw InvalidInputException("ngram: stats table contains NULLs; the index is malformed");
			}
			auto entry = gram_index.find(gram_data[gram_idx]);
			if (entry == gram_index.end()) {
				continue;
			}
			if (row_data[row_idx] <= 0 || segment_data[segment_idx] <= 0) {
				throw InvalidInputException("ngram: invalid gram row in stats table; the index is malformed");
			}
			auto rows = NumericCast<idx_t>(row_data[row_idx]);
			auto segments = NumericCast<idx_t>(segment_data[segment_idx]);
			CheckedAtomicAdd(totals[entry->second].row_count, rows);
			CheckedAtomicAdd(totals[entry->second].segment_count, segments);
		}
	                        });
	vector<GramStats> result(grams.size());
	for (idx_t gram = 0; gram < grams.size(); gram++) {
		result[gram].row_count = totals[gram].row_count.load();
		result[gram].segment_count = totals[gram].segment_count.load();
	}
	return result;
}

static bool CheckedAdd(idx_t &target, idx_t value) {
	if (value > std::numeric_limits<idx_t>::max() - target) {
		return false;
	}
	target += value;
	return true;
}

static bool CheckedMultiply(idx_t left, idx_t right, idx_t &result) {
	if (left != 0 && right > std::numeric_limits<idx_t>::max() / left) {
		return false;
	}
	result = left * right;
	return true;
}

unique_ptr<ProbePlan> PlanIndexProbe(ClientContext &context, DuckTransaction &tx, DuckTableEntry &segments_entry,
                                     DuckTableEntry &stats_entry, const vector<string> &grams, idx_t max_grams,
                                     int64_t hwm, idx_t table_rows, double candidate_fraction, idx_t worker_cap) {
	D_ASSERT(!grams.empty());
	D_ASSERT(worker_cap > 0);
	auto plan = make_uniq<ProbePlan>();
	plan->segments_entry = &segments_entry;
	plan->hwm = hwm;

	// Both callers supply distinct grams. Account the full needle before
	// copying it or reading stats, then retain the rarest K. The deliberately
	// conservative allowance covers string copies, hash nodes/buckets, stats
	// counters and sort indexes without depending on STL node layouts. Each
	// stats worker also gets 256 KiB for its scan chunk/state; stats lookup
	// itself is allocation-free, while ordinary DuckDB allocator buffers are
	// not charged to BufferManager reservations.
	auto memory_budget = ProbeMemoryBudget(context);
	auto stats_workers = StatsWorkers(context, stats_entry);
	idx_t preflight_bytes = 4096;
	idx_t per_gram_bytes;
	idx_t stats_worker_bytes;
	if (!CheckedMultiply(grams.size(), idx_t(256), per_gram_bytes) ||
	    !CheckedMultiply(stats_workers, idx_t(256 * 1024), stats_worker_bytes) ||
	    !CheckedAdd(preflight_bytes, per_gram_bytes) || !CheckedAdd(preflight_bytes, stats_worker_bytes)) {
		plan->decline_reason = "probe planning size overflow";
		return plan;
	}
	for (auto &gram : grams) {
		idx_t string_bytes;
		if (!CheckedMultiply(gram.size(), idx_t(2), string_bytes) || !CheckedAdd(preflight_bytes, string_bytes)) {
			plan->decline_reason = "probe planning size overflow";
			return plan;
		}
	}
	if (preflight_bytes > memory_budget) {
		plan->decline_reason = "query grams exceed query memory budget";
		return plan;
	}
	plan->memory_reservation = make_uniq<ProbeMemoryReservation>(BufferManager::GetBufferManager(context),
	                                                           preflight_bytes);
	auto all_stats = ReadGramStats(context, tx, stats_entry, grams, stats_workers);
	vector<idx_t> order(grams.size());
	for (idx_t i = 0; i < order.size(); i++) {
		order[i] = i;
	}
	std::stable_sort(order.begin(), order.end(), [&](idx_t a, idx_t b) {
		return all_stats[a].row_count < all_stats[b].row_count;
	});
	order.resize(MinValue(order.size(), max_grams));
	vector<GramStats> selected_stats;
	selected_stats.reserve(order.size());
	plan->grams.reserve(order.size());
	for (auto index : order) {
		plan->grams.push_back(grams[index]);
		selected_stats.push_back(all_stats[index]);
	}

	idx_t descriptor_count = 0;
	for (auto &stats : selected_stats) {
		if (stats.segment_count > stats.row_count) {
			throw InvalidInputException("ngram: stats segment_count exceeds row_count; the index is malformed");
		}
		if (!CheckedAdd(descriptor_count, stats.segment_count)) {
			throw InvalidInputException("ngram: stats segment counts overflow; the index is malformed");
		}
	}
	auto visible_segment_rows = segments_entry.GetStorage().GetTotalRows();
	if (!CheckedAdd(visible_segment_rows, LocalStorage::Get(tx).AddedRows(segments_entry.GetStorage())) ||
	    descriptor_count > visible_segment_rows) {
		throw InvalidInputException("ngram: stats describe more segment rows than exist; the index is malformed");
	}
	idx_t manifest_bytes;
	idx_t gram_scratch_bytes;
	if (!CheckedMultiply(order.size(), sizeof(idx_t), gram_scratch_bytes) ||
	    !CheckedMultiply(descriptor_count, sizeof(ProbeDescriptor) + sizeof(ProbeSegment), manifest_bytes) ||
	    !CheckedAdd(manifest_bytes, gram_scratch_bytes) ||
	    !CheckedAdd(manifest_bytes, idx_t(4096))) {
		plan->decline_reason = "segment manifest size overflow";
		return plan;
	}
	if (manifest_bytes > memory_budget - preflight_bytes) {
		plan->decline_reason = "segment manifest exceeds query memory budget";
		return plan;
	}
	plan->memory_reservation->Grow(manifest_bytes);
	auto reserved_bytes = preflight_bytes + manifest_bytes;
	plan->descriptors.reserve(descriptor_count);
	plan->segments.reserve(descriptor_count);

	vector<StorageIndex> column_ids;
	vector<LogicalType> types;
	AddShadowColumn(segments_entry, "gram", LogicalTypeId::VARCHAR, column_ids, types);
	AddShadowColumn(segments_entry, "segment_no", LogicalTypeId::BIGINT, column_ids, types);
	AddShadowColumn(segments_entry, "rowid_count", LogicalTypeId::BIGINT, column_ids, types);
	column_ids.emplace_back(StorageIndex(COLUMN_IDENTIFIER_ROW_ID));
	types.emplace_back(LogicalType::ROW_TYPE);

	vector<idx_t> found_rows(plan->grams.size(), 0);
	vector<idx_t> found_descriptors(plan->grams.size(), 0);
	for (idx_t gram_index = 0; gram_index < plan->grams.size(); gram_index++) {
		TableFilterSet filters;
		filters.PushFilter(ColumnIndex(0),
		                   make_uniq<ConstantFilter>(ExpressionType::COMPARE_EQUAL, Value(plan->grams[gram_index])));
		ScanShadowTable(context, tx, segments_entry.GetStorage(), column_ids, types, &filters, [&](DataChunk &chunk) {
			UnifiedVectorFormat gram_format, segment_format, count_format, rowid_format;
			chunk.data[0].ToUnifiedFormat(chunk.size(), gram_format);
			chunk.data[1].ToUnifiedFormat(chunk.size(), segment_format);
			chunk.data[2].ToUnifiedFormat(chunk.size(), count_format);
			chunk.data[3].ToUnifiedFormat(chunk.size(), rowid_format);
			auto gram_data = UnifiedVectorFormat::GetData<string_t>(gram_format);
			auto segment_data = UnifiedVectorFormat::GetData<int64_t>(segment_format);
			auto count_data = UnifiedVectorFormat::GetData<int64_t>(count_format);
			auto rowid_data = UnifiedVectorFormat::GetData<row_t>(rowid_format);
			for (idx_t r = 0; r < chunk.size(); r++) {
				auto gram_idx = gram_format.sel->get_index(r);
				auto segment_idx = segment_format.sel->get_index(r);
				auto count_idx = count_format.sel->get_index(r);
				auto rowid_idx = rowid_format.sel->get_index(r);
				if (!gram_format.validity.RowIsValid(gram_idx) || !segment_format.validity.RowIsValid(segment_idx) ||
				    !count_format.validity.RowIsValid(count_idx) || !rowid_format.validity.RowIsValid(rowid_idx)) {
					throw InvalidInputException("ngram: segments table contains NULLs; the index is malformed");
				}
				auto &gram = gram_data[gram_idx];
				if (gram != string_t(plan->grams[gram_index]) || segment_data[segment_idx] < 0 ||
				    hwm < 0 || segment_data[segment_idx] > (hwm >> SEGMENT_SHIFT) || count_data[count_idx] <= 0) {
					throw InvalidInputException("ngram: invalid segments-table descriptor; the index is malformed");
				}
				if (plan->descriptors.size() >= descriptor_count ||
				    found_descriptors[gram_index] >= selected_stats[gram_index].segment_count) {
					throw InvalidInputException("ngram: segments and stats descriptor counts disagree; the index is malformed");
				}
				auto count = NumericCast<idx_t>(count_data[count_idx]);
				if (count > (idx_t(1) << SEGMENT_SHIFT)) {
					throw InvalidInputException("ngram: segment row_count exceeds its rowid range; the index is malformed");
				}
				if (!CheckedAdd(found_rows[gram_index], count)) {
					throw InvalidInputException("ngram: segments row counts overflow; the index is malformed");
				}
				found_descriptors[gram_index]++;
				plan->descriptors.push_back(ProbeDescriptor {segment_data[segment_idx], gram_index,
				                                                   rowid_data[rowid_idx], count});
			}
		});
		if (found_descriptors[gram_index] != selected_stats[gram_index].segment_count ||
		    found_rows[gram_index] != selected_stats[gram_index].row_count) {
			throw InvalidInputException("ngram: segments and stats counts disagree; the index is malformed");
		}
	}
	if (plan->descriptors.size() != descriptor_count) {
		throw InvalidInputException("ngram: segments and stats descriptor counts disagree; the index is malformed");
	}
	std::sort(plan->descriptors.begin(), plan->descriptors.end(), [](const ProbeDescriptor &a, const ProbeDescriptor &b) {
		if (a.segment_no != b.segment_no) {
			return a.segment_no < b.segment_no;
		}
		if (a.gram_index != b.gram_index) {
			return a.gram_index < b.gram_index;
		}
		return a.posting_rowid < b.posting_rowid;
	});

	auto hard_work_limit = MaxProbeRowids(context);
	vector<idx_t> counts;
	counts.reserve(plan->grams.size());
	idx_t estimated_decoded_rowids = 0;
	idx_t peak_worker_bytes = 0;
	string resource_decline;
	for (idx_t begin = 0; begin < plan->descriptors.size();) {
		idx_t end = begin + 1;
		while (end < plan->descriptors.size() &&
		       plan->descriptors[end].segment_no == plan->descriptors[begin].segment_no) {
			end++;
		}
		counts.clear();
		idx_t current_gram = DConstants::INVALID_INDEX;
		for (idx_t i = begin; i < end; i++) {
			if (plan->descriptors[i].gram_index != current_gram) {
				current_gram = plan->descriptors[i].gram_index;
				counts.push_back(0);
			}
			if (!CheckedAdd(counts.back(), plan->descriptors[i].posting_count)) {
				throw InvalidInputException("ngram: segment posting count overflow; the index is malformed");
			}
		}
		auto segment_start = NumericCast<idx_t>(plan->descriptors[begin].segment_no) << SEGMENT_SHIFT;
		auto segment_capacity = MinValue<idx_t>((idx_t(1) << SEGMENT_SHIFT),
		                                        NumericCast<idx_t>(hwm) - segment_start + 1);
		for (auto count : counts) {
			if (count > segment_capacity) {
				throw InvalidInputException(
				    "ngram: gram posting count exceeds its segment rowid range; the index is malformed");
			}
		}
		if (counts.size() != plan->grams.size()) {
			begin = end;
			continue;
		}

		idx_t candidate_bound = segment_capacity;
		for (auto count : counts) {
			candidate_bound = MinValue(candidate_bound, count);
		}
		plan->segments.push_back(ProbeSegment {plan->descriptors[begin].segment_no, begin, end});
		if (!resource_decline.empty()) {
			begin = end;
			continue;
		}
		for (auto count : counts) {
			if (!CheckedAdd(estimated_decoded_rowids, count)) {
				resource_decline = "probe work estimate overflow";
				break;
			}
		}
		if (!resource_decline.empty()) {
			begin = end;
			continue;
		}
		if (estimated_decoded_rowids > hard_work_limit) {
			resource_decline = "decoded-rowid work budget exceeded";
			begin = end;
			continue;
		}
		if (!CheckedAdd(plan->candidate_upper_bound, candidate_bound)) {
			resource_decline = "candidate estimate overflow";
			begin = end;
			continue;
		}

		idx_t peak_rows = counts[0];
		if (counts.size() > 1) {
			idx_t current_bound = MinValue(segment_capacity, counts[0]);
			for (idx_t gram = 1; gram < counts.size(); gram++) {
				auto result_bound = MinValue(current_bound, MinValue(segment_capacity, counts[gram]));
				idx_t step_peak = current_bound;
				if (!CheckedAdd(step_peak, counts[gram]) || !CheckedAdd(step_peak, result_bound)) {
					resource_decline = "segment memory estimate overflow";
					break;
				}
				peak_rows = MaxValue(peak_rows, step_peak);
				current_bound = result_bound;
			}
		}
		if (!resource_decline.empty()) {
			begin = end;
			continue;
		}
		idx_t peak_bytes;
		if (!CheckedMultiply(peak_rows, sizeof(row_t), peak_bytes) ||
		    !CheckedAdd(peak_bytes, idx_t(256 * 1024))) {
			resource_decline = "segment memory estimate overflow";
			begin = end;
			continue;
		}
		peak_worker_bytes = MaxValue(peak_worker_bytes, peak_bytes);
		begin = end;
	}

	if (resource_decline.empty() && estimated_decoded_rowids > std::numeric_limits<idx_t>::max() / sizeof(row_t)) {
		resource_decline = "decoded-byte estimate overflow";
	}
	if (!resource_decline.empty()) {
		plan->decline_reason = std::move(resource_decline);
		return plan;
	}
	if (candidate_fraction >= 0 &&
	    static_cast<double>(plan->candidate_upper_bound) > candidate_fraction * static_cast<double>(table_rows)) {
		plan->decline_reason = "candidate fraction exceeded";
		return plan;
	}
	if (plan->segments.empty()) {
		plan->admitted = true;
		return plan;
	}
	if (peak_worker_bytes > memory_budget - reserved_bytes) {
		plan->decline_reason = "one posting segment exceeds query memory budget";
		return plan;
	}
	auto possible_workers = (memory_budget - reserved_bytes) / peak_worker_bytes;
	plan->max_threads = MinValue<idx_t>(plan->segments.size(),
	                                  MinValue<idx_t>(worker_cap, MinValue<idx_t>(ProbeThreads(context), possible_workers)));
	D_ASSERT(plan->max_threads > 0);
	idx_t workspace_bytes;
	if (!CheckedMultiply(peak_worker_bytes, plan->max_threads, workspace_bytes)) {
		plan->decline_reason = "worker memory estimate overflow";
		return plan;
	}
	plan->memory_reservation->Grow(workspace_bytes);
	plan->admitted = true;
	return plan;
}

static void DecodeDescriptorRange(ClientContext &context, DuckTransaction &tx, ProbePlan &plan,
                                  const ProbeSegment &segment, idx_t gram_index, idx_t &descriptor_cursor,
                                  vector<row_t> &postings) {
	auto &descriptors = plan.descriptors;
	idx_t begin = descriptor_cursor;
	idx_t end = begin;
	idx_t expected = 0;
	while (end < segment.descriptor_end && descriptors[end].gram_index == gram_index) {
		if (!CheckedAdd(expected, descriptors[end].posting_count)) {
			throw InvalidInputException("ngram: segment posting count overflow; the index is malformed");
		}
		end++;
	}
	if (begin == end) {
		throw InvalidInputException("ngram: admitted segment is missing a gram; the index is malformed");
	}
	descriptor_cursor = end;
	vector<row_t>().swap(postings);
	postings.reserve(expected);

	vector<StorageIndex> column_ids;
	vector<LogicalType> types;
	AddShadowColumn(*plan.segments_entry, "gram", LogicalTypeId::VARCHAR, column_ids, types);
	AddShadowColumn(*plan.segments_entry, "segment_no", LogicalTypeId::BIGINT, column_ids, types);
	AddShadowColumn(*plan.segments_entry, "postings", LogicalTypeId::BLOB, column_ids, types);
	AddShadowColumn(*plan.segments_entry, "rowid_count", LogicalTypeId::BIGINT, column_ids, types);
	DataChunk chunk;
	chunk.Initialize(Allocator::Get(context), types);
	ColumnFetchState fetch_state;
	Vector rowids(LogicalType::ROW_TYPE, STANDARD_VECTOR_SIZE);
	auto rowid_data = FlatVector::GetData<row_t>(rowids);
	for (idx_t offset = begin; offset < end; offset += STANDARD_VECTOR_SIZE) {
		if (context.interrupted.load(std::memory_order_relaxed)) {
			throw InterruptException();
		}
		auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, end - offset);
		for (idx_t i = 0; i < count; i++) {
			rowid_data[i] = descriptors[offset + i].posting_rowid;
		}
		chunk.Reset();
		// ColumnFetchState retains every pinned block it has seen. Drop the
		// previous batch only after its BLOBs have been decoded so fragmented
		// generations cannot accumulate query-wide pins.
		fetch_state = ColumnFetchState();
		plan.segments_entry->GetStorage().Fetch(tx, chunk, column_ids, rowids, count, fetch_state);
		if (chunk.size() != count) {
			throw InvalidInputException("ngram: a manifest posting row vanished; the index is malformed");
		}
		UnifiedVectorFormat gram_format, segment_format, blob_format, count_format;
		chunk.data[0].ToUnifiedFormat(count, gram_format);
		chunk.data[1].ToUnifiedFormat(count, segment_format);
		chunk.data[2].ToUnifiedFormat(count, blob_format);
		chunk.data[3].ToUnifiedFormat(count, count_format);
		auto gram_data = UnifiedVectorFormat::GetData<string_t>(gram_format);
		auto segment_data = UnifiedVectorFormat::GetData<int64_t>(segment_format);
		auto blob_data = UnifiedVectorFormat::GetData<string_t>(blob_format);
		auto count_data = UnifiedVectorFormat::GetData<int64_t>(count_format);
		for (idx_t r = 0; r < count; r++) {
			auto gram_idx = gram_format.sel->get_index(r);
			auto segment_idx = segment_format.sel->get_index(r);
			auto blob_idx = blob_format.sel->get_index(r);
			auto count_idx = count_format.sel->get_index(r);
			if (!gram_format.validity.RowIsValid(gram_idx) || !segment_format.validity.RowIsValid(segment_idx) ||
			    !blob_format.validity.RowIsValid(blob_idx) || !count_format.validity.RowIsValid(count_idx)) {
				throw InvalidInputException("ngram: segments table contains NULLs; the index is malformed");
			}
			auto &gram = gram_data[gram_idx];
			auto &blob = blob_data[blob_idx];
			auto encoded_count = PostingsCount(blob.GetData(), blob.GetSize());
			auto &descriptor = descriptors[offset + r];
			if (gram != string_t(plan.grams[gram_index]) ||
			    segment_data[segment_idx] != segment.segment_no || count_data[count_idx] <= 0 ||
			    NumericCast<idx_t>(count_data[count_idx]) != descriptor.posting_count ||
			    encoded_count != descriptor.posting_count || encoded_count > expected - postings.size()) {
				throw InvalidInputException("ngram: posting row disagrees with its manifest; the index is malformed");
			}
			DecodePostings(blob.GetData(), blob.GetSize(), postings);
			plan.decoded_rowids.fetch_add(encoded_count);
		}
	}
	if (postings.size() != expected) {
		throw InvalidInputException("ngram: posting row count disagrees with its manifest; the index is malformed");
	}
	if (end - begin > 1) {
		// Each individual blob is strictly ascending. Only generation union
		// needs sorting and a cross-generation duplicate check.
		std::sort(postings.begin(), postings.end());
		if (std::adjacent_find(postings.begin(), postings.end()) != postings.end()) {
			throw InvalidInputException("ngram: refresh generations contain duplicate rowids; the index is malformed");
		}
	}
	auto segment_start = segment.segment_no << SEGMENT_SHIFT;
	auto segment_end = segment_start + (int64_t(1) << SEGMENT_SHIFT);
	for (auto rowid : postings) {
		if (rowid < segment_start || rowid >= segment_end || rowid > plan.hwm) {
			throw InvalidInputException("ngram: posting rowid lies outside its segment; the index is malformed");
		}
	}
}

bool NextCandidateSegment(ClientContext &context, DuckTransaction &tx, ProbePlan &plan, vector<row_t> &candidates,
                          idx_t &segment_ordinal) {
	segment_ordinal = plan.next_segment.fetch_add(1);
	vector<row_t>().swap(candidates);
	if (segment_ordinal >= plan.segments.size()) {
		return false;
	}
	auto &segment = plan.segments[segment_ordinal];
	idx_t descriptor_cursor = segment.descriptor_begin;
	vector<row_t> postings;
	DecodeDescriptorRange(context, tx, plan, segment, 0, descriptor_cursor, postings);
	candidates = std::move(postings);
	for (idx_t gram = 1; gram < plan.grams.size() && !candidates.empty(); gram++) {
		DecodeDescriptorRange(context, tx, plan, segment, gram, descriptor_cursor, postings);
		vector<row_t> intersection;
		intersection.reserve(MinValue(candidates.size(), postings.size()));
		std::set_intersection(candidates.begin(), candidates.end(), postings.begin(), postings.end(),
		                      std::back_inserter(intersection));
		candidates = std::move(intersection);
	}
	return true;
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
	constexpr idx_t FETCH_BATCHES_PER_SEGMENT = (idx_t(1) << SEGMENT_SHIFT) / STANDARD_VECTOR_SIZE;
	state.fetch_batch_base = state.probe ? state.probe->segments.size() * FETCH_BATCHES_PER_SEGMENT : 0;
	state.max_threads = (state.probe ? state.probe->max_threads : 0) + SearchCoreScanUnits(context, *state.storage, state);
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
                       SearchCoreLocal &local,
                       const std::function<idx_t(DataChunk &, SelectionVector &)> &recheck, DataChunk &output) {
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
			constexpr idx_t FETCH_BATCHES_PER_SEGMENT = (idx_t(1) << SEGMENT_SHIFT) / STANDARD_VECTOR_SIZE;
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

static DuckTableEntry &ResolveSearchBase(ClientContext &context, const QueryBindData &bind) {
	auto &base = ResolveExistingTable(context, bind.catalog_name, bind.schema_name, bind.table_name, "table");
	// a prepared statement can outlive DROP+CREATE; re-validate the schema the
	// query was bound against
	idx_t position = 0;
	for (auto &col : base.GetColumns().Logical()) {
		if (col.Generated() || position >= bind.types.size() || col.Name() != bind.names[position] ||
		    col.Type() != bind.types[position]) {
			throw InvalidInputException("ngram_search: table %s changed since the query was bound; re-prepare it",
			                            bind.table_name);
		}
		position++;
	}
	if (position != bind.types.size()) {
		throw InvalidInputException("ngram_search: table %s changed since the query was bound; re-prepare it",
		                            bind.table_name);
	}
	return base;
}

//! An upper bound on the row groups the tail scan can hand out, so the
//! executor spawns threads in proportion to the work. A negative high-water
//! mark means the tail scan is a full scan (short needle, or an index built on
//! an empty table). Always at least one, for the transaction-local rows.
static unique_ptr<GlobalTableFunctionState> SearchInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind = input.bind_data->Cast<QueryBindData>();
	auto state = make_uniq<SearchGlobalState>();

	auto &base = ResolveSearchBase(context, bind);
	auto &storage = base.GetStorage();
	state->core.storage = &storage;
	state->core.tx = &DuckTransaction::Get(context, base.ParentCatalog());
	state->vacuum_lock = DuckTransactionManager::Get(storage.GetAttached()).SharedVacuumLock();

	state->options = bind.bound_options;
	auto meta = TryResolveExistingTable(context, bind.catalog_name, bind.shadow_schema,
	                                    MetaTableName(bind.column_name), "ngram index meta table");
	if (!meta) {
		state->fallback_reason = "index unavailable";
	} else {
		auto info = ReadMeta(context, *state->core.tx, *meta, bind.Target());
		state->options = info.options;
		auto stale_reason = CertainStaleReason(info, ComputeTableFingerprint(context, base));
		if (!stale_reason.empty()) {
			state->fallback_reason = "index stale";
		} else {
			auto guard_reason = RowIdGuardReason(context, base, info);
			if (!guard_reason.empty()) {
				state->fallback_reason = "rowid guard: " + guard_reason;
			} else {
				state->core.hwm = info.hwm_rowid;
			}
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
			throw InternalException("ngram_search: unsupported virtual column");
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
		auto segments = TryResolveExistingTable(context, bind.catalog_name, bind.shadow_schema,
		                                        SegmentsTableName(bind.column_name), "ngram index segments table");
		auto stats = TryResolveExistingTable(context, bind.catalog_name, bind.shadow_schema,
		                                     StatsTableName(bind.column_name), "ngram index stats table");
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
	ExecuteSearchCore(context, data, state.core, lstate.core,
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
		result["Ngram Mode"] = StringUtil::Format("index (<= %llu candidates, %llu decoded rowids)",
		                                          state.core.probe->candidate_upper_bound,
		                                          state.core.probe->decoded_rowids.load());
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

	auto &meta = ResolveExistingTable(context, bind.catalog_name, bind.shadow_schema, MetaTableName(bind.column_name),
	                                  "ngram index meta table");
	auto info = ReadMeta(context, *state->tx, meta, bind.Target());
	ThrowIfCandidatesCertainlyStale(context, base, info, bind);
	state->hwm = info.hwm_rowid;
	auto guard_reason = RowIdGuardReason(context, base, info);

	auto decomposition = DecomposeNeedle(bind.needle.data(), bind.needle.size(), info.options);
	if (!guard_reason.empty()) {
		state->all_rowids = true;
	} else if (decomposition.too_short) {
		state->all_rowids = true;
	} else {
		auto &segments = ResolveExistingTable(context, bind.catalog_name, bind.shadow_schema,
		                                      SegmentsTableName(bind.column_name), "ngram index segments table");
		auto &stats = ResolveExistingTable(context, bind.catalog_name, bind.shadow_schema,
		                                   StatsTableName(bind.column_name), "ngram index stats table");
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
		result["Ngram Decoded Rowids"] = to_string(state.probe->decoded_rowids.load());
	}
	return result;
}

//===----------------------------------------------------------------------===//
// Registration
//===----------------------------------------------------------------------===//

void RegisterSearchFunctions(ExtensionLoader &loader) {
	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	config.AddExtensionOption("ngram_max_grams_per_query",
	                          "ngram index queries probe at most this many of the needle's rarest grams",
	                          LogicalType::BIGINT, Value::BIGINT(DEFAULT_MAX_GRAMS_PER_QUERY));
	config.AddExtensionOption("ngram_max_candidate_fraction",
	                          "full-result ngram queries scan when the candidate upper bound exceeds this fraction",
	                          LogicalType::DOUBLE, Value::DOUBLE(DEFAULT_MAX_CANDIDATE_FRACTION));
	config.AddExtensionOption("ngram_max_probe_rowids",
	                          "hard limit on posting rowids an ngram query may decode before scanning or erroring",
	                          LogicalType::BIGINT, Value::BIGINT(DEFAULT_MAX_PROBE_ROWIDS));

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
