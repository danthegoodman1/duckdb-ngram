#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parallel/task_executor.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/local_storage.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "ngram/index_pragmas.hpp"
#include "ngram/maintenance.hpp"
#include "ngram/ngram_search.hpp"
#include "ngram/postings_codec.hpp"
#include "ngram/search_core.hpp"
#include "ngram/trigram.hpp"

#include <algorithm>
#include <functional>

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
// ngram_search(table, needle[, column := ...]) returns exactly the rows a
// brute-force scan would return: candidates are fetched through
// DataTable::Fetch and rechecked against the real predicate, then a tail scan
// (rowid > high-water mark, which also covers transaction-local rows) unions
// in every row the index has never seen. A shared vacuum lock is held from
// index probe through the last fetch so checkpoint vacuum cannot move rowids
// mid-query.
//
// Both phases run on as many threads as there is work for. The probe happens
// once, in init_global; after it the candidate list is immutable and threads
// claim disjoint STANDARD_VECTOR_SIZE blocks of it through an atomic counter,
// while the tail scan hands out one row group at a time through DuckDB's own
// parallel cursor (which covers the transaction-local rows after the
// committed ones). The same rows are visited, once each; output order is
// preserved for ordered sinks by the batch index the scan reports.
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
//! exhaustive for INSERTs and DELETEs as long as indexed rowids are where the
//! build recorded them: rows past the index high-water mark and uncommitted
//! transaction-local rows are found by a brute-force tail scan, deleted rows
//! are filtered by transaction visibility, and every candidate is rechecked
//! against the real string. Recheck makes false positives impossible in every
//! scenario.
//!
//! The staleness contract, in full:
//! (a) An in-place UPDATE of a row at or below the high-water mark is missed
//!     until the index is rebuilt. duckdb v1.5.5 updates rows in place (unless
//!     an ART index forces delete+insert) and offers no trigger or change feed
//!     naming the updated rows — CREATE TRIGGER is a parser error — so there
//!     is no sound way to find them incrementally; PRAGMA ngram_refresh
//!     therefore covers appends only, and updated rows need drop + create.
//! (b) Checkpoint vacuum after DELETEs of indexed rows merges row groups and
//!     moves surviving rowids out from under the index's postings. The shared
//!     vacuum lock held for the duration of every query means this can only
//!     happen between queries, never mid-query. Where the table is left
//!     shorter than the recorded high-water mark this is detected here and
//!     raised as an error; a vacuum whose row loss is masked by later appends
//!     before anything looks is not detectable on v1.5.5 (see
//!     ngram/maintenance.hpp) and stays a misses-only gap.
//! (c) DROP TABLE + re-CREATE under the same name: the shadow schema survives
//!     the drop and the name-based ownership guard matches the new
//!     incarnation. Detected — and refused here — whenever the recreation
//!     happened in the running database instance (catalog oids are handed out
//!     per process, so a recorded oid is comparable only within the instance
//!     that wrote it) or changed the table's column list. A same-shape
//!     recreation in an earlier session is not detectable: v1.5.5 persists no
//!     table identity token, and catalog dependencies are in-memory only.
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
	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, name);
	auto entry = Catalog::GetEntry(context, catalog, schema, lookup, OnEntryNotFound::RETURN_NULL);
	if (!entry || entry->type != CatalogType::TABLE_ENTRY || !entry->Cast<TableCatalogEntry>().IsDuckTable()) {
		throw CatalogException("ngram: %s %s.%s no longer exists; was the table or index dropped after binding?", what,
		                       schema, name);
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

MetaInfo ReadMeta(ClientContext &context, DuckTransaction &tx, DuckTableEntry &meta_entry, const ShadowTarget &bind) {
	MetaInfo info;
	{
		// columns every format version has: ownership and the version itself,
		// checked before reading anything version-specific
		vector<StorageIndex> column_ids;
		vector<LogicalType> types;
		AddShadowColumn(meta_entry, "format_version", LogicalTypeId::INTEGER, column_ids, types);
		AddShadowColumn(meta_entry, "schema_name", LogicalTypeId::VARCHAR, column_ids, types);
		AddShadowColumn(meta_entry, "table_name", LogicalTypeId::VARCHAR, column_ids, types);
		ScanMetaRow(context, tx, meta_entry, bind, column_ids, types, [&](DataChunk &chunk, idx_t r) {
			// the shadow schema name is not injective across base tables; refuse
			// to answer through a shadow schema owned by someone else
			auto owner_schema = StringValue::Get(chunk.GetValue(1, r));
			auto owner_table = StringValue::Get(chunk.GetValue(2, r));
			if (!StringUtil::CIEquals(owner_schema, bind.schema_name) ||
			    !StringUtil::CIEquals(owner_table, bind.table_name)) {
				throw CatalogException("ngram shadow schema collision: %s belongs to the index on %s.%s, not to "
				                       "%s.%s; no usable ngram index exists",
				                       bind.shadow_schema, owner_schema, owner_table, bind.schema_name,
				                       bind.table_name);
			}
			auto format_version = chunk.GetValue(0, r).GetValue<int64_t>();
			if (format_version != NGRAM_FORMAT_VERSION) {
				// never guess at another version's column layout
				throw InvalidInputException(
				    "ngram: the index on %s.%s uses meta format_version %lld, but this version of the extension "
				    "writes and reads format_version %lld; drop and rebuild the index",
				    bind.table_name, bind.column_name, format_version, NGRAM_FORMAT_VERSION);
			}
		});
	}
	vector<StorageIndex> column_ids;
	vector<LogicalType> types;
	AddShadowColumn(meta_entry, "gram_size", LogicalTypeId::INTEGER, column_ids, types);
	AddShadowColumn(meta_entry, "case_insensitive", LogicalTypeId::BOOLEAN, column_ids, types);
	AddShadowColumn(meta_entry, "hwm_rowid", LogicalTypeId::BIGINT, column_ids, types);
	AddShadowColumn(meta_entry, "schema_fingerprint", LogicalTypeId::VARCHAR, column_ids, types);
	AddShadowColumn(meta_entry, "table_oid", LogicalTypeId::BIGINT, column_ids, types);
	AddShadowColumn(meta_entry, "instance_id", LogicalTypeId::VARCHAR, column_ids, types);
	AddShadowColumn(meta_entry, "row_samples", LogicalTypeId::VARCHAR, column_ids, types);
	AddShadowColumn(meta_entry, "column_name", LogicalTypeId::VARCHAR, column_ids, types);
	AddShadowColumn(meta_entry, "catalog_oid", LogicalTypeId::BIGINT, column_ids, types);
	AddShadowColumn(meta_entry, "column_type", LogicalTypeId::VARCHAR, column_ids, types);
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
		info.schema_fingerprint = StringValue::Get(chunk.GetValue(3, r));
		info.table_oid = chunk.GetValue(4, r).GetValue<int64_t>();
		info.instance_id = StringValue::Get(chunk.GetValue(5, r));
		info.row_samples = StringValue::Get(chunk.GetValue(6, r));
		info.column_name = StringValue::Get(chunk.GetValue(7, r));
		info.catalog_oid = chunk.GetValue(8, r).GetValue<int64_t>();
		info.column_type = StringValue::Get(chunk.GetValue(9, r));
	});
	return info;
}

//! The explicit path answers with the index or not at all: when a detector
//! proves the index no longer describes the table, returning the rows it can
//! still find would be a silent, unbounded miss, so the query fails with the
//! repair instead. Costs one O(1) read of the table's rowid count plus its
//! column list; the row-group layout check is left to the maintenance
//! pragmas, where walking every row group is affordable.
static void ThrowIfCertainlyStale(ClientContext &context, DuckTableEntry &base, const MetaInfo &info,
                                  const QueryBindData &bind, const char *fn) {
	auto reason = CertainStaleReason(info, ComputeTableFingerprint(context, base));
	if (reason.empty()) {
		return;
	}
	throw InvalidInputException("%s: the ngram index on %s.%s is stale and cannot be used: %s. Rebuild it: PRAGMA "
	                            "drop_ngram_index('%s', '%s') then PRAGMA create_ngram_index('%s', '%s')",
	                            fn, bind.table_name, bind.column_name, reason, bind.table_name, bind.column_name,
	                            bind.table_name, bind.column_name);
}

//===----------------------------------------------------------------------===//
// Index probe: rarest-first selection + per-segment intersection
//===----------------------------------------------------------------------===//

//! Run `body(unit)` for every unit in [0, units) across the scheduler's
//! threads, or inline when there is only one of either. The probe's two scans
//! and its per-segment intersection are read-only passes whose results the
//! caller combines, so they parallelize with a plain TaskExecutor and need no
//! operator pipeline of their own. Every unit writes to its own slot, so the
//! work is deterministic: the same units produce the same values whatever
//! order the threads run them in.
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

//! A gram's postings inside one segment, decoded. The codec writes every blob
//! sorted and deduplicated, and every rowid of a (gram, segment_no) blob lies
//! inside that segment's 2^SEGMENT_SHIFT window — so two segments never
//! overlap, and concatenating them in ascending order produces a globally
//! sorted list with no sort at all. Only a key carrying more than one blob (a
//! refresh generation, or a partial segment left by an index an older version
//! of this extension built) needs merging.
struct SegmentPostings {
	int64_t segment_no = 0;
	vector<row_t> rowids;
};

//! Posting counts per needle gram, for rarest-first selection. A rowid indexed
//! under the same gram by two generations is counted twice, which only biases
//! the ordering, never the result.
static unordered_map<string, int64_t> GramPostingCounts(ClientContext &context, DuckTransaction &tx,
                                                        DuckTableEntry &stats_entry, const vector<string> &grams,
                                                        idx_t workers) {
	unordered_map<string, idx_t> gram_index;
	for (idx_t i = 0; i < grams.size(); i++) {
		gram_index.emplace(grams[i], i);
	}
	// one row of counters per worker, summed at the end: no sharing, and the
	// sum is the same whatever order the row groups were claimed in
	vector<vector<int64_t>> partials(workers, vector<int64_t>(grams.size(), 0));

	vector<StorageIndex> column_ids;
	vector<LogicalType> types;
	AddShadowColumn(stats_entry, "gram", LogicalTypeId::VARCHAR, column_ids, types);
	AddShadowColumn(stats_entry, "row_count", LogicalTypeId::BIGINT, column_ids, types);
	ParallelScanShadowTable(context, tx, stats_entry.GetStorage(), column_ids, types, nullptr, workers,
	                        [&](DataChunk &chunk, idx_t worker) {
		                        UnifiedVectorFormat gram_format, count_format;
		                        chunk.data[0].ToUnifiedFormat(chunk.size(), gram_format);
		                        chunk.data[1].ToUnifiedFormat(chunk.size(), count_format);
		                        auto gram_data = UnifiedVectorFormat::GetData<string_t>(gram_format);
		                        auto count_data = UnifiedVectorFormat::GetData<int64_t>(count_format);
		                        auto &partial = partials[worker];
		                        string gram_scratch;
		                        for (idx_t r = 0; r < chunk.size(); r++) {
			                        auto gram_idx = gram_format.sel->get_index(r);
			                        auto count_idx = count_format.sel->get_index(r);
			                        if (!gram_format.validity.RowIsValid(gram_idx) ||
			                            !count_format.validity.RowIsValid(count_idx)) {
				                        continue;
			                        }
			                        gram_scratch.assign(gram_data[gram_idx].GetData(), gram_data[gram_idx].GetSize());
			                        auto entry = gram_index.find(gram_scratch);
			                        if (entry != gram_index.end()) {
				                        partial[entry->second] += count_data[count_idx];
			                        }
		                        }
	                        });

	unordered_map<string, int64_t> counts;
	for (idx_t i = 0; i < grams.size(); i++) {
		int64_t total = 0;
		for (auto &partial : partials) {
			total += partial[i];
		}
		counts[grams[i]] = total;
	}
	return counts;
}

//! Distinct segments the surviving candidates occupy, ascending. Candidates are
//! sorted, so this is one pass and no hashing.
static vector<int64_t> LiveSegments(const vector<row_t> &candidates) {
	vector<int64_t> live;
	for (auto rowid : candidates) {
		auto segment_no = rowid >> SEGMENT_SHIFT;
		if (live.empty() || live.back() != segment_no) {
			live.push_back(segment_no);
		}
	}
	return live;
}

//! Decode every blob a gram has, in parallel, one SegmentPostings per blob.
static vector<SegmentPostings> ScanGramSegments(ClientContext &context, DuckTransaction &tx,
                                                DuckTableEntry &segments_entry, const vector<StorageIndex> &column_ids,
                                                const vector<LogicalType> &types, const string &gram, bool first,
                                                const vector<row_t> &candidates, const vector<int64_t> &live_segments,
                                                idx_t workers) {
	// one filtered scan per gram: the equality filter prunes row groups via
	// zone maps and skips reading postings blobs of non-matching rows
	TableFilterSet filters;
	filters.PushFilter(ColumnIndex(0), make_uniq<ConstantFilter>(ExpressionType::COMPARE_EQUAL, Value(gram)));

	vector<vector<SegmentPostings>> partials(workers);
	ParallelScanShadowTable(
	    context, tx, segments_entry.GetStorage(), column_ids, types, &filters, workers,
	    [&](DataChunk &chunk, idx_t worker) {
		    UnifiedVectorFormat segment_format, blob_format, min_format, max_format;
		    chunk.data[1].ToUnifiedFormat(chunk.size(), segment_format);
		    chunk.data[2].ToUnifiedFormat(chunk.size(), blob_format);
		    chunk.data[3].ToUnifiedFormat(chunk.size(), min_format);
		    chunk.data[4].ToUnifiedFormat(chunk.size(), max_format);
		    auto segment_data = UnifiedVectorFormat::GetData<int64_t>(segment_format);
		    auto blob_data = UnifiedVectorFormat::GetData<string_t>(blob_format);
		    auto min_data = UnifiedVectorFormat::GetData<int64_t>(min_format);
		    auto max_data = UnifiedVectorFormat::GetData<int64_t>(max_format);
		    auto &partial = partials[worker];
		    for (idx_t r = 0; r < chunk.size(); r++) {
			    auto segment_idx = segment_format.sel->get_index(r);
			    auto blob_idx = blob_format.sel->get_index(r);
			    auto min_idx = min_format.sel->get_index(r);
			    auto max_idx = max_format.sel->get_index(r);
			    if (!segment_format.validity.RowIsValid(segment_idx) || !blob_format.validity.RowIsValid(blob_idx) ||
			        !min_format.validity.RowIsValid(min_idx) || !max_format.validity.RowIsValid(max_idx)) {
				    throw InvalidInputException("ngram: segments table contains NULLs; the index is malformed");
			    }
			    if (!first) {
				    // skip blobs that provably cannot intersect the surviving
				    // candidates (pure optimization: dropped postings are
				    // outside the candidate set, so the intersection is
				    // unchanged)
				    if (!std::binary_search(live_segments.begin(), live_segments.end(), segment_data[segment_idx])) {
					    continue;
				    }
				    auto lower = std::lower_bound(candidates.begin(), candidates.end(), min_data[min_idx]);
				    if (lower == candidates.end() || *lower > max_data[max_idx]) {
					    continue;
				    }
			    }
			    auto &blob = blob_data[blob_idx];
			    SegmentPostings decoded;
			    decoded.segment_no = segment_data[segment_idx];
			    DecodePostings(blob.GetData(), blob.GetSize(), decoded.rowids);
			    partial.push_back(std::move(decoded));
		    }
	    });

	vector<SegmentPostings> scanned;
	idx_t total = 0;
	for (auto &partial : partials) {
		total += partial.size();
	}
	scanned.reserve(total);
	for (auto &partial : partials) {
		for (auto &entry : partial) {
			scanned.push_back(std::move(entry));
		}
	}
	// blobs arrive in whatever order the workers claimed row groups; ordering
	// them by segment is what makes the result independent of that
	std::sort(scanned.begin(), scanned.end(),
	          [](const SegmentPostings &a, const SegmentPostings &b) { return a.segment_no < b.segment_no; });
	return scanned;
}

//! Fold a gram's blobs into the surviving candidate list. Each segment is
//! handled independently — segments partition the rowid space, so a segment's
//! slice of `candidates` is a contiguous range — and the per-segment results
//! are concatenated in segment order, which is already globally sorted.
static vector<row_t> FoldSegments(ClientContext &context, vector<SegmentPostings> &scanned,
                                  const vector<row_t> &candidates, bool first, idx_t workers) {
	// group boundaries: scanned is sorted by segment, and a key with a single
	// blob (the common case) needs neither merge nor sort
	vector<idx_t> group_starts;
	for (idx_t i = 0; i < scanned.size(); i++) {
		if (i == 0 || scanned[i].segment_no != scanned[i - 1].segment_no) {
			group_starts.push_back(i);
		}
	}
	vector<vector<row_t>> results(group_starts.size());
	ParallelForEachUnit(context, group_starts.size(), workers, [&](idx_t group) {
		auto begin = group_starts[group];
		auto end = group + 1 < group_starts.size() ? group_starts[group + 1] : scanned.size();

		vector<row_t> union_scratch;
		if (end - begin > 1) {
			for (auto i = begin; i < end; i++) {
				union_scratch.insert(union_scratch.end(), scanned[i].rowids.begin(), scanned[i].rowids.end());
			}
			std::sort(union_scratch.begin(), union_scratch.end());
			union_scratch.erase(std::unique(union_scratch.begin(), union_scratch.end()), union_scratch.end());
		}
		// the single-blob case is the common one and owns its rowids already, so
		// the first gram hands them straight over rather than copying what can
		// be hundreds of millions of entries
		auto &merged = end - begin > 1 ? union_scratch : scanned[begin].rowids;
		if (first) {
			results[group] = std::move(merged);
			return;
		}
		auto segment_start = scanned[begin].segment_no << SEGMENT_SHIFT;
		auto segment_end = segment_start + (int64_t(1) << SEGMENT_SHIFT);
		auto lower = std::lower_bound(candidates.begin(), candidates.end(), static_cast<row_t>(segment_start));
		auto upper = std::lower_bound(lower, candidates.end(), static_cast<row_t>(segment_end));
		std::set_intersection(lower, upper, merged.begin(), merged.end(), std::back_inserter(results[group]));
	});

	idx_t total = 0;
	for (auto &result : results) {
		total += result.size();
	}
	vector<row_t> folded;
	folded.reserve(total);
	for (auto &result : results) {
		folded.insert(folded.end(), result.begin(), result.end());
	}
	return folded;
}

//! Candidate rowids among indexed rows for a decomposed needle: the sorted
//! intersection of the posting lists of the up-to-max_grams rarest grams.
//! Every narrowing here is superset-preserving: probing fewer grams can only
//! grow the intersection, and a gram with no postings proves no indexed row
//! contains the needle.
//!
//! Grams are probed in sequence because each one's live-segment and min/max
//! pruning depends on the survivors of the last; the work inside a gram — the
//! filtered scan, the blob decode, and the intersection — runs across the
//! scheduler's threads. The candidate set is a deterministic function of the
//! index and the needle: thread count and claim order change nothing.
vector<row_t> ProbeIndex(ClientContext &context, DuckTransaction &tx, DuckTableEntry &segments_entry,
                         DuckTableEntry &stats_entry, const vector<string> &grams, idx_t max_grams) {
	D_ASSERT(!grams.empty());
	auto workers = ProbeThreads(context);
	auto counts = GramPostingCounts(context, tx, stats_entry, grams, workers);

	vector<string> ordered = grams;
	std::stable_sort(ordered.begin(), ordered.end(),
	                 [&](const string &a, const string &b) { return counts[a] < counts[b]; });
	if (counts[ordered[0]] == 0) {
		// a needle gram indexed in no row: no indexed row can contain the needle
		return vector<row_t>();
	}
	if (ordered.size() > max_grams) {
		ordered.resize(max_grams);
	}

	vector<StorageIndex> column_ids;
	vector<LogicalType> types;
	AddShadowColumn(segments_entry, "gram", LogicalTypeId::VARCHAR, column_ids, types);
	AddShadowColumn(segments_entry, "segment_no", LogicalTypeId::BIGINT, column_ids, types);
	AddShadowColumn(segments_entry, "postings", LogicalTypeId::BLOB, column_ids, types);
	AddShadowColumn(segments_entry, "min_rowid", LogicalTypeId::BIGINT, column_ids, types);
	AddShadowColumn(segments_entry, "max_rowid", LogicalTypeId::BIGINT, column_ids, types);

	vector<row_t> candidates;
	for (idx_t gram_idx = 0; gram_idx < ordered.size(); gram_idx++) {
		bool first = gram_idx == 0;
		auto live_segments = first ? vector<int64_t>() : LiveSegments(candidates);
		auto scanned = ScanGramSegments(context, tx, segments_entry, column_ids, types, ordered[gram_idx], first,
		                                candidates, live_segments, workers);
		candidates = FoldSegments(context, scanned, candidates, first, workers);
		if (candidates.empty()) {
			break;
		}
	}
	return candidates;
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

enum class SearchPhase { FETCH, TAIL, DONE };

//! Everything the scan's threads share. Immutable once init_global returns,
//! except `next_fetch_block` (atomic) and `parallel_scan` (which hands out row
//! groups under its own mutex).
struct SearchGlobalState : public GlobalTableFunctionState {
	DataTable *storage = nullptr;
	DuckTransaction *tx = nullptr;
	//! Held from before the index probe until this state dies (after the last
	//! fetch): checkpoint vacuum must not move rowids while we hold candidate
	//! rowids. Unconditional, the safe superset of v1.5.5's built-in gate; a
	//! shared lock has no thread affinity, so one key covers every thread.
	unique_ptr<StorageLockKey> vacuum_lock;
	int64_t hwm = -1;
	//! The needle in comparison form plus the index's options; each thread
	//! copies these into its own RecheckState, which carries fold scratch.
	string needle_cmp;
	GramOptions options;
	idx_t search_column_idx = 0;
	vector<LogicalType> base_types;
	vector<StorageIndex> column_ids;

	//! Candidate rowids (sorted); empty in full-scan fallback mode. Threads
	//! claim STANDARD_VECTOR_SIZE-sized blocks of it by index.
	vector<row_t> candidates;
	atomic<idx_t> next_fetch_block {0};
	idx_t fetch_block_count = 0;

	//! The tail scan (rowid > hwm, which also covers transaction-local rows),
	//! handed out one row group at a time.
	vector<StorageIndex> tail_column_ids;
	vector<LogicalType> tail_types;
	unique_ptr<TableFilterSet> tail_filters;
	ParallelTableScanState parallel_scan;

	idx_t max_threads = 1;

	idx_t MaxThreads() const override {
		return max_threads;
	}
};

//! Per-thread scan state. Nothing here may be shared: DataTable::Fetch writes
//! through its ColumnFetchState, a TableScanState owns per-thread filter
//! state, and the recheck fold reuses a scratch buffer.
struct SearchLocalState : public LocalTableFunctionState {
	SearchPhase phase = SearchPhase::FETCH;

	DataChunk fetch_chunk;
	ColumnFetchState fetch_state;

	TableScanState tail_state;
	DataChunk tail_chunk;
	//! Whether tail_state currently holds a row group claimed from the
	//! parallel cursor.
	bool tail_unit_active = false;

	RecheckState recheck;
	SelectionVector sel;

	//! Batch index of the chunk this thread emitted last; ordered sinks
	//! reassemble the output by it.
	idx_t batch_index = 0;
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
static idx_t TailScanUnits(ClientContext &context, DataTable &storage, const SearchGlobalState &state) {
	if (state.hwm < 0) {
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

static unique_ptr<GlobalTableFunctionState> SearchInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind = input.bind_data->Cast<QueryBindData>();
	auto state = make_uniq<SearchGlobalState>();

	auto &base = ResolveSearchBase(context, bind);
	auto &storage = base.GetStorage();
	state->storage = &storage;
	state->tx = &DuckTransaction::Get(context, base.ParentCatalog());
	state->vacuum_lock = DuckTransactionManager::Get(storage.GetAttached()).SharedVacuumLock();

	auto &meta = ResolveExistingTable(context, bind.catalog_name, bind.shadow_schema, MetaTableName(bind.column_name),
	                                  "ngram index meta table");
	auto info = ReadMeta(context, *state->tx, meta, bind.Target());
	ThrowIfCertainlyStale(context, base, info, bind, "ngram_search");
	state->hwm = info.hwm_rowid;
	state->options = info.options;
	state->search_column_idx = bind.search_column_idx;
	state->base_types = bind.types;
	for (auto &col : base.GetColumns().Logical()) {
		state->column_ids.push_back(base.GetStorageIndex(ColumnIndex(col.Logical().index)));
	}

	if (info.options.case_insensitive) {
		vector<idx_t> offsets;
		NormalizeString(bind.needle.data(), bind.needle.size(), info.options, state->needle_cmp, offsets);
	} else {
		state->needle_cmp = bind.needle;
	}

	auto decomposition = DecomposeNeedle(bind.needle.data(), bind.needle.size(), info.options);
	if (decomposition.too_short) {
		// the index cannot be probed; the tail scan becomes a full scan, which
		// is still exhaustive
		state->hwm = -1;
	} else {
		auto &segments = ResolveExistingTable(context, bind.catalog_name, bind.shadow_schema,
		                                      SegmentsTableName(bind.column_name), "ngram index segments table");
		auto &stats = ResolveExistingTable(context, bind.catalog_name, bind.shadow_schema,
		                                   StatsTableName(bind.column_name), "ngram index stats table");
		state->candidates =
		    ProbeIndex(context, *state->tx, segments, stats, decomposition.grams, MaxGramsPerQuery(context));
		// the index never legitimately references rowids past its own
		// high-water mark; if it somehow does, dropping them here prevents
		// double-returning rows the tail scan will visit
		state->candidates.erase(
		    std::upper_bound(state->candidates.begin(), state->candidates.end(), static_cast<row_t>(state->hwm)),
		    state->candidates.end());
	}
	state->fetch_block_count = (state->candidates.size() + STANDARD_VECTOR_SIZE - 1) / STANDARD_VECTOR_SIZE;

	// the tail scan reads one extra trailing rowid column so the hwm filter
	// has something to apply to
	state->tail_column_ids = state->column_ids;
	state->tail_column_ids.push_back(StorageIndex(COLUMN_IDENTIFIER_ROW_ID));
	state->tail_types = state->base_types;
	state->tail_types.push_back(LogicalType::ROW_TYPE);
	if (state->hwm >= 0) {
		// rowid > hwm: zone maps skip fully-indexed row groups, and
		// transaction-local rows (rowid >= MAX_ROW_ID) always pass
		state->tail_filters = make_uniq<TableFilterSet>();
		state->tail_filters->PushFilter(
		    ColumnIndex(state->base_types.size()),
		    make_uniq<ConstantFilter>(ExpressionType::COMPARE_GREATERTHAN, Value::BIGINT(state->hwm)));
	}
	// The parallel cursor needs no empty-table special case: an empty row-group
	// collection leaves it with no current row group and the transaction-local
	// side is null-guarded, so the first claim reports "nothing left".
	storage.InitializeParallelScan(context, state->parallel_scan, NO_COLUMN_INDEXES);
	state->max_threads = state->fetch_block_count + TailScanUnits(context, storage, *state);
	return std::move(state);
}

static unique_ptr<LocalTableFunctionState> SearchInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                           GlobalTableFunctionState *global_state) {
	auto &gstate = global_state->Cast<SearchGlobalState>();
	auto state = make_uniq<SearchLocalState>();
	state->phase = gstate.fetch_block_count > 0 ? SearchPhase::FETCH : SearchPhase::TAIL;
	state->recheck.options = gstate.options;
	state->recheck.needle_cmp = gstate.needle_cmp;
	state->fetch_chunk.Initialize(Allocator::Get(context.client), gstate.base_types);
	state->tail_state.Initialize(gstate.tail_column_ids, &context.client, gstate.tail_filters.get());
	state->tail_chunk.Initialize(Allocator::Get(context.client), gstate.tail_types);
	state->sel.Initialize(STANDARD_VECTOR_SIZE);
	return std::move(state);
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

static void SearchFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<SearchGlobalState>();
	auto &lstate = data.local_state->Cast<SearchLocalState>();
	while (true) {
		switch (lstate.phase) {
		case SearchPhase::FETCH: {
			// claim the next block of candidate rowids; blocks are disjoint,
			// so the fetched rows partition the candidate set exactly once
			auto block = state.next_fetch_block++;
			if (block >= state.fetch_block_count) {
				lstate.phase = SearchPhase::TAIL;
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
				hits = lstate.recheck.Recheck(lstate.fetch_chunk, state.search_column_idx, lstate.sel);
			}
			if (hits == 0) {
				if (YieldEmpty(data)) {
					return;
				}
				continue;
			}
			output.Slice(lstate.fetch_chunk, lstate.sel, hits);
			return;
		}
		case SearchPhase::TAIL: {
			if (!lstate.tail_unit_active) {
				if (state.storage->NextParallelScan(context, state.parallel_scan, lstate.tail_state) == 0) {
					lstate.phase = SearchPhase::DONE;
					continue;
				}
				lstate.tail_unit_active = true;
			}
			lstate.tail_chunk.Reset();
			state.storage->Scan(*state.tx, lstate.tail_chunk, lstate.tail_state);
			if (lstate.tail_chunk.size() == 0) {
				// this row group is done; the next call claims another
				lstate.tail_unit_active = false;
				if (YieldEmpty(data)) {
					return;
				}
				continue;
			}
			// committed row groups are numbered from 1 and the local-storage
			// numbering resumes past the committed total, so tail batches
			// always sort after every fetch block
			lstate.batch_index = state.fetch_block_count + lstate.tail_state.table_state.batch_index +
			                     lstate.tail_state.local_state.batch_index;
			idx_t hits = lstate.recheck.Recheck(lstate.tail_chunk, state.search_column_idx, lstate.sel);
			if (hits == 0) {
				if (YieldEmpty(data)) {
					return;
				}
				continue;
			}
			// the tail chunk carries the filtered rowid as a trailing extra
			// column; emit only the base columns
			for (idx_t c = 0; c < output.ColumnCount(); c++) {
				output.data[c].Slice(lstate.tail_chunk.data[c], lstate.sel, hits);
			}
			output.SetCardinality(hits);
			return;
		}
		case SearchPhase::DONE:
			// this thread is retired; the others carry on independently
			return;
		}
	}
}

//! Ordered sinks reassemble a parallel scan's output by batch index. Fetch
//! blocks carry their block number and tail batches follow them, so the
//! reassembled order is the one the single-threaded scan produced: candidate
//! rowids ascending, then the tail in storage order.
static OperatorPartitionData SearchGetPartitionData(ClientContext &context, TableFunctionGetPartitionInput &input) {
	auto &lstate = input.local_state->Cast<SearchLocalState>();
	return OperatorPartitionData(lstate.batch_index);
}

//===----------------------------------------------------------------------===//
// ngram_candidates execution
//===----------------------------------------------------------------------===//

struct CandidatesGlobalState : public GlobalTableFunctionState {
	DataTable *storage = nullptr;
	DuckTransaction *tx = nullptr;
	unique_ptr<StorageLockKey> vacuum_lock;
	int64_t hwm = -1;

	//! Probed mode: emit this sorted list.
	vector<row_t> candidates;
	idx_t offset = 0;

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
	ThrowIfCertainlyStale(context, base, info, bind, "ngram_candidates");
	state->hwm = info.hwm_rowid;

	auto decomposition = DecomposeNeedle(bind.needle.data(), bind.needle.size(), info.options);
	if (decomposition.too_short) {
		state->all_rowids = true;
	} else {
		auto &segments = ResolveExistingTable(context, bind.catalog_name, bind.shadow_schema,
		                                      SegmentsTableName(bind.column_name), "ngram index segments table");
		auto &stats = ResolveExistingTable(context, bind.catalog_name, bind.shadow_schema,
		                                   StatsTableName(bind.column_name), "ngram index stats table");
		state->candidates =
		    ProbeIndex(context, *state->tx, segments, stats, decomposition.grams, MaxGramsPerQuery(context));
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
	auto remaining = state.candidates.size() - state.offset;
	idx_t count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, remaining);
	if (count == 0) {
		return;
	}
	auto result = FlatVector::GetData<int64_t>(output.data[0]);
	memcpy(result, state.candidates.data() + state.offset, count * sizeof(int64_t));
	state.offset += count;
	output.SetCardinality(count);
}

//===----------------------------------------------------------------------===//
// Registration
//===----------------------------------------------------------------------===//

void RegisterSearchFunctions(ExtensionLoader &loader) {
	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	config.AddExtensionOption("ngram_max_grams_per_query",
	                          "ngram index queries probe at most this many of the needle's rarest grams",
	                          LogicalType::BIGINT, Value::BIGINT(DEFAULT_MAX_GRAMS_PER_QUERY));

	TableFunction candidates("ngram_candidates", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                         CandidatesFunction, CandidatesBind, CandidatesInitGlobal);
	loader.RegisterFunction(candidates);

	TableFunction search("ngram_search", {LogicalType::VARCHAR, LogicalType::VARCHAR}, SearchFunction, SearchBind,
	                     SearchInitGlobal, SearchInitLocal);
	search.get_partition_data = SearchGetPartitionData;
	search.named_parameters["col"] = LogicalType::VARCHAR;
	loader.RegisterFunction(search);
}

} // namespace ngram
} // namespace duckdb
