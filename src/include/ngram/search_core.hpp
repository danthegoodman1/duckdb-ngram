//===----------------------------------------------------------------------===//
// ngram/search_core.hpp: storage-table access, shadow-table scans, and the fetch/scan/emit state machine shared by
// ngram_search and NGRAM_INDEX_SCAN.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "ngram/probe.hpp"

namespace duckdb {

class DataTable;
class DuckTableEntry;
class DuckTransaction;
class TableFilterSet;

namespace ngram {

//! Initialize a committed + transaction-local storage scan. Equivalent to
//! DataTable::InitializeScan, except that a table with no committed rows
//! (e.g. shadow tables created inside the current transaction) initializes
//! only the transaction-local phase — v1.5.5's committed-scan init asserts on
//! an empty row-group collection in DEBUG builds, and an uninitialized
//! committed phase scans nothing, which is exactly right.
void InitializeExhaustiveScan(ClientContext &context, DuckTransaction &tx, DataTable &storage, TableScanState &state,
                              const vector<StorageIndex> &column_ids, optional_ptr<TableFilterSet> filters);

//! Resolve a table that must still exist at execution time (base table or
//! shadow table); throws a CatalogException naming `what` when it is gone.
DuckTableEntry &ResolveExistingTable(ClientContext &context, const string &catalog, const string &schema,
                                     const string &name, const char *what);

//! Missing-only variant for transparent execution fallback. A present object
//! of the wrong kind is corruption/name collision and still throws.
optional_ptr<DuckTableEntry> TryResolveExistingTable(ClientContext &context, const string &catalog,
                                                     const string &schema, const string &name, const char *what);

//! The base table a query was bound against, or `changed_message` (with the
//! table name as its argument) when a prepared statement outlived a DROP plus
//! CREATE that changed the logical schema.
DuckTableEntry &ResolveBoundBase(ClientContext &context, const string &catalog, const string &schema,
                                 const string &table, const vector<string> &names, const vector<LogicalType> &types,
                                 const char *changed_message);

//! Append the storage index and type of `column_name` to a projection, or
//! throw if the table does not look like this extension built it.
void AddShadowColumn(DuckTableEntry &entry, const string &column_name, LogicalTypeId expected,
                     vector<StorageIndex> &column_ids, vector<LogicalType> &types);

void ThrowIfInterrupted(ClientContext &context);

//! Scan an entire table (committed storage plus this transaction's local rows)
//! through the caller's transaction, invoking fn per non-empty chunk.
void ScanShadowTable(ClientContext &context, DuckTransaction &tx, DataTable &storage,
                     const vector<StorageIndex> &column_ids, const vector<LogicalType> &types,
                     optional_ptr<TableFilterSet> filters, const std::function<void(DataChunk &)> &fn);

//! Scan a whole table in parallel, one row group per claim. `body` is called
//! once per chunk with the worker's own index so it can accumulate into a
//! private slot.
void ParallelScanShadowTable(ClientContext &context, DuckTransaction &tx, DataTable &storage,
                             const vector<StorageIndex> &column_ids, const vector<LogicalType> &types,
                             optional_ptr<TableFilterSet> filters, idx_t workers,
                             const std::function<void(DataChunk &, idx_t)> &body);

enum class SearchCorePhase : uint8_t { FETCH, SCAN, DONE };

//! Projection-neutral execution state shared by ngram_search and the
//! transparent NGRAM_INDEX_SCAN. Policy-specific init supplies layouts,
//! filters, HWM and an optional admitted probe.
struct SearchCoreGlobal {
	DataTable *storage = nullptr;
	DuckTransaction *tx = nullptr;
	int64_t hwm = -1;
	unique_ptr<ProbePlan> probe;
	atomic<idx_t> next_probe_thread {0};
	idx_t fetch_batch_base = 0;

	vector<StorageIndex> fetch_column_ids;
	vector<LogicalType> fetch_types;
	//! Output column -> fetch/scan column. INVALID_INDEX synthesizes the empty
	//! BOOLEAN virtual column used only to carry cardinality for count(*).
	vector<idx_t> output_ids;

	vector<StorageIndex> scan_column_ids;
	vector<LogicalType> scan_types;
	unique_ptr<TableFilterSet> scan_filters;
	ParallelTableScanState parallel_scan;
	idx_t max_threads = 1;
};

struct SearchCoreLocal {
	SearchCorePhase phase = SearchCorePhase::FETCH;
	DataChunk fetch_chunk;
	ColumnFetchState fetch_state;
	vector<row_t> candidates;
	idx_t candidate_offset = 0;
	idx_t segment_ordinal = 0;

	TableScanState scan_state;
	DataChunk scan_chunk;
	bool scan_unit_active = false;
	SelectionVector sel;
	idx_t batch_index = 0;
};

//! Add the tail/full-scan rowid filter, initialize the parallel cursor and set
//! the bounded thread count after policy-specific init has populated `state`.
void FinalizeSearchCore(ClientContext &context, SearchCoreGlobal &state);

//! Initialize per-thread buffers and assign at most probe->max_threads locals
//! to candidate decoding; remaining locals start on the disjoint scan phase.
void InitializeSearchCoreLocal(ExecutionContext &context, SearchCoreGlobal &global, SearchCoreLocal &local);

//! Shared candidate fetch, scan, projection and scheduling loop. `recheck`
//! selects exact matches from either fetched candidates or scan chunks.
void ExecuteSearchCore(ClientContext &context, TableFunctionInput &data, SearchCoreGlobal &global,
                       SearchCoreLocal &local, const std::function<idx_t(DataChunk &, SelectionVector &)> &recheck,
                       DataChunk &output);

} // namespace ngram
} // namespace duckdb
