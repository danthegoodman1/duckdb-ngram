//===----------------------------------------------------------------------===//
// ngram/search_core.hpp: storage-table access, shadow-table scans, and the fetch/scan/emit state machine shared by
// ngram_search and NGRAM_INDEX_SCAN.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "ngram/probe.hpp"

#include <condition_variable>
#include <deque>

namespace duckdb {

class DataTable;
class DuckTableEntry;
class DuckTransaction;
class ExpressionExecutor;
class TableFilterSet;

namespace ngram {

//! A pushed table filter comparing one scanned column against `value`. Table
//! filters are expressions, so this wraps the comparison around the
//! BoundReferenceExpression(0) subject a single-column filter is evaluated on.
unique_ptr<TableFilter> ConstantComparisonFilter(ExpressionType type, Value value);

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

//! Initialize a committed scan of rows [start_row, end_row): positioned at
//! the row group and vector holding start_row and stopped at end_row, so it
//! visits only the row groups those rows occupy. The scan starts at the
//! vector's first row, so the caller's filters must exclude the rows of that
//! vector before start_row, or tolerate them. A row group the filters' zone
//! maps exclude is skipped, as the host's own scan skips it. v1.5.5 keeps
//! DataTable's offset initializer private; its row-group collection exposes
//! the same steps, and like DataTable::InitializeScan they take no checkpoint
//! lock. Local storage stays uninitialized: callers scan the committed
//! collection state directly. Returns the rows the scan can visit: from the
//! first vector's start to end_row, before zone-map pruning.
idx_t InitializeBoundedScan(ClientContext &context, DataTable &storage, TableScanState &state,
                            const vector<StorageIndex> &column_ids, optional_ptr<TableFilterSet> filters,
                            idx_t start_row, idx_t end_row);

//! Run `body(unit)` for every unit in [0, units) across at most `workers` of
//! the scheduler's threads, or inline when there is only one of either. The
//! caller's work must be order-independent and must not share mutable state
//! between units without its own synchronization.
void ParallelForEachUnit(ClientContext &context, idx_t units, idx_t workers, const std::function<void(idx_t)> &body);

enum class SearchCorePhase : uint8_t { FETCH, SCAN, DONE };

//! One intersected segment whose candidate rowids are being handed out in
//! FETCH_BATCH_ROWS batches.
struct PublishedCandidates {
	PublishedCandidates(idx_t segment_ordinal_p, shared_ptr<vector<row_t>> rowids_p)
	    : segment_ordinal(segment_ordinal_p), rowids(std::move(rowids_p)) {
	}
	idx_t segment_ordinal;
	shared_ptr<vector<row_t>> rowids;
	idx_t next_offset = 0;
};

//! The shared cursor over intersected segments. A fetch worker takes the next
//! batch of a published segment when one is waiting and otherwise decodes the
//! next admitted segment, so every worker fetches whatever segment is
//! published rather than only its own. Segments publish in ordinal order: a
//! decoded segment waits in `pending` until every lower ordinal has
//! published. Every batch a worker claims therefore has a higher batch index
//! than its previous one, which the host requires of each pipeline thread.
struct CandidateQueue {
	mutex lock;
	std::condition_variable published;
	//! Published segments, in ordinal order, with batches still to hand out.
	std::deque<PublishedCandidates> ready;
	//! Decoded segments whose predecessors are still decoding, by ordinal.
	map<idx_t, shared_ptr<vector<row_t>>> pending;
	//! The ordinal that publishes next.
	idx_t next_publish = 0;
	//! Segments claimed for decoding whose result is not yet in `pending`.
	idx_t decoding = 0;
	//! A decode threw; the remaining workers finish with the tail scan while
	//! the host propagates that error.
	bool failed = false;
};

//! Projection-neutral execution state shared by ngram_search and the
//! transparent NGRAM_INDEX_SCAN. Policy-specific init supplies layouts,
//! filters, HWM and an optional admitted probe.
struct SearchCoreGlobal {
	DataTable *storage = nullptr;
	DuckTransaction *tx = nullptr;
	int64_t hwm = -1;
	unique_ptr<ProbePlan> probe;
	atomic<idx_t> next_probe_thread {0};
	CandidateQueue candidates;
	idx_t fetch_batch_base = 0;

	vector<StorageIndex> fetch_column_ids;
	vector<LogicalType> fetch_types;
	//! Output column -> fetch/scan column. INVALID_INDEX synthesizes the empty
	//! BOOLEAN virtual column used only to carry cardinality for count(*).
	vector<idx_t> output_ids;

	//! Native filters every storage scan evaluates: the transparent scan's
	//! pushed filters, or the explicit search's contains predicate. Rows a
	//! scan produces under them need no recheck.
	unique_ptr<TableFilterSet> scan_filters;
	//! Positions in fetch_column_ids of the columns the recheck reads, in
	//! the order the recheck expression references them; policy init sets
	//! it before FinalizeSearchCore.
	vector<idx_t> recheck_positions;
	//! A per-row candidate fetch reads the probe layout first, the recheck
	//! columns then the rowid, and the extra layout, every other column, only
	//! for the rows the recheck keeps: a fetched column costs from a tenth of
	//! a string fetch (bit-packed integers) to nine times it (short FSST
	//! strings), so wide projections are paid per match, not per candidate.
	//! Both layouts are positions in fetch_column_ids; output_sources maps
	//! each output column to (in the probe layout, index in that layout).
	vector<idx_t> probe_positions;
	vector<idx_t> extra_positions;
	vector<StorageIndex> probe_column_ids;
	vector<LogicalType> probe_types;
	vector<StorageIndex> extra_column_ids;
	vector<LogicalType> extra_types;
	idx_t probe_rowid_position = 0;
	vector<std::pair<bool, idx_t>> output_sources;
	//! Position of the rowid column in the fetch projection. Bounded scans
	//! start at a vector boundary, so each carries a rowid filter that
	//! excludes the rows of that vector before its bound.
	idx_t fetch_rowid_position = 0;
	//! The committed rows past the index, [tail_start, tail_end), scanned in
	//! units of tail_unit_rows through the host's offset scan, so a scan
	//! visits the row groups those rows occupy. Unit tail_units is the
	//! transaction-local storage, whose rows all lie past the index.
	idx_t tail_start = 0;
	idx_t tail_end = 0;
	idx_t tail_unit_rows = 1;
	idx_t tail_units = 0;
	atomic<idx_t> next_tail_unit {0};
	idx_t max_threads = 1;

	//! Physical work per access path, for profiling and bounded-work tests:
	//! rows fetched by rowid, rows the range scans and tail scans can visit
	//! (their vector-aligned spans, before zone-map pruning), and the rows of
	//! the transaction's local storage, which its scan visits whole.
	atomic<idx_t> fetched_rows {0};
	atomic<idx_t> range_rows {0};
	atomic<idx_t> tail_rows {0};
	atomic<idx_t> local_rows {0};
};

struct SearchCoreLocal {
	SearchCoreLocal() : hit_rowids(LogicalType::ROW_TYPE, STANDARD_VECTOR_SIZE) {
	}

	SearchCorePhase phase = SearchCorePhase::FETCH;
	//! Range-scan output in the full fetch layout.
	DataChunk fetch_chunk;
	//! Per-row fetch output: the probe layout for every candidate of the
	//! batch, then the extra layout for the rows the recheck kept, whose
	//! rowids hit_rowids carries between the two fetches.
	DataChunk probe_chunk;
	DataChunk extra_chunk;
	Vector hit_rowids;
	ColumnFetchState fetch_state;
	ColumnFetchState extra_state;
	//! The claimed batch: rowids [candidate_offset, candidate_end) of the
	//! published segment `segment_ordinal`.
	shared_ptr<vector<row_t>> candidates;
	idx_t candidate_offset = 0;
	idx_t candidate_end = 0;
	idx_t segment_ordinal = 0;
	ProbeDecodeScratch decode;
	//! A dense batch in progress as a committed scan bounded to the batch's
	//! rowid span; the filter excludes the rows before its first rowid. Both
	//! are fresh per batch: a TableScanState keeps appending filter info when
	//! initialized again.
	unique_ptr<TableScanState> range_state;
	unique_ptr<TableFilterSet> range_filters;

	//! The tail unit in progress: a bounded committed scan with its own rowid
	//! lower bound beside the pushed filters, or the local storage scan when
	//! scan_local_storage is set. Fresh per unit.
	unique_ptr<TableScanState> scan_state;
	unique_ptr<TableFilterSet> scan_filters;
	bool scan_local_storage = false;
	DataChunk scan_chunk;
	SelectionVector sel;
	idx_t batch_index = 0;
	//! Rows this thread fetched or could scan, for the host's rows-scanned
	//! metric, which it reads per thread.
	idx_t rows_scanned = 0;
};

//! Fetched columns outside the recheck's inputs and the rowid: what a wide
//! projection adds to every kept row's fetch. Available before
//! FinalizeSearchCore, for the probe's admission.
idx_t ExtraFetchColumns(const SearchCoreGlobal &state);

//! Partition the tail, split the fetch layout, drop empty filter sets and
//! set the bounded thread count after policy-specific init has populated
//! `state`.
void FinalizeSearchCore(ClientContext &context, SearchCoreGlobal &state);

//! Rows of `chunk` the recheck keeps, selected into `sel`: every row when a
//! storage scan produced the chunk under the scan filters or there is no
//! executor, the executor's selection otherwise. DEBUG builds check that a
//! natively filtered chunk passes the executor whole.
idx_t SelectRechecked(ExpressionExecutor *executor, DataChunk &chunk, SelectionVector &sel, bool natively_filtered);

//! Initialize per-thread buffers and assign at most probe->max_threads locals
//! to candidate decoding; remaining locals start on the disjoint scan phase.
void InitializeSearchCoreLocal(ExecutionContext &context, SearchCoreGlobal &global, SearchCoreLocal &local);

//! Shared candidate fetch, scan, projection and scheduling loop.
//! `recheck(chunk, sel, natively_filtered)` selects exact matches from
//! either fetched candidates or scan chunks; natively_filtered says a storage
//! scan produced the chunk under the global scan filters, so those already
//! hold for every row.
void ExecuteSearchCore(ClientContext &context, TableFunctionInput &data, SearchCoreGlobal &global,
                       SearchCoreLocal &local,
                       const std::function<idx_t(DataChunk &, SelectionVector &, bool)> &recheck, DataChunk &output);

} // namespace ngram
} // namespace duckdb
