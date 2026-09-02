//===----------------------------------------------------------------------===//
//                         ngram
//
// ngram/search_core.hpp
//
// The index-probe core shared by the explicit query path (ngram_search /
// ngram_candidates, src/ngram_search.cpp) and the transparent optimizer
// rewrite (src/ngram_rewrite.cpp): shadow-table resolution, meta reading, and
// the rarest-first posting-list intersection. Definitions live in
// src/ngram_search.cpp.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "ngram/trigram.hpp"

namespace duckdb {

class DataTable;
class DuckTableEntry;
class DuckTransaction;
class BufferManager;
class TableFilterSet;

namespace ngram {

//! ngram index queries probe at most this many of the needle's rarest grams.
idx_t MaxGramsPerQuery(ClientContext &context);

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

//! Identity of the index a query runs against, for ownership validation and
//! error messages in shadow-table reads.
struct ShadowTarget {
	string schema_name;
	string table_name;
	string column_name;
	string shadow_schema;
};

struct MetaInfo {
	GramOptions options;
	//! Highest committed rowid the index covers; rows past it are found by a
	//! brute-force tail scan.
	int64_t hwm_rowid = -1;
	//! The indexed column, as the meta row records it.
	string column_name;
	//! Table facts recorded at build/refresh, compared against the table's
	//! current facts to detect staleness the index cannot repair (see
	//! ngram/maintenance.hpp).
	string schema_fingerprint;
	//! The indexed column's own type, checked when the table's identity is
	//! proven and the full column list therefore says nothing about it.
	string column_type;
	int64_t table_oid = 0;
	int64_t catalog_oid = 0;
	string instance_id;
	//! Exact zero-posting DuckDB index that prevents rowid-moving vacuum and
	//! latches reuse of a truncated trailing rowid range.
	string guard_name;
	string guard_token;
};

class ProbeMemoryReservation {
public:
	ProbeMemoryReservation(BufferManager &manager, idx_t size);
	~ProbeMemoryReservation();
	void Grow(idx_t size);

private:
	BufferManager &manager;
	idx_t size;
};

//! One visible segments-table row needed by the selected grams. The shared
//! vacuum fence keeps posting_rowid stable between manifest scan and fetch.
struct ProbeDescriptor {
	ProbeDescriptor() = default;
	ProbeDescriptor(int64_t segment_no_p, idx_t gram_index_p, row_t posting_rowid_p, idx_t posting_count_p)
	    : segment_no(segment_no_p), gram_index(gram_index_p), posting_rowid(posting_rowid_p),
	      posting_count(posting_count_p) {
	}
	int64_t segment_no = 0;
	idx_t gram_index = 0;
	row_t posting_rowid = 0;
	idx_t posting_count = 0;
};

//! One rowid segment admitted for bounded probing. Its descriptors occupy the
//! half-open range [descriptor_begin, descriptor_end) in ProbePlan.
struct ProbeSegment {
	ProbeSegment() = default;
	ProbeSegment(int64_t segment_no_p, idx_t descriptor_begin_p, idx_t descriptor_end_p)
	    : segment_no(segment_no_p), descriptor_begin(descriptor_begin_p), descriptor_end(descriptor_end_p) {
	}
	int64_t segment_no = 0;
	idx_t descriptor_begin = 0;
	idx_t descriptor_end = 0;
};

//! Pre-decoding admission result and the immutable work manifest shared by
//! candidate-source workers. Its size follows segment rows, never postings or
//! final candidates.
struct ProbePlan {
	DuckTableEntry *segments_entry = nullptr;
	int64_t hwm = -1;
	vector<string> grams;
	vector<ProbeDescriptor> descriptors;
	vector<ProbeSegment> segments;
	atomic<idx_t> next_segment {0};
	idx_t candidate_upper_bound = 0;
	idx_t max_threads = 0;
	bool admitted = false;
	string decline_reason;
	idx_t stats_rows_scanned = 0;
	idx_t stats_chunks_scanned = 0;
	atomic<idx_t> decoded_rowids {0};
	unique_ptr<ProbeMemoryReservation> memory_reservation;
};

struct MetaHeader {
	int64_t format_version = -1;
	string schema_name, table_name, column_name;
};

//! Read and validate the single meta row of an index (format version,
//! ownership, gram options, high-water mark); throws when the shadow tables do
//! not look like this extension built them for `target`.
MetaInfo ReadMeta(ClientContext &context, DuckTransaction &tx, DuckTableEntry &meta_entry, const ShadowTarget &target);

MetaHeader ReadMetaHeader(ClientContext &context, DuckTransaction &tx, DuckTableEntry &meta_entry,
                          const ShadowTarget &target);

//! Read only the common ownership/version prefix. Used by DROP to recognize
//! the known guard-less v2 layout without weakening normal readers.
int64_t ReadMetaFormatVersion(ClientContext &context, DuckTransaction &tx, DuckTableEntry &meta_entry,
                              const ShadowTarget &target);

//! Build a segment manifest and admit its decoded work before touching a
//! postings blob. A negative candidate_fraction disables that gate (used by
//! ngram_candidates, which has no full-result scan substitute). worker_cap is
//! one for that serial API and unlimited for parallel exact scans.
unique_ptr<ProbePlan> PlanIndexProbe(ClientContext &context, DuckTransaction &tx, DuckTableEntry &segments_entry,
                                     DuckTableEntry &stats_entry, const vector<string> &grams, idx_t max_grams,
                                     int64_t hwm, idx_t table_rows, double candidate_fraction, idx_t worker_cap);

//! Claim, decode, union and intersect one admitted rowid segment. Returns
//! false when no segment remains. Candidate rowids are sorted and belong to
//! the claimed segment; segment_ordinal supplies deterministic global order.
bool NextCandidateSegment(ClientContext &context, DuckTransaction &tx, ProbePlan &plan, vector<row_t> &candidates,
                          idx_t &segment_ordinal);

//! Shared query-resource settings.
double MaxCandidateFraction(ClientContext &context);

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
