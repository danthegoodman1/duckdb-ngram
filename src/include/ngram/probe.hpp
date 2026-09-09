//===----------------------------------------------------------------------===//
// ngram/probe.hpp: the index probe: manifest planning and admission, then per-segment posting decode and intersection.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/storage/table/scan_state.hpp"

namespace duckdb {

class BufferManager;
class DuckTableEntry;
class DuckTransaction;

namespace ngram {

//! A hard reservation against the buffer manager for the query's probe
//! scratch. Grow and Shrink are safe from the manifest workers' threads.
class ProbeMemoryReservation {
public:
	ProbeMemoryReservation(BufferManager &manager, idx_t size);
	~ProbeMemoryReservation();
	void Grow(idx_t size);
	void Shrink(idx_t size);

private:
	BufferManager &manager;
	atomic<idx_t> size;
};

//! Bytes the decode path holds in rowid buffers: every worker's scratch and
//! every candidate vector alive between decode and the last fetch of its
//! batches. `peak` is compared to the plan's workspace reservation.
struct ProbeDecodeTracker {
	atomic<idx_t> live {0};
	atomic<idx_t> peak {0};

	void Add(idx_t bytes);
	void Release(idx_t bytes);
};

//! One visible segments-table row needed by the selected grams. The shared
//! vacuum fence keeps posting_rowid stable between manifest scan and fetch.
struct ProbeDescriptor {
	ProbeDescriptor() = default;
	ProbeDescriptor(int64_t segment_no_p, idx_t gram_index_p, row_t posting_rowid_p, idx_t posting_count_p,
	                row_t min_rowid_p, row_t max_rowid_p)
	    : segment_no(segment_no_p), gram_index(gram_index_p), posting_rowid(posting_rowid_p),
	      posting_count(posting_count_p), min_rowid(min_rowid_p), max_rowid(max_rowid_p) {
	}
	int64_t segment_no = 0;
	idx_t gram_index = 0;
	row_t posting_rowid = 0;
	idx_t posting_count = 0;
	//! The postings' first and last rowid: the row's own zone map, which
	//! bounds where the segment's candidates can lie.
	row_t min_rowid = 0;
	row_t max_rowid = 0;
};

//! Rows a bounded range scan reads for the cost of one fetch by rowid. A
//! batch whose candidates fill their span this densely is read as a range
//! scan, and the admission gate prices a segment at its span over this ratio
//! when that is below its candidate bound. Measured on enwik9 at one thread
//! (docs/review/2026-09-09/cost_observations.json, range_threads_1.history
//! against fetch_threads_1.history): a scattered fetch costs 1.35 us and a
//! span row 0.29 us, about 4.6 to one; four rounds toward scanning.
constexpr idx_t RANGE_ROWS_PER_FETCH = 4;

//! Candidates are handed to fetch workers in batches of at least this many
//! rowids (fewer only at a segment's end), so fetch parallelism follows the
//! candidate count rather than the segment count. A batch runs on to the
//! next vector-aligned rowid boundary, so it holds fewer than twice this
//! many.
constexpr idx_t FETCH_BATCH_ROWS = STANDARD_VECTOR_SIZE;

//! One rowid segment admitted for bounded probing. Its descriptors occupy the
//! half-open range [descriptor_begin, descriptor_end) in ProbePlan, sorted by
//! gram index; `gram_order` lists the gram indexes smallest posting list
//! first, the order the intersection runs in.
struct ProbeSegment {
	ProbeSegment() = default;
	ProbeSegment(int64_t segment_no_p, idx_t descriptor_begin_p, idx_t descriptor_end_p)
	    : segment_no(segment_no_p), descriptor_begin(descriptor_begin_p), descriptor_end(descriptor_end_p) {
	}
	int64_t segment_no = 0;
	idx_t descriptor_begin = 0;
	idx_t descriptor_end = 0;
	vector<idx_t> gram_order;
};

//! Pre-decoding admission result and the immutable work manifest shared by
//! candidate-source workers. Its size follows segment rows, never postings or
//! final candidates.
struct ProbePlan {
	DuckTableEntry *segments_entry = nullptr;
	int64_t hwm = -1;
	//! The selected grams' storage keys; gram_index below indexes this.
	vector<uhugeint_t> keys;
	vector<ProbeDescriptor> descriptors;
	vector<ProbeSegment> segments;
	atomic<idx_t> next_segment {0};
	idx_t candidate_upper_bound = 0;
	idx_t max_threads = 0;
	bool admitted = false;
	string decline_reason;
	//! Segments-table rows read while collecting the manifest for every gram
	//! of the needle, before the rarest K were kept, and the rows the
	//! positioned manifest scans could visit: the vector-aligned spans of the
	//! key-column segments whose zone maps admitted a key.
	idx_t manifest_rows_scanned = 0;
	idx_t manifest_rows_visited = 0;
	//! Fetch-equivalent rows the admission gate compared with the candidate
	//! fraction: each segment's candidate bound, or its rowid span at
	//! range-scan cost when that is cheaper.
	idx_t admission_rows = 0;
	atomic<idx_t> decoded_rowids {0};
	unique_ptr<ProbeMemoryReservation> memory_reservation;
	//! The part of the reservation that pays for decode scratch and alive
	//! candidate vectors across max_threads workers.
	idx_t workspace_bytes = 0;
	//! Shared with the deleters of published candidate vectors, which may
	//! outlive the plan.
	shared_ptr<ProbeDecodeTracker> tracker;
	//! The segments-table projection every decode fetches, resolved once.
	vector<StorageIndex> decode_column_ids;
	vector<LogicalType> decode_types;
};

//! Buffers one thread reuses across the segments it decodes.
struct ProbeDecodeScratch {
	ProbeDecodeScratch() : rowids(LogicalType::ROW_TYPE, STANDARD_VECTOR_SIZE) {
	}
	vector<row_t> postings;
	vector<row_t> intersection;
	//! The fetched descriptor rows; initialized with the plan's decode types
	//! on first use.
	DataChunk chunk;
	Vector rowids;
	bool initialized = false;
	//! Rowid-buffer bytes this thread currently has charged to the tracker.
	idx_t tracked_bytes = 0;
};

//! The most distinct needle keys the query memory budget admits, before any
//! per-worker scan scratch: the ceiling needle decomposition stops at.
idx_t MaxProbeKeys(ClientContext &context);

//! Build a segment manifest and admit its decoded work before touching a
//! postings blob. The candidate fraction compares the plan's fetch-equivalent
//! rows, each fetched column beyond the recheck's counted as one more fetch
//! per candidate, with the rows the index covers, hwm + 1; a negative
//! fraction disables that gate (used by ngram_candidates, which has no
//! full-result scan substitute). worker_cap is one for that serial API and
//! unlimited for parallel exact scans.
unique_ptr<ProbePlan> PlanIndexProbe(ClientContext &context, DuckTransaction &tx, DuckTableEntry &segments_entry,
                                     const vector<uhugeint_t> &keys, idx_t max_grams, int64_t hwm,
                                     double candidate_fraction, idx_t worker_cap, idx_t extra_columns);

//! Decode, union and intersect the admitted segment at `segment_ordinal` into
//! `candidates`: sorted rowids that all belong to that segment. On return the
//! scratch's retained buffers and `candidates` are charged to the tracker
//! through `scratch.tracked_bytes`; a caller that hands `candidates` on must
//! call TrackPublishedCandidates.
void DecodeCandidateSegment(ClientContext &context, DuckTransaction &tx, ProbePlan &plan, idx_t segment_ordinal,
                            ProbeDecodeScratch &scratch, vector<row_t> &candidates);

//! Move a decoded vector's bytes from the worker's charge to a published
//! charge that its deleter releases: a shared vector that lives until the
//! last fetch of its batches finishes.
shared_ptr<vector<row_t>> TrackPublishedCandidates(ProbePlan &plan, ProbeDecodeScratch &scratch,
                                                   vector<row_t> &&candidates);

//! Claim the next admitted segment and decode it (the serial form used by
//! ngram_candidates). Returns false when no segment remains; segment_ordinal
//! supplies deterministic global order.
bool NextCandidateSegment(ClientContext &context, DuckTransaction &tx, ProbePlan &plan, ProbeDecodeScratch &scratch,
                          vector<row_t> &candidates, idx_t &segment_ordinal);

} // namespace ngram
} // namespace duckdb
