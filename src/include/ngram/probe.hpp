//===----------------------------------------------------------------------===//
// ngram/probe.hpp: the index probe: manifest planning and admission, then per-segment posting decode and intersection.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/common/atomic.hpp"

namespace duckdb {

class BufferManager;
class DuckTableEntry;
class DuckTransaction;

namespace ngram {

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

} // namespace ngram
} // namespace duckdb
