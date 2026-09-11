//===----------------------------------------------------------------------===//
// ngram/test_hooks.hpp: barriers the C++ harness installs to make a scheduler
// race deterministic. Production code calls a hook only when one is set, so
// the cost is one null check at each publication point.
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb.hpp"

#include <functional>

namespace duckdb {
namespace ngram {

struct NgramTestHooks {
	//! Called by the worker that decoded segment `ordinal` of a probe, before
	//! it hands the segment to the candidate queue. The harness blocks here to
	//! hold one segment back while later ones publish, or interrupts the query.
	std::function<void(idx_t ordinal)> before_segment_publish;
	//! Called by a maintenance script's append call before each chunk it
	//! appends to the segments table. The harness interrupts here to cancel a
	//! refresh or merge at a point where rows are provably in flight.
	std::function<void()> before_maintenance_append_chunk;
};

//! The process-wide hooks; empty unless a harness set them.
NgramTestHooks &GetNgramTestHooks();

} // namespace ngram
} // namespace duckdb
