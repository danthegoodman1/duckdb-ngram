//===----------------------------------------------------------------------===//
// ngram/settings.hpp: the extension's session settings and their readers.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace ngram {

//! ngram index queries probe at most this many of the needle's rarest grams.
idx_t MaxGramsPerQuery(ClientContext &context);

//! Full-result queries scan instead of probing when the candidate upper bound
//! exceeds this share of the table's rows.
double MaxCandidateFraction(ClientContext &context);

//! Posting rowids one query may decode before it scans or errors.
idx_t MaxProbeRowids(ClientContext &context);

//! Bytes one query's probe may reserve: a quarter of memory_limit, capped.
idx_t ProbeMemoryBudget(ClientContext &context);

//! Whether the optimizer rewrites qualifying filters into index scans.
bool AutoAccelerateEnabled(ClientContext &context);

//! The ngram_build_partitions setting; 0 sizes partitions from memory_limit.
idx_t ConfiguredBuildPartitions(ClientContext &context);

void RegisterSettings(ExtensionLoader &loader);

} // namespace ngram
} // namespace duckdb
