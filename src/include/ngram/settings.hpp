//===----------------------------------------------------------------------===//
// ngram/settings.hpp: the extension's session settings and their readers.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace ngram {

//! ngram index queries probe at most this many of the needle's rarest grams.
idx_t MaxGramsPerQuery(ClientContext &context);

//! Queries over DuckLake tables probe at most this many of the needle's
//! rarest grams.
idx_t MaxLakeGramsPerQuery(ClientContext &context);

//! Full-result queries scan instead of probing when the fetch-equivalent
//! candidate bound exceeds this share of the indexed rows.
double MaxCandidateFraction(ClientContext &context);

//! Posting rowids one query may decode before it scans or errors.
idx_t MaxProbeRowids(ClientContext &context);

//! Bytes one query's probe may reserve: a quarter of memory_limit, capped.
idx_t ProbeMemoryBudget(ClientContext &context);

//! A query over a DuckLake table scans instead of reading its candidates
//! when they fall in more than this share of the table's row groups.
double MaxLakeRowGroupFraction(ClientContext &context);

//! Whether the optimizer rewrites qualifying filters into index scans.
bool AutoAccelerateEnabled(ClientContext &context);

//! The ngram_build_partitions setting; 0 sizes partitions from memory_limit.
idx_t ConfiguredBuildPartitions(ClientContext &context);

void RegisterSettings(ExtensionLoader &loader);

} // namespace ngram
} // namespace duckdb
