//===----------------------------------------------------------------------===//
//                         ngram
//
// ngram/ngram_rewrite.hpp
//
// Transparent optimizer rewrite: an OptimizerExtension that swaps qualifying
// seq_scan LogicalGets (contains / LIKE / ILIKE filters over an indexed
// column) for the NGRAM_INDEX_SCAN table function, plus the
// ngram_auto_accelerate and ngram_max_candidate_fraction settings.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace ngram {

void RegisterRewrite(ExtensionLoader &loader);

} // namespace ngram
} // namespace duckdb
