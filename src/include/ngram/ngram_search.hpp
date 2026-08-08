//===----------------------------------------------------------------------===//
//                         ngram
//
// ngram/ngram_search.hpp
//
// Explicit query path: ngram_candidates / ngram_search table functions and the
// ngram_max_grams_per_query setting.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace ngram {

void RegisterSearchFunctions(ExtensionLoader &loader);

} // namespace ngram
} // namespace duckdb
