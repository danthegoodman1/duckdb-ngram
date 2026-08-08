//===----------------------------------------------------------------------===//
//                         ngram
//
// ngram/index_pragmas.hpp
//
// create_ngram_index / drop_ngram_index / ngram_index_stats pragmas.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace ngram {

void RegisterIndexPragmas(ExtensionLoader &loader);

} // namespace ngram
} // namespace duckdb
