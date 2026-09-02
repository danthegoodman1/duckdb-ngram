//===----------------------------------------------------------------------===//
//                         ngram
//
// ngram/maintenance.hpp
//
// Index maintenance: the ngram_refresh / ngram_compact pragmas and the
// creation barrier they share with create_ngram_index.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "ngram/search_core.hpp"

namespace duckdb {

class DuckTransactionManager;
class TableCatalogEntry;

namespace ngram {

//! Storage layout this extension version writes and reads: the registry row
//! carries the metadata, and postings and stats live in two tables named by
//! the index id. Every reader lists other versions as drop-only.
constexpr int64_t NGRAM_FORMAT_VERSION = 4;

//! Internal execution-time functions used by generated maintenance scripts.
constexpr const char *NGRAM_MAINTENANCE_GUARD = "__ngram_maintenance_guard";
constexpr const char *NGRAM_CREATION_FINISH = "__ngram_creation_finish";

//! True while this context owns the creation EXCLUSIVE for `manager`. Seal
//! readers can then use that lock instead of trying to nest a shared lock.
bool ContextOwnsCreationBarrier(ClientContext &context, DuckTransactionManager &manager);

//! Registers the ngram_refresh and ngram_compact pragmas.
void RegisterMaintenance(ExtensionLoader &loader);

} // namespace ngram
} // namespace duckdb
