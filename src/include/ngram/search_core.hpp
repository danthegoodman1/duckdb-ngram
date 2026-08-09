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
#include "ngram/trigram.hpp"

namespace duckdb {

class DataTable;
class DuckTableEntry;
class DuckTransaction;
class TableFilterSet;
class TableScanState;

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
	int64_t hwm_rowid = -1;
};

//! Read and validate the single meta row of an index (format version,
//! ownership, gram options, high-water mark); throws when the shadow tables do
//! not look like this extension built them for `target`.
MetaInfo ReadMeta(ClientContext &context, DuckTransaction &tx, DuckTableEntry &meta_entry, const ShadowTarget &target);

//! Candidate rowids among indexed rows for a set of needle grams: the sorted
//! intersection of the posting lists of the up-to-max_grams rarest grams.
//! Superset-preserving; a gram with no postings proves no indexed row matches.
vector<row_t> ProbeIndex(ClientContext &context, DuckTransaction &tx, DuckTableEntry &segments_entry,
                         DuckTableEntry &stats_entry, const vector<string> &grams, idx_t max_grams);

} // namespace ngram
} // namespace duckdb
