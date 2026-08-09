//===----------------------------------------------------------------------===//
//                         ngram
//
// ngram/index_pragmas.hpp
//
// create_ngram_index / drop_ngram_index / ngram_index_stats pragmas, plus the
// shadow-storage naming scheme and target resolution shared with the query
// path (ngram_search / ngram_candidates).
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {

class TableCatalogEntry;

namespace ngram {

//! Postings are bucketed by rowid range: segment_no = rowid >> SEGMENT_SHIFT.
//! Build and query must agree on this constant.
constexpr int64_t SEGMENT_SHIFT = 20;

//! The schema holding a base table's shadow tables (ngram_<schema>_<table>)
string ShadowSchemaName(const string &schema, const string &table);

//! Per-column shadow tables inside the shadow schema (ngram_<schema>_<table>)
string MetaTableName(const string &column);
string SegmentsTableName(const string &column);
string StatsTableName(const string &column);

struct ResolvedTarget {
	string catalog_name;
	string schema_name;
	string table_name;
	string column_name;
	string shadow_schema;
	//! The resolved base table; valid for the duration of the resolving statement.
	optional_ptr<TableCatalogEntry> entry;
};

//! Resolve a user-supplied table name (optionally schema/catalog-qualified) to a
//! base table. Throws binder errors for views and temporary tables. With
//! require_column set, additionally validates the column for index builds
//! (exists, VARCHAR, no user column shadowing rowid). column_name comes back in
//! the catalog's spelling whenever the column exists on the table, so generated
//! names and name comparisons are casing-stable.
ResolvedTarget ResolveTarget(ClientContext &context, const string &table_input, const string &column_name,
                             bool require_column);

//! Whether a table named `name` exists in the target's shadow schema.
bool ShadowTableExists(ClientContext &context, const ResolvedTarget &target, const string &name);

//! Names of meta_* tables currently present in the target's shadow schema.
vector<string> ExistingMetaTables(ClientContext &context, const ResolvedTarget &target);

void RegisterIndexPragmas(ExtensionLoader &loader);

} // namespace ngram
} // namespace duckdb
