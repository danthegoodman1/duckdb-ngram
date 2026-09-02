//===----------------------------------------------------------------------===//
// ngram/catalog.hpp: the registry, index locations, storage naming, and SQL quoting for generated scripts.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/common/string_util.hpp"
#include "ngram/gram.hpp"

namespace duckdb {

class DuckTableEntry;
class TableCatalogEntry;

namespace ngram {

//! Postings are bucketed by rowid range: segment_no = rowid >> SEGMENT_SHIFT.
//! Build and query must agree on this constant.
constexpr int64_t SEGMENT_SHIFT = 20;

//! The schema holding the registry and every index's two storage tables.
constexpr const char *NGRAM_SCHEMA = "__ngram";
constexpr const char *REGISTRY_TABLE = "registry";
constexpr int32_t REGISTRY_VERSION = 2;
//! Generated rowid guards are named by the id of the index that created them.
constexpr const char *GUARD_PREFIX = "__ngram_guard_";

//! Storage layout this extension version writes and reads: the registry row
//! carries the metadata, and postings and stats live in two tables named by
//! the index id. Every reader lists other versions as drop-only.
constexpr int64_t NGRAM_FORMAT_VERSION = 4;

struct ResolvedTarget {
	string catalog_name;
	string schema_name;
	string table_name;
	string column_name;
	//! The resolved base table; valid for the duration of the resolving statement.
	optional_ptr<TableCatalogEntry> entry;

	//! The table as a quoted three-part name for generated SQL.
	string Qualified() const;
};

//! One registry row's identity: the index id, the column it indexes, and the
//! registry table it was read from. Storage tables are named by the id.
struct IndexLocation {
	string index_ref;
	string column_name;
	idx_t registry_oid = 0;
	string guard_name;
	string guard_token;

	string Hex() const {
		return StringUtil::Replace(index_ref, "-", "");
	}
	string SegmentsTable() const {
		return "segments_" + Hex();
	}
	string StatsTable() const {
		return "stats_" + Hex();
	}
};

//! The metadata a registry row carries for every read path.
struct MetaInfo {
	GramOptions options;
	//! Highest committed rowid the index covers; rows past it are found by a
	//! brute-force tail scan.
	int64_t hwm_rowid = -1;
	//! The indexed column, as the registry row records it.
	string column_name;
	//! The table's zero-posting DuckDB index that prevents rowid-moving vacuum
	//! and latches reuse of a truncated trailing rowid range. Its name and
	//! random token are the only proof that postings still describe the table.
	string guard_name;
	string guard_token;
};

//! One registry row: the index's identity, the metadata every read path
//! validates, and the reason the row is unusable when it is.
struct RegistryRow {
	string index_ref, owner_key, schema_name, table_name, column_name;
	int64_t format_version = -1;
	MetaInfo meta;
	string error;
};

struct RegistrySnapshot {
	idx_t oid = 0;
	//! Registry version 1 (format 3) had only the six identity columns. Its
	//! rows are listed and drop-only; nothing else reads that layout.
	bool legacy_shape = false;
	vector<RegistryRow> rows;
};

//! Every registry row of `catalog_name`, in this transaction's snapshot. Raises
//! when the registry table exists but is not one this extension wrote.
RegistrySnapshot ReadRegistry(ClientContext &context, const string &catalog_name);

//! The registry as create_ngram_index needs it: absent (bootstrap) or in the
//! current shape. A registry version 1 database must be emptied by id first.
RegistrySnapshot ReadRegistryForCreate(ClientContext &context, const string &catalog_name);

//! The locations of `target`'s rows in `registry` (see ExistingIndexes).
vector<IndexLocation> Locations(const RegistrySnapshot &registry, const ResolvedTarget &target, bool lenient);
IndexLocation LocationOf(const RegistryRow &row, idx_t registry_oid);

//! Resolve a user-supplied table name (optionally schema/catalog-qualified) to a
//! base table. Throws binder errors for views and temporary tables. With
//! require_column set, additionally validates the column for index builds
//! (exists, VARCHAR, no user column shadowing rowid). column_name comes back in
//! the catalog's spelling whenever the column exists on the table, so generated
//! names and name comparisons are casing-stable.
ResolvedTarget ResolveTarget(ClientContext &context, const string &table_input, const string &column_name,
                             bool require_column);

//! The registry rows owned by `target`: the one for its column, or every row of
//! its table when column_name is empty. A row this extension cannot use (other
//! format, corrupt values) raises with its reason; `lenient` skips such rows
//! and an unreadable registry instead, for the optimizer's decline path.
vector<IndexLocation> ExistingIndexes(ClientContext &context, const ResolvedTarget &target, bool lenient = false);
void RequireUniqueIndexColumns(const vector<IndexLocation> &indexes);

//! Registry rows of `target`'s table, other than `except_ref`, that record
//! `guard_name`. The table's guard is dropped only when this is zero.
idx_t OtherGuardReferences(ClientContext &context, const ResolvedTarget &target, const string &guard_name,
                           const string &except_ref);

void ValidateRegistryForCreate(ClientContext &context, const string &catalog_name, idx_t expected_registry_oid,
                               bool expected_bootstrap);

//! The NGRAM_ROWID_GUARD indexes on `table` whose names carry GUARD_PREFIX, in
//! the calling transaction's catalog snapshot.
vector<string> PrefixedGuardNames(ClientContext &context, DuckTableEntry &table);

//! The guard token a registry-version-1 (format 3) index recorded in its own
//! meta table, or empty when that table cannot be read.
string LegacyGuardToken(ClientContext &context, const string &catalog_name, const string &schema_name);

//! The index id named by a storage table (segments_<hex> or stats_<hex>).
bool ParseStorageName(const string &name, string &index_ref, bool &segments);
bool IsCanonicalUUID(const string &value);

//! The length-framed, case-folded (schema, table, column) key of a registry
//! row, and its hex spelling for a generated literal.
string OwnerKey(const string &schema, const string &table, const string &column);
string Hex(const string &input);

//! Committed rowid space the table has allocated (tombstoned rows included),
//! i.e. max committed rowid + 1.
int64_t TableTotalRows(TableCatalogEntry &table);

//! Quote an identifier / a string literal, or qualify a built-in function so
//! generated SQL cannot resolve a same-named macro from the current schema.
string Ident(const string &name);
string Lit(const string &value);
string SystemFunction(const string &name);

//! A storage table (or the registry) of `catalog_name` as a qualified name.
string StorageTable(const string &catalog_name, const string &table);
string Registry(const string &catalog_name);

//! A fresh, quoted temp-table name for one generated script.
string ScratchName(const char *purpose);

} // namespace ngram
} // namespace duckdb
