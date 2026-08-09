#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "ngram/index_pragmas.hpp"
#include "ngram/maintenance.hpp"
#include "ngram/search_core.hpp"

#include <algorithm>

namespace duckdb {
namespace ngram {

//! The index lives in ordinary tables under one schema per indexed base table
//! (ngram_<schema>_<table>), three tables per indexed column. Nothing persisted
//! references extension functions, so a database opened without the extension
//! reads and writes normally and the index schema is inert.

string ShadowSchemaName(const string &schema, const string &table) {
	return "ngram_" + schema + "_" + table;
}

string MetaTableName(const string &column) {
	return "meta_" + column;
}

string SegmentsTableName(const string &column) {
	return "segments_" + column;
}

string StatsTableName(const string &column) {
	return "stats_" + column;
}

string Ident(const string &name) {
	return KeywordHelper::WriteOptionallyQuoted(name);
}

string Lit(const string &value) {
	return KeywordHelper::WriteQuoted(value);
}

ResolvedTarget ResolveTarget(ClientContext &context, const string &table_input, const string &column_name,
                             bool require_column) {
	auto qname = QualifiedName::Parse(table_input);
	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, qname.name);
	auto entry = Catalog::GetEntry(context, qname.catalog, qname.schema, lookup, OnEntryNotFound::THROW_EXCEPTION);
	// a TABLE_ENTRY lookup can also return a view (they share a catalog set)
	if (entry->type != CatalogType::TABLE_ENTRY) {
		if (entry->type == CatalogType::VIEW_ENTRY) {
			throw BinderException("%s is a view, not a table; ngram indexes require a base table", table_input);
		}
		throw BinderException("%s is not a table; ngram indexes require a base table", table_input);
	}
	auto &table_entry = entry->Cast<TableCatalogEntry>();
	if (table_entry.ParentCatalog().IsTemporaryCatalog()) {
		throw BinderException("%s is a temporary table; ngram indexes on temporary tables are not supported",
		                      table_input);
	}
	if (require_column) {
		// a user column named rowid (identifiers match case-insensitively) shadows
		// the pseudo-column the build reads as the row identifier, so the build
		// would silently index that column's values instead of row ids
		for (auto &col : table_entry.GetColumns().Logical()) {
			if (StringUtil::Lower(col.Name()) == "rowid") {
				throw BinderException("cannot build an ngram index on %s: its column \"%s\" shadows the rowid "
				                      "pseudo-column used as the row identifier",
				                      table_input, col.Name());
			}
		}
		if (!table_entry.ColumnExists(column_name)) {
			throw CatalogException("Table %s does not have a column named %s", table_input, column_name);
		}
		auto &column = table_entry.GetColumn(column_name);
		if (column.Type().id() != LogicalTypeId::VARCHAR) {
			throw BinderException("ngram indexes require a VARCHAR column; %s.%s is %s", table_input, column_name,
			                      column.Type().ToString());
		}
	}
	ResolvedTarget target;
	target.catalog_name = table_entry.ParentCatalog().GetName();
	target.schema_name = table_entry.ParentSchema().name;
	target.table_name = table_entry.name;
	// store the catalog's spelling of the column: lookups are case-insensitive,
	// but generated shadow-table names and name comparisons are not. When the
	// column no longer exists on the base table (e.g. dropping an orphaned
	// index after the column was removed), the user's spelling passes through.
	target.column_name = column_name;
	if (!column_name.empty() && table_entry.ColumnExists(column_name)) {
		target.column_name = table_entry.GetColumn(column_name).Name();
	}
	target.shadow_schema = ShadowSchemaName(target.schema_name, target.table_name);
	target.entry = &table_entry;
	return target;
}

bool ShadowTableExists(ClientContext &context, const ResolvedTarget &target, const string &name) {
	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, name);
	auto entry =
	    Catalog::GetEntry(context, target.catalog_name, target.shadow_schema, lookup, OnEntryNotFound::RETURN_NULL);
	return entry != nullptr;
}

//! ngram_<schema>_<table> is readable but not injective: schema a, table b_c
//! and schema a_b, table c both map to ngram_a_b_c. Every meta table records
//! its owner, and every operation checks that record before touching shadow
//! storage. Pragma callbacks cannot run queries, so the check is compiled into
//! the generated SQL: error() raises at execution time when the meta names a
//! different base table.

//! SQL condition: this meta row records an owner other than target
static string MetaMismatchCondition(const ResolvedTarget &target) {
	return "schema_name IS DISTINCT FROM " + Lit(target.schema_name) + " OR table_name IS DISTINCT FROM " +
	       Lit(target.table_name);
}

//! SQL scalar counting the meta rows that name target as their owner. The
//! guards compare it against 1: a meta table that is empty, holds several
//! rows, or names someone else is equally unusable.
static string OwnedMetaRowCount(const ResolvedTarget &target, const string &meta_qualified) {
	return "(SELECT count(*) FROM " + meta_qualified + " WHERE NOT (" + MetaMismatchCondition(target) + "))";
}

//! SQL expression raising the collision error, naming both tables. Reads the
//! owner back out of the meta table, so it works from a statement that has no
//! FROM clause of its own.
static string CollisionErrorCall(const ResolvedTarget &target, const string &meta_qualified) {
	return "error('ngram shadow schema collision: ' || " + Lit(target.shadow_schema) +
	       " || ' belongs to the index on ' || coalesce((SELECT coalesce(schema_name, '?') || '.' || "
	       "coalesce(table_name, '?') FROM " +
	       meta_qualified + " LIMIT 1), '(no owner recorded)') || ', not to ' || " +
	       Lit(target.schema_name + "." + target.table_name) + ")";
}

//! Wrap a guard expression in a statement that produces no result set. A bare
//! `SELECT <guard>` works but prints a NULL row per guard, which is what a
//! user of these pragmas actually sees; `SET VARIABLE` evaluates the same
//! expression, raises the same error, and returns nothing. The statement count
//! is unchanged, so the preprocessor still wraps the expansion in one
//! transaction exactly as before.
string SilentGuard(const string &guard_expression) {
	return "SET VARIABLE " + string(NGRAM_GUARD_VARIABLE) + " = (SELECT " + guard_expression + ");\n";
}

//! Statement raising the collision error unless the meta table holds exactly
//! one row naming target as its owner. Evaluating a scalar subquery rather
//! than a per-row CASE keeps the guard from passing vacuously on an empty
//! meta table.
string OwnershipGuard(const ResolvedTarget &target, const string &meta_qualified) {
	return SilentGuard("CASE WHEN " + OwnedMetaRowCount(target, meta_qualified) + " <> 1 THEN " +
	                   CollisionErrorCall(target, meta_qualified) + " END");
}

vector<string> ExistingMetaTables(ClientContext &context, const ResolvedTarget &target) {
	vector<string> result;
	auto schema_entry =
	    Catalog::GetSchema(context, target.catalog_name, target.shadow_schema, OnEntryNotFound::RETURN_NULL);
	if (!schema_entry) {
		return result;
	}
	schema_entry->Scan(context, CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
		const string prefix = "meta_";
		if (entry.name.rfind(prefix, 0) == 0) {
			result.push_back(entry.name);
		}
	});
	return result;
}

static string CreateNgramIndexQuery(ClientContext &context, const FunctionParameters &parameters) {
	auto table_input = parameters.values[0].ToString();
	auto column_name = parameters.values[1].ToString();

	int32_t gram_size = 3;
	bool case_insensitive = true;
	for (auto &entry : parameters.named_parameters) {
		if (entry.second.IsNull()) {
			throw BinderException("create_ngram_index: parameter %s cannot be NULL", entry.first);
		}
		if (entry.first == "gram") {
			gram_size = entry.second.GetValue<int32_t>();
		} else if (entry.first == "case_insensitive") {
			case_insensitive = entry.second.GetValue<bool>();
		} else {
			throw BinderException("create_ngram_index: unknown named parameter %s", entry.first);
		}
	}
	if (gram_size < 1) {
		throw InvalidInputException("create_ngram_index: gram must be at least 1, got %d", gram_size);
	}

	auto target = ResolveTarget(context, table_input, column_name, true);
	if (!target.entry->IsDuckTable()) {
		// the index reads rowids and row storage directly; a table living in a
		// foreign catalog (sqlite, postgres, ...) has neither
		throw BinderException("create_ngram_index: %s is not a DuckDB base table", table_input);
	}
	// generated names and the meta row must use the catalog's spelling
	column_name = target.column_name;

	auto base = Ident(target.catalog_name) + "." + Ident(target.schema_name) + "." + Ident(target.table_name);
	auto shadow = Ident(target.catalog_name) + "." + Ident(target.shadow_schema);
	auto meta = shadow + "." + Ident(MetaTableName(column_name));
	auto segments = shadow + "." + Ident(SegmentsTableName(column_name));
	auto stats = shadow + "." + Ident(StatsTableName(column_name));
	auto column = Ident(column_name);
	auto gram_str = to_string(gram_size);
	auto ci_str = case_insensitive ? "true" : "false";

	// No BEGIN/COMMIT here: DuckDB's statement preprocessor wraps a multi-statement
	// pragma expansion in a transaction itself (statement_preprocessor.cpp).
	//
	// The build runs through DuckDB's own engine so it is parallel and can spill.
	// It materializes (rowid, gram) pairs into a temp table first: feeding an
	// aggregate directly from the unnest pipeline OOMs under a memory limit
	// (duckdb 1.5.5), while the temp-table CTAS streams and the later aggregate
	// scans spill. Segments are bucketed by rowid range (not count) to keep the
	// build a plain GROUP BY and to give Phase 5 merges a natural key. Duplicate
	// (gram, rowid) instances survive until the codec, which sorts and dedupes.
	string script;
	// __ngram_ is this extension's reserved temp-table namespace; suffixing the
	// shadow schema and column keeps concurrent builds of different targets from
	// sharing one scratch table
	string pairs = Ident("__ngram_build_pairs_" + target.shadow_schema + "_" + column_name);
	// Ownership guards run first: each existing meta table must name this base
	// table. The guard on this column's own meta also converts a matching meta
	// into the "already exists" error; if that meta is somehow empty, the
	// CREATE TABLE below still fails on the name clash.
	for (auto &meta_name : ExistingMetaTables(context, target)) {
		auto meta_qualified = shadow + "." + Ident(meta_name);
		if (meta_name == MetaTableName(column_name)) {
			script += SilentGuard("CASE WHEN " + OwnedMetaRowCount(target, meta_qualified) + " <> 1 THEN " +
			                      CollisionErrorCall(target, meta_qualified) + " ELSE error(" +
			                      Lit("An ngram index already exists on " + target.table_name + "." + column_name +
			                          " (" + target.shadow_schema + "); use drop_ngram_index first") +
			                      ") END");
		} else {
			script += OwnershipGuard(target, meta_qualified);
		}
	}
	// Only committed rows are indexed. A transaction-local rowid is reassigned
	// at commit, so recording one would leave the index pointing at a rowid
	// that never exists (and, before this filter, made ngram_search fetch a
	// vanished local row and take the database down with an internal error).
	// Uncommitted rows are found by the tail scan instead, and land past the
	// high-water mark when they commit.
	auto committed_only = "rowid < " + to_string(LOCAL_ROWID_START);
	auto fingerprint = ComputeTableFingerprint(context, *target.entry);
	// witnesses are drawn over the rowids the build is about to index; the
	// callback runs before it, so the bound is the table's committed end
	auto row_samples = BuildRowSampleDigest(context, *target.entry, column_name, fingerprint.total_rows - 1);
	script += "CREATE SCHEMA IF NOT EXISTS " + shadow + ";\n";
	script += "CREATE TABLE " + meta + " AS SELECT " + to_string(NGRAM_FORMAT_VERSION) + " AS format_version, " +
	          Lit(target.schema_name) + " AS schema_name, " + Lit(target.table_name) + " AS table_name, " +
	          Lit(column_name) + " AS column_name, " + gram_str + " AS gram_size, " + ci_str +
	          " AS case_insensitive, "
	          "(SELECT coalesce(max(rowid), -1) FROM " +
	          base + " WHERE " + committed_only + ") AS hwm_rowid, " + Lit(fingerprint.schema_fingerprint) +
	          " AS schema_fingerprint, " + Lit(fingerprint.ColumnType(column_name)) + " AS column_type, " +
	          to_string(fingerprint.table_oid) + "::BIGINT AS table_oid, " + to_string(fingerprint.catalog_oid) +
	          "::BIGINT AS catalog_oid, " + Lit(fingerprint.instance_id) + " AS instance_id, " + Lit(row_samples) +
	          " AS row_samples;\n";
	script += "CREATE OR REPLACE TEMP TABLE " + pairs +
	          " AS "
	          "SELECT rowid AS r, rowid >> " +
	          to_string(SEGMENT_SHIFT) + " AS segment_no, unnest(trigrams(" + column + ", " + gram_str + ", " + ci_str +
	          ")) AS gram FROM " + base + " WHERE " + committed_only + " AND " + column + " IS NOT NULL;\n";
	// The sorted-stream packer holds one (gram, segment_no) run per thread instead
	// of every group at once. The sort feeding it spills, but external sorting
	// costs roughly 12-15 MB of overhead per thread, so the build's memory floor
	// scales with the thread count (a 12 MB corpus OOMs near 150-200 MB with 24
	// threads yet passes at 150 MB with 8). Parallel packing can split a key into
	// per-thread partial segments, which readers union; duplicate (gram, rowid)
	// instances straddling a thread boundary can inflate stats row_count by that
	// split count, which only biases rarest-first gram selection, never results.
	script += "CREATE TABLE " + segments +
	          " AS "
	          "SELECT gram, segment_no, 0 AS generation, postings, rowid_count, min_rowid, max_rowid "
	          "FROM ngram_pack_postings((SELECT gram, segment_no, r FROM " +
	          pairs + " ORDER BY gram, segment_no));\n";
	script += "CREATE TABLE " + stats +
	          " AS "
	          "SELECT gram, sum(rowid_count)::BIGINT AS row_count, count(*)::BIGINT AS segment_count FROM " +
	          segments + " GROUP BY gram;\n";
	script += "DROP TABLE " + pairs + ";\n";
	return script;
}

static string DropNgramIndexQuery(ClientContext &context, const FunctionParameters &parameters) {
	auto table_input = parameters.values[0].ToString();
	auto column_name = parameters.values[1].ToString();

	auto target = ResolveTarget(context, table_input, column_name, false);
	// shadow-table names use the catalog's spelling when the column still exists
	column_name = target.column_name;
	if (!ShadowTableExists(context, target, MetaTableName(column_name))) {
		throw CatalogException("No ngram index exists on %s.%s", target.table_name, column_name);
	}

	// Count everything else living in the shadow schema, across every catalog set
	// a schema holds (TABLE_ENTRY also covers views, MACRO_ENTRY covers scalar and
	// aggregate functions, TABLE_MACRO_ENTRY covers table functions). Anything
	// beyond this column's three tables must survive the drop.
	auto schema_entry =
	    Catalog::GetSchema(context, target.catalog_name, target.shadow_schema, OnEntryNotFound::THROW_EXCEPTION);
	static const CatalogType SCHEMA_ENTRY_TYPES[] = {
	    CatalogType::TABLE_ENTRY,           CatalogType::INDEX_ENTRY,
	    CatalogType::SEQUENCE_ENTRY,        CatalogType::MACRO_ENTRY,
	    CatalogType::TABLE_MACRO_ENTRY,     CatalogType::TYPE_ENTRY,
	    CatalogType::COLLATION_ENTRY,       CatalogType::COPY_FUNCTION_ENTRY,
	    CatalogType::PRAGMA_FUNCTION_ENTRY, CatalogType::COORDINATE_SYSTEM_ENTRY};
	idx_t other_entries = 0;
	for (auto scan_type : SCHEMA_ENTRY_TYPES) {
		schema_entry->Scan(context, scan_type, [&](CatalogEntry &entry) {
			// identifiers match case-insensitively, and column_name carries the
			// user's spelling whenever the column is gone from the base table
			// (dropping an orphaned index): comparing case-sensitively would
			// count this index's own tables as foreign and leave the shadow
			// schema behind, empty
			bool own_table = entry.type == CatalogType::TABLE_ENTRY &&
			                 (StringUtil::CIEquals(entry.name, MetaTableName(column_name)) ||
			                  StringUtil::CIEquals(entry.name, SegmentsTableName(column_name)) ||
			                  StringUtil::CIEquals(entry.name, StatsTableName(column_name)));
			if (!own_table) {
				other_entries++;
			}
		});
	}

	auto shadow = Ident(target.catalog_name) + "." + Ident(target.shadow_schema);
	string script;
	// refuse to drop through a shadow schema owned by a different base table
	script += OwnershipGuard(target, shadow + "." + Ident(MetaTableName(column_name)));
	script += "DROP TABLE " + shadow + "." + Ident(MetaTableName(column_name)) + ";\n";
	script += "DROP TABLE " + shadow + "." + Ident(SegmentsTableName(column_name)) + ";\n";
	script += "DROP TABLE " + shadow + "." + Ident(StatsTableName(column_name)) + ";\n";
	if (other_entries == 0) {
		// plain DROP SCHEMA (never CASCADE): if anything appeared since the scan,
		// failing loudly beats destroying it
		script += "DROP SCHEMA " + shadow + ";\n";
	}
	return script;
}

static string NgramIndexStatsQuery(ClientContext &context, const FunctionParameters &parameters) {
	auto table_input = parameters.values[0].ToString();

	auto target = ResolveTarget(context, table_input, string(), false);
	if (!target.entry->IsDuckTable()) {
		throw BinderException("ngram_index_stats: %s is not a DuckDB base table", table_input);
	}
	auto schema_entry =
	    Catalog::GetSchema(context, target.catalog_name, target.shadow_schema, OnEntryNotFound::RETURN_NULL);
	if (!schema_entry) {
		throw CatalogException("No ngram indexes exist on %s", target.table_name);
	}

	vector<string> columns;
	schema_entry->Scan(context, CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
		const string prefix = "meta_";
		if (entry.name.rfind(prefix, 0) == 0) {
			columns.push_back(entry.name.substr(prefix.size()));
		}
	});
	if (columns.empty()) {
		throw CatalogException("No ngram indexes exist on %s", target.table_name);
	}

	// A stats run is also the place a user looks to decide whether to refresh
	// or compact, so it reports the table facts the maintenance pragmas
	// compare against: how far the index lags the table, how fragmented the
	// segments are, and whether a detector already knows the index is dead.
	// The staleness verdict and the table's rowid count are read here, in the
	// pragma callback, and embedded as literals.
	auto fingerprint = ComputeTableFingerprint(context, *target.entry);
	auto &transaction = DuckTransaction::Get(context, target.entry->ParentCatalog());

	auto shadow = Ident(target.catalog_name) + "." + Ident(target.shadow_schema);
	string query;
	std::sort(columns.begin(), columns.end());
	for (auto &indexed_column : columns) {
		auto meta = shadow + "." + Ident(MetaTableName(indexed_column));
		auto segments = shadow + "." + Ident(SegmentsTableName(indexed_column));
		auto stats = shadow + "." + Ident(StatsTableName(indexed_column));
		string staleness;
		{
			auto &meta_entry = ResolveExistingTable(context, target.catalog_name, target.shadow_schema,
			                                        MetaTableName(indexed_column), "ngram index meta table");
			ShadowTarget shadow_target {target.schema_name, target.table_name, indexed_column, target.shadow_schema};
			// ReadMeta raises the collision error itself when this meta row
			// names a different base table, which is what the pragma should do
			// rather than silently reporting nothing
			auto info = ReadMeta(context, transaction, meta_entry, shadow_target);
			staleness = CertainStaleReason(info, fingerprint);
			if (staleness.empty()) {
				staleness = SampleStaleReason(context, *target.entry, info);
			}
		}
		if (!query.empty()) {
			query += "UNION ALL ";
		}
		query += "SELECT column_name, gram_size, case_insensitive, hwm_rowid, " +
		         to_string(fingerprint.total_rows - 1) +
		         "::BIGINT AS table_max_rowid, "
		         "(SELECT count(DISTINCT gram) FROM " +
		         stats +
		         ") AS distinct_grams, "
		         "(SELECT count(*) FROM " +
		         segments +
		         ") AS segments, "
		         "(SELECT count(*) FROM (SELECT gram, segment_no FROM " +
		         segments +
		         " GROUP BY gram, segment_no HAVING count(*) > 1)) AS fragmented_keys, "
		         "(SELECT count(DISTINCT generation) FROM " +
		         segments +
		         ") AS generations, "
		         "(SELECT coalesce(sum(rowid_count), 0) FROM " +
		         segments +
		         ") AS posting_entries, "
		         "(SELECT coalesce(sum(octet_length(postings)), 0) FROM " +
		         segments + ") AS postings_bytes, " + (staleness.empty() ? string("NULL::VARCHAR") : Lit(staleness)) +
		         " AS stale_reason FROM " + meta + " ";
	}
	query += "ORDER BY column_name;";
	return query;
}

void RegisterIndexPragmas(ExtensionLoader &loader) {
	auto create_fun = PragmaFunction::PragmaCall("create_ngram_index", CreateNgramIndexQuery,
	                                             {LogicalType::VARCHAR, LogicalType::VARCHAR});
	create_fun.named_parameters["gram"] = LogicalType::INTEGER;
	create_fun.named_parameters["case_insensitive"] = LogicalType::BOOLEAN;
	loader.RegisterFunction(create_fun);

	loader.RegisterFunction(PragmaFunction::PragmaCall("drop_ngram_index", DropNgramIndexQuery,
	                                                   {LogicalType::VARCHAR, LogicalType::VARCHAR}));

	loader.RegisterFunction(
	    PragmaFunction::PragmaCall("ngram_index_stats", NgramIndexStatsQuery, {LogicalType::VARCHAR}));
}

} // namespace ngram
} // namespace duckdb
