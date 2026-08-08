#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "ngram/index_pragmas.hpp"

namespace duckdb {
namespace ngram {

//! The index lives in ordinary tables under one schema per indexed base table
//! (ngram_<schema>_<table>), three tables per indexed column. Nothing persisted
//! references extension functions, so a database opened without the extension
//! reads and writes normally and the index schema is inert.

static string ShadowSchemaName(const string &schema, const string &table) {
	return "ngram_" + schema + "_" + table;
}

static string MetaTableName(const string &column) {
	return "meta_" + column;
}

static string SegmentsTableName(const string &column) {
	return "segments_" + column;
}

static string StatsTableName(const string &column) {
	return "stats_" + column;
}

static string Ident(const string &name) {
	return KeywordHelper::WriteOptionallyQuoted(name);
}

static string Lit(const string &value) {
	return KeywordHelper::WriteQuoted(value);
}

struct ResolvedTarget {
	string catalog_name;
	string schema_name;
	string table_name;
	string column_name;
	string shadow_schema;
};

static ResolvedTarget ResolveTarget(ClientContext &context, const string &table_input, const string &column_name,
                                    bool require_column) {
	auto qname = QualifiedName::Parse(table_input);
	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, qname.name);
	auto &table_entry =
	    Catalog::GetEntry(context, qname.catalog, qname.schema, lookup, OnEntryNotFound::THROW_EXCEPTION)
	        ->Cast<TableCatalogEntry>();
	if (require_column) {
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
	target.column_name = column_name;
	target.shadow_schema = ShadowSchemaName(target.schema_name, target.table_name);
	return target;
}

static bool ShadowTableExists(ClientContext &context, const ResolvedTarget &target, const string &name) {
	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, name);
	auto entry =
	    Catalog::GetEntry(context, target.catalog_name, target.shadow_schema, lookup, OnEntryNotFound::RETURN_NULL);
	return entry != nullptr;
}

static string CreateNgramIndexQuery(ClientContext &context, const FunctionParameters &parameters) {
	auto table_input = parameters.values[0].ToString();
	auto column_name = parameters.values[1].ToString();

	int32_t gram_size = 3;
	bool case_insensitive = true;
	for (auto &entry : parameters.named_parameters) {
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
	if (ShadowTableExists(context, target, MetaTableName(column_name))) {
		throw CatalogException("An ngram index already exists on %s.%s (%s); use drop_ngram_index first",
		                       target.table_name, column_name, target.shadow_schema);
	}

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
	string pairs = Ident("__ngram_build_pairs");
	script += "CREATE SCHEMA IF NOT EXISTS " + shadow + ";\n";
	script += "CREATE TABLE " + meta +
	          " AS SELECT "
	          "1 AS format_version, " +
	          Lit(target.schema_name) + " AS schema_name, " + Lit(target.table_name) + " AS table_name, " +
	          Lit(column_name) + " AS column_name, " + gram_str + " AS gram_size, " + ci_str +
	          " AS case_insensitive, "
	          "(SELECT coalesce(max(rowid), -1) FROM " +
	          base + ") AS hwm_rowid;\n";
	script += "CREATE OR REPLACE TEMP TABLE " + pairs +
	          " AS "
	          "SELECT rowid AS r, rowid >> 20 AS segment_no, unnest(trigrams(" +
	          column + ", " + gram_str + ", " + ci_str + ")) AS gram FROM " + base + " WHERE " + column +
	          " IS NOT NULL;\n";
	script += "CREATE TABLE " + segments +
	          " AS "
	          "SELECT gram, segment_no, 0 AS generation, "
	          "ngram_encode_postings(list(r)) AS postings, "
	          "count(DISTINCT r)::BIGINT AS rowid_count, min(r) AS min_rowid, max(r) AS max_rowid "
	          "FROM " +
	          pairs + " GROUP BY gram, segment_no;\n";
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
	if (!ShadowTableExists(context, target, MetaTableName(column_name))) {
		throw CatalogException("No ngram index exists on %s.%s", target.table_name, column_name);
	}

	auto schema_entry =
	    Catalog::GetSchema(context, target.catalog_name, target.shadow_schema, OnEntryNotFound::THROW_EXCEPTION);
	idx_t other_tables = 0;
	schema_entry->Scan(context, CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
		if (entry.name != MetaTableName(column_name) && entry.name != SegmentsTableName(column_name) &&
		    entry.name != StatsTableName(column_name)) {
			other_tables++;
		}
	});

	auto shadow = Ident(target.catalog_name) + "." + Ident(target.shadow_schema);
	if (other_tables == 0) {
		return "DROP SCHEMA " + shadow + " CASCADE;\n";
	}
	string script;
	script += "DROP TABLE " + shadow + "." + Ident(MetaTableName(column_name)) + ";\n";
	script += "DROP TABLE " + shadow + "." + Ident(SegmentsTableName(column_name)) + ";\n";
	script += "DROP TABLE " + shadow + "." + Ident(StatsTableName(column_name)) + ";\n";
	return script;
}

static string NgramIndexStatsQuery(ClientContext &context, const FunctionParameters &parameters) {
	auto table_input = parameters.values[0].ToString();

	auto target = ResolveTarget(context, table_input, string(), false);
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

	auto shadow = Ident(target.catalog_name) + "." + Ident(target.shadow_schema);
	string query;
	for (auto &indexed_column : columns) {
		auto meta = shadow + "." + Ident(MetaTableName(indexed_column));
		auto segments = shadow + "." + Ident(SegmentsTableName(indexed_column));
		auto stats = shadow + "." + Ident(StatsTableName(indexed_column));
		if (!query.empty()) {
			query += "UNION ALL ";
		}
		query += "SELECT column_name, gram_size, case_insensitive, hwm_rowid, "
		         "(SELECT count(*) FROM " +
		         stats +
		         ") AS distinct_grams, "
		         "(SELECT count(*) FROM " +
		         segments +
		         ") AS segments, "
		         "(SELECT coalesce(sum(rowid_count), 0) FROM " +
		         segments +
		         ") AS posting_entries, "
		         "(SELECT coalesce(sum(octet_length(postings)), 0) FROM " +
		         segments + ") AS postings_bytes FROM " + meta + " ";
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
