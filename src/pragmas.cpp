//===----------------------------------------------------------------------===//
// pragmas.cpp: the PRAGMA entry points: parameter parsing, target resolution, and the query or script each one
// returns to the statement preprocessor.
//===----------------------------------------------------------------------===//

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "ngram/build_sql.hpp"
#include "ngram/index_state.hpp"
#include "ngram_extension.hpp"

#include <algorithm>

namespace duckdb {
namespace ngram {

static string RequireStringParam(const Value &value, const char *fn, const char *name) {
	if (value.IsNull()) {
		throw BinderException("%s: parameter %s cannot be NULL", fn, name);
	}
	auto text = value.ToString();
	if (text.empty()) {
		throw BinderException("%s: parameter %s cannot be empty", fn, name);
	}
	return text;
}

static int64_t RequireRowBound(const Value &value) {
	if (value.IsNull()) {
		throw BinderException("ngram_refresh: parameter max_rows cannot be NULL");
	}
	auto rows = value.GetValue<int64_t>();
	if (rows < 1) {
		throw InvalidInputException("ngram_refresh: max_rows must be at least 1, got %lld", rows);
	}
	return rows;
}

//! ngram_refresh's parameters: the column filter and the row bound, which may
//! be given positionally or by name but not both.
static void ParseRefreshParameters(const FunctionParameters &parameters, string &only_column, bool &bounded,
                                   int64_t &max_rows) {
	if (parameters.values.size() > 1) {
		max_rows = RequireRowBound(parameters.values[1]);
		bounded = true;
	}
	for (auto &entry : parameters.named_parameters) {
		if (entry.first == "col") {
			only_column = RequireStringParam(entry.second, "ngram_refresh", "col");
		} else if (entry.first == "max_rows") {
			if (bounded) {
				throw BinderException("ngram_refresh: max_rows was given twice, positionally and by name");
			}
			max_rows = RequireRowBound(entry.second);
			bounded = true;
		} else {
			throw BinderException("ngram_refresh: unknown named parameter %s", entry.first);
		}
	}
}

//! Gram size and case flag from create_ngram_index's named parameters.
static GramOptions ParseCreateOptions(const FunctionParameters &parameters) {
	int32_t gram_size = 3;
	GramOptions options;
	for (auto &entry : parameters.named_parameters) {
		if (entry.second.IsNull()) {
			throw BinderException("create_ngram_index: parameter %s cannot be NULL", entry.first);
		}
		if (entry.first == "gram") {
			gram_size = entry.second.GetValue<int32_t>();
		} else if (entry.first == "case_insensitive") {
			options.case_insensitive = entry.second.GetValue<bool>();
		} else {
			throw BinderException("create_ngram_index: unknown named parameter %s", entry.first);
		}
	}
	if (gram_size < 1) {
		throw InvalidInputException("create_ngram_index: gram must be at least 1, got %d", gram_size);
	}
	options.gram_size = NumericCast<idx_t>(gram_size);
	return options;
}

static string CreateNgramIndexQuery(ClientContext &context, const FunctionParameters &parameters) {
	auto table_input = parameters.values[0].ToString();
	auto options = ParseCreateOptions(parameters);
	auto target = ResolveTarget(context, table_input, parameters.values[1].ToString(), true);
	if (!target.entry->IsDuckTable()) {
		// the index reads rowids and row storage directly; a table living in a
		// foreign catalog (sqlite, postgres, ...) has neither
		throw BinderException("create_ngram_index: %s is not a DuckDB base table", table_input);
	}
	return CreateIndexScript(context, target, options);
}

static string DropNgramIndexByIdQuery(ClientContext &context, const FunctionParameters &parameters) {
	auto catalog_name = parameters.values[0].ToString();
	auto index_ref = parameters.values[1].ToString();
	auto database = DatabaseManager::Get(context).GetDatabase(context, catalog_name);
	if (!database || database->IsReadOnly()) {
		throw InvalidInputException("drop_ngram_index_by_id: catalog %s is missing or read-only", catalog_name);
	}
	auto index = FindObserved(context, catalog_name, index_ref);
	if (!index.location.registry_oid) {
		throw InvalidInputException("drop_ngram_index_by_id: %s: %s; drop the storage tables of that id in the %s "
		                            "schema manually",
		                            index_ref, index.reason, NGRAM_SCHEMA);
	}
	return DropIndexScript(context, index);
}

static string DropNgramIndexQuery(ClientContext &context, const FunctionParameters &parameters) {
	auto table_input = parameters.values[0].ToString();
	auto column_name = parameters.values[1].ToString();

	auto target = ResolveTarget(context, table_input, column_name, false);
	auto indexes = ExistingIndexes(context, target);
	RequireUniqueIndexColumns(indexes);
	if (indexes.empty()) {
		throw CatalogException("No ngram index exists on %s.%s", target.table_name, target.column_name);
	}
	FunctionParameters by_id;
	by_id.values = {Value(target.catalog_name), Value(indexes[0].index_ref)};
	return DropNgramIndexByIdQuery(context, by_id);
}

static string NgramIndexStatsQuery(ClientContext &context, const FunctionParameters &parameters) {
	auto table_input = parameters.values[0].ToString();

	auto target = ResolveTarget(context, table_input, string(), false);
	if (!target.entry->IsDuckTable()) {
		throw BinderException("ngram_index_stats: %s is not a DuckDB base table", table_input);
	}
	auto indexes = ExistingIndexes(context, target);
	RequireUniqueIndexColumns(indexes);
	if (indexes.empty()) {
		throw CatalogException("No ngram indexes exist on %s", target.table_name);
	}

	// A stats run is where a user decides whether to refresh or compact, so it
	// reports the table facts the maintenance pragmas compare against: how far
	// the index lags the table, how fragmented the segments are, and whether the
	// guard already knows the index is dead. The guard verdict and the table's
	// rowid count are read here, in the pragma callback, and embedded as literals.
	auto total_rows = TableTotalRows(*target.entry);
	auto base = target.Qualified();
	auto count = SystemFunction("count");
	auto encode = SystemFunction("encode");
	auto subquery = [](const string &select) {
		return "(SELECT " + select + ")";
	};
	string query;
	std::sort(indexes.begin(), indexes.end(), [](const IndexLocation &left, const IndexLocation &right) {
		return left.column_name < right.column_name;
	});
	for (auto &location : indexes) {
		auto segments = StorageTable(target.catalog_name, location.SegmentsTable());
		auto stats = StorageTable(target.catalog_name, location.StatsTable());
		auto verdict = ValidateIndex(context, target, location);
		if (verdict.availability != IndexAvailability::AVAILABLE) {
			throw CatalogException("ngram: index %s no longer exists; was it dropped after binding?",
			                       location.index_ref);
		}
		auto &staleness = verdict.reason;
		if (!query.empty()) {
			query += "UNION ALL ";
		}
		// remaining_tail is what a bounded-refresh loop watches from outside the
		// call: the committed rows the index does not cover yet, counted against
		// the registry row's own mark, so it is exact even though deletes leave
		// rowid gaps below table_max_rowid.
		query +=
		    "SELECT m.column_name, m.gram_size, m.case_insensitive, m.hwm_rowid, " + to_string(total_rows - 1) +
		    "::BIGINT AS table_max_rowid, " +
		    subquery(count + "(*) FROM " + base + " WHERE rowid > m.hwm_rowid AND rowid < " + to_string(MAX_ROW_ID)) +
		    " AS remaining_tail, " + subquery(count + "(DISTINCT " + encode + "(gram)) FROM " + stats) +
		    " AS distinct_grams, " + subquery(count + "(*) FROM " + segments) + " AS segments, " +
		    subquery(count + "(*) FROM (SELECT " + encode + "(gram) AS gram_key, segment_no FROM " + segments +
		             " GROUP BY " + encode + "(gram), segment_no HAVING " + count + "(*) > 1)") +
		    " AS fragmented_keys, " + subquery(count + "(DISTINCT generation) FROM " + segments) + " AS generations, " +
		    subquery("coalesce(" + SystemFunction("sum") + "(rowid_count), 0) FROM " + segments) +
		    " AS posting_entries, " +
		    subquery("coalesce(" + SystemFunction("sum") + "(" + SystemFunction("octet_length") +
		             "(postings)), 0) FROM " + segments) +
		    " AS postings_bytes, " + (staleness.empty() ? string("NULL::VARCHAR") : Lit(staleness)) +
		    " AS stale_reason FROM " + Registry(target.catalog_name) +
		    " m WHERE m.index_id = " + Lit(location.index_ref) + "::UUID ";
	}
	query += "ORDER BY column_name;";
	return query;
}

static constexpr const char *OBSERVED_COLUMNS =
    "v(database_name,index_ref,schema_name,table_name,column_name,format_version,status,reason)";

static string ValuesRow(const ObservedIndex &index) {
	auto value = [](const string &text) {
		return text.empty() ? string("NULL::VARCHAR") : Lit(text);
	};
	return "(" + Lit(index.catalog_name) + ", " + Lit(index.location.index_ref) + ", " + value(index.schema_name) +
	       ", " + value(index.table_name) + ", " + value(index.location.column_name) + ", " +
	       (index.format_version < 0 ? "NULL::BIGINT" : to_string(index.format_version) + "::BIGINT") + ", " +
	       Lit(index.status) + ", " + value(index.reason) + ")";
}

static string NgramIndexesQuery(ClientContext &context, const FunctionParameters &) {
	vector<ObservedIndex> indexes;
	for (auto &database : DatabaseManager::Get(context).GetDatabases(context)) {
		if (!database->HasStorageManager() || !database->GetCatalog().IsDuckCatalog()) {
			continue;
		}
		auto observed = ObserveCatalog(context, database->GetName());
		indexes.insert(indexes.end(), std::make_move_iterator(observed.begin()),
		               std::make_move_iterator(observed.end()));
	}
	string query = "SELECT * FROM (VALUES ";
	if (indexes.empty()) {
		query += "(NULL::VARCHAR,NULL::VARCHAR,NULL::VARCHAR,NULL::VARCHAR,NULL::VARCHAR,NULL::BIGINT,NULL::VARCHAR,"
		         "NULL::VARCHAR)";
	} else {
		for (idx_t i = 0; i < indexes.size(); i++) {
			if (i) {
				query += ",";
			}
			query += ValuesRow(indexes[i]);
		}
	}
	query += ") " + string(OBSERVED_COLUMNS);
	if (indexes.empty()) {
		query += " WHERE false";
	}
	query += " ORDER BY database_name,index_ref";
	return query;
}

static string NgramIndexStatusQuery(ClientContext &context, const FunctionParameters &parameters) {
	auto index = FindObserved(context, parameters.values[0].ToString(), parameters.values[1].ToString());
	return "SELECT * FROM (VALUES " + ValuesRow(index) + ") " + OBSERVED_COLUMNS;
}

static string RefreshNgramIndexQuery(ClientContext &context, const FunctionParameters &parameters) {
	auto table_input = parameters.values[0].ToString();
	string only_column;
	bool bounded = false;
	int64_t max_rows = 0;
	ParseRefreshParameters(parameters, only_column, bounded, max_rows);
	auto target = ResolveTarget(context, table_input, string(), false);
	if (!target.entry->IsDuckTable()) {
		throw BinderException("ngram_refresh: %s is not a DuckDB base table", table_input);
	}
	return RefreshScript(context, target, only_column, bounded, max_rows);
}

static string CompactNgramIndexQuery(ClientContext &context, const FunctionParameters &parameters) {
	auto table_input = parameters.values[0].ToString();
	string only_column;
	bool purge = false;
	for (auto &entry : parameters.named_parameters) {
		if (entry.first == "col") {
			only_column = RequireStringParam(entry.second, "ngram_compact", "col");
		} else if (entry.first == "purge") {
			if (entry.second.IsNull()) {
				throw BinderException("ngram_compact: parameter purge cannot be NULL");
			}
			purge = entry.second.GetValue<bool>();
		} else {
			throw BinderException("ngram_compact: unknown named parameter %s", entry.first);
		}
	}
	auto target = ResolveTarget(context, table_input, string(), false);
	if (!target.entry->IsDuckTable()) {
		throw BinderException("ngram_compact: %s is not a DuckDB base table", table_input);
	}
	return CompactScript(context, target, only_column, purge);
}

void RegisterPragmas(ExtensionLoader &loader) {
	auto create_fun = PragmaFunction::PragmaCall("create_ngram_index", CreateNgramIndexQuery,
	                                             {LogicalType::VARCHAR, LogicalType::VARCHAR});
	create_fun.named_parameters["gram"] = LogicalType::INTEGER;
	create_fun.named_parameters["case_insensitive"] = LogicalType::BOOLEAN;
	loader.RegisterFunction(create_fun);

	loader.RegisterFunction(PragmaFunction::PragmaCall("drop_ngram_index", DropNgramIndexQuery,
	                                                   {LogicalType::VARCHAR, LogicalType::VARCHAR}));

	loader.RegisterFunction(
	    PragmaFunction::PragmaCall("ngram_index_stats", NgramIndexStatsQuery, {LogicalType::VARCHAR}));
	loader.RegisterFunction(PragmaFunction::PragmaStatement("ngram_indexes", NgramIndexesQuery));
	loader.RegisterFunction(PragmaFunction::PragmaCall("ngram_index_status", NgramIndexStatusQuery,
	                                                   {LogicalType::VARCHAR, LogicalType::VARCHAR}));
	loader.RegisterFunction(PragmaFunction::PragmaCall("drop_ngram_index_by_id", DropNgramIndexByIdQuery,
	                                                   {LogicalType::VARCHAR, LogicalType::VARCHAR}));

	// two overloads so the bound can be written either way: positionally,
	// PRAGMA ngram_refresh('t', 100000), or by name, max_rows = 100000 (pragma
	// named parameters take =, not :=)
	PragmaFunctionSet refresh_set("ngram_refresh");
	for (auto &arguments :
	     vector<vector<LogicalType>> {{LogicalType::VARCHAR}, {LogicalType::VARCHAR, LogicalType::BIGINT}}) {
		auto refresh = PragmaFunction::PragmaCall("ngram_refresh", RefreshNgramIndexQuery, arguments);
		refresh.named_parameters["col"] = LogicalType::VARCHAR;
		refresh.named_parameters["max_rows"] = LogicalType::BIGINT;
		refresh_set.AddFunction(std::move(refresh));
	}
	loader.RegisterFunction(std::move(refresh_set));

	auto compact = PragmaFunction::PragmaCall("ngram_compact", CompactNgramIndexQuery, {LogicalType::VARCHAR});
	compact.named_parameters["col"] = LogicalType::VARCHAR;
	compact.named_parameters["purge"] = LogicalType::BOOLEAN;
	loader.RegisterFunction(compact);
}

} // namespace ngram
} // namespace duckdb
