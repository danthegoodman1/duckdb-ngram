//===----------------------------------------------------------------------===//
// pragmas.cpp: the PRAGMA entry points and the metadata table functions: parameter parsing, target resolution, and
// the query or script each one returns to the statement preprocessor.
//===----------------------------------------------------------------------===//

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
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

//! The drop script of the index `index_ref` in `catalog_name`: the form that
//! needs no base table, for orphaned, malformed and old-format rows.
static string DropByReference(ClientContext &context, const string &catalog_name, const string &index_ref) {
	auto database = DatabaseManager::Get(context).GetDatabase(context, catalog_name);
	if (!database || database->IsReadOnly()) {
		throw InvalidInputException("drop_ngram_index: catalog %s is missing or read-only", catalog_name);
	}
	auto index = FindObserved(context, catalog_name, index_ref);
	if (!index.location.registry_oid) {
		throw InvalidInputException("drop_ngram_index: %s: %s; drop the storage tables of that id in the %s schema "
		                            "manually",
		                            index_ref, index.reason, NGRAM_SCHEMA);
	}
	return DropIndexScript(context, index);
}

//! drop_ngram_index(table, column) drops the one index of that column;
//! drop_ngram_index(index_ref, catalog = 'db') drops by reference, in the
//! current database unless a catalog is named, since copied attached
//! databases may hold the same reference.
static string DropNgramIndexQuery(ClientContext &context, const FunctionParameters &parameters) {
	string catalog_name;
	auto named = parameters.named_parameters.find("catalog");
	if (named != parameters.named_parameters.end()) {
		catalog_name = RequireStringParam(named->second, "drop_ngram_index", "catalog");
	}
	if (parameters.values.size() == 1) {
		if (catalog_name.empty()) {
			catalog_name = DatabaseManager::GetDefaultDatabase(context);
		}
		return DropByReference(context, catalog_name, parameters.values[0].ToString());
	}
	if (!catalog_name.empty()) {
		throw BinderException("drop_ngram_index: the catalog parameter belongs to the index_ref form; qualify the "
		                      "table name instead");
	}
	auto table_input = parameters.values[0].ToString();
	auto column_name = parameters.values[1].ToString();
	auto target = ResolveTarget(context, table_input, column_name, false);
	auto indexes = ExistingIndexes(context, target);
	RequireUniqueIndexColumns(indexes);
	if (indexes.empty()) {
		throw CatalogException("No ngram index exists on %s.%s", target.table_name, target.column_name);
	}
	return DropByReference(context, target.catalog_name, indexes[0].index_ref);
}

//! The statistics SELECT of every index of `table_input`. A stats run is
//! where a user decides whether to refresh or compact, so it reports the
//! table facts the maintenance pragmas compare against: how far the index
//! lags the table, how fragmented the segments are, and whether the guard
//! already knows the index is dead. Every fact is read by the SELECT itself,
//! the guard verdict through one join with ngram_indexes(), so one executing
//! statement sees one snapshot; only the list of indexes is resolved at bind.
static string IndexStatsSelect(ClientContext &context, const string &table_input) {
	auto target = ResolveTarget(context, table_input, string(), false);
	if (!target.entry->IsDuckTable()) {
		throw BinderException("ngram_index_stats: %s is not a DuckDB base table", table_input);
	}
	auto indexes = ExistingIndexes(context, target);
	RequireUniqueIndexColumns(indexes);
	if (indexes.empty()) {
		throw CatalogException("No ngram indexes exist on %s", target.table_name);
	}
	auto base = target.Qualified();
	auto count = SystemFunction("count");
	auto subquery = [](const string &select) {
		return "(SELECT " + select + ")";
	};
	auto committed = " AND rowid < " + to_string(MAX_ROW_ID);
	std::sort(indexes.begin(), indexes.end(), [](const IndexLocation &left, const IndexLocation &right) {
		return left.column_name < right.column_name;
	});
	string rows;
	for (auto &location : indexes) {
		auto segments = StorageTable(target.catalog_name, location.SegmentsTable());
		if (!rows.empty()) {
			rows += " UNION ALL ";
		}
		// remaining_tail is what a bounded-refresh loop watches from outside the
		// call: the committed rows the index does not cover yet, counted against
		// the registry row's own mark, so it is exact even though deletes leave
		// rowid gaps below table_max_rowid, the highest committed live rowid.
		// Rows this transaction appended carry provisional rowids past
		// MAX_ROW_ID and are in neither.
		rows +=
		    "SELECT m.index_id::VARCHAR AS index_ref, m.column_name, m.gram_size, m.case_insensitive, "
		    "m.hwm_rowid, " +
		    subquery("coalesce(" + SystemFunction("max") + "(rowid), -1)::BIGINT FROM " + base + " WHERE true" +
		             committed) +
		    " AS table_max_rowid, " + subquery(count + "(*) FROM " + base + " WHERE rowid > m.hwm_rowid" + committed) +
		    " AS remaining_tail, " + subquery(count + "(DISTINCT gram_key) FROM " + segments) + " AS distinct_grams, " +
		    subquery(count + "(*) FROM " + segments) + " AS segments, " +
		    subquery(count + "(*) FROM (SELECT gram_key, segment_no FROM " + segments +
		             " GROUP BY gram_key, segment_no HAVING " + count + "(*) > 1)") +
		    " AS fragmented_keys, " + subquery(count + "(DISTINCT generation) FROM " + segments) + " AS generations, " +
		    subquery("coalesce(" + SystemFunction("sum") + "(rowid_count), 0) FROM " + segments) +
		    " AS posting_entries, " +
		    subquery("coalesce(" + SystemFunction("sum") + "(" + SystemFunction("octet_length") +
		             "(postings)), 0) FROM " + segments) +
		    " AS postings_bytes FROM " + Registry(target.catalog_name) +
		    " m WHERE m.index_id = " + Lit(location.index_ref) + "::UUID";
	}
	// the guard verdict of every index from one observation of the listing
	return "SELECT s.column_name, s.gram_size, s.case_insensitive, s.hwm_rowid, s.table_max_rowid, "
	       "s.remaining_tail, s.distinct_grams, s.segments, s.fragmented_keys, s.generations, s.posting_entries, "
	       "s.postings_bytes, i.reason AS stale_reason FROM (" +
	       rows + ") s LEFT JOIN " + SystemFunction("ngram_indexes") +
	       "() i ON i.database_name = " + Lit(target.catalog_name) +
	       " AND i.index_ref = s.index_ref ORDER BY s.column_name";
}

//! ngram_index_stats(table) as a table function: the SELECT above replaces
//! the call at bind, so it composes in FROM clauses and joins.
static unique_ptr<TableRef> IndexStatsBindReplace(ClientContext &context, TableFunctionBindInput &input) {
	auto table_input = RequireStringParam(input.inputs[0], "ngram_index_stats", "table");
	Parser parser(context.GetParserOptions());
	parser.ParseQuery(IndexStatsSelect(context, table_input));
	D_ASSERT(parser.statements.size() == 1 && parser.statements[0]->type == StatementType::SELECT_STATEMENT);
	auto select = unique_ptr_cast<SQLStatement, SelectStatement>(std::move(parser.statements[0]));
	return make_uniq<SubqueryRef>(std::move(select));
}

static string NgramIndexStatsQuery(ClientContext &context, const FunctionParameters &parameters) {
	return "SELECT * FROM " + SystemFunction("ngram_index_stats") + "(" + Lit(parameters.values[0].ToString()) + ");";
}

//===----------------------------------------------------------------------===//
// ngram_indexes(): every index and stray storage object of every attached
// DuckDB catalog, with its lifecycle status. The catalogs are observed when
// the statement executes, so a row describes the executing snapshot; the
// listing is the cheap status, ngram_index_stats the full aggregation.
//===----------------------------------------------------------------------===//

struct IndexesGlobalState : public GlobalTableFunctionState {
	vector<ObservedIndex> rows;
	idx_t offset = 0;
};

static unique_ptr<FunctionData> IndexesBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	names = {"database_name", "index_ref",      "schema_name", "table_name",
	         "column_name",   "format_version", "status",      "reason"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                LogicalType::VARCHAR, LogicalType::BIGINT,  LogicalType::VARCHAR, LogicalType::VARCHAR};
	return make_uniq<TableFunctionData>();
}

static unique_ptr<GlobalTableFunctionState> IndexesInitGlobal(ClientContext &context, TableFunctionInitInput &) {
	auto state = make_uniq<IndexesGlobalState>();
	for (auto &database : DatabaseManager::Get(context).GetDatabases(context)) {
		if (!database->HasStorageManager() || !database->GetCatalog().IsDuckCatalog()) {
			continue;
		}
		auto observed = ObserveCatalog(context, database->GetName());
		state->rows.insert(state->rows.end(), std::make_move_iterator(observed.begin()),
		                   std::make_move_iterator(observed.end()));
	}
	std::sort(state->rows.begin(), state->rows.end(), [](const ObservedIndex &left, const ObservedIndex &right) {
		if (left.catalog_name != right.catalog_name) {
			return left.catalog_name < right.catalog_name;
		}
		return left.location.index_ref < right.location.index_ref;
	});
	return state;
}

static void IndexesFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<IndexesGlobalState>();
	auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, state.rows.size() - state.offset);
	auto text = [](const string &value) {
		return value.empty() ? Value(LogicalType::VARCHAR) : Value(value);
	};
	for (idx_t i = 0; i < count; i++) {
		auto &row = state.rows[state.offset + i];
		output.SetValue(0, i, Value(row.catalog_name));
		output.SetValue(1, i, Value(row.location.index_ref));
		output.SetValue(2, i, text(row.schema_name));
		output.SetValue(3, i, text(row.table_name));
		output.SetValue(4, i, text(row.location.column_name));
		output.SetValue(5, i, row.format_version < 0 ? Value(LogicalType::BIGINT) : Value::BIGINT(row.format_version));
		output.SetValue(6, i, Value(row.status));
		output.SetValue(7, i, text(row.reason));
	}
	state.offset += count;
	output.SetCardinality(count);
}

static string NgramIndexesQuery(ClientContext &context, const FunctionParameters &) {
	return "SELECT * FROM " + SystemFunction("ngram_indexes") + "() ORDER BY database_name, index_ref";
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

	// drop_ngram_index(table, column) and drop_ngram_index(index_ref, catalog = 'db')
	PragmaFunctionSet drop_set("drop_ngram_index");
	for (auto &arguments :
	     vector<vector<LogicalType>> {{LogicalType::VARCHAR}, {LogicalType::VARCHAR, LogicalType::VARCHAR}}) {
		auto drop = PragmaFunction::PragmaCall("drop_ngram_index", DropNgramIndexQuery, arguments);
		drop.named_parameters["catalog"] = LogicalType::VARCHAR;
		drop_set.AddFunction(std::move(drop));
	}
	loader.RegisterFunction(std::move(drop_set));

	// the metadata table functions and their pragma spellings
	TableFunction indexes("ngram_indexes", {}, IndexesFunction, IndexesBind, IndexesInitGlobal);
	loader.RegisterFunction(indexes);
	TableFunction stats("ngram_index_stats", {LogicalType::VARCHAR}, nullptr);
	stats.bind_replace = IndexStatsBindReplace;
	loader.RegisterFunction(stats);
	loader.RegisterFunction(
	    PragmaFunction::PragmaCall("ngram_index_stats", NgramIndexStatsQuery, {LogicalType::VARCHAR}));
	loader.RegisterFunction(PragmaFunction::PragmaStatement("ngram_indexes", NgramIndexesQuery));

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
