//===----------------------------------------------------------------------===//
// lake_catalog.cpp: DuckLake catalogs as the index sees them: the caller's snapshot, the reader that queries the
// lake at it, the registry rows, and whether an index still describes its table.
//===----------------------------------------------------------------------===//

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/common/enums/query_result_state.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "ngram/lake.hpp"

namespace duckdb {
namespace ngram {

bool IsLakeTable(TableCatalogEntry &table) {
	return table.ParentCatalog().GetCatalogType() == LAKE_CATALOG_TYPE;
}

bool IsLakeCatalog(ClientContext &context, const string &catalog_name) {
	auto database = DatabaseManager::Get(context).GetDatabase(context, Identifier(catalog_name));
	return database && database->GetCatalog().GetCatalogType() == LAKE_CATALOG_TYPE;
}

int64_t LakeTransactionSnapshot(ClientContext &context, const string &catalog_name) {
	// DuckLake fixes a transaction's snapshot at its first read and reports it
	// through ducklake_current_snapshot, whose bind reads it from the calling
	// transaction. Binding and running that function here, rather than in SQL,
	// is what lets planning learn the snapshot while DuckDB holds the context.
	EntryLookupInfo lookup(
	    CatalogType::TABLE_FUNCTION_ENTRY,
	    QualifiedName(Identifier(SYSTEM_CATALOG), Identifier(DEFAULT_SCHEMA), Identifier("ducklake_current_snapshot")));
	auto entry = Catalog::GetEntry(context, lookup, OnEntryNotFound::RETURN_NULL);
	if (!entry || entry->type != CatalogType::TABLE_FUNCTION_ENTRY) {
		throw InvalidInputException("ngram: %s is a DuckLake catalog, but ducklake_current_snapshot is missing",
		                            catalog_name);
	}
	auto definition = entry->Cast<TableFunctionCatalogEntry>().functions.GetFunctionByArguments(
	    context, vector<LogicalType> {LogicalType::VARCHAR});
	BoundTableFunction function(definition);
	vector<Value> inputs {Value(catalog_name)};
	named_argument_map_t named_parameters;
	vector<LogicalType> input_table_types;
	vector<Identifier> input_table_names;
	TableFunctionRef ref;
	TableFunctionBindInput bind_input(inputs, named_parameters, input_table_types, input_table_names,
	                                  function.function_info.get(), nullptr, function, ref);
	vector<LogicalType> return_types;
	vector<Identifier> names;
	auto bind_data = function.bind(context, bind_input, return_types, names);
	if (return_types.size() != 1) {
		throw InternalException("ngram: ducklake_current_snapshot returned %llu columns", return_types.size());
	}
	TableFunctionInitInput init_input(bind_data.get(), vector<column_t> {0}, vector<idx_t>(), nullptr);
	auto global_state = function.init_global ? function.init_global(context, init_input) : nullptr;
	ThreadContext thread(context);
	ExecutionContext execution(context, thread, nullptr);
	auto local_state = function.init_local ? function.init_local(execution, init_input, global_state.get()) : nullptr;
	DataChunk chunk;
	chunk.Initialize(Allocator::Get(context), return_types);
	TableFunctionInput input(bind_data.get(), local_state.get(), global_state.get());
	function.function(context, input, chunk);
	if (chunk.size() != 1) {
		throw InternalException("ngram: ducklake_current_snapshot returned %llu rows", chunk.size());
	}
	return chunk.GetValue(0, 0).GetValue<int64_t>();
}

int64_t LakeSchemaVersion(ClientContext &context, const string &catalog_name) {
	auto &catalog = Catalog::GetCatalog(context, Identifier(catalog_name));
	auto version = catalog.GetCatalogVersion(context);
	if (!version.IsValid()) {
		throw InternalException("ngram: DuckLake catalog %s reports no catalog version", catalog_name);
	}
	return NumericCast<int64_t>(version.GetIndex());
}

bool LakeWrittenByTransaction(ClientContext &context, const string &catalog_name) {
	auto modified = MetaTransaction::Get(context).ModifiedDatabase();
	return modified && modified->GetName().GetIdentifierName() == catalog_name;
}

//===----------------------------------------------------------------------===//
// LakeReader
//===----------------------------------------------------------------------===//

//! Settings the caller set for its own session (S3 credentials and endpoints
//! among them) apply to the reader's reads as well.
static void CopySessionSettings(ClientContext &from, ClientContext &to) {
	auto count = DBConfig::GetOptionCount() + DBConfig::GetConfig(from).user_settings.GetExtensionSettings().size();
	auto &global = DBConfig::GetConfig(from).user_settings;
	for (idx_t index = 0; index < count; index++) {
		if (!from.config.user_settings.IsSet(index)) {
			continue;
		}
		Value value;
		from.config.user_settings.TryGetSetting(global, index, value);
		to.config.user_settings.SetUserSetting(index, std::move(value));
	}
}

LakeReader::LakeReader(ClientContext &context_p, string catalog_name_p)
    : context(context_p), catalog_name(std::move(catalog_name_p)),
      snapshot(LakeTransactionSnapshot(context_p, catalog_name)) {
}

LakeReader::~LakeReader() {
}

string LakeReader::At() const {
	return " AT (VERSION => " + to_string(snapshot) + ")";
}

string LakeReader::IndexTable(const string &table) const {
	return StorageTable(catalog_name, table);
}

Connection &LakeReader::GetConnection() {
	if (!connection) {
		connection = make_uniq<Connection>(*context.db);
		CopySessionSettings(context, *connection->context);
	}
	return *connection;
}

bool LakeReader::TryExecute(const string &sql, string &error) {
	auto result = GetConnection().Query(sql);
	if (result->HasError()) {
		error = result->GetError();
		return false;
	}
	return true;
}

bool LakeReader::TryQuery(const string &sql, LakeRows &rows, string &error) {
	auto &connection = GetConnection();
	auto result = connection.Submit(sql);
	// the caller's interrupt stops the read too
	while (!result->HasError()) {
		if (context.IsInterrupted()) {
			connection.Interrupt();
		}
		auto state = result->ExecuteTask();
		if (state == QueryResultState::READY || state == QueryResultState::FINISHED ||
		    state == QueryResultState::EXECUTION_ERROR) {
			break;
		}
		if (state == QueryResultState::BLOCKED || state == QueryResultState::NO_TASKS_AVAILABLE) {
			result->WaitForTask();
		}
	}
	context.InterruptCheck();
	if (result->HasError()) {
		error = result->GetError();
		return false;
	}
	rows.rows.clear();
	while (auto chunk = result->Fetch()) {
		for (idx_t row = 0; row < chunk->size(); row++) {
			vector<Value> values;
			for (idx_t column = 0; column < chunk->ColumnCount(); column++) {
				values.push_back(chunk->GetValue(column, row));
			}
			rows.rows.push_back(std::move(values));
		}
	}
	return true;
}

LakeRows LakeReader::Query(const string &sql, const char *what) {
	LakeRows rows;
	string error;
	if (!TryQuery(sql, rows, error)) {
		throw InvalidInputException("ngram: reading %s of DuckLake catalog %s failed: %s", what, catalog_name, error);
	}
	return rows;
}

//===----------------------------------------------------------------------===//
// Registry
//===----------------------------------------------------------------------===//

string LakeIndex::Hex() const {
	return StringUtil::Replace(index_ref, "-", "");
}

string LakeIndex::SegmentsTable() const {
	return "segments_" + Hex();
}

bool LakeRegistryExists(ClientContext &context, const string &catalog_name) {
	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, QualifiedName(Identifier(catalog_name), Identifier(NGRAM_SCHEMA),
	                                                               Identifier(REGISTRY_TABLE)));
	auto entry = Catalog::GetEntry(context, lookup, OnEntryNotFound::RETURN_NULL);
	return entry && entry->type == CatalogType::TABLE_ENTRY;
}

vector<LakeIndex> ReadLakeRegistry(LakeReader &reader, const string &schema_name, const string &table_name) {
	vector<LakeIndex> result;
	// the caller's catalog holds the same snapshot, so a lake that never had
	// an index costs no query
	if (!LakeRegistryExists(reader.Context(), reader.CatalogName())) {
		return result;
	}
	string sql = "SELECT registry_version, index_id::VARCHAR, schema_name, table_name, column_name, table_id, "
	             "format_version, tokenizer, min_gram, max_gram, case_insensitive, covered_snapshot, schema_version "
	             "FROM " +
	             reader.IndexTable(REGISTRY_TABLE) + reader.At() + " WHERE kind = 'index'";
	if (!table_name.empty()) {
		sql += " AND lower(schema_name) = lower(" + Lit(schema_name) + ") AND lower(table_name) = lower(" +
		       Lit(table_name) + ")";
	}
	sql += " ORDER BY index_id";
	auto rows = reader.Query(sql, "the ngram registry");
	for (idx_t row = 0; row < rows.RowCount(); row++) {
		auto value = [&](idx_t column) {
			return rows.GetValue(column, row);
		};
		LakeIndex index;
		index.index_ref = value(1).ToString();
		index.schema_name = value(2).ToString();
		index.table_name = value(3).ToString();
		index.column_name = value(4).ToString();
		index.table_id = value(5).GetValue<int64_t>();
		index.format_version = value(6).GetValue<int64_t>();
		auto registry_version = value(0).GetValue<int64_t>();
		if (registry_version != LAKE_REGISTRY_VERSION || index.format_version != LAKE_FORMAT_VERSION) {
			index.error =
			    StringUtil::Format("registry version %lld, format %lld; this extension reads registry "
			                       "version %lld, format %lld; rebuild the index",
			                       registry_version, index.format_version, LAKE_REGISTRY_VERSION, LAKE_FORMAT_VERSION);
			result.push_back(std::move(index));
			continue;
		}
		try {
			index.options.tokenizer = ParseTokenizer(value(7).ToString(), "ngram");
			index.options.min_gram = NumericCast<idx_t>(value(8).GetValue<int64_t>());
			index.options.max_gram = NumericCast<idx_t>(value(9).GetValue<int64_t>());
			index.options.case_insensitive = value(10).GetValue<bool>();
			ValidateGramOptions(index.options, "ngram");
		} catch (std::exception &ex) {
			index.error = "unreadable gram options: " + ErrorData(ex).RawMessage();
		}
		index.covered_snapshot = value(11).GetValue<int64_t>();
		auto version = value(12);
		index.schema_version = version.IsNull() ? -1 : version.GetValue<int64_t>();
		result.push_back(std::move(index));
	}
	return result;
}

string LakeTailSource(LakeReader &reader, const string &schema_name, const string &table_name,
                      int64_t covered_snapshot) {
	auto feed = SystemFunction("ducklake_table_insertions") + "(" + Lit(reader.CatalogName()) + ", " +
	            Lit(schema_name) + ", " + Lit(table_name) + ", " + to_string(covered_snapshot + 1) + ", " +
	            to_string(reader.Snapshot()) + ")";
	LakeRows probe;
	string error;
	if (reader.TryQuery("SELECT 1 FROM " + feed + " LIMIT 0", probe, error)) {
		return feed;
	}
	return "(SELECT *, rowid FROM " + Ident(reader.CatalogName()) + "." + Ident(schema_name) + "." + Ident(table_name) +
	       reader.At() + " WHERE snapshot_id > " + to_string(covered_snapshot) + ")";
}

string LakeStaleReason(ClientContext &context, LakeReader &reader, const LakeIndex &index) {
	if (!index.error.empty()) {
		return index.error;
	}
	if (index.covered_snapshot > reader.Snapshot()) {
		return "the index covers a later snapshot than the transaction reads";
	}
	// Only a schema change can make postings disagree with the rows the change
	// feed leaves out: a column dropped and added back under the same name
	// reads its default for every older row. An unchanged schema version
	// proves no table changed; a changed one sends the check to the snapshot
	// history, which names every altered and dropped table.
	if (index.schema_version >= 0 && index.schema_version == LakeSchemaVersion(context, reader.CatalogName())) {
		return string();
	}
	auto id = Lit(to_string(index.table_id));
	auto rows = reader.Query(
	    "SELECT " + SystemFunction("count") + "(*), " + SystemFunction("count") + "(*) FILTER (WHERE " +
	        SystemFunction("list_contains") + "(changes['tables_altered'], " + id + ") OR " +
	        SystemFunction("list_contains") + "(changes['tables_dropped'], " + id + ")) FROM " +
	        SystemFunction("ducklake_snapshots") + "(" + Lit(reader.CatalogName()) + ") WHERE snapshot_id > " +
	        to_string(index.covered_snapshot) + " AND snapshot_id <= " + to_string(reader.Snapshot()),
	    "the snapshot history");
	if (rows.GetValue(1, 0).GetValue<int64_t>() > 0) {
		return "the table was altered after the index's last refresh; rebuild the index";
	}
	// snapshot ids are consecutive, so a short count means expired snapshots,
	// whose changes can no longer be read
	if (rows.GetValue(0, 0).GetValue<int64_t>() != reader.Snapshot() - index.covered_snapshot) {
		return "snapshots after the index's last refresh were expired before a refresh; rebuild the index";
	}
	return string();
}

} // namespace ngram
} // namespace duckdb
