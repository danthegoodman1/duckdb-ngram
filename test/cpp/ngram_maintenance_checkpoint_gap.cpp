#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/execution/index/unbound_index.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/parser/statement/transaction_statement.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "duckdb/transaction/local_storage.hpp"
#include "core_functions_extension.hpp"
#include "ngram_extension.hpp"
#include "ngram/fence.hpp"
#include "ngram/rowid_guard.hpp"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <thread>
#if defined(__linux__)
#include <sys/wait.h>
#endif

using namespace duckdb;

static unique_ptr<MaterializedQueryResult> Query(Connection &con, const string &sql) {
	auto result = con.Query(sql);
	if (result->HasError()) {
		throw std::runtime_error(result->GetError() + "\nSQL: " + sql);
	}
	return result;
}

static void Check(Connection &con, const string &sql) {
	Query(con, sql);
}

static int64_t ScalarInt64(Connection &con, const string &sql) {
	return Query(con, sql)->GetValue(0, 0).GetValue<int64_t>();
}

static string ScalarString(Connection &con, const string &sql) {
	return Query(con, sql)->GetValue(0, 0).ToString();
}

static string IndexHex(Connection &con, const string &table_name, const string &column_name,
                       const string &catalog_name = string()) {
	auto registry = catalog_name.empty() ? string("__ngram.registry") : catalog_name + ".__ngram.registry";
	return ScalarString(con, "SELECT replace(index_id::VARCHAR, '-', '') FROM " + registry +
	                             " WHERE table_name=" + KeywordHelper::WriteQuoted(table_name) +
	                             " AND column_name=" + KeywordHelper::WriteQuoted(column_name));
}

static string IndexRef(Connection &con, const string &table_name, const string &column_name,
                       const string &catalog_name = string()) {
	auto registry = catalog_name.empty() ? string("__ngram.registry") : catalog_name + ".__ngram.registry";
	return ScalarString(con, "SELECT index_id::VARCHAR FROM " + registry +
	                             " WHERE table_name=" + KeywordHelper::WriteQuoted(table_name) +
	                             " AND column_name=" + KeywordHelper::WriteQuoted(column_name));
}

//! `__ngram.registry WHERE <owner>`: the registry row of one index, for reads.
static string OwnerRow(const string &table_name, const string &column_name) {
	return "__ngram.registry WHERE table_name=" + KeywordHelper::WriteQuoted(table_name) +
	       " AND column_name=" + KeywordHelper::WriteQuoted(column_name);
}

static string GuardName(Connection &con, const string &table_name, const string &column_name) {
	return ScalarString(con, "SELECT guard_name FROM " + OwnerRow(table_name, column_name));
}

static string IndexStatus(Connection &con, const string &table_name, const string &column_name) {
	auto catalog = ScalarString(con, "SELECT current_database()");
	auto result = Query(con, "PRAGMA ngram_index_status(" + KeywordHelper::WriteQuoted(catalog) + ", " +
	                             KeywordHelper::WriteQuoted(IndexRef(con, table_name, column_name)) + ")");
	return result->GetValue(6, 0).ToString();
}

static unique_ptr<MaterializedQueryResult> StatusByRef(Connection &con, const string &catalog,
                                                       const string &index_ref) {
	return Query(con, "PRAGMA ngram_index_status(" + KeywordHelper::WriteQuoted(catalog) + "," +
	                      KeywordHelper::WriteQuoted(index_ref) + ")");
}

static string StatusName(Connection &con, const string &catalog, const string &index_ref) {
	return StatusByRef(con, catalog, index_ref)->GetValue(6, 0).ToString();
}

static string StatusReason(Connection &con, const string &catalog, const string &index_ref) {
	auto result = StatusByRef(con, catalog, index_ref);
	return result->GetValue(7, 0).IsNull() ? string() : result->GetValue(7, 0).ToString();
}

static void DropByRef(Connection &con, const string &catalog, const string &index_ref) {
	Check(con, "PRAGMA drop_ngram_index_by_id(" + KeywordHelper::WriteQuoted(catalog) + "," +
	               KeywordHelper::WriteQuoted(index_ref) + ")");
}

static void ExpectStatus(Connection &con, const string &catalog, const string &index_ref, const string &expected) {
	auto actual = StatusName(con, catalog, index_ref);
	if (actual != expected) {
		throw std::runtime_error("expected index " + index_ref + " status " + expected + ", got " + actual + ": " +
		                         StatusReason(con, catalog, index_ref));
	}
}

static string TestUUID(uint64_t value) {
	char buffer[37];
	snprintf(buffer, sizeof(buffer), "00000000-0000-4000-8000-%012llx", static_cast<unsigned long long>(value));
	return buffer;
}

//! `[catalog.]__ngram.<part>_<hex>` for part in {segments, stats}.
static string StorageTable(Connection &con, const string &table_name, const string &column_name, const string &part,
                           const string &catalog_name = string()) {
	auto catalog = catalog_name.empty() ? string() : catalog_name + ".";
	return catalog + "__ngram." + part + "_" + IndexHex(con, table_name, column_name, catalog_name);
}

static void DropStorage(Connection &con, const string &table_name, const string &column_name) {
	Check(con, "DROP TABLE " + StorageTable(con, table_name, column_name, "segments"));
	Check(con, "DROP TABLE " + StorageTable(con, table_name, column_name, "stats"));
}

static idx_t StorageTableCount(Connection &con) {
	return NumericCast<idx_t>(ScalarInt64(
	    con, "SELECT count(*) FROM duckdb_tables() WHERE schema_name='__ngram' AND table_name<>'registry'"));
}

static void CopyFile(const string &source, const string &target) {
	std::ifstream input(source, std::ios::binary);
	std::ofstream output(target, std::ios::binary | std::ios::trunc);
	if (!input.is_open() || !output.is_open()) {
		throw std::runtime_error("failed to open database file for copying");
	}
	output << input.rdbuf();
	if (!output.good()) {
		throw std::runtime_error("failed to copy database file");
	}
}

static bool HasRowIdGuard(Connection &con, const string &table_name) {
	bool found = false;
	con.context->RunFunctionInTransaction([&]() {
		auto &table = Catalog::GetEntry<TableCatalogEntry>(
		                  *con.context, DatabaseManager::GetDefaultDatabase(*con.context), "main", table_name)
		                  .Cast<DuckTableEntry>();
		for (auto &entry : table.GetStorage().GetDataTableInfo()->GetIndexes().IndexEntries()) {
			if (entry.index->GetIndexType() == ngram::NGRAM_ROWID_GUARD_TYPE) {
				found = true;
				break;
			}
		}
	});
	return found;
}

static bool RowIdGuardIsBound(Connection &con, const string &table_name) {
	bool found = false;
	bool bound = false;
	con.context->RunFunctionInTransaction([&]() {
		auto &table = Catalog::GetEntry<TableCatalogEntry>(
		                  *con.context, DatabaseManager::GetDefaultDatabase(*con.context), "main", table_name)
		                  .Cast<DuckTableEntry>();
		for (auto &entry : table.GetStorage().GetDataTableInfo()->GetIndexes().IndexEntries()) {
			if (entry.index->GetIndexType() != ngram::NGRAM_ROWID_GUARD_TYPE) {
				continue;
			}
			if (found) {
				throw std::runtime_error("expected exactly one rowid guard on " + table_name);
			}
			found = true;
			bound = entry.index->IsBound();
		}
	});
	if (!found) {
		throw std::runtime_error("missing rowid guard on " + table_name);
	}
	return bound;
}

static void SetRowIdGuardBindState(Connection &con, const string &table_name, const string &guard_name,
                                   IndexBindState state, bool require_unbound) {
	bool found = false;
	con.context->RunFunctionInTransaction([&]() {
		auto &table = Catalog::GetEntry<TableCatalogEntry>(
		                  *con.context, DatabaseManager::GetDefaultDatabase(*con.context), "main", table_name)
		                  .Cast<DuckTableEntry>();
		for (auto &entry : table.GetStorage().GetDataTableInfo()->GetIndexes().IndexEntries()) {
			if (entry.index->GetIndexName() != guard_name) {
				continue;
			}
			if (require_unbound && entry.index->IsBound()) {
				throw std::runtime_error("expected an unbound rowid guard on " + table_name);
			}
			entry.bind_state.store(state);
			found = true;
			break;
		}
	});
	if (!found) {
		throw std::runtime_error("missing rowid guard " + guard_name + " on " + table_name);
	}
}

static void ExpectError(Connection &con, const string &sql, const string &needle) {
	auto result = con.Query(sql);
	if (!result->HasError() || result->GetError().find(needle) == string::npos) {
		throw std::runtime_error("expected error containing '" + needle + "', got: " +
		                         (result->HasError() ? result->GetError() : result->ToString()) + "\nSQL: " + sql);
	}
}

static void Rollback(Connection &con) {
	con.Query("ROLLBACK");
}

static void ExpectPreparedWriteFailure(Connection &con, PreparedStatement &prepared) {
	auto result = prepared.Execute();
	if (result->HasError()) {
		Rollback(con);
		return;
	}
	auto commit = con.Query("COMMIT");
	if (!commit->HasError()) {
		throw std::runtime_error("a write prepared against pre-guard storage committed");
	}
	Rollback(con);
}

static int64_t PreparedScalar(PreparedStatement &prepared) {
	// Materialize: a streamed result keeps the statement's transaction and
	// operator states alive until the connection's next statement, which would
	// block a creation barrier taken by another connection in the meantime.
	vector<Value> values;
	auto result = prepared.Execute(values, false);
	if (result->HasError()) {
		throw std::runtime_error("prepared query failed: " + result->GetError());
	}
	auto chunk = result->Fetch();
	if (!chunk || chunk->size() != 1) {
		throw std::runtime_error("prepared scalar returned no single row");
	}
	return chunk->GetValue(0, 0).GetValue<int64_t>();
}

static void ExpectPreparedError(PreparedStatement &prepared, const string &needle) {
	auto result = prepared.Execute();
	if (!result->HasError() || result->GetError().find(needle) == string::npos) {
		throw std::runtime_error("expected prepared error containing '" + needle +
		                         "', got: " + (result->HasError() ? result->GetError() : result->ToString()));
	}
}

static vector<unique_ptr<SQLStatement>> Expand(Connection &con, const string &sql) {
	auto statements = con.context->ParseStatements(sql);
	if (statements.empty() || statements.front()->type != StatementType::TRANSACTION_STATEMENT ||
	    statements.front()->Cast<TransactionStatement>().info->type != TransactionType::BEGIN_TRANSACTION) {
		throw std::runtime_error("pragma expansion did not begin with a generated transaction");
	}
	return statements;
}

static idx_t ExecuteThrough(Connection &con, vector<unique_ptr<SQLStatement>> &statements, const string &marker) {
	for (idx_t i = 0; i < statements.size(); i++) {
		if (!statements[i]) {
			continue;
		}
		auto text = statements[i]->ToString();
		auto result = con.Query(std::move(statements[i]));
		if (result->HasError()) {
			throw std::runtime_error(result->GetError() + "\nStatement: " + text);
		}
		if (text.find(marker) != string::npos) {
			return i + 1;
		}
	}
	throw std::runtime_error("generated statement marker not found: " + marker);
}

static idx_t ExecuteBeforeCommit(Connection &con, vector<unique_ptr<SQLStatement>> &statements) {
	for (idx_t i = 0; i < statements.size(); i++) {
		if (!statements[i]) {
			continue;
		}
		auto text = statements[i]->ToString();
		if (statements[i]->type == StatementType::TRANSACTION_STATEMENT &&
		    statements[i]->Cast<TransactionStatement>().info->type == TransactionType::COMMIT) {
			return i;
		}
		auto result = con.Query(std::move(statements[i]));
		if (result->HasError()) {
			throw std::runtime_error(result->GetError() + "\nStatement: " + text);
		}
	}
	throw std::runtime_error("generated transaction has no COMMIT");
}

static string ExecuteRemainingForError(Connection &con, vector<unique_ptr<SQLStatement>> &statements, idx_t start = 0) {
	for (idx_t i = start; i < statements.size(); i++) {
		if (!statements[i]) {
			continue;
		}
		auto result = con.Query(std::move(statements[i]));
		if (result->HasError()) {
			return result->GetError();
		}
	}
	return string();
}

static void RemoveDatabase(const string &path) {
	std::remove(path.c_str());
	std::remove((path + ".wal").c_str());
}

static void LoadNgram(DuckDB &db) {
	db.LoadStaticExtension<NgramExtension>();
}

static void LoadCoreFunctions(DuckDB &db) {
	db.LoadStaticExtension<CoreFunctionsExtension>();
}

static void StockWriteChild(const string &path, const string &kind) {
	string sql;
	bool expect_success = false;
	if (kind == "insert") {
		sql = "INSERT INTO wal_rows VALUES (300000, 'stock needle insert')";
	} else if (kind == "update") {
		sql = "UPDATE wal_rows SET s=s WHERE id=0";
	} else if (kind == "delete") {
		sql = "DELETE FROM wal_rows WHERE id=0";
	} else if (kind == "alter") {
		sql = "ALTER TABLE wal_rows DROP COLUMN s";
	} else if (kind == "add") {
		sql = "ALTER TABLE wal_rows ADD COLUMN stock_extra INTEGER";
		expect_success = true;
	} else {
		std::_Exit(2);
	}
	DBConfig config;
	config.options.load_extensions = false;
	DuckDB stock(path, &config);
	Connection con(stock);
	auto result = con.Query(sql);
	if ((expect_success && result->HasError()) ||
	    (!expect_success && (!result->HasError() || result->GetError().find("NGRAM_ROWID_GUARD") == string::npos))) {
		std::cerr << (result->HasError() ? result->GetError() : "write unexpectedly succeeded") << "\n";
		std::cerr.flush();
		std::_Exit(1);
	}
	std::_Exit(0);
}

static void StockWriteFails(const string &executable, const string &path, const string &kind) {
	string command;
	if (kind == "delete") {
#if defined(__linux__)
		command = "timeout 3s \"" + executable + "\" --stock-write \"" + path + "\" delete";
#else
		// v1.5.5's unknown-index DELETE busy-spin is a Linux host
		// characterization. Never start an unbounded child on other platforms.
		return;
#endif
	} else {
		command = "\"" + executable + "\" --stock-write \"" + path + "\" " + kind;
	}
	auto status = std::system(command.c_str());
	if (status == 0) {
		return;
	}
#if defined(__linux__)
	// v1.5.5 DELETE retries the same failed unbound-index bind and busy-spins.
	// The bounded child proves it still cannot mutate the table; loading the
	// extension is required for usable write behavior.
	if (kind == "delete" && status != -1 && WIFEXITED(status) && WEXITSTATUS(status) == 124) {
		return;
	}
#endif
	throw std::runtime_error("stock DuckDB did not fail closed for " + kind);
}

static void MutateGuardOption(Connection &con, const string &table_name, const string &guard_name, const string &option,
                              Value value) {
	auto catalog_name = DatabaseManager::GetDefaultDatabase(*con.context);
	bool found = false;
	con.context->RunFunctionInTransaction([&]() {
		auto &table =
		    Catalog::GetEntry<TableCatalogEntry>(*con.context, catalog_name, "main", table_name).Cast<DuckTableEntry>();
		for (auto &entry : table.GetStorage().GetDataTableInfo()->GetIndexes().IndexEntries()) {
			auto &index = *entry.index;
			if (index.GetIndexName() != guard_name || index.IsBound()) {
				continue;
			}
			auto &storage = const_cast<IndexStorageInfo &>(index.Cast<UnboundIndex>().GetStorageInfo());
			storage.options[option] = std::move(value);
			found = true;
		}
	});
	if (!found) {
		throw std::runtime_error("failed to mutate unbound rowid guard option for " + table_name + "." + guard_name);
	}
}

static void MutateGuardSource(Connection &con, const string &table_name, const string &guard_name) {
	MutateGuardOption(con, table_name, guard_name, "ngram_duckdb_source_id", Value("mismatched-test-source"));
}

static void TestCreationSchedules(const string &path) {
	RemoveDatabase(path);
	DuckDB db(path);
	{
		Connection host(db);
		auto version = Query(host, "SELECT library_version, source_id FROM system.main.pragma_version()");
		auto library_version = version->GetValue(0, 0).ToString();
		auto source_id = version->GetValue(1, 0).ToString();
		// The extension's pin: v1.5.5 built from this commit, with source_id an
		// abbreviation of it that has at least seven characters.
		const string pinned_commit = "d8cdaa33fda8df955cc76ef58a280f68f4cd43fa";
		bool pinned_source = source_id.size() >= 7 && source_id.size() <= pinned_commit.size() &&
		                     pinned_commit.compare(0, source_id.size(), source_id) == 0;
		if (library_version != "v1.5.5" || !pinned_source) {
			throw std::runtime_error("host pragma_version is outside the pinned rowid-guard runtime");
		}
	}
	LoadNgram(db);
	Connection setup(db), creator(db), old_update(db), old_insert(db), old_delete(db);
	Check(setup, "SET checkpoint_threshold='1TB'");
	Check(setup, "CREATE TABLE stale(id INTEGER, s VARCHAR)");
	Check(setup, "INSERT INTO stale VALUES (1, 'original needle')");

	Check(old_update, "BEGIN");
	Check(old_insert, "BEGIN");
	Check(old_delete, "BEGIN");
	auto update = old_update.Prepare("UPDATE stale SET s='stale update needle' WHERE id=1");
	auto insert = old_insert.Prepare("INSERT INTO stale VALUES (2, 'stale insert needle')");
	auto deletion = old_delete.Prepare("DELETE FROM stale WHERE id=1");
	if (update->HasError() || insert->HasError() || deletion->HasError()) {
		throw std::runtime_error("failed to prepare stale-writer discriminator");
	}
	Check(creator, "PRAGMA create_ngram_index('stale', 's')");
	ExpectPreparedWriteFailure(old_update, *update);
	ExpectPreparedWriteFailure(old_insert, *insert);
	ExpectPreparedWriteFailure(old_delete, *deletion);
	if (ScalarInt64(setup, "SELECT count(*) FROM stale WHERE id=1 AND s='original needle'") != 1 ||
	    ScalarInt64(setup, "SELECT count(*) FROM ngram_search('stale', 'needle')") != 1) {
		throw std::runtime_error("stale prepared writes changed the guarded table");
	}

	Check(setup, "CREATE TABLE active(id INTEGER, s VARCHAR)");
	Check(setup, "INSERT INTO active VALUES (1, 'x')");
	Connection writer(db);
	Check(writer, "BEGIN");
	Check(writer, "UPDATE active SET s='active writer' WHERE id=1");
	ExpectError(creator, "PRAGMA create_ngram_index('active', 's')", "active writer");
	Rollback(writer);
	Check(creator, "PRAGMA create_ngram_index('active', 's')");

	Check(setup, "CREATE TABLE recent(id INTEGER, s VARCHAR)");
	Connection old_creator(db);
	Check(old_creator, "BEGIN");
	Check(old_creator, "SELECT count(*) FROM recent");
	Check(writer, "INSERT INTO recent VALUES (1, 'recent writer')");
	ExpectError(old_creator, "PRAGMA create_ngram_index('recent', 's')", "recently committed writer");
	Rollback(old_creator);
	Check(creator, "PRAGMA create_ngram_index('recent', 's')");

	Check(setup, "CREATE TABLE local_insert(id INTEGER, s VARCHAR)");
	Check(setup, "INSERT INTO local_insert VALUES (1, 'base needle')");
	Check(creator, "BEGIN");
	Check(creator, "INSERT INTO local_insert VALUES (2, 'local needle')");
	Check(creator, "PRAGMA create_ngram_index('local_insert', 's')");
	Check(creator, "COMMIT");
	if (ScalarInt64(setup, "SELECT count(*) FROM ngram_search('local_insert', 'needle')") != 2) {
		throw std::runtime_error("same-transaction local insert was not exhaustive");
	}

	Check(setup, "CREATE TABLE rejected(id INTEGER, s VARCHAR)");
	Check(setup, "INSERT INTO rejected VALUES (1, 'x')");
	for (auto &dml : vector<string> {"UPDATE rejected SET s='y'", "DELETE FROM rejected"}) {
		Check(creator, "BEGIN");
		Check(creator, dml);
		ExpectError(creator, "PRAGMA create_ngram_index('rejected', 's')", "cannot follow updates, deletes");
		Rollback(creator);
	}
	Check(creator, "BEGIN");
	Check(creator, "CREATE TABLE unrelated_catalog_change(i INTEGER)");
	ExpectError(creator, "PRAGMA create_ngram_index('rejected', 's')", "catalog changes");
	Rollback(creator);

	// The second index of a table records the guard the first one created and
	// runs no barrier, so it may follow the first in one transaction.
	Check(setup, "CREATE TABLE twice(a VARCHAR, b VARCHAR)");
	Check(setup, "INSERT INTO twice VALUES ('alpha needle', 'beta needle')");
	Check(creator, "BEGIN");
	Check(creator, "PRAGMA create_ngram_index('twice', 'a')");
	Check(creator, "PRAGMA create_ngram_index('twice', 'b')");
	Check(creator, "COMMIT");
	if (ScalarInt64(setup, "SELECT count(*) FROM duckdb_indexes() WHERE table_name='twice'") != 1 ||
	    ScalarInt64(setup, "SELECT count(*) FROM ngram_search('twice', 'needle', col='b')") != 1) {
		throw std::runtime_error("two indexes created in one transaction did not share the guard");
	}

	// A post-install error must not leave the external EXCLUSIVE held for the
	// remainder of an explicit user transaction.
	Check(setup, "CREATE TABLE build_error(id INTEGER, s VARCHAR)");
	auto build = Expand(creator, "PRAGMA create_ngram_index('build_error', 's')");
	ExecuteThrough(creator, build, "__ngram_creation_finish");
	ExpectError(creator, "SELECT error('deterministic post-finish build error')", "post-finish build error");
	Rollback(creator);
	Check(writer, "INSERT INTO build_error VALUES (1, 'writer resumed')");

	// Closing a connection while the ADD barrier is still held rolls the
	// transaction back and releases the lock.
	Check(setup, "CREATE TABLE close_release(id INTEGER, s VARCHAR)");
	{
		auto closing = make_uniq<Connection>(db);
		auto body = Expand(*closing, "PRAGMA create_ngram_index('close_release', 's')");
		ExecuteThrough(*closing, body, "ADD COLUMN");
	}
	Check(writer, "INSERT INTO close_release VALUES (1, 'writer resumed')");

	// Stage an old-snapshot append physically, then let the creator install its
	// replacement table and guard. The writer's failed final commit reverts that
	// append and deliberately restores the old storage to MAIN; the creator must
	// then fail its own COMMIT instead of publishing against the wrong storage.
	// Keep this host-internal rollback discriminator in a fresh catalog: pinned
	// v1.5.5 DEBUG cannot safely revert an unrelated registry-table append after
	// the deliberately forced DataTable rollback conflict.
	DuckDB staged_db(nullptr);
	LoadNgram(staged_db);
	Connection staged_setup(staged_db), staged_creator(staged_db), staged_writer(staged_db);
	Check(staged_setup, "CREATE TABLE staged_revert(id INTEGER, s VARCHAR)");
	// Revert at a byte boundary: v1.5.5 DEBUG's validity reverter otherwise
	// checks padding bits against the logical row count and invalidates the DB.
	Check(staged_setup,
	      "INSERT INTO staged_revert SELECT i, CASE WHEN i=0 THEN 'base needle' ELSE 'x' END FROM range(8) t(i)");
	Check(staged_writer, "BEGIN");
	Check(staged_writer, "INSERT INTO staged_revert VALUES (8, 'staged needle')");
	auto &staged_catalog =
	    Catalog::GetCatalog(*staged_writer.context, ScalarString(staged_writer, "SELECT current_database()"));
	DuckTransaction::Get(*staged_writer.context, staged_catalog).GetLocalStorage().Commit(nullptr);
	auto staged_create = Expand(staged_creator, "PRAGMA create_ngram_index('staged_revert', 's')");
	auto staged_next = ExecuteThrough(staged_creator, staged_create, "__ngram_creation_finish");
	auto writer_commit = staged_writer.Query("COMMIT");
	if (!writer_commit->HasError()) {
		throw std::runtime_error("physically staged pre-barrier append unexpectedly committed");
	}
	string creator_error;
	string creator_statement;
	for (idx_t i = staged_next; i < staged_create.size(); i++) {
		if (!staged_create[i]) {
			continue;
		}
		creator_statement = staged_create[i]->ToString();
		auto result = staged_creator.Query(std::move(staged_create[i]));
		if (result->HasError()) {
			creator_error = result->GetError();
			break;
		}
	}
	if (creator_statement.find("COMMIT") == string::npos ||
	    creator_error.find("underlying table state was reverted") == string::npos) {
		throw std::runtime_error("creator did not fail its COMMIT after RevertAppend: " + creator_error);
	}
	Rollback(staged_creator);
	if (ScalarInt64(staged_setup, "SELECT count(*) FROM staged_revert") != 8 ||
	    HasRowIdGuard(staged_setup, "staged_revert") ||
	    ScalarInt64(staged_setup, "SELECT count(*) FROM duckdb_schemas() WHERE schema_name='__ngram'") != 0 ||
	    ScalarInt64(staged_setup, "SELECT count(*) FROM duckdb_tables() WHERE schema_name='__ngram'") != 0) {
		throw std::runtime_error("failed creator published data or leaked registry/storage/guard after RevertAppend");
	}
	Check(staged_creator, "PRAGMA create_ngram_index('staged_revert', 's')");
	if (ScalarInt64(staged_setup, "SELECT count(*) FROM ngram_search('staged_revert', 'needle')") != 1) {
		throw std::runtime_error("create retry after RevertAppend was not exact");
	}

	// Preserve the Phase 10 discriminator directly: the context-owned shared
	// vacuum fence survives the scalar statement and is released by rollback.
	Connection fence(db), checkpoint(db);
	auto fenced_refresh = Expand(fence, "PRAGMA ngram_refresh('stale')");
	ExecuteThrough(fence, fenced_refresh, "__ngram_maintenance_guard");
	Check(checkpoint, "BEGIN");
	auto catalog_name = ScalarString(checkpoint, "SELECT current_database()");
	auto &catalog = Catalog::GetCatalog(*checkpoint.context, catalog_name);
	auto &manager = DuckTransaction::Get(*checkpoint.context, catalog).GetTransactionManager();
	auto blocked = manager.TryGetVacuumLock();
	if (blocked) {
		throw std::runtime_error("context-held maintenance fence did not exclude checkpoint");
	}
	Rollback(checkpoint);
	Rollback(fence);
	Check(checkpoint, "BEGIN");
	auto released = manager.TryGetVacuumLock();
	if (!released) {
		throw std::runtime_error("maintenance fence remained held after rollback");
	}
	released.reset();
	Rollback(checkpoint);
}

static void TestSharedGuardAndDrop(const string &path) {
	RemoveDatabase(path);
	{
		DuckDB db(path);
		LoadNgram(db);
		Connection con(db), ddl(db);
		auto catalog = ScalarString(con, "SELECT current_database()");
		Check(con, "SET checkpoint_threshold='1TB'");

		// The table's one guard covers every VARCHAR present at creation, so an
		// update of any of them is delete+insert even before that column is indexed.
		Check(con, "CREATE TABLE broad(s VARCHAR UNIQUE, other VARCHAR)");
		Check(con, "INSERT INTO broad VALUES ('x', 'old')");
		Check(con, "PRAGMA create_ngram_index('broad', 's')");
		auto before = ScalarInt64(con, "SELECT rowid FROM broad");
		Check(con, "UPDATE broad SET other='new'");
		if (ScalarInt64(con, "SELECT rowid FROM broad") == before) {
			throw std::runtime_error("the table guard did not cover every existing VARCHAR column");
		}
		Check(con, "PRAGMA create_ngram_index('broad', 'other')");
		if (ScalarInt64(con, "SELECT count(DISTINCT guard_name || guard_token) FROM __ngram.registry WHERE "
		                     "table_name='broad'") != 1 ||
		    ScalarInt64(con, "SELECT count(*) FROM duckdb_indexes() WHERE table_name='broad'") != 1 ||
		    ScalarInt64(con, "SELECT count(*) FROM ngram_search('broad', 'new', col='other')") != 1) {
			throw std::runtime_error("the second index did not share the table guard");
		}

		Check(con, "CREATE TABLE incarnated(s VARCHAR)");
		Check(con, "INSERT INTO incarnated VALUES ('incarnation needle')");
		Check(con, "PRAGMA create_ngram_index('incarnated', 's')");
		auto guard = GuardName(con, "incarnated", "s");
		Check(con, "DROP INDEX " + guard);
		Check(con, "CREATE INDEX " + guard + " ON incarnated USING NGRAM_ROWID_GUARD(s)");
		if (ScalarInt64(con, "SELECT count(*) FROM ngram_search('incarnated', 'needle')") != 1) {
			throw std::runtime_error("token mismatch did not fall back exhaustively");
		}
		ExpectError(con, "PRAGMA drop_ngram_index('incarnated', 's')", "dropped and re-created");
		Check(con, "DROP INDEX " + guard);
		Check(con, "PRAGMA drop_ngram_index('incarnated', 's')");

		// Another format's row is drop-only: the generic token-checked drop removes
		// row, storage and guard, while the table-named drop and the query paths
		// name the remedy.
		Check(con, "CREATE TABLE other_format(s VARCHAR)");
		Check(con, "INSERT INTO other_format VALUES ('other needle')");
		Check(con, "PRAGMA create_ngram_index('other_format', 's')");
		auto other_ref = IndexRef(con, "other_format", "s");
		Check(con, "UPDATE __ngram.registry SET format_version=5 WHERE table_name='other_format'");
		ExpectStatus(con, catalog, other_ref, "MALFORMED");
		ExpectError(con, "PRAGMA drop_ngram_index('other_format', 's')", "format 5");
		ExpectError(con, "SELECT * FROM ngram_search('other_format', 'needle')", "format 5");
		Check(con, "SET ngram_auto_accelerate=true");
		if (ScalarInt64(con, "SELECT count(*) FROM other_format WHERE s LIKE '%needle%'") != 1) {
			throw std::runtime_error("an unreadable registry row did not decline transparent acceleration");
		}
		DropByRef(con, catalog, other_ref);
		if (HasRowIdGuard(con, "other_format") ||
		    ScalarInt64(con, "SELECT count(*) FROM __ngram.registry WHERE table_name='other_format'") != 0 ||
		    StorageTableCount(con) != 4) {
			throw std::runtime_error("generic drop of another format left guard, row, or storage");
		}

		// A missing guard makes both public query paths scan, but a same-named
		// replacement on another table must block recovery rather than be dropped.
		Check(con, "CREATE TABLE drop_cross_owner(s VARCHAR)");
		Check(con, "INSERT INTO drop_cross_owner VALUES ('cross needle')");
		Check(con, "PRAGMA create_ngram_index('drop_cross_owner', 's')");
		guard = GuardName(con, "drop_cross_owner", "s");
		Check(con, "DROP INDEX " + guard);
		if (ScalarInt64(con, "SELECT count(*) FROM ngram_search('drop_cross_owner', 'needle')") != 1 ||
		    ScalarInt64(con, "SELECT count(*) FROM drop_cross_owner WHERE s LIKE '%needle%'") != 1 ||
		    Query(con, "EXPLAIN SELECT * FROM drop_cross_owner WHERE s LIKE '%needle%'")->ToString().find("SEQ_SCAN") ==
		        string::npos) {
			throw std::runtime_error("missing rowid guard did not choose exact explicit/transparent fallback");
		}
		Check(con, "CREATE TABLE drop_cross_other(s VARCHAR)");
		Check(con, "CREATE INDEX " + guard + " ON drop_cross_other USING NGRAM_ROWID_GUARD(s)");
		ExpectError(con, "PRAGMA drop_ngram_index('drop_cross_owner', 's')", "different table");
		if (!HasRowIdGuard(con, "drop_cross_other") ||
		    ScalarInt64(con, "SELECT count(*) FROM " + OwnerRow("drop_cross_owner", "s")) != 1) {
			throw std::runtime_error("cross-table guard collision was not preserved by public drop");
		}
		Check(con, "DROP INDEX " + guard);
		Check(con, "PRAGMA drop_ngram_index('drop_cross_owner', 's')");

		// Dropping one of two indexes never touches the shared guard; a table
		// whose guard is missing accepts no new index until every index is
		// rebuilt; dropping the last index removes an absent guard's row cleanly.
		Check(con, "CREATE TABLE overlap(a VARCHAR, b VARCHAR)");
		Check(con, "INSERT INTO overlap VALUES ('alpha needle', 'beta needle')");
		Check(con, "PRAGMA create_ngram_index('overlap', 'a')");
		Check(con, "PRAGMA create_ngram_index('overlap', 'b')");
		Check(con, "PRAGMA drop_ngram_index('overlap', 'a')");
		if (!HasRowIdGuard(con, "overlap") || IndexStatus(con, "overlap", "b") != "READY") {
			throw std::runtime_error("dropping one index of two changed the shared guard");
		}
		Check(con, "DROP INDEX " + GuardName(con, "overlap", "b"));
		if (IndexStatus(con, "overlap", "b") != "SCAN_ONLY" ||
		    ScalarInt64(con, "SELECT count(*) FROM ngram_search('overlap', 'needle', col='b')") != 1) {
			throw std::runtime_error("a missing shared guard did not make the remaining index scan-only");
		}
		ExpectError(con, "PRAGMA create_ngram_index('overlap', 'a')", "drop the table's ngram indexes");
		Check(con, "PRAGMA drop_ngram_index('overlap', 'b')");
		if (HasRowIdGuard(con, "overlap") || StorageTableCount(con) != 4) {
			throw std::runtime_error("dropping the last index left a guard or storage");
		}

		// Both directions of the drop preprocessor/execution race fail closed.
		Check(con, "CREATE TABLE drop_missing(s VARCHAR)");
		Check(con, "PRAGMA create_ngram_index('drop_missing', 's')");
		guard = GuardName(con, "drop_missing", "s");
		Check(con, "DROP INDEX " + guard);
		auto missing_drop = Expand(con, "PRAGMA drop_ngram_index('drop_missing', 's')");
		Check(ddl, "CREATE INDEX " + guard + " ON drop_missing USING NGRAM_ROWID_GUARD(s)");
		auto error = ExecuteRemainingForError(con, missing_drop);
		if (error.find("dropped and re-created") == string::npos) {
			throw std::runtime_error("missing-to-recreated drop race did not fail closed: " + error);
		}
		Rollback(con);
		Check(ddl, "DROP INDEX " + guard);
		Check(con, "PRAGMA drop_ngram_index('drop_missing', 's')");

		Check(con, "CREATE TABLE drop_exact(s VARCHAR)");
		Check(con, "PRAGMA create_ngram_index('drop_exact', 's')");
		guard = GuardName(con, "drop_exact", "s");
		auto exact_drop = Expand(con, "PRAGMA drop_ngram_index('drop_exact', 's')");
		Check(ddl, "DROP INDEX " + guard);
		Check(ddl, "CREATE INDEX " + guard + " ON drop_exact USING NGRAM_ROWID_GUARD(s)");
		error = ExecuteRemainingForError(con, exact_drop);
		if (error.find("dropped and re-created") == string::npos) {
			throw std::runtime_error("exact-to-recreated drop race did not fail closed: " + error);
		}
		Rollback(con);
		Check(ddl, "DROP INDEX " + guard);
		Check(con, "PRAGMA drop_ngram_index('drop_exact', 's')");

		// A second index registered between planning and executing a drop keeps
		// the guard, and a second create planned before the first commits is
		// refused rather than recording a guard it never validated.
		Check(con, "CREATE TABLE guard_refcount(a VARCHAR, b VARCHAR)");
		Check(con, "INSERT INTO guard_refcount VALUES ('alpha needle', 'beta needle')");
		Check(con, "PRAGMA create_ngram_index('guard_refcount', 'a')");
		auto refcount_drop = Expand(con, "PRAGMA drop_ngram_index('guard_refcount', 'a')");
		Check(ddl, "PRAGMA create_ngram_index('guard_refcount', 'b')");
		error = ExecuteRemainingForError(con, refcount_drop);
		if (error.find("shares the rowid guard") == string::npos) {
			throw std::runtime_error("drop did not re-count guard references at execution: " + error);
		}
		Rollback(con);
		if (!HasRowIdGuard(con, "guard_refcount") || IndexStatus(con, "guard_refcount", "b") != "READY") {
			throw std::runtime_error("failed refcount drop changed the shared guard");
		}
		Check(con, "PRAGMA drop_ngram_index('guard_refcount', 'a')");
		Check(con, "CREATE TABLE guard_first(a VARCHAR, b VARCHAR)");
		auto second_create = Expand(con, "PRAGMA create_ngram_index('guard_first', 'b')");
		Check(ddl, "PRAGMA create_ngram_index('guard_first', 'a')");
		error = ExecuteRemainingForError(con, second_create);
		if (error.find("changed while the statement was being prepared") == string::npos) {
			throw std::runtime_error("create planned without a guard did not notice the new guard: " + error);
		}
		Rollback(con);
		Check(con, "PRAGMA create_ngram_index('guard_first', 'b')");
		if (ScalarInt64(con, "SELECT count(*) FROM duckdb_indexes() WHERE table_name='guard_first'") != 1) {
			throw std::runtime_error("retried second create did not share the table guard");
		}

		Check(con, "CREATE TABLE drop_binding(s VARCHAR)");
		Check(con, "INSERT INTO drop_binding VALUES ('binding needle')");
		Check(con, "PRAGMA create_ngram_index('drop_binding', 's')");
		Check(con, "FORCE CHECKPOINT");
	}
	{
		DBConfig config;
		config.options.load_extensions = false;
		DuckDB db(path, &config);
		LoadCoreFunctions(db);
		Connection con(db);
		LoadNgram(db); // live connection deliberately skips startup eager binding
		auto guard = GuardName(con, "drop_binding", "s");
		SetRowIdGuardBindState(con, "drop_binding", guard, IndexBindState::BINDING, true);
		ExpectError(con, "PRAGMA drop_ngram_index('drop_binding', 's')", "being bound");
		Rollback(con);
		if (!HasRowIdGuard(con, "drop_binding") ||
		    ScalarInt64(con, "SELECT count(*) FROM " + OwnerRow("drop_binding", "s")) != 1) {
			throw std::runtime_error("drop changed a guard while its binder owned the physical entry");
		}
		SetRowIdGuardBindState(con, "drop_binding", guard, IndexBindState::UNBOUND, true);
		auto drop = Expand(con, "PRAGMA drop_ngram_index('drop_binding', 's')");
		auto next = ExecuteThrough(con, drop, ngram::NGRAM_MAINTENANCE_GUARD);
		if (!RowIdGuardIsBound(con, "drop_binding")) {
			throw std::runtime_error("drop validation succeeded before the exact guard became bound");
		}
		auto error = ExecuteRemainingForError(con, drop, next);
		if (!error.empty()) {
			throw std::runtime_error("bound rowid guard drop failed: " + error);
		}
	}
}

// SQLLogic's query() can read a runtime-named storage table but deliberately
// rejects DDL/DML, so the corruption matrix lives here.

static void TestVacuumAndConflict(const string &path) {
	RemoveDatabase(path);
	DBConfig config;
	config.SetOptionByName("vacuum_rebuild_indexes", Value::UBIGINT(500000));
	DuckDB db(path, &config);
	LoadNgram(db);
	Connection con(db);
	Check(con, "SET checkpoint_threshold='1TB'");
	Check(con, "SET ngram_auto_accelerate=true");

	// Prove the configured host would move this exact shape with an ART alone;
	// otherwise the guarded assertion below would not discriminate the branch.
	Check(con, "CREATE TABLE art_vacuum_control AS SELECT i id FROM range(122881) t(i)");
	Check(con, "CREATE INDEX art_vacuum_control_idx ON art_vacuum_control(id)");
	Check(con, "DELETE FROM art_vacuum_control WHERE id < 122880");
	Check(con, "FORCE CHECKPOINT");
	if (ScalarInt64(con, "SELECT rowid FROM art_vacuum_control WHERE id=122880") != 0) {
		throw std::runtime_error("vacuum_rebuild_indexes control did not trigger moving vacuum");
	}
	Check(con, "DROP TABLE art_vacuum_control");

	// A non-ART guard permanently excludes DuckDB's ART-only moving-vacuum
	// path, even when vacuum_rebuild_indexes would otherwise move live rows.
	Check(con, "CREATE TABLE vacuum_rows AS SELECT i id, CASE WHEN i=122880 THEN 'vacuum needle' ELSE 'x' END s "
	           "FROM range(122881) t(i)");
	Check(con, "PRAGMA create_ngram_index('vacuum_rows', 's')");
	auto before = ScalarInt64(con, "SELECT rowid FROM vacuum_rows WHERE id=122880");
	Check(con, "DELETE FROM vacuum_rows WHERE id < 122880");
	Check(con, "FORCE CHECKPOINT");
	if (ScalarInt64(con, "SELECT rowid FROM vacuum_rows WHERE id=122880") != before ||
	    ScalarInt64(con, "SELECT count(*) FROM ngram_search('vacuum_rows', 'vacuum needle')") != 1 ||
	    Query(con, "EXPLAIN SELECT * FROM vacuum_rows WHERE s LIKE '%vacuum needle%'")
	            ->ToString()
	            .find("NGRAM_INDEX_SCAN") == string::npos) {
		throw std::runtime_error("non-ART guard did not prevent moving vacuum with rebuild enabled");
	}

	// The checkpoint above reclaimed rowids 122880 and up, so an append into
	// them is a real reuse even when a later UNIQUE index rejects the commit:
	// the guard latches before the host rolls the rows back, and exact search
	// falls back until rebuild. TestRejectedCommitKeepsGuard covers the rejected
	// commit into a fresh range, which must not latch.
	Check(con, "CREATE TABLE conflict_rows AS SELECT i id, CASE WHEN i=42 THEN 'conflict needle' ELSE 'x' END s "
	           "FROM range(130000) t(i)");
	Check(con, "PRAGMA create_ngram_index('conflict_rows', 's')");
	Check(con, "DELETE FROM conflict_rows WHERE rowid >= 122880");
	Check(con, "FORCE CHECKPOINT");
	Connection pending(db), ddl(db);
	Check(pending, "BEGIN");
	Check(pending, "INSERT INTO conflict_rows VALUES (42, 'rejected needle')");
	// The insert was accepted before this index existed. Its final commit now
	// visits the older guard before the newly installed UNIQUE ART rejects it.
	Check(ddl, "CREATE UNIQUE INDEX conflict_unique ON conflict_rows(id)");
	auto rejected = pending.Query("COMMIT");
	if (!rejected->HasError() || rejected->GetError().find("duplicate key") == string::npos ||
	    ScalarInt64(con, "SELECT count(*) FROM conflict_rows") != 122880) {
		throw std::runtime_error("UNIQUE conflict discriminator did not reject the reused rowid: " +
		                         (rejected->HasError() ? rejected->GetError() : rejected->ToString()));
	}
	auto stats = Query(con, "PRAGMA ngram_index_stats('conflict_rows')");
	if (stats->GetValue(12, 0).ToString().find("cannot exclude reuse") == string::npos ||
	    ScalarInt64(con, "SELECT count(*) FROM ngram_search('conflict_rows', 'conflict needle')") != 1 ||
	    Query(con, "EXPLAIN SELECT * FROM conflict_rows WHERE s LIKE '%conflict needle%'")
	            ->ToString()
	            .find("NGRAM_INDEX_SCAN") != string::npos) {
		throw std::runtime_error("rejected append did not conservatively force exact scan fallback");
	}
}

static void TestRejectedCommitKeepsGuard(const string &path) {
	// A commit that a later UNIQUE index rejects has already advanced the
	// guard's maximum when the host rolls its rows back, so the next append
	// starts at the same rowid. No checkpoint ran between the two appends, so
	// no vacuum can have reclaimed that range, and the guard stays usable.
	RemoveDatabase(path);
	DuckDB db(path);
	LoadNgram(db);
	Connection con(db);
	Check(con, "SET checkpoint_threshold='1TB'");
	Check(con, "SET ngram_auto_accelerate=true");
	Check(con, "CREATE TABLE retried(id INTEGER, s VARCHAR)");
	Check(con, "INSERT INTO retried SELECT i, 'row ' || i FROM range(1000) t(i)");
	Check(con, "PRAGMA create_ngram_index('retried', 's')");
	// Created after the guard, so a commit visits the guard first and the ART
	// second.
	Check(con, "CREATE UNIQUE INDEX retried_unique ON retried(id)");
	Connection first(db), second(db);
	Check(first, "BEGIN");
	Check(first, "INSERT INTO retried VALUES (1000, 'first writer needle')");
	Check(second, "INSERT INTO retried VALUES (1000, 'second writer needle')");
	auto rejected = first.Query("COMMIT");
	if (!rejected->HasError() || rejected->GetError().find("duplicate key") == string::npos ||
	    ScalarInt64(con, "SELECT count(*) FROM retried") != 1001) {
		throw std::runtime_error("UNIQUE index did not reject the duplicate commit: " +
		                         (rejected->HasError() ? rejected->GetError() : rejected->ToString()));
	}
	// The retried range: the next append starts at the rowid the rejected
	// commit consumed.
	Check(con, "INSERT INTO retried VALUES (1001, 'retried needle')");
	auto stats = Query(con, "PRAGMA ngram_index_stats('retried')");
	if (!stats->GetValue(12, 0).IsNull()) {
		throw std::runtime_error("rejected commit latched the guard: " + stats->GetValue(12, 0).ToString());
	}
	if (ScalarInt64(con, "SELECT count(*) FROM ngram_search('retried', 'needle')") != 2 ||
	    ScalarInt64(con, "SELECT count(*) FROM retried WHERE s LIKE '%needle%'") != 2 ||
	    Query(con, "EXPLAIN SELECT * FROM retried WHERE s LIKE '%needle%'")->ToString().find("NGRAM_INDEX_SCAN") ==
	        string::npos) {
		throw std::runtime_error("index mode was lost after a rejected commit into a fresh rowid range");
	}
	Check(con, "PRAGMA ngram_refresh('retried')");
	if (ScalarInt64(con, "SELECT count(*) FROM ngram_search('retried', 'needle')") != 2) {
		throw std::runtime_error("refresh after a rejected commit lost a row");
	}
}

static void TestConcurrentDropsStrandGuard(const string &path) {
	// Two drops of a table's last two indexes that run concurrently each count
	// the other's registry row in their own snapshot, so neither drops the
	// shared guard. The next first-index create removes that leftover before
	// its ADD/DROP COLUMN barrier, which the leftover would otherwise block.
	RemoveDatabase(path);
	DuckDB db(path);
	LoadNgram(db);
	Connection con(db);
	Check(con, "SET checkpoint_threshold='1TB'");
	Check(con, "CREATE TABLE stranded(a VARCHAR, b VARCHAR)");
	Check(con, "INSERT INTO stranded VALUES ('alpha needle', 'beta'), ('gamma', 'delta needle')");
	Check(con, "PRAGMA create_ngram_index('stranded', 'a')");
	Check(con, "PRAGMA create_ngram_index('stranded', 'b')");
	auto stranded_guard = GuardName(con, "stranded", "a");
	auto guard_rows = [](const string &guard_name) {
		return "SELECT count(*) FROM duckdb_indexes() WHERE table_name='stranded' AND index_name=" +
		       KeywordHelper::WriteQuoted(guard_name);
	};
	const char *prefixed_guards =
	    "SELECT count(*) FROM duckdb_indexes() WHERE table_name='stranded' AND starts_with(index_name, "
	    "'__ngram_guard_')";
	Connection first(db), second(db);
	Check(first, "BEGIN");
	Check(second, "BEGIN");
	Check(first, "PRAGMA drop_ngram_index('stranded', 'a')");
	Check(second, "PRAGMA drop_ngram_index('stranded', 'b')");
	Check(first, "COMMIT");
	Check(second, "COMMIT");
	if (ScalarInt64(con, "SELECT count(*) FROM __ngram.registry WHERE table_name='stranded'") != 0 ||
	    ScalarInt64(con, guard_rows(stranded_guard)) != 1) {
		throw std::runtime_error("concurrent drops of the last two indexes did not leave the shared guard behind");
	}
	Check(con, "PRAGMA create_ngram_index('stranded', 'a')");
	auto fresh_guard = GuardName(con, "stranded", "a");
	if (fresh_guard == stranded_guard || ScalarInt64(con, prefixed_guards) != 1 ||
	    ScalarInt64(con, guard_rows(fresh_guard)) != 1 || IndexStatus(con, "stranded", "a") != "READY") {
		throw std::runtime_error("create after concurrent drops did not replace the leftover guard");
	}
	if (ScalarInt64(con, "SELECT count(*) FROM ngram_search('stranded', 'needle')") != 1 ||
	    ScalarString(con, "SELECT a FROM ngram_search('stranded', 'needle')") != "alpha needle" ||
	    ScalarInt64(con, "SELECT count(*) FROM ngram_search('stranded', 'gamma')") != 1) {
		throw std::runtime_error("the index created over a leftover guard did not search exactly");
	}
}

static void WALChild(const string &path) {
	RemoveDatabase(path);
	DuckDB db(path);
	LoadNgram(db);
	Connection con(db);
	Check(con, "SET checkpoint_threshold='1TB'");
	Check(con, "CREATE TABLE wal_rows AS SELECT i id, CASE WHEN i=0 THEN 'wal needle base' ELSE 'x' END s "
	           "FROM range(125000) t(i)");
	Check(con, "BEGIN");
	Check(con, "INSERT INTO wal_rows VALUES (125000, 'wal needle local')");
	Check(con, "PRAGMA create_ngram_index('wal_rows', 's')");
	Check(con, "COMMIT");
	Check(con, "INSERT INTO wal_rows VALUES (125001, 'wal needle later')");
	std::_Exit(0);
}

static void ReuseWALChild(const string &path) {
	RemoveDatabase(path);
	DuckDB db(path);
	LoadNgram(db);
	Connection con(db);
	Check(con, "SET checkpoint_threshold='1TB'");
	Check(con, "CREATE TABLE wal_rows AS SELECT i id, CASE WHEN i=0 THEN 'seal needle base' ELSE 'x' END s "
	           "FROM range(250000) t(i)");
	Check(con, "PRAGMA create_ngram_index('wal_rows', 's')");
	Check(con, "DELETE FROM wal_rows WHERE rowid >= 122880");
	Check(con, "FORCE CHECKPOINT");
	Check(con, "INSERT INTO wal_rows SELECT 300000+i, CASE WHEN i=0 THEN 'seal needle reused' ELSE 'x' END "
	           "FROM range(5000) t(i)");
	if (ScalarInt64(con, "SELECT rowid FROM wal_rows WHERE id=300000") != 122880) {
		throw std::runtime_error("checkpoint-seal discriminator did not reuse the expected rowid");
	}
	std::_Exit(0);
}

static void TestWALAndStock(const string &executable, const string &path) {
	RemoveDatabase(path);
	string command = "\"" + executable + "\" --wal-child \"" + path + "\"";
	if (std::system(command.c_str()) != 0) {
		throw std::runtime_error("WAL crash child failed");
	}
	{
		DuckDB db(path);
		LoadNgram(db);
		Connection con(db);
		auto stats = Query(con, "PRAGMA ngram_index_stats('wal_rows')");
		if (ScalarInt64(con, "SELECT count(*) FROM wal_rows") != 125002 ||
		    ScalarInt64(con, "SELECT count(*) FROM ngram_search('wal_rows', 'wal needle')") != 3 ||
		    !stats->GetValue(12, 0).IsNull()) {
			throw std::runtime_error("CREATE WAL state did not distinguish creator-local and later appends");
		}
		Check(con, "DELETE FROM wal_rows WHERE rowid >= 122880");
		Check(con, "FORCE CHECKPOINT");
		Check(con, "INSERT INTO wal_rows SELECT 200000+i, CASE WHEN i=0 THEN 'wal needle reused' ELSE 'x' END "
		           "FROM range(5000) t(i)");
		stats = Query(con, "PRAGMA ngram_index_stats('wal_rows')");
		if (ScalarInt64(con, "SELECT count(*) FROM ngram_search('wal_rows', 'wal needle')") != 2 ||
		    stats->GetValue(12, 0).ToString().find("cannot exclude reuse") == string::npos) {
			throw std::runtime_error("trailing rowid reuse did not latch unsafe after WAL replay");
		}
		Check(con, "FORCE CHECKPOINT");
	}

	// A checkpoint-loaded unknown index remains readable in stock DuckDB.
	// DML and guard-touching ALTER fail closed; an unrelated ADD COLUMN is safe.
	{
		DBConfig config;
		config.options.load_extensions = false;
		DuckDB stock(path, &config);
		Connection con(stock);
		if (ScalarInt64(con, "SELECT id FROM wal_rows LIMIT 1") < 0) {
			throw std::runtime_error("stock DuckDB could not read a guarded table");
		}
	}
	for (auto &kind : vector<string> {"insert", "update", "delete", "alter"}) {
		StockWriteFails(executable, path, kind);
	}
	StockWriteFails(executable, path, "add");
	{
		DuckDB db(path);
		LoadNgram(db);
		Connection con(db);
		if (ScalarInt64(con, "SELECT count(*) FROM ngram_search('wal_rows', 'wal needle')") != 2) {
			throw std::runtime_error("extension reopen after stock read was not exhaustive");
		}
	}
}

static void TestIncompatibleGuardQuarantine(const string &path) {
	RemoveDatabase(path);
	{
		DuckDB db(path);
		LoadNgram(db);
		Connection con(db);
		Check(con, "CREATE TABLE valid_guard(s VARCHAR, note VARCHAR)");
		Check(con, "INSERT INTO valid_guard VALUES ('valid needle', 'note')");
		Check(con, "PRAGMA create_ngram_index('valid_guard', 's')");
		// Rendered/copyable DDL has no private WITH options; the index type stamps
		// its version/source/token internally.
		Check(con, "CREATE INDEX unrelated_guard ON valid_guard USING NGRAM_ROWID_GUARD(note)");
		auto rendered = ScalarString(con, "SELECT sql FROM duckdb_indexes() WHERE index_name='unrelated_guard'");
		Check(con, "DROP INDEX unrelated_guard");
		Check(con, rendered);
		Check(con, "CREATE TABLE bad_guard(s VARCHAR)");
		Check(con, "INSERT INTO bad_guard VALUES ('bad needle')");
		Check(con, "PRAGMA create_ngram_index('bad_guard', 's')");
		Check(con, "CREATE TABLE max_guard(s VARCHAR)");
		Check(con, "INSERT INTO max_guard VALUES ('max needle')");
		Check(con, "PRAGMA create_ngram_index('max_guard', 's')");
		Check(con, "FORCE CHECKPOINT");
	}
	string bad_guard_name, max_guard_name;
	{
		DBConfig config;
		config.options.load_extensions = false;
		DuckDB db(path, &config);
		LoadCoreFunctions(db);
		Connection con(db); // both guards are still unbound here
		bad_guard_name = GuardName(con, "bad_guard", "s");
		max_guard_name = GuardName(con, "max_guard", "s");
		MutateGuardSource(con, "valid_guard", "unrelated_guard");
		MutateGuardSource(con, "bad_guard", bad_guard_name);
		MutateGuardOption(con, "max_guard", max_guard_name, "ngram_guard_max_seen", Value::BIGINT(-1));
		LoadNgram(db); // live connection: callback cannot bind either table
		Check(con, "SET ngram_auto_accelerate=true");
		for (idx_t i = 0; i < 2; i++) {
			if (ScalarInt64(con, "SELECT count(*) FROM ngram_search('valid_guard', 'needle')") != 1 ||
			    ScalarInt64(con, "SELECT count(*) FROM ngram_search('bad_guard', 'needle')") != 1) {
				throw std::runtime_error("an incompatible unbound guard poisoned repeated exact search");
			}
		}
		auto stats = Query(con, "PRAGMA ngram_index_stats('bad_guard')");
		if (stats->GetValue(12, 0).ToString().find("incompatible") == string::npos) {
			throw std::runtime_error("incompatible guard was not visible in ngram_index_stats");
		}
		if (ScalarInt64(con, "SELECT count(*) FROM ngram_search('max_guard', 'needle')") != 1 ||
		    ScalarInt64(con, "SELECT count(*) FROM max_guard WHERE s LIKE '%needle%'") != 1 ||
		    Query(con, "EXPLAIN SELECT * FROM max_guard WHERE s LIKE '%needle%'")->ToString().find("SEQ_SCAN") ==
		        string::npos ||
		    Query(con, "PRAGMA ngram_index_stats('max_guard')")->GetValue(12, 0).ToString().find("not observed") ==
		        string::npos) {
			throw std::runtime_error("max-behind guard did not choose and report exact fallback");
		}
		ExpectError(con, "PRAGMA ngram_refresh('max_guard')", "not observed");
		// DuckDB's ordinary write bind creates a non-throwing quarantine guard.
		// It still enforces the column dependency but can never prove cleanliness.
		Check(con, "UPDATE bad_guard SET s='bad needle updated'");
		Check(con, "UPDATE valid_guard SET s='valid needle updated'");
		if (ScalarInt64(con, "SELECT count(*) FROM ngram_search('bad_guard', 'updated')") != 1 ||
		    ScalarInt64(con, "SELECT count(*) FROM ngram_search('valid_guard', 'updated')") != 1) {
			throw std::runtime_error("quarantine bind made a public write inexact");
		}
	}
	{
		DuckDB db(path);
		LoadNgram(db); // strict eager pre-screen leaves incompatible guards unbound
		Connection con(db);
		for (idx_t i = 0; i < 2; i++) {
			if (ScalarInt64(con, "SELECT count(*) FROM ngram_search('bad_guard', 'updated')") != 1 ||
			    Query(con, "PRAGMA ngram_index_stats('bad_guard')")->GetValue(12, 0).ToString().find("incompatible") ==
			        string::npos) {
				throw std::runtime_error("quarantine state did not survive reopen without a bind spin");
			}
		}
		Check(con, "PRAGMA drop_ngram_index('bad_guard', 's')");
	}
}

static void TestUnboundCheckpointSeal(const string &executable, const string &path) {
	RemoveDatabase(path);
	string command = "\"" + executable + "\" --reuse-child \"" + path + "\"";
	if (std::system(command.c_str()) != 0) {
		throw std::runtime_error("unbound-checkpoint WAL child failed");
	}
	{
		DBConfig config;
		config.options.load_extensions = false;
		DuckDB stock(path, &config);
		// Shutdown checkpoints with no ClientContext and therefore cannot bind
		// the guard or apply its buffered WAL replays.
	}
	{
		DuckDB db(path);
		LoadNgram(db);
		Connection con(db);
		// The startup callback bound a stale persisted seal but has not inspected
		// it. A checkpoint must latch the mismatch before writing a fresh seal.
		Check(con, "FORCE CHECKPOINT");
		auto stats = Query(con, "PRAGMA ngram_index_stats('wal_rows')");
		if (stats->GetValue(12, 0).ToString().find("cannot exclude reuse") == string::npos ||
		    ScalarInt64(con, "SELECT count(*) FROM ngram_search('wal_rows', 'seal needle')") != 2) {
			throw std::runtime_error("unbound checkpoint did not fail closed through the iteration seal");
		}
	}
}

static void TestCleanShutdownSeal(const string &path) {
	RemoveDatabase(path);
	{
		DuckDB db(path);
		LoadNgram(db);
		Connection con(db);
		Check(con, "CREATE TABLE guarded(s VARCHAR)");
		Check(con, "INSERT INTO guarded VALUES ('clean needle')");
		Check(con, "PRAGMA create_ngram_index('guarded', 's')");
		Check(con, "ALTER TABLE guarded ADD COLUMN appended VARCHAR");
		Check(con, "FORCE CHECKPOINT");
	}
	{
		DuckDB db(path);
		LoadNgram(db);
		Connection con(db);
		if (!RowIdGuardIsBound(con, "guarded")) {
			throw std::runtime_error("startup extension load did not eagerly bind the guard");
		}
		Check(con, "CREATE TABLE startup_unrelated(i INTEGER)");
		Check(con, "INSERT INTO startup_unrelated VALUES (1)");
		// Startup loaded the extension before the first Connection and eagerly
		// bound the guard; clean shutdown must preserve the seal.
	}
	{
		DuckDB db(path);
		LoadNgram(db);
		Connection con(db);
		auto stats = Query(con, "PRAGMA ngram_index_stats('guarded')");
		if (!stats->GetValue(12, 0).IsNull() ||
		    ScalarInt64(con, "SELECT count(*) FROM ngram_search('guarded', 'needle')") != 1) {
			throw std::runtime_error("ordinary clean shutdown conservatively invalidated an untouched guard");
		}
	}
	{
		DBConfig config;
		config.options.load_extensions = false;
		DuckDB db(path, &config);
		LoadCoreFunctions(db);
		Connection con(db);
		LoadNgram(db); // live connection: OnExtensionLoaded deliberately skips
		if (RowIdGuardIsBound(con, "guarded")) {
			throw std::runtime_error("dynamic extension load unexpectedly bound the guard with a live connection");
		}
		Check(con, "CREATE TABLE force_unrelated(i INTEGER)");
		Check(con, "INSERT INTO force_unrelated VALUES (1)");
		Check(con, "FORCE CHECKPOINT");
		if (!RowIdGuardIsBound(con, "guarded")) {
			throw std::runtime_error("contextful checkpoint did not bind the untouched guard");
		}
	}
	{
		DuckDB db(path);
		LoadNgram(db);
		Connection con(db);
		if (!Query(con, "PRAGMA ngram_index_stats('guarded')")->GetValue(12, 0).IsNull()) {
			throw std::runtime_error("contextful checkpoint failed to reseal an untouched guard");
		}
	}
	{
		DBConfig config;
		config.options.load_extensions = false;
		DuckDB db(path, &config);
		LoadCoreFunctions(db);
		Connection con(db);
		LoadNgram(db); // last-close callback is the only bind in this session
		Check(con, "CREATE TABLE close_unrelated(i INTEGER)");
		Check(con, "INSERT INTO close_unrelated VALUES (1)");
	}
	{
		DuckDB db(path);
		LoadNgram(db);
		Connection con(db);
		if (!Query(con, "PRAGMA ngram_index_stats('guarded')")->GetValue(12, 0).IsNull()) {
			throw std::runtime_error("last-close eager bind failed to reseal an untouched guard");
		}
	}
}

// SQLLogic's query() can read a runtime-named storage table but deliberately
// rejects DDL/DML, so the corruption matrix lives here.
static void TestStorageCorruption() {
	DuckDB db(nullptr);
	LoadNgram(db);
	Connection con(db);
	Check(con, "SET checkpoint_threshold='1GB'");
	Check(con, "SET ngram_auto_accelerate=true");

	// A lost registry row leaves storage that is listed, never used, and never
	// dropped by id; restoring the row restores the index.
	Check(con, "CREATE TABLE guarded(s VARCHAR)");
	Check(con, "INSERT INTO guarded VALUES ('a tent')");
	Check(con, "PRAGMA create_ngram_index('guarded', 's')");
	auto guarded_ref = IndexRef(con, "guarded", "s");
	Check(con, "CREATE TEMP TABLE guarded_row_copy AS SELECT * FROM " + OwnerRow("guarded", "s"));
	Check(con, "DELETE FROM " + OwnerRow("guarded", "s"));
	ExpectStatus(con, "memory", guarded_ref, "MALFORMED");
	if (StatusReason(con, "memory", guarded_ref).find("no registry row") == string::npos) {
		throw std::runtime_error("storage without a row was not listed as such");
	}
	ExpectError(con, "PRAGMA drop_ngram_index('guarded', 's')", "No ngram index");
	ExpectError(con, "PRAGMA drop_ngram_index_by_id('memory'," + KeywordHelper::WriteQuoted(guarded_ref) + ")",
	            "manually");
	ExpectError(con, "SELECT * FROM ngram_search('guarded', 'tent')", "no ngram index");
	if (ScalarInt64(con, "SELECT count(*) FROM guarded WHERE s LIKE '%tent%'") != 1) {
		throw std::runtime_error("orphaned storage changed a plain LIKE");
	}
	Check(con, "INSERT INTO __ngram.registry SELECT * FROM guarded_row_copy");
	ExpectStatus(con, "memory", guarded_ref, "READY");
	Check(con, "PRAGMA drop_ngram_index('guarded', 's')");

	// Unicode-aware SQL lower() must never substitute for DuckDB identifier
	// ownership: a row edit is caught against the binary owner key.
	Check(con, "CREATE TABLE \"Ä\"(s VARCHAR)");
	Check(con, "INSERT INTO \"Ä\" VALUES ('a tent')");
	Check(con, "PRAGMA create_ngram_index('\"Ä\"', 's')");
	Check(con, "UPDATE __ngram.registry SET table_name='ä' WHERE table_name='Ä'");
	if (IndexStatus(con, "ä", "s") != "MALFORMED") {
		throw std::runtime_error("registry Unicode owner mismatch was not MALFORMED");
	}
	ExpectError(con, "SELECT * FROM ngram_search('\"Ä\"', 'tent', col='s')", "owner key");
	ExpectError(con, "PRAGMA drop_ngram_index('\"Ä\"', 's')", "owner key");
	Check(con, "UPDATE __ngram.registry SET table_name='Ä' WHERE table_name='ä'");
	Check(con, "PRAGMA drop_ngram_index('\"Ä\"', 's')");

	// A format version is never guessed at by any reader or maintenance path.
	Check(con, "CREATE TABLE versioned(s VARCHAR)");
	Check(con, "INSERT INTO versioned VALUES ('a tent')");
	Check(con, "PRAGMA create_ngram_index('versioned', 's')");
	Check(con, "UPDATE __ngram.registry SET format_version=1 WHERE table_name='versioned'");
	ExpectError(con, "SELECT * FROM ngram_search('versioned', 'tent')", "format 1");
	ExpectError(con, "PRAGMA ngram_refresh('versioned')", "format 1");
	ExpectError(con, "PRAGMA ngram_compact('versioned')", "format 1");
	ExpectError(con, "PRAGMA ngram_index_stats('versioned')", "format 1");
	if (ScalarInt64(con, "SELECT count(*) FROM versioned WHERE s LIKE '%tent%'") != 1 ||
	    Query(con, "EXPLAIN SELECT count(*) FROM versioned WHERE s LIKE '%tent%'")->ToString().find("SEQ_SCAN") ==
	        string::npos) {
		throw std::runtime_error("an unreadable format did not decline transparent acceleration");
	}
	Check(con, "UPDATE __ngram.registry SET format_version=4 WHERE table_name='versioned'");
	Check(con, "PRAGMA ngram_refresh('versioned')");

	// Compaction validates every selected row and every failure is atomic.
	Check(con, "CREATE TABLE compact_bad(s VARCHAR)");
	Check(con, "INSERT INTO compact_bad VALUES ('aaa')");
	Check(con, "PRAGMA create_ngram_index('compact_bad', 's')");
	Check(con, "INSERT INTO compact_bad VALUES ('aaa')");
	Check(con, "PRAGMA ngram_refresh('compact_bad')");
	auto compact_segments = StorageTable(con, "compact_bad", "s", "segments");
	Check(con, "UPDATE " + compact_segments + " SET segment_no=NULL WHERE gram='aaa'");
	ExpectError(con, "PRAGMA ngram_compact('compact_bad')", "malformed segments-table key");
	Check(con, "UPDATE " + compact_segments + " SET segment_no=0, gram=NULL");
	ExpectError(con, "PRAGMA ngram_compact('compact_bad', purge=true)", "malformed segments-table key");
	Check(con, "UPDATE " + compact_segments + " SET gram='aa'");
	ExpectError(con, "PRAGMA ngram_compact('compact_bad')", "malformed segments-table key");
	Check(con, "UPDATE " + compact_segments + " SET gram='aaa'");
	Check(con, "UPDATE " + compact_segments +
	               " SET postings=ngram_encode_postings([1048576]::BIGINT[]), rowid_count=1, "
	               "min_rowid=1048576, max_rowid=1048576 WHERE generation=0");
	ExpectError(con, "PRAGMA ngram_compact('compact_bad')", "outside its declared segment or indexed range");
	Check(con, "UPDATE compact_bad SET s=NULL");
	ExpectError(con, "PRAGMA ngram_compact('compact_bad', purge=true)",
	            "outside its declared segment or indexed range");
	Check(con, "UPDATE " + compact_segments +
	               " SET postings=ngram_encode_postings([0]::BIGINT[]), rowid_count=2, min_rowid=0, max_rowid=0 "
	               "WHERE generation=0");
	ExpectError(con, "PRAGMA ngram_compact('compact_bad')", "descriptor disagrees");
	if (ScalarString(con, "SELECT count(*) || ':' || count(DISTINCT generation) FROM " + compact_segments) != "2:2") {
		throw std::runtime_error("failed compaction changed malformed segment generations");
	}

	// Refresh validates the old stats layout before atomically rewriting it.
	Check(con, "CREATE TABLE refresh_bad_stats(s VARCHAR)");
	Check(con, "INSERT INTO refresh_bad_stats VALUES ('aaa')");
	Check(con, "PRAGMA create_ngram_index('refresh_bad_stats', 's')");
	auto refresh_segments = StorageTable(con, "refresh_bad_stats", "s", "segments");
	auto refresh_stats = StorageTable(con, "refresh_bad_stats", "s", "stats");
	Check(con, "UPDATE " + refresh_stats + " SET row_count=-1, segment_count=-1 WHERE encode(gram)=encode('aaa')");
	Check(con, "INSERT INTO " + refresh_stats + " VALUES ('aaa', 2, 2)");
	Check(con, "INSERT INTO refresh_bad_stats VALUES ('aaa')");
	ExpectError(con, "PRAGMA ngram_refresh('refresh_bad_stats')", "invalid stats row");
	auto refresh_digest = ScalarString(
	    con, "SELECT concat((SELECT hwm_rowid FROM " + OwnerRow("refresh_bad_stats", "s") +
	             "),':',(SELECT count(*) FROM " + refresh_segments + "),':',(SELECT max(generation) FROM " +
	             refresh_segments + "),':',(SELECT count(*) FROM " + refresh_stats +
	             "),':',(SELECT min(row_count) FROM " + refresh_stats + "),':',(SELECT max(row_count) FROM " +
	             refresh_stats + "),':',(SELECT sum(row_count) FROM " + refresh_stats + "))");
	if (refresh_digest != "0:1:0:2:-1:2:1") {
		throw std::runtime_error("failed refresh changed malformed stats state: " + refresh_digest);
	}

	// Missing storage after bind is availability, not corruption.
	Check(con, "CREATE TABLE unavailable(id INTEGER, s VARCHAR)");
	Check(con, "INSERT INTO unavailable VALUES (1, 'a tent'), (2, 'nothing')");
	Check(con, "PRAGMA create_ngram_index('unavailable', 's')");
	auto unavailable = con.Prepare("SELECT id FROM ngram_search('unavailable', 'tent')");
	if (unavailable->HasError()) {
		throw std::runtime_error("failed to prepare missing-storage fallback discriminator");
	}
	auto unavailable_segments = StorageTable(con, "unavailable", "s", "segments");
	Check(con, "CREATE TEMP TABLE unavailable_segments_copy AS SELECT * FROM " + unavailable_segments);
	Check(con, "DROP TABLE " + unavailable_segments);
	auto unavailable_result = unavailable->Execute();
	auto unavailable_chunk = unavailable_result->HasError() ? nullptr : unavailable_result->Fetch();
	if (!unavailable_chunk || unavailable_chunk->size() != 1 ||
	    unavailable_chunk->GetValue(0, 0).GetValue<int32_t>() != 1) {
		throw std::runtime_error("prepared search did not fall back after storage removal");
	}
	if (ScalarInt64(con, "SELECT id FROM unavailable WHERE contains(s, 'tent')") != 1) {
		throw std::runtime_error("transparent scan changed after storage removal");
	}
	Check(con, "CREATE TABLE " + unavailable_segments + " AS SELECT * FROM unavailable_segments_copy");
	Check(con, "DROP TABLE unavailable_segments_copy");

	// Invalid HWM values are fatal on explicit and candidate paths and decline
	// the transparent one.
	Check(con, "CREATE TABLE bad_hwm(id INTEGER, s VARCHAR)");
	Check(con, "INSERT INTO bad_hwm VALUES (1, 'aaaa'), (2, 'nothing')");
	Check(con, "PRAGMA create_ngram_index('bad_hwm', 's')");
	Check(con, "UPDATE __ngram.registry SET hwm_rowid=-2 WHERE table_name='bad_hwm'");
	ExpectError(con, "SELECT count(*) FROM ngram_search('bad_hwm', 'aaaa')", "hwm_rowid -2");
	if (ScalarInt64(con, "SELECT count(*) FROM bad_hwm WHERE contains(s, 'aaaa')") != 1) {
		throw std::runtime_error("an invalid hwm did not decline transparent acceleration");
	}
	Check(con, "UPDATE __ngram.registry SET hwm_rowid=36028797018960000 WHERE table_name='bad_hwm'");
	ExpectError(con, "SELECT count(*) FROM ngram_candidates('bad_hwm', 's', 'aaaa')", "hwm_rowid 36028797018960000");

	// Impossible stats/capacity and NULL requested rows remain structural errors.
	Check(con, "CREATE TABLE bad_stats(id INTEGER, s VARCHAR)");
	Check(con, "INSERT INTO bad_stats VALUES (1, 'aaaa'), (2, 'aaaa')");
	Check(con, "PRAGMA create_ngram_index('bad_stats', 's')");
	auto bad_stats = StorageTable(con, "bad_stats", "s", "stats");
	Check(con, "UPDATE " + bad_stats + " SET row_count=1000000, segment_count=1000000 WHERE gram='aaa'");
	ExpectError(con, "SELECT count(*) FROM ngram_search('bad_stats', 'aaaa')",
	            "stats describe more segment rows than exist");
	ExpectError(con, "SELECT count(*) FROM bad_stats WHERE contains(s, 'aaaa')",
	            "stats describe more segment rows than exist");
	Check(con, "UPDATE " + bad_stats + " SET row_count=NULL WHERE gram='aaa'");
	if (ScalarInt64(con, "SELECT count(*) FROM ngram_search('bad_stats', 'bad')") != 0) {
		throw std::runtime_error("unrequested malformed stats row poisoned another gram");
	}
	ExpectError(con, "SELECT count(*) FROM ngram_search('bad_stats', 'aaaa')", "requested stats row contains NULLs");

	Check(con, "CREATE TABLE bad_capacity(id INTEGER, s VARCHAR)");
	Check(con, "INSERT INTO bad_capacity VALUES (1, 'aaaa'), (2, 'aaaa')");
	Check(con, "PRAGMA create_ngram_index('bad_capacity', 's')");
	Check(con, "UPDATE " + StorageTable(con, "bad_capacity", "s", "stats") + " SET row_count=3 WHERE gram='aaa'");
	Check(con, "UPDATE " + StorageTable(con, "bad_capacity", "s", "segments") + " SET rowid_count=3 WHERE gram='aaa'");
	ExpectError(con, "SELECT count(*) FROM ngram_search('bad_capacity', 'aaaa')",
	            "gram posting count exceeds its segment rowid range");
	ExpectError(con, "SELECT count(*) FROM bad_capacity WHERE contains(s, 'aaaa')",
	            "gram posting count exceeds its segment rowid range");

	// Blob corruption reaches the shared decoder on both exact adapters.
	Check(con, "CREATE TABLE bad_blob(id INTEGER, s VARCHAR)");
	Check(con, "INSERT INTO bad_blob SELECT i, 'aaaa' FROM range(1000) t(i)");
	Check(con, "PRAGMA create_ngram_index('bad_blob', 's')");
	Check(con, "SET ngram_max_candidate_fraction=1.0");
	Check(con, "UPDATE " + StorageTable(con, "bad_blob", "s", "segments") + " SET postings=from_hex('626164')");
	ExpectError(con, "SELECT count(*) FROM ngram_search('bad_blob', 'aaaa')", "unknown postings blob format");
	ExpectError(con, "SELECT count(*) FROM bad_blob WHERE contains(s, 'aaaa')", "unknown postings blob format");
	Check(con, "RESET ngram_max_candidate_fraction");

	// Present wrong-kind owned storage is fatal; absence alone may decline.
	Check(con, "CREATE TABLE wrong_stats(id INTEGER, s VARCHAR)");
	Check(con, "INSERT INTO wrong_stats VALUES (1, 'aaaa')");
	Check(con, "PRAGMA create_ngram_index('wrong_stats', 's')");
	auto wrong_ref = IndexRef(con, "wrong_stats", "s");
	auto wrong_table = StorageTable(con, "wrong_stats", "s", "stats");
	Check(con, "DROP TABLE " + wrong_table);
	Check(con, "CREATE VIEW " + wrong_table + " AS SELECT 1 AS not_stats");
	ExpectError(con, "SELECT count(*) FROM wrong_stats WHERE contains(s, 'aaaa')", "wrong catalog type");
	ExpectStatus(con, "memory", wrong_ref, "MALFORMED");
	Check(con, "DROP VIEW " + wrong_table);
	DropByRef(con, "memory", wrong_ref);

	// A later-segment corruption cannot hide behind an earlier resource decline.
	Check(con, "CREATE TABLE late_bad AS SELECT i::INTEGER id, CASE WHEN i IN (0,1,1048576) "
	           "THEN 'aaaa'::VARCHAR ELSE NULL::VARCHAR END s FROM range(1048578) t(i)");
	Check(con, "PRAGMA create_ngram_index('late_bad', 's')");
	Check(con, "UPDATE " + StorageTable(con, "late_bad", "s", "stats") + " SET row_count=5 WHERE gram='aaa'");
	Check(con, "UPDATE " + StorageTable(con, "late_bad", "s", "segments") +
	               " SET rowid_count=3 WHERE gram='aaa' AND segment_no=1");
	Check(con, "SET ngram_max_probe_rowids=1");
	ExpectError(con, "SELECT count(*) FROM ngram_search('late_bad', 'aaaa')",
	            "gram posting count exceeds its segment rowid range");
	ExpectError(con, "SELECT count(*) FROM late_bad WHERE contains(s, 'aaaa')",
	            "gram posting count exceeds its segment rowid range");
	Check(con, "RESET ngram_max_probe_rowids");
}

static void TestRegistryLifecycle(const string &path) {
	RemoveDatabase(path);
	string reopen_ref;
	{
		DuckDB db(path);
		LoadNgram(db);
		Connection con(db);
		auto catalog = ScalarString(con, "SELECT current_database()");
		Check(con, "CREATE TABLE lifecycle(s VARCHAR, t VARCHAR)");
		Check(con, "INSERT INTO lifecycle VALUES ('first needle','second needle')");
		Check(con, "PRAGMA create_ngram_index('lifecycle','s')");
		auto ref = IndexRef(con, "lifecycle", "s");
		auto hex = IndexHex(con, "lifecycle", "s");
		auto guard = GuardName(con, "lifecycle", "s");
		if (ref.size() != 36 || ref[14] != '4' || string("89ab").find(ref[19]) == string::npos ||
		    guard != "__ngram_guard_" + hex) {
			throw std::runtime_error("UUIDv4 and guard derivation disagree");
		}
		if (ScalarInt64(con, "SELECT count(*) FROM duckdb_columns() WHERE schema_name='__ngram' AND "
		                     "table_name='registry' AND NOT is_nullable") != 12 ||
		    ScalarInt64(con, "SELECT count(*) FROM duckdb_constraints() WHERE schema_name='__ngram' AND "
		                     "table_name='registry'") != 14 ||
		    ScalarString(con, "SELECT typeof(index_id)||':'||typeof(owner_key) FROM __ngram.registry") != "UUID:BLOB") {
			throw std::runtime_error("registry shape, nullability, or constraints changed");
		}
		auto listed = Query(con, "PRAGMA ngram_indexes");
		auto status = Query(con, "PRAGMA ngram_index_status(" + KeywordHelper::WriteQuoted(catalog) + "," +
		                             KeywordHelper::WriteQuoted(ref) + ")");
		if (IndexStatus(con, "lifecycle", "s") != "READY" || listed->types[5].id() != LogicalTypeId::BIGINT ||
		    status->types[5].id() != LogicalTypeId::BIGINT) {
			throw std::runtime_error("list/status schema or READY state changed");
		}
		ExpectError(con, "ALTER TABLE lifecycle RENAME TO LIFECYCLE", "Dependency");
		ExpectError(con, "ALTER TABLE lifecycle RENAME COLUMN s TO S", "Dependency");
		Check(con, "PRAGMA drop_ngram_index_by_id(" + KeywordHelper::WriteQuoted(catalog) + "," +
		               KeywordHelper::WriteQuoted(ref) + ")");
		Check(con, "ALTER TABLE lifecycle RENAME TO renamed_lifecycle");
		Check(con, "PRAGMA create_ngram_index('renamed_lifecycle','s')");
		if (IndexStatus(con, "renamed_lifecycle", "s") != "READY") {
			throw std::runtime_error("drop-ID, rename, rebuild did not return READY");
		}

		// Two indexes share the table guard; dropping the first keeps the second.
		Check(con, "PRAGMA create_ngram_index('renamed_lifecycle','t')");
		Check(con, "PRAGMA drop_ngram_index('renamed_lifecycle','s')");
		if (ScalarInt64(con, "SELECT count(*) FROM ngram_search('renamed_lifecycle','second',col='t')") != 1 ||
		    IndexStatus(con, "renamed_lifecycle", "t") != "READY") {
			throw std::runtime_error("dropping the first column's index damaged the second");
		}
		Check(con, "PRAGMA drop_ngram_index('renamed_lifecycle','t')");

		// Orphans remain addressable without resolving a base table.
		Check(con, "CREATE TABLE orphaned(s VARCHAR)");
		Check(con, "PRAGMA create_ngram_index('orphaned','s')");
		auto orphan_ref = IndexRef(con, "orphaned", "s");
		Check(con, "DROP TABLE orphaned");
		if (IndexStatus(con, "orphaned", "s") != "ORPHAN") {
			throw std::runtime_error("base drop was not listed ORPHAN");
		}
		Check(con, "PRAGMA drop_ngram_index_by_id(" + KeywordHelper::WriteQuoted(catalog) + "," +
		               KeywordHelper::WriteQuoted(orphan_ref) + ")");

		// A replacement table under the recorded name has no guard: scan-only,
		// and an ID drop must not touch the replacement or its foreign ART.
		Check(con, "CREATE TABLE replaced(s VARCHAR)");
		Check(con, "INSERT INTO replaced VALUES ('old needle')");
		Check(con, "PRAGMA create_ngram_index('replaced','s')");
		auto replaced_ref = IndexRef(con, "replaced", "s");
		Check(con, "CREATE OR REPLACE TABLE replaced(s VARCHAR)");
		Check(con, "INSERT INTO replaced VALUES ('replacement survives')");
		Check(con, "CREATE INDEX replacement_art ON replaced(s)");
		if (IndexStatus(con, "replaced", "s") != "SCAN_ONLY" ||
		    ScalarInt64(con, "SELECT count(*) FROM ngram_search('replaced','survives')") != 1) {
			throw std::runtime_error("CREATE OR REPLACE was not SCAN_ONLY with an exact scan");
		}
		Check(con, "PRAGMA drop_ngram_index_by_id(" + KeywordHelper::WriteQuoted(catalog) + "," +
		               KeywordHelper::WriteQuoted(replaced_ref) + ")");
		if (ScalarString(con, "SELECT s FROM replaced") != "replacement survives" ||
		    ScalarInt64(con, "SELECT count(*) FROM duckdb_indexes() WHERE index_name='replacement_art'") != 1) {
			throw std::runtime_error("orphan cleanup touched a replacement table or foreign ART");
		}

		// Long identifiers exercise raw unsigned length bytes above 127.
		string long_name(140, 'x');
		Check(con, "CREATE TABLE " + KeywordHelper::WriteOptionallyQuoted(long_name) + "(s VARCHAR)");
		Check(con, "PRAGMA create_ngram_index(" + KeywordHelper::WriteQuoted(long_name) + ",'s')");
		if (IndexStatus(con, long_name, "s") != "READY") {
			throw std::runtime_error("long owner-key identifier did not round-trip");
		}
		Check(con, "PRAGMA drop_ngram_index(" + KeywordHelper::WriteQuoted(long_name) + ",'s')");

		Check(con, "CREATE TABLE reopen_ready(s VARCHAR, extra INTEGER)");
		Check(con, "INSERT INTO reopen_ready VALUES ('reopen needle',1)");
		Check(con, "PRAGMA create_ngram_index('reopen_ready','s')");
		reopen_ref = IndexRef(con, "reopen_ready", "s");
		Check(con, "FORCE CHECKPOINT");
	}
	{
		DuckDB db(path);
		LoadNgram(db);
		Connection con(db);
		auto catalog = ScalarString(con, "SELECT current_database()");
		if (IndexStatus(con, "reopen_ready", "s") != "READY" ||
		    ScalarInt64(con, "SELECT count(*) FROM ngram_search('reopen_ready','needle')") != 1) {
			throw std::runtime_error("healthy persisted allocation was not READY after reopen");
		}
		ExpectError(con, "PRAGMA create_ngram_index('reopen_ready','s')", "already exists");
		// A guard dropped by hand makes reads scan-only; the ID drop still removes
		// the row and storage and frees the table for rename.
		Check(con, "DROP INDEX " + GuardName(con, "reopen_ready", "s"));
		if (IndexStatus(con, "reopen_ready", "s") != "SCAN_ONLY") {
			throw std::runtime_error("missing guard after reopen was not SCAN_ONLY");
		}
		Check(con, "PRAGMA drop_ngram_index_by_id(" + KeywordHelper::WriteQuoted(catalog) + "," +
		               KeywordHelper::WriteQuoted(reopen_ref) + ")");
		if (StorageTableCount(con) != 0) {
			throw std::runtime_error("scan-only ID drop left storage");
		}
		Check(con, "ALTER TABLE reopen_ready RENAME TO reopen_renamed");
	}
	RemoveDatabase(path);
}

static void TestRegistryBootstrapAndConflicts() {
	// Bootstrap is transactional: any later build error removes the registry,
	// storage tables, guard, and scratch objects as one unit.
	{
		DuckDB db(nullptr);
		LoadNgram(db);
		Connection con(db), other(db);
		Check(con, "CREATE TABLE bootstrap_fail(s VARCHAR)");
		auto failed = Expand(con, "PRAGMA create_ngram_index('bootstrap_fail','s')");
		ExecuteBeforeCommit(con, failed);
		if (ScalarInt64(con, "SELECT count(*) FROM __ngram.registry") != 1 || StorageTableCount(con) != 2 ||
		    !HasRowIdGuard(con, "bootstrap_fail")) {
			throw std::runtime_error("full bootstrap allocation was not staged before rollback");
		}
		ExpectError(con, "SELECT error('rollback bootstrap')", "rollback bootstrap");
		Rollback(con);
		if (ScalarInt64(other, "SELECT count(*) FROM duckdb_schemas() WHERE schema_name='__ngram'") != 0 ||
		    HasRowIdGuard(other, "bootstrap_fail") ||
		    ScalarInt64(other, "SELECT count(*) FROM duckdb_tables() WHERE database_name='temp' AND "
		                       "table_name LIKE '__ngram_%'") != 0) {
			throw std::runtime_error("failed bootstrap leaked registry, storage, or guard");
		}
		Check(con, "CREATE TABLE bootstrap_a(s VARCHAR)");
		Check(con, "CREATE TABLE bootstrap_b(s VARCHAR)");
		Connection bootstrap_a(db), bootstrap_b(db);
		auto create_a = Expand(bootstrap_a, "PRAGMA create_ngram_index('bootstrap_a','s')");
		auto create_b = Expand(bootstrap_b, "PRAGMA create_ngram_index('bootstrap_b','s')");
		if (!ExecuteRemainingForError(bootstrap_a, create_a).empty()) {
			throw std::runtime_error("first concurrent bootstrap create failed");
		}
		auto bootstrap_error = ExecuteRemainingForError(bootstrap_b, create_b);
		if (bootstrap_error.find("registry changed") == string::npos) {
			throw std::runtime_error("second concurrent bootstrap did not fail its stale plan: " + bootstrap_error);
		}
		Rollback(bootstrap_b);
		if (ScalarInt64(con, "SELECT count(*) FROM __ngram.registry") != 1 || StorageTableCount(con) != 2 ||
		    HasRowIdGuard(con, "bootstrap_b")) {
			throw std::runtime_error("failed concurrent bootstrap leaked its owner allocation");
		}
		Check(con, "PRAGMA create_ngram_index('bootstrap_b','s')");
		if (ScalarInt64(con, "SELECT count(DISTINCT owner_key) FROM __ngram.registry") != 2) {
			throw std::runtime_error("concurrent bootstrap retry did not preserve both owners");
		}
		Check(con, "PRAGMA drop_ngram_index('bootstrap_a','s')");
		Check(con, "PRAGMA drop_ngram_index('bootstrap_b','s')");
		Check(con, "CREATE TABLE bootstrap(s VARCHAR)");
		Check(con, "INSERT INTO bootstrap VALUES ('needle')");
		auto build = Expand(con, "PRAGMA create_ngram_index('bootstrap','s')");
		auto next = ExecuteThrough(con, build, "CREATE TABLE");
		auto error = ExecuteRemainingForError(con, build, next);
		if (!error.empty()) {
			throw std::runtime_error("bootstrap control build failed: " + error);
		}
		Check(con, "PRAGMA drop_ngram_index('bootstrap','s')");

		Check(con, "CREATE TABLE duplicate(s VARCHAR)");
		Connection first(db), second(db);
		auto first_create = Expand(first, "PRAGMA create_ngram_index('duplicate','s')");
		auto second_create = Expand(second, "PRAGMA create_ngram_index('duplicate','s')");
		if (!ExecuteRemainingForError(first, first_create).empty()) {
			throw std::runtime_error("first concurrent create failed");
		}
		auto duplicate_error = ExecuteRemainingForError(second, second_create);
		if (duplicate_error.empty()) {
			throw std::runtime_error("preprocessed concurrent duplicate create unexpectedly succeeded");
		}
		Rollback(second);
		if (ScalarInt64(second, "SELECT count(*) FROM __ngram.registry WHERE table_name='duplicate'") != 1 ||
		    StorageTableCount(second) != 2) {
			throw std::runtime_error("concurrent duplicate retry leaked an allocation");
		}
	}

	// Owner-key equality uses DuckDB's ASCII identifier fold, not SQL Unicode
	// lower(); separators cannot alias because each component is length framed.
	{
		DuckDB db(nullptr);
		LoadNgram(db);
		Connection con(db);
		Check(con, "CREATE SCHEMA a");
		Check(con, "CREATE SCHEMA a_b");
		Check(con, "CREATE TABLE a.b_c(s VARCHAR)");
		Check(con, "CREATE TABLE a_b.c(s VARCHAR)");
		Check(con, "CREATE TABLE \"Ä\"(s VARCHAR)");
		Check(con, "CREATE TABLE \"ä\"(s VARCHAR)");
		Check(con, "PRAGMA create_ngram_index('a.b_c','s')");
		Check(con, "PRAGMA create_ngram_index('a_b.c','s')");
		Check(con, "PRAGMA create_ngram_index('\"Ä\"','s')");
		Check(con, "PRAGMA create_ngram_index('\"ä\"','s')");
		if (ScalarInt64(con, "SELECT count(DISTINCT owner_key) FROM __ngram.registry") != 4) {
			throw std::runtime_error("injective/Unicode owner keys collided");
		}
		// A row re-keyed to another id names storage that does not exist, and the
		// storage it left behind has no row: both are listed MALFORMED.
		Check(con, "CREATE TABLE owner_unique(s VARCHAR)");
		Check(con, "PRAGMA create_ngram_index('owner_unique','s')");
		auto ref = IndexRef(con, "owner_unique", "s");
		Check(con, "CREATE TEMP TABLE rekeyed AS SELECT * FROM " + OwnerRow("owner_unique", "s"));
		Check(con, "DELETE FROM __ngram.registry WHERE index_id=" + KeywordHelper::WriteQuoted(ref) + "::UUID");
		Check(con, "INSERT INTO __ngram.registry SELECT registry_version," + KeywordHelper::WriteQuoted(TestUUID(1)) +
		               "::UUID,owner_key,schema_name,table_name,column_name,format_version,gram_size,"
		               "case_insensitive,hwm_rowid,guard_name,guard_token FROM rekeyed");
		if (StatusName(con, "memory", TestUUID(1)) != "MALFORMED" || StatusName(con, "memory", ref) != "MALFORMED") {
			throw std::runtime_error("registry id/storage mismatch was not observable");
		}
	}
}

static void TestCatalogIdentity(const string &path, const string &clone_path) {
	RemoveDatabase(path);
	RemoveDatabase(clone_path);
	string index_ref;
	{
		DuckDB side(path);
		LoadNgram(side);
		Connection con(side);
		Check(con, "CREATE TABLE docs(s VARCHAR)");
		Check(con, "CREATE TABLE unindexed(s VARCHAR)");
		Check(con, "INSERT INTO docs VALUES ('catalog needle')");
		Check(con, "PRAGMA create_ngram_index('docs','s')");
		index_ref = IndexRef(con, "docs", "s");
		Check(con, "FORCE CHECKPOINT");
	}
	CopyFile(path, clone_path);
	{
		DuckDB db(nullptr);
		LoadNgram(db);
		Connection con(db);
		Check(con, "ATTACH " + KeywordHelper::WriteQuoted(path) + " AS leftdb");
		Check(con, "ATTACH " + KeywordHelper::WriteQuoted(clone_path) + " AS rightdb");
		if (StatusName(con, "leftdb", index_ref) != "READY" || StatusName(con, "rightdb", index_ref) != "READY" ||
		    ScalarInt64(con, "SELECT count(*) FROM ngram_search('leftdb.main.docs','needle')") != 1 ||
		    ScalarInt64(con, "SELECT count(*) FROM ngram_search('rightdb.main.docs','needle')") != 1) {
			throw std::runtime_error("attached clone identity was not catalog-qualified and READY");
		}
		DropByRef(con, "leftdb", index_ref);
		if (StatusName(con, "rightdb", index_ref) != "READY" ||
		    ScalarInt64(con, "SELECT count(*) FROM ngram_search('rightdb.main.docs','needle')") != 1) {
			throw std::runtime_error("qualified drop crossed into a cloned catalog");
		}
		Check(con, "DETACH leftdb");
		Check(con, "DETACH rightdb");
		Check(con, "ATTACH " + KeywordHelper::WriteQuoted(clone_path) + " AS rebound");
		if (StatusName(con, "rebound", index_ref) != "READY") {
			throw std::runtime_error("healthy DETACH/ATTACH allocation was not READY");
		}
		Check(con, "DETACH rebound");
		Check(con, "ATTACH " + KeywordHelper::WriteQuoted(clone_path) + " AS ro (READ_ONLY)");
		if (StatusName(con, "ro", index_ref) != "READY" ||
		    ScalarInt64(con, "SELECT count(*) FROM ngram_search('ro.main.docs','needle')") != 1) {
			throw std::runtime_error("read-only catalog did not list/search a healthy index");
		}
		ExpectError(con, "PRAGMA drop_ngram_index_by_id('ro'," + KeywordHelper::WriteQuoted(index_ref) + ")",
		            "read-only");
		ExpectError(con, "PRAGMA create_ngram_index('ro.main.unindexed','s')", "read-only");
	}
	RemoveDatabase(path);
	RemoveDatabase(clone_path);
}

static void TestFormat3Fixture(const string &fixture, const string &path) {
	RemoveDatabase(path);
	CopyFile(fixture, path);
	DuckDB db(nullptr);
	LoadNgram(db);
	Connection con(db);
	Check(con, "ATTACH " + KeywordHelper::WriteQuoted(path) + " AS f3");
	auto listed = Query(con, "PRAGMA ngram_indexes");
	if (listed->RowCount() != 1 || listed->GetValue(0, 0).ToString() != "f3" ||
	    listed->GetValue(5, 0).GetValue<int64_t>() != 3 || listed->GetValue(6, 0).ToString() != "MALFORMED" ||
	    listed->GetValue(7, 0).ToString().find("format 3") == string::npos) {
		throw std::runtime_error("format-3 fixture was not listed MALFORMED with its format");
	}
	auto ref = listed->GetValue(1, 0).ToString();
	ExpectError(con, "PRAGMA create_ngram_index('f3.main.docs', 't')", "predates format 4");
	// The token-checked drop removes the guard, the row, the old storage schema
	// and, with the last row, the old registry; the table is untouched and a
	// fresh index is READY.
	DropByRef(con, "f3", ref);
	if (Query(con, "PRAGMA ngram_indexes")->RowCount() != 0 ||
	    ScalarInt64(con, "SELECT count(*) FROM duckdb_schemas() WHERE database_name='f3' AND schema_name LIKE "
	                     "'__ngram%'") != 0 ||
	    ScalarInt64(con, "SELECT count(*) FROM duckdb_indexes() WHERE database_name='f3'") != 0 ||
	    ScalarInt64(con, "SELECT count(*) FROM f3.docs") != 3) {
		throw std::runtime_error("format-3 drop left ngram objects or touched the table");
	}
	Check(con, "PRAGMA create_ngram_index('f3.main.docs', 's')");
	if (StatusName(con, "f3", IndexRef(con, "docs", "s", "f3")) != "READY" ||
	    ScalarInt64(con, "SELECT count(*) FROM ngram_search('f3.main.docs', 'tent')") != 2) {
		throw std::runtime_error("rebuild after the format-3 drop was not READY and exact");
	}
	Check(con, "DETACH f3");
	RemoveDatabase(path);
}

static void TestRegistryCorruption() {
	const string catalog = "memory";
	// Losing the row, or the whole registry, leaves storage that is listed but
	// cannot be dropped by id. New indexes are unaffected.
	for (bool whole_registry : {false, true}) {
		DuckDB db(nullptr);
		LoadNgram(db);
		Connection con(db);
		Check(con, "CREATE TABLE recoverable(s VARCHAR)");
		Check(con, "INSERT INTO recoverable VALUES ('recover needle')");
		Check(con, "PRAGMA create_ngram_index('recoverable','s')");
		auto ref = IndexRef(con, "recoverable", "s");
		auto guard = GuardName(con, "recoverable", "s");
		auto segments = StorageTable(con, "recoverable", "s", "segments");
		auto stats = StorageTable(con, "recoverable", "s", "stats");
		if (whole_registry) {
			Check(con, "DROP TABLE __ngram.registry");
		} else {
			Check(con, "DELETE FROM __ngram.registry WHERE index_id=" + KeywordHelper::WriteQuoted(ref) + "::UUID");
		}
		ExpectStatus(con, catalog, ref, "MALFORMED");
		ExpectError(con, "PRAGMA drop_ngram_index_by_id('memory'," + KeywordHelper::WriteQuoted(ref) + ")", "manually");
		Check(con, "CREATE TABLE unblocked(s VARCHAR)");
		Check(con, "PRAGMA create_ngram_index('unblocked','s')");
		Check(con, "PRAGMA drop_ngram_index('unblocked','s')");
		Check(con, "DROP TABLE " + segments);
		Check(con, "DROP TABLE " + stats);
		Check(con, "DROP INDEX " + guard);
		if (Query(con, "PRAGMA ngram_indexes")->RowCount() != 0) {
			throw std::runtime_error("manual cleanup of orphaned storage left a listing");
		}
	}

	// A valid row whose storage vanished is removable: the row holds the guard
	// token, so the generic drop still proves the guard before removing it.
	{
		DuckDB db(nullptr);
		LoadNgram(db);
		Connection con(db);
		Check(con, "CREATE TABLE dangling(s VARCHAR)");
		Check(con, "PRAGMA create_ngram_index('dangling','s')");
		auto ref = IndexRef(con, "dangling", "s");
		DropStorage(con, "dangling", "s");
		ExpectStatus(con, catalog, ref, "MALFORMED");
		DropByRef(con, catalog, ref);
		if (HasRowIdGuard(con, "dangling") || ScalarInt64(con, "SELECT count(*) FROM __ngram.registry") != 0) {
			throw std::runtime_error("dropping a row without storage left its guard or row");
		}
	}

	// A wrong-kind registry is reported per storage id. It must not poison
	// unrelated LIKE, and neither create nor ID drop may trust it.
	{
		DuckDB db(nullptr);
		LoadNgram(db);
		Connection con(db);
		Check(con, "SET ngram_auto_accelerate=true");
		Check(con, "CREATE TABLE registry_bad(s VARCHAR)");
		Check(con, "INSERT INTO registry_bad VALUES ('owned needle')");
		Check(con, "PRAGMA create_ngram_index('registry_bad','s')");
		auto ref = IndexRef(con, "registry_bad", "s");
		Check(con, "CREATE TABLE unrelated(s VARCHAR)");
		Check(con, "INSERT INTO unrelated VALUES ('plain needle')");
		Check(con, "CREATE TABLE registry_saved AS SELECT * FROM __ngram.registry");
		Check(con, "DROP TABLE __ngram.registry");
		Check(con, "CREATE VIEW __ngram.registry AS SELECT * FROM registry_saved");
		ExpectStatus(con, catalog, ref, "MALFORMED");
		if (ScalarInt64(con, "SELECT count(*) FROM unrelated WHERE contains(s,'needle')") != 1 ||
		    ScalarInt64(con, "SELECT count(*) FROM registry_bad WHERE contains(s,'needle')") != 1) {
			throw std::runtime_error("malformed global registry poisoned LIKE");
		}
		ExpectError(con, "PRAGMA create_ngram_index('unrelated','s')", "ordinary DuckDB table");
		ExpectError(con, "PRAGMA drop_ngram_index_by_id('memory'," + KeywordHelper::WriteQuoted(ref) + ")", "manually");
	}

	// Row edits are caught against the binary owner key; the row stays
	// removable by id because it still holds its guard token.
	for (auto &field : vector<pair<string, string>> {{"owner_key", "from_hex('00')"},
	                                                 {"schema_name", "'other'"},
	                                                 {"table_name", "'other'"},
	                                                 {"column_name", "'other'"}}) {
		DuckDB db(nullptr);
		LoadNgram(db);
		Connection con(db);
		Check(con, "CREATE TABLE row_bad(s VARCHAR)");
		Check(con, "PRAGMA create_ngram_index('row_bad','s')");
		auto ref = IndexRef(con, "row_bad", "s");
		Check(con, "UPDATE __ngram.registry SET " + field.first + "=" + field.second +
		               " WHERE index_id=" + KeywordHelper::WriteQuoted(ref) + "::UUID");
		ExpectStatus(con, catalog, ref, "MALFORMED");
		Check(con, "CREATE TABLE unblocked(s VARCHAR)");
		Check(con, "PRAGMA create_ngram_index('unblocked','s')");
		if (field.first == "owner_key") {
			DropByRef(con, catalog, ref);
			if (HasRowIdGuard(con, "row_bad") || StorageTableCount(con) != 2) {
				throw std::runtime_error("generic drop of a row with a bad owner key left guard or storage");
			}
		}
	}
	// Unsupported row versions and IDs outside the canonical UUIDv4 space.
	for (bool bad_version : {true, false}) {
		DuckDB db(nullptr);
		LoadNgram(db);
		Connection con(db);
		Check(con, "CREATE TABLE registry_value_bad(s VARCHAR)");
		Check(con, "PRAGMA create_ngram_index('registry_value_bad','s')");
		auto ref = IndexRef(con, "registry_value_bad", "s");
		if (bad_version) {
			Check(con, "UPDATE __ngram.registry SET registry_version=9");
		} else {
			Check(con, "UPDATE __ngram.registry SET index_id='00000000-0000-1000-8000-000000000001'::UUID");
		}
		auto listed = Query(con, "PRAGMA ngram_indexes");
		if (listed->RowCount() == 0 || listed->GetValue(6, 0).ToString() != "MALFORMED") {
			throw std::runtime_error("bad registry version/UUID was not listed MALFORMED");
		}
		Check(con, "CREATE TABLE create_blocked(s VARCHAR)");
		if (bad_version) {
			Check(con, "PRAGMA create_ngram_index('create_blocked','s')");
			DropByRef(con, catalog, ref);
			if (HasRowIdGuard(con, "registry_value_bad")) {
				throw std::runtime_error("generic drop of an unsupported row version left its guard");
			}
		} else {
			ExpectError(con, "PRAGMA create_ngram_index('create_blocked','s')", "noncanonical ID");
			ExpectError(con, "PRAGMA drop_ngram_index_by_id('memory'," + KeywordHelper::WriteQuoted(ref) + ")",
			            "manually");
			if (!HasRowIdGuard(con, "registry_value_bad")) {
				throw std::runtime_error("noncanonical ID refusal changed the guard");
			}
		}
	}

	// Either storage table is required. Absence declines transparent
	// acceleration exactly and is droppable; a present wrong-kind object is fatal.
	for (auto &part : vector<string> {"segments", "stats"}) {
		DuckDB db(nullptr);
		LoadNgram(db);
		Connection con(db);
		Check(con, "SET ngram_auto_accelerate=true");
		Check(con, "CREATE TABLE partial(s VARCHAR)");
		Check(con, "INSERT INTO partial VALUES ('partial needle')");
		Check(con, "PRAGMA create_ngram_index('partial','s')");
		auto ref = IndexRef(con, "partial", "s");
		auto table = StorageTable(con, "partial", "s", part);
		Check(con, "DROP TABLE " + table);
		ExpectStatus(con, catalog, ref, "MALFORMED");
		if (ScalarInt64(con, "SELECT count(*) FROM partial WHERE contains(s,'needle')") != 1) {
			throw std::runtime_error("missing " + part + " did not fall back transparently");
		}
		Check(con, "CREATE VIEW " + table + " AS SELECT 1 AS wrong_kind");
		ExpectError(con, "SELECT count(*) FROM partial WHERE contains(s,'needle')", "wrong catalog type");
		ExpectError(con, "PRAGMA drop_ngram_index_by_id('memory'," + KeywordHelper::WriteQuoted(ref) + ")", "View");
		Check(con, "DROP VIEW " + table);
		DropByRef(con, catalog, ref);
		if (HasRowIdGuard(con, "partial") || StorageTableCount(con) != 0) {
			throw std::runtime_error("dropping a partial allocation left guard or storage");
		}
	}

	// A guard name the table does not carry is scan-only, never corruption.
	{
		DuckDB db(nullptr);
		LoadNgram(db);
		Connection con(db);
		Check(con, "CREATE TABLE guard_bad(s VARCHAR)");
		Check(con, "INSERT INTO guard_bad VALUES ('guard needle')");
		Check(con, "PRAGMA create_ngram_index('guard_bad','s')");
		auto ref = IndexRef(con, "guard_bad", "s");
		Check(con, "UPDATE __ngram.registry SET guard_name='foreign_guard'");
		ExpectStatus(con, catalog, ref, "SCAN_ONLY");
		if (ScalarInt64(con, "SELECT count(*) FROM ngram_search('guard_bad','needle')") != 1) {
			throw std::runtime_error("a foreign guard name did not fall back exhaustively");
		}
	}

	// Foreign objects in the reserved schema are listed beside healthy indexes.
	{
		DuckDB db(nullptr);
		LoadNgram(db);
		Connection con(db);
		Check(con, "CREATE TABLE foreign_schema(s VARCHAR)");
		Check(con, "PRAGMA create_ngram_index('foreign_schema','s')");
		auto ref = IndexRef(con, "foreign_schema", "s");
		Check(con, "CREATE TABLE __ngram.foreign_object(i INTEGER)");
		Check(con, "CREATE TABLE __ngram.segments_deadbeef(i INTEGER)");
		Check(con, "CREATE VIEW __ngram.stats_00000000000040008000000000000001 AS SELECT 1");
		ExpectStatus(con, catalog, ref, "READY");
		auto listed = Query(con, "PRAGMA ngram_indexes");
		idx_t malformed = 0;
		for (idx_t row = 0; row < listed->RowCount(); row++) {
			malformed += listed->GetValue(6, row).ToString() == "MALFORMED";
		}
		if (listed->RowCount() != 4 || malformed != 3) {
			throw std::runtime_error("foreign objects in the reserved schema were not listed individually");
		}
	}
}

static void TestExecutionIdentityRaces() {
	const string catalog = "memory";
	// Plans bound to a removed storage table treat the index as unavailable for
	// exhaustive adapters, while candidate-only execution reports the loss.
	for (auto &part : vector<string> {"segments", "stats"}) {
		DuckDB db(nullptr);
		LoadNgram(db);
		Connection con(db);
		Check(con, "SET ngram_auto_accelerate=true");
		Check(con, "CREATE TABLE absent_part(s VARCHAR)");
		Check(con, "INSERT INTO absent_part VALUES ('race needle')");
		Check(con, "PRAGMA create_ngram_index('absent_part','s')");
		auto exact = con.Prepare("SELECT count(*) FROM ngram_search('absent_part','needle')");
		auto transparent = con.Prepare("SELECT count(*) FROM absent_part WHERE contains(s,'needle')");
		auto candidate = con.Prepare("SELECT count(*) FROM ngram_candidates('absent_part','s','needle')");
		if (exact->HasError() || transparent->HasError() || candidate->HasError()) {
			throw std::runtime_error("failed to prepare absent-component race");
		}
		Check(con, "DROP TABLE " + StorageTable(con, "absent_part", "s", part));
		if (PreparedScalar(*exact) != 1 || PreparedScalar(*transparent) != 1) {
			throw std::runtime_error("prepared exhaustive query did not fall back after " + part + " removal");
		}
		ExpectPreparedError(*candidate, "no longer exists");
	}

	// Storage tables carry no identity of their own: one re-created at the same
	// name with the same content is read, and every adapter stays exact.
	for (auto &part : vector<string> {"segments", "stats"}) {
		DuckDB db(nullptr);
		LoadNgram(db);
		Connection setup(db), ddl(db);
		Check(setup, "SET ngram_auto_accelerate=true");
		Check(setup, "CREATE TABLE swapped_part(s VARCHAR)");
		Check(setup, "INSERT INTO swapped_part VALUES ('swap needle')");
		Check(setup, "PRAGMA create_ngram_index('swapped_part','s')");
		auto table = StorageTable(setup, "swapped_part", "s", part);
		Check(setup, "CREATE TABLE backup_" + part + " AS SELECT * FROM " + table);
		auto exact = setup.Prepare("SELECT count(*) FROM ngram_search('swapped_part','needle')");
		auto transparent = setup.Prepare("SELECT count(*) FROM swapped_part WHERE contains(s,'needle')");
		auto candidate = setup.Prepare("SELECT count(*) FROM ngram_candidates('swapped_part','s','needle')");
		Check(ddl, "DROP TABLE " + table);
		Check(ddl, "CREATE TABLE " + table + " AS SELECT * FROM backup_" + part);
		if (PreparedScalar(*exact) != 1 || PreparedScalar(*transparent) != 1 || PreparedScalar(*candidate) != 1) {
			throw std::runtime_error("prepared query was not exact after " + part + " replacement");
		}
	}

	// A LIKE plan prepared under auto-acceleration by a reader keeps returning
	// the matching rows across a drop and re-create of the same index by the
	// writing connection. This covers the prepared statement surviving another
	// connection's DDL and does not reach the changed_is_absent fallback: the
	// re-created index has a new id, so a stale plan resolves as absent, and a
	// prepared base-table scan re-binds after any catalog change before it
	// executes.
	{
		DuckDB db(nullptr);
		LoadNgram(db);
		Connection reader(db), ddl(db);
		Check(ddl, "CREATE TABLE recreated(s VARCHAR)");
		Check(ddl, "INSERT INTO recreated VALUES ('x marks the row'), ('no match')");
		Check(ddl, "PRAGMA create_ngram_index('recreated','s')");
		Check(reader, "SET ngram_auto_accelerate=true");
		auto like = reader.Prepare("SELECT count(*) FROM recreated WHERE s LIKE '%x%'");
		if (like->HasError() || PreparedScalar(*like) != 1) {
			throw std::runtime_error("prepared LIKE did not find the row before the index was re-created");
		}
		Check(ddl, "PRAGMA drop_ngram_index('recreated','s')");
		Check(ddl, "PRAGMA create_ngram_index('recreated','s')");
		if (PreparedScalar(*like) != 1) {
			throw std::runtime_error("prepared LIKE did not find the row after the index was re-created");
		}
	}

	// Dropping and rebuilding the same owner invalidates cached exact,
	// candidate, and transparent plans; normal prepared-statement rebinding keeps
	// every result exact against the replacement allocation.
	{
		DuckDB db(nullptr);
		LoadNgram(db);
		Connection con(db);
		Check(con, "SET ngram_auto_accelerate=true");
		Check(con, "CREATE TABLE rebuilt(s VARCHAR)");
		Check(con, "INSERT INTO rebuilt VALUES ('rebuild needle')");
		Check(con, "PRAGMA create_ngram_index('rebuilt','s')");
		auto exact = con.Prepare("SELECT count(*) FROM ngram_search('rebuilt','needle')");
		auto transparent = con.Prepare("SELECT count(*) FROM rebuilt WHERE contains(s,'needle')");
		auto candidate = con.Prepare("SELECT count(*) FROM ngram_candidates('rebuilt','s','needle')");
		auto old_ref = IndexRef(con, "rebuilt", "s");
		DropByRef(con, catalog, old_ref);
		Check(con, "PRAGMA create_ngram_index('rebuilt','s')");
		if (PreparedScalar(*exact) != 1 || PreparedScalar(*transparent) != 1) {
			throw std::runtime_error("prepared exhaustive adapters were not exact after same-owner rebuild");
		}
		ExpectPreparedError(*candidate, "storage was removed after binding");
	}

	// Maintenance scripts pin the registry row at execution, not only when the
	// pragma is preprocessed.
	for (auto &pragma : vector<string> {"PRAGMA ngram_refresh('maint_race')", "PRAGMA ngram_compact('maint_race')"}) {
		DuckDB db(nullptr);
		LoadNgram(db);
		Connection planned(db), ddl(db);
		Check(planned, "CREATE TABLE maint_race(s VARCHAR)");
		Check(planned, "INSERT INTO maint_race VALUES ('old needle')");
		Check(planned, "PRAGMA create_ngram_index('maint_race','s')");
		Check(planned, "INSERT INTO maint_race VALUES ('tail needle')");
		auto script = Expand(planned, pragma);
		auto old_ref = IndexRef(ddl, "maint_race", "s");
		DropByRef(ddl, catalog, old_ref);
		Check(ddl, "PRAGMA create_ngram_index('maint_race','s')");
		auto error = ExecuteRemainingForError(planned, script);
		if (error.find("storage is unavailable") == string::npos) {
			throw std::runtime_error("maintenance replacement race did not fail closed: " + error);
		}
		Rollback(planned);
		if (IndexStatus(ddl, "maint_race", "s") != "READY" ||
		    ScalarInt64(ddl, "SELECT count(*) FROM ngram_search('maint_race','needle')") != 2) {
			throw std::runtime_error("failed stale maintenance script damaged rebuilt allocation");
		}
	}

	// A live-base ID drop planned before CREATE OR REPLACE cleans only the old
	// allocation. An orphan plan similarly preserves a newly created base.
	for (bool orphan_first : {false, true}) {
		DuckDB db(nullptr);
		LoadNgram(db);
		Connection planned(db), ddl(db);
		Check(planned, "CREATE TABLE base_race(s VARCHAR)");
		Check(planned, "INSERT INTO base_race VALUES ('old needle')");
		Check(planned, "PRAGMA create_ngram_index('base_race','s')");
		auto ref = IndexRef(planned, "base_race", "s");
		if (orphan_first) {
			Check(ddl, "DROP TABLE base_race");
		}
		auto script =
		    Expand(planned, "PRAGMA drop_ngram_index_by_id('memory'," + KeywordHelper::WriteQuoted(ref) + ")");
		if (orphan_first) {
			Check(ddl, "CREATE TABLE base_race(s VARCHAR)");
		} else {
			Check(ddl, "CREATE OR REPLACE TABLE base_race(s VARCHAR)");
		}
		Check(ddl, "INSERT INTO base_race VALUES ('replacement survives')");
		Check(ddl, "CREATE INDEX replacement_guardrail ON base_race(s)");
		auto error = ExecuteRemainingForError(planned, script);
		if (!error.empty()) {
			throw std::runtime_error("replacement-safe ID drop failed: " + error);
		}
		if (ScalarString(ddl, "SELECT s FROM base_race") != "replacement survives" ||
		    ScalarInt64(ddl, "SELECT count(*) FROM duckdb_indexes() WHERE index_name='replacement_guardrail'") != 1) {
			throw std::runtime_error("preprocessed ID drop touched replacement base state");
		}
	}

	// Registry row disappearance or change across preprocessing is an
	// execution error; a row that is gone at preprocessing is not droppable.
	{
		DuckDB db(nullptr);
		LoadNgram(db);
		Connection planned(db), ddl(db);
		Check(planned, "CREATE TABLE row_race(s VARCHAR)");
		Check(planned, "PRAGMA create_ngram_index('row_race','s')");
		auto ref = IndexRef(planned, "row_race", "s");
		Check(ddl, "CREATE TABLE row_backup AS SELECT * FROM __ngram.registry WHERE index_id=" +
		               KeywordHelper::WriteQuoted(ref) + "::UUID");
		auto removed =
		    Expand(planned, "PRAGMA drop_ngram_index_by_id('memory'," + KeywordHelper::WriteQuoted(ref) + ")");
		Check(ddl, "DELETE FROM __ngram.registry WHERE index_id=" + KeywordHelper::WriteQuoted(ref) + "::UUID");
		auto error = ExecuteRemainingForError(planned, removed);
		if (error.find("removed after") == string::npos) {
			throw std::runtime_error("registry-row deletion race did not fail closed: " + error);
		}
		Rollback(planned);
		ExpectError(planned, "PRAGMA drop_ngram_index_by_id('memory'," + KeywordHelper::WriteQuoted(ref) + ")",
		            "manually");
		Check(ddl, "INSERT INTO __ngram.registry SELECT * FROM row_backup");
		auto changed =
		    Expand(planned, "PRAGMA drop_ngram_index_by_id('memory'," + KeywordHelper::WriteQuoted(ref) + ")");
		Check(ddl, "UPDATE __ngram.registry SET table_name='changed' WHERE index_id=" +
		               KeywordHelper::WriteQuoted(ref) + "::UUID");
		error = ExecuteRemainingForError(planned, changed);
		if (error.find("changed after") == string::npos) {
			throw std::runtime_error("registry-row mutation race did not fail closed: " + error);
		}
		Rollback(planned);
		if (!HasRowIdGuard(ddl, "row_race")) {
			throw std::runtime_error("failed row-mutation drop changed the guard");
		}
	}

	// ID-drop and query execution pin the registry table itself.
	{
		DuckDB db(nullptr);
		LoadNgram(db);
		Connection planned(db), ddl(db);
		Check(planned, "CREATE TABLE registry_swap(s VARCHAR)");
		Check(planned, "INSERT INTO registry_swap VALUES ('registry needle')");
		Check(planned, "PRAGMA create_ngram_index('registry_swap','s')");
		auto ref = IndexRef(planned, "registry_swap", "s");
		auto exact = planned.Prepare("SELECT count(*) FROM ngram_search('registry_swap','needle')");
		auto transparent = planned.Prepare("SELECT count(*) FROM registry_swap WHERE contains(s,'needle')");
		auto candidate = planned.Prepare("SELECT count(*) FROM ngram_candidates('registry_swap','s','needle')");
		auto drop = Expand(planned, "PRAGMA drop_ngram_index_by_id('memory'," + KeywordHelper::WriteQuoted(ref) + ")");
		Check(ddl, "CREATE TABLE registry_backup AS SELECT * FROM __ngram.registry");
		Check(ddl, "DROP TABLE __ngram.registry");
		Check(ddl, "CREATE TABLE __ngram.registry(registry_version INTEGER NOT NULL, index_id UUID PRIMARY KEY, "
		           "owner_key BLOB UNIQUE NOT NULL, schema_name VARCHAR NOT NULL, table_name VARCHAR NOT NULL, "
		           "column_name VARCHAR NOT NULL, format_version BIGINT NOT NULL, gram_size BIGINT NOT NULL, "
		           "case_insensitive BOOLEAN NOT NULL, hwm_rowid BIGINT NOT NULL, guard_name VARCHAR NOT NULL, "
		           "guard_token VARCHAR NOT NULL)");
		Check(ddl, "INSERT INTO __ngram.registry SELECT * FROM registry_backup");
		// F4 coverage for IndexLocationAvailable(..., changed_is_absent) on a replaced registry table.
		if (PreparedScalar(*exact) != 1 || PreparedScalar(*transparent) != 1) {
			throw std::runtime_error("exhaustive registry replacement race was not exact");
		}
		ExpectPreparedError(*candidate, "registry changed");
		auto error = ExecuteRemainingForError(planned, drop);
		if (error.find("registry changed") == string::npos) {
			throw std::runtime_error("ID drop did not pin registry table OID: " + error);
		}
		Rollback(planned);
		if (!HasRowIdGuard(ddl, "registry_swap") || StatusName(ddl, catalog, ref) != "READY") {
			throw std::runtime_error("registry-OID race damaged the allocation");
		}
	}

	// A same-named guard appearing on another table between preprocessing and
	// execution is preserved and blocks the drop atomically.
	{
		DuckDB db(nullptr);
		LoadNgram(db);
		Connection planned(db), ddl(db);
		Check(planned, "CREATE TABLE guard_race(s VARCHAR)");
		Check(planned, "PRAGMA create_ngram_index('guard_race','s')");
		auto ref = IndexRef(planned, "guard_race", "s");
		auto guard = GuardName(planned, "guard_race", "s");
		Check(ddl, "DROP INDEX " + guard);
		auto drop = Expand(planned, "PRAGMA drop_ngram_index_by_id('memory'," + KeywordHelper::WriteQuoted(ref) + ")");
		Check(ddl, "CREATE TABLE other_guard(s VARCHAR)");
		Check(ddl, "CREATE INDEX " + guard + " ON other_guard USING NGRAM_ROWID_GUARD(s)");
		auto error = ExecuteRemainingForError(planned, drop);
		if (error.find("different table") == string::npos) {
			throw std::runtime_error("cross-table guard execution race did not fail closed: " + error);
		}
		Rollback(planned);
		if (!HasRowIdGuard(ddl, "other_guard") ||
		    ScalarInt64(ddl, "SELECT count(*) FROM __ngram.registry WHERE index_id=" + KeywordHelper::WriteQuoted(ref) +
		                         "::UUID") != 1) {
			throw std::runtime_error("failed guard-collision ID drop changed either allocation");
		}
	}
}

static void TestRegistryScale() {
	DuckDB db(nullptr);
	LoadNgram(db);
	Connection con(db);
	Check(con, "CREATE TABLE scale_live(s VARCHAR)");
	Check(con, "INSERT INTO scale_live VALUES ('scale needle')");
	Check(con, "PRAGMA create_ngram_index('scale_live','s')");
	auto live_ref = IndexRef(con, "scale_live", "s");
	// Rows for tables that do not exist, with owner keys that cannot match: all
	// listed MALFORMED beside the one live index.
	string insert = "INSERT INTO __ngram.registry VALUES ";
	for (idx_t i = 1; i < 10000; i++) {
		if (i > 1) {
			insert += ",";
		}
		auto table = "scale_" + to_string(i);
		insert += "(2," + KeywordHelper::WriteQuoted(TestUUID(i)) + "::UUID,encode(" +
		          KeywordHelper::WriteQuoted(table) + "),'main'," + KeywordHelper::WriteQuoted(table) +
		          ",'s',4,3,true,-1,'g','t')";
	}
	Check(con, insert);
	auto start = std::chrono::steady_clock::now();
	ExpectStatus(con, "memory", live_ref, "READY");
	auto status_ms =
	    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
	start = std::chrono::steady_clock::now();
	auto listed = Query(con, "PRAGMA ngram_indexes");
	auto list_ms =
	    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
	idx_t ready = 0, malformed = 0;
	for (idx_t row = 0; row < listed->RowCount(); row++) {
		auto status = listed->GetValue(6, row).ToString();
		ready += status == "READY";
		malformed += status == "MALFORMED";
	}
#ifdef DEBUG
	constexpr int64_t STATUS_LIMIT_MS = 30000, LIST_LIMIT_MS = 120000;
#else
	constexpr int64_t STATUS_LIMIT_MS = 5000, LIST_LIMIT_MS = 20000;
#endif
	if (listed->RowCount() != 10000 || ready != 1 || malformed != 9999 || status_ms > STATUS_LIMIT_MS ||
	    list_ms > LIST_LIMIT_MS) {
		throw std::runtime_error(
		    "10k registry lookup/list exceeded bounded gate: rows=" + to_string(listed->RowCount()) +
		    ", status_ms=" + to_string(status_ms) + ", list_ms=" + to_string(list_ms));
	}
	start = std::chrono::steady_clock::now();
	DropByRef(con, "memory", live_ref);
	auto drop_ms =
	    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
	if (drop_ms > STATUS_LIMIT_MS || HasRowIdGuard(con, "scale_live")) {
		throw std::runtime_error("10k registry ID drop exceeded bounded gate: " + to_string(drop_ms) + " ms");
	}
	std::cerr << "registry-scale rows=10000 status_ms=" << status_ms << " list_ms=" << list_ms << " drop_ms=" << drop_ms
	          << "\n";
}

static void TestQueryCancellation() {
	DuckDB db(nullptr);
	LoadNgram(db);
	Connection con(db);
	Check(con, "SET threads=1");
	Check(con, "SET ngram_max_candidate_fraction=1.0");
	Check(con, "CREATE TABLE cancel_rows AS SELECT i::BIGINT id, 'aaaaaaaa payload'::VARCHAR s "
	           "FROM range(200000) t(i)");
	Check(con, "PRAGMA create_ngram_index('cancel_rows', 's')");
	Check(con, "SET memory_limit='64MB'");
	unique_ptr<MaterializedQueryResult> result;
	std::thread worker([&]() { result = con.Query("SELECT sum(id) FROM ngram_search('cancel_rows', 'aaaa')"); });
	std::this_thread::sleep_for(std::chrono::milliseconds(10));
	con.Interrupt();
	worker.join();
	if (!result || !result->HasError() || result->GetError().find("Interrupt") == string::npos) {
		throw std::runtime_error("bounded query did not propagate cancellation");
	}
	// Cancellation must release both the hard memory reservation and shared
	// vacuum fence; the same connection and an exclusive checkpoint remain usable.
	if (ScalarInt64(con, "SELECT 42") != 42) {
		throw std::runtime_error("connection unusable after query cancellation");
	}

	// Both compaction plans are generated multi-statement transactions. Split a
	// bounded index into valid, disjoint refresh-like generations: this leaves
	// enough total decode work to interrupt reliably without making one large
	// blob an uninterruptible sanitizer bottleneck. Then require a byte-sensitive
	// storage digest and every scratch table to roll back before reuse.
	Check(con, "SET memory_limit='512MB'");
	auto cancel_segments = StorageTable(con, "cancel_rows", "s", "segments");
	auto cancel_stats = StorageTable(con, "cancel_rows", "s", "stats");
	Check(con, "CREATE TEMP TABLE cancel_split AS SELECT gram, segment_no, generation, "
	           "struct_extract(segment, 'postings') AS postings, "
	           "struct_extract(segment, 'rowid_count') AS rowid_count, "
	           "struct_extract(segment, 'min_rowid') AS min_rowid, "
	           "struct_extract(segment, 'max_rowid') AS max_rowid FROM ("
	           "SELECT gram, segment_no, r % 32 AS generation, ngram_pack_segment(r) AS segment "
	           "FROM ngram_unpack_postings((SELECT gram, segment_no, postings "
	           "FROM " +
	               cancel_segments + ")) GROUP BY gram, segment_no, generation)");
	Check(con, "DELETE FROM " + cancel_segments);
	Check(con, "INSERT INTO " + cancel_segments +
	               " SELECT * FROM cancel_split "
	               "ORDER BY encode(gram), segment_no, generation");
	Check(con, "DELETE FROM " + cancel_stats);
	Check(con, "INSERT INTO " + cancel_stats +
	               " SELECT decode(gram_key), sum(rowid_count)::BIGINT, "
	               "count(*)::BIGINT FROM (SELECT encode(gram) AS gram_key, rowid_count "
	               "FROM " +
	               cancel_segments + ") GROUP BY gram_key ORDER BY gram_key");
	Check(con, "DROP TABLE cancel_split");
	auto maintenance_digest = [&]() {
		return ScalarString(
		    con,
		    "SELECT concat("
		    "(SELECT count(*) || ':' || sum(hash(gram,segment_no,generation,postings,rowid_count,min_rowid,max_rowid)) "
		    "FROM " +
		        cancel_segments +
		        "), '|', "
		        "(SELECT count(*) || ':' || sum(hash(gram,row_count,segment_count)) "
		        "FROM " +
		        cancel_stats +
		        "), "
		        "'|', (SELECT hwm_rowid FROM " +
		        OwnerRow("cancel_rows", "s") + "))");
	};
	auto before = maintenance_digest();
	for (auto &pragma :
	     vector<string> {"PRAGMA ngram_compact('cancel_rows')", "PRAGMA ngram_compact('cancel_rows', purge=true)"}) {
		result.reset();
		std::thread maintenance([&]() { result = con.Query(pragma); });
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
		con.Interrupt();
		maintenance.join();
		if (!result || !result->HasError() || result->GetError().find("Interrupt") == string::npos) {
			throw std::runtime_error(pragma + " did not propagate cancellation");
		}
		if (maintenance_digest() != before ||
		    ScalarInt64(con, "SELECT count(*) FROM duckdb_tables() WHERE database_name='temp' AND "
		                     "table_name LIKE '__ngram_%'") != 0) {
			throw std::runtime_error(pragma + " left partial storage state or scratch tables after cancellation");
		}
	}
	Check(con, "FORCE CHECKPOINT");
}

int main(int argc, char **argv) {
	try {
		if (argc == 3 && string(argv[1]) == "--wal-child") {
			WALChild(argv[2]);
		}
		if (argc == 3 && string(argv[1]) == "--reuse-child") {
			ReuseWALChild(argv[2]);
		}
		if (argc == 4 && string(argv[1]) == "--stock-write") {
			StockWriteChild(argv[2], argv[3]);
		}
		if (argc == 3 && string(argv[1]) == "--quarantine") {
			TestIncompatibleGuardQuarantine(argv[2]);
			return 0;
		}
		if (argc != 2 && argc != 3) {
			std::cerr << "usage: ngram_maintenance_checkpoint_gap DATABASE [FORMAT3_FIXTURE]\n";
			return 2;
		}
		auto unique =
		    string(argv[1]) + "." + to_string(std::chrono::high_resolution_clock::now().time_since_epoch().count());
		TestCreationSchedules(unique + ".creation");
		TestSharedGuardAndDrop(unique + ".guards");
		TestVacuumAndConflict(unique + ".vacuum");
		TestRejectedCommitKeepsGuard(unique + ".retried");
		TestConcurrentDropsStrandGuard(unique + ".stranded");
		TestWALAndStock(argv[0], unique + ".wal");
		TestUnboundCheckpointSeal(argv[0], unique + ".seal");
		TestCleanShutdownSeal(unique + ".clean");
		TestIncompatibleGuardQuarantine(unique + ".quarantine");
		TestStorageCorruption();
		TestRegistryLifecycle(unique + ".lifecycle");
		TestRegistryBootstrapAndConflicts();
		TestCatalogIdentity(unique + ".catalog", unique + ".clone");
		if (argc == 3) {
			TestFormat3Fixture(argv[2], unique + ".format3");
		} else {
			std::cerr << "format3-fixture skipped: no fixture path given\n";
		}
		TestRegistryCorruption();
		TestExecutionIdentityRaces();
		TestRegistryScale();
		TestQueryCancellation();
		RemoveDatabase(unique + ".creation");
		RemoveDatabase(unique + ".guards");
		RemoveDatabase(unique + ".vacuum");
		RemoveDatabase(unique + ".retried");
		RemoveDatabase(unique + ".stranded");
		RemoveDatabase(unique + ".wal");
		RemoveDatabase(unique + ".seal");
		RemoveDatabase(unique + ".clean");
		RemoveDatabase(unique + ".quarantine");
		RemoveDatabase(unique + ".lifecycle");
		RemoveDatabase(unique + ".catalog");
		RemoveDatabase(unique + ".clone");
		return 0;
	} catch (std::exception &ex) {
		std::cerr << ex.what() << "\n";
		return 1;
	}
}
