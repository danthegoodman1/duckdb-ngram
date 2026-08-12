#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/execution/index/unbound_index.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/statement/transaction_statement.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "duckdb/transaction/local_storage.hpp"
#include "core_functions_extension.hpp"
#include "ngram_extension.hpp"
#include "ngram/rowid_guard.hpp"

#include <chrono>
#include <cstdio>
#include <cstdlib>
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

static bool HasRowIdGuard(Connection &con, const string &table_name) {
	bool found = false;
	con.context->RunFunctionInTransaction([&]() {
		auto &table = Catalog::GetEntry<TableCatalogEntry>(*con.context, DatabaseManager::GetDefaultDatabase(*con.context),
		                                                "main", table_name)
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
		auto &table = Catalog::GetEntry<TableCatalogEntry>(*con.context, DatabaseManager::GetDefaultDatabase(*con.context),
		                                                "main", table_name)
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

static void MutateGuardOption(Connection &con, const string &table_name, const string &guard_name,
                              const string &option, Value value) {
	auto catalog_name = DatabaseManager::GetDefaultDatabase(*con.context);
	bool found = false;
	con.context->RunFunctionInTransaction([&]() {
		auto &table = Catalog::GetEntry<TableCatalogEntry>(*con.context, catalog_name, "main", table_name)
		                  .Cast<DuckTableEntry>();
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
		if (library_version != "v1.5.5" || (source_id != "d8cdaa33" && source_id != "d8cdaa33fd")) {
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

	Check(setup, "CREATE TABLE twice(a VARCHAR, b VARCHAR)");
	Check(creator, "BEGIN");
	Check(creator, "PRAGMA create_ngram_index('twice', 'a')");
	ExpectError(creator, "PRAGMA create_ngram_index('twice', 'b')", "catalog changes");
	Rollback(creator);

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
	Check(setup, "CREATE TABLE staged_revert(id INTEGER, s VARCHAR)");
	// Revert at a byte boundary: v1.5.5 DEBUG's validity reverter otherwise
	// checks padding bits against the logical row count and invalidates the DB.
	Check(setup, "INSERT INTO staged_revert SELECT i, CASE WHEN i=0 THEN 'base needle' ELSE 'x' END FROM range(8) t(i)");
	Check(writer, "BEGIN");
	Check(writer, "INSERT INTO staged_revert VALUES (8, 'staged needle')");
	auto &staged_catalog = Catalog::GetCatalog(*writer.context, ScalarString(writer, "SELECT current_database()"));
	DuckTransaction::Get(*writer.context, staged_catalog).GetLocalStorage().Commit(nullptr);
	auto staged_create = Expand(creator, "PRAGMA create_ngram_index('staged_revert', 's')");
	auto staged_next = ExecuteThrough(creator, staged_create, "__ngram_creation_finish");
	auto writer_commit = writer.Query("COMMIT");
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
		auto result = creator.Query(std::move(staged_create[i]));
		if (result->HasError()) {
			creator_error = result->GetError();
			break;
		}
	}
	if (creator_statement.find("COMMIT") == string::npos || creator_error.find("underlying table state was reverted") ==
	                                                            string::npos) {
		throw std::runtime_error("creator did not fail its COMMIT after RevertAppend: " + creator_error);
	}
	Rollback(creator);
	if (ScalarInt64(setup, "SELECT count(*) FROM staged_revert") != 8 || HasRowIdGuard(setup, "staged_revert")) {
		throw std::runtime_error("failed creator published guard or data after RevertAppend");
	}
	Check(creator, "PRAGMA create_ngram_index('staged_revert', 's')");
	if (ScalarInt64(setup, "SELECT count(*) FROM ngram_search('staged_revert', 'needle')") != 1) {
		throw std::runtime_error("create retry after RevertAppend was not exact");
	}

	// Preserve the Phase 10 discriminator directly: the context-owned shared
	// vacuum fence survives the scalar statement and is released by rollback.
	Connection fence(db), checkpoint(db);
	Check(fence, "BEGIN");
	Check(fence,
	      "SELECT system.main.__ngram_maintenance_guard('ngram_refresh', current_database(), 'main', 'stale', 's', "
	      "false, hwm_rowid, table_oid, schema_fingerprint, gram_size, case_insensitive, '', '', '', '', 0, '') "
	      "FROM ngram_main_stale.meta_s");
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

static void TestProtectorsAndDrop(const string &path) {
	RemoveDatabase(path);
	{
		DuckDB db(path);
		LoadNgram(db);
		Connection con(db);
		Check(con, "SET checkpoint_threshold='1TB'");
		Check(con, "CREATE TABLE reopened(s VARCHAR, note VARCHAR)");
		Check(con, "INSERT INTO reopened VALUES ('needle', 'n')");
		Check(con, "CREATE INDEX reopened_art ON reopened(s)");
		Check(con, "FORCE CHECKPOINT");
	}
	{
		DuckDB db(path);
		LoadNgram(db);
		Connection con(db);
		Check(con, "PRAGMA create_ngram_index('reopened', 's')");
		auto before = ScalarInt64(con, "SELECT rowid FROM reopened");
		Check(con, "UPDATE reopened SET note='changed'");
		if (ScalarInt64(con, "SELECT rowid FROM reopened") != before) {
			throw std::runtime_error("native ART fallback guard covered an unproved column");
		}

		Check(con, "CREATE TABLE art_age(s VARCHAR)");
		Connection old(db), ddl(db), creator(db);
		Check(old, "BEGIN");
		Check(old, "SELECT count(*) FROM duckdb_indexes()");
		Check(ddl, "CREATE INDEX art_age_idx ON art_age(s)");
		ExpectError(creator, "PRAGMA create_ngram_index('art_age', 's')", "roll back");
		Rollback(old);
		Check(creator, "PRAGMA create_ngram_index('art_age', 's')");

		Check(con, "CREATE TABLE prepared_art(s VARCHAR)");
		Check(con, "INSERT INTO prepared_art VALUES ('before')");
		Connection prepared_con(db);
		auto prepared = prepared_con.Prepare("UPDATE prepared_art SET s='prepared native needle'");
		if (prepared->HasError()) {
			throw std::runtime_error("failed to prepare native ART rebind discriminator");
		}
		Check(ddl, "CREATE INDEX prepared_art_idx ON prepared_art(s)");
		Check(creator, "PRAGMA create_ngram_index('prepared_art', 's')");
		auto prepared_result = prepared->Execute();
		if (prepared_result->HasError() ||
		    ScalarInt64(con, "SELECT count(*) FROM ngram_search('prepared_art', 'needle')") != 1) {
			throw std::runtime_error("autocommit plan prepared before native ART did not rebind exhaustively");
		}

		// Internal UNIQUE ARTs do not steal the normal broad-barrier path.
		Check(con, "CREATE TABLE internal_art(s VARCHAR UNIQUE, other VARCHAR)");
		Check(con, "INSERT INTO internal_art VALUES ('x', 'old')");
		Check(con, "PRAGMA create_ngram_index('internal_art', 's')");
		before = ScalarInt64(con, "SELECT rowid FROM internal_art");
		Check(con, "UPDATE internal_art SET other='new'");
		if (ScalarInt64(con, "SELECT rowid FROM internal_art") == before) {
			throw std::runtime_error("normal barrier guard was not broad across existing VARCHAR columns");
		}

		Check(con, "CREATE TABLE incarnated(s VARCHAR)");
		Check(con, "INSERT INTO incarnated VALUES ('incarnation needle')");
		Check(con, "PRAGMA create_ngram_index('incarnated', 's')");
		auto guard = ScalarString(con, "SELECT guard_name FROM ngram_main_incarnated.meta_s");
		Check(con, "DROP INDEX " + guard);
		Check(con, "CREATE INDEX " + guard + " ON incarnated USING NGRAM_ROWID_GUARD(s)");
		if (ScalarInt64(con, "SELECT count(*) FROM ngram_search('incarnated', 'needle')") != 1) {
			throw std::runtime_error("token mismatch did not fall back exhaustively");
		}
		ExpectError(con, "PRAGMA drop_ngram_index('incarnated', 's')", "dropped and re-created");
		Check(con, "DROP INDEX " + guard);
		Check(con, "PRAGMA drop_ngram_index('incarnated', 's')");

		// Editing only the version cannot disguise a v3 meta layout as legacy.
		Check(con, "CREATE TABLE drop_hybrid(s VARCHAR)");
		Check(con, "PRAGMA create_ngram_index('drop_hybrid', 's')");
		Check(con, "UPDATE ngram_main_drop_hybrid.meta_s SET format_version=2");
		ExpectError(con, "PRAGMA drop_ngram_index('drop_hybrid', 's')", "contains rowid guard columns");
		if (!HasRowIdGuard(con, "drop_hybrid") ||
		    ScalarInt64(con, "SELECT count(*) FROM ngram_main_drop_hybrid.meta_s") != 1) {
			throw std::runtime_error("malformed v2/v3 hybrid drop changed guard or metadata");
		}
		Check(con, "UPDATE ngram_main_drop_hybrid.meta_s SET format_version=3");
		Check(con, "PRAGMA drop_ngram_index('drop_hybrid', 's')");

		// A truly guard-less v2 layout remains removable after upgrading.
		Check(con, "CREATE TABLE drop_legacy(s VARCHAR)");
		Check(con, "PRAGMA create_ngram_index('drop_legacy', 's')");
		guard = ScalarString(con, "SELECT guard_name FROM ngram_main_drop_legacy.meta_s");
		Check(con, "DROP INDEX " + guard);
		Check(con, "ALTER TABLE ngram_main_drop_legacy.meta_s DROP COLUMN guard_name");
		Check(con, "ALTER TABLE ngram_main_drop_legacy.meta_s DROP COLUMN guard_token");
		Check(con, "UPDATE ngram_main_drop_legacy.meta_s SET format_version=2");
		Check(con, "PRAGMA drop_ngram_index('drop_legacy', 's')");
		if (ScalarInt64(con, "SELECT count(*) FROM information_schema.schemata "
		                     "WHERE schema_name='ngram_main_drop_legacy'") != 0) {
			throw std::runtime_error("guard-less v2 public drop left its shadow schema");
		}

		// A missing guard makes both public query paths scan, but a same-named
		// replacement on another table must block recovery rather than be dropped.
		Check(con, "CREATE TABLE drop_cross_owner(s VARCHAR)");
		Check(con, "INSERT INTO drop_cross_owner VALUES ('cross needle')");
		Check(con, "PRAGMA create_ngram_index('drop_cross_owner', 's')");
		guard = ScalarString(con, "SELECT guard_name FROM ngram_main_drop_cross_owner.meta_s");
		Check(con, "DROP INDEX " + guard);
		Check(con, "SET ngram_auto_accelerate=true");
		if (ScalarInt64(con, "SELECT count(*) FROM ngram_search('drop_cross_owner', 'needle')") != 1 ||
		    ScalarInt64(con, "SELECT count(*) FROM drop_cross_owner WHERE s LIKE '%needle%'") != 1 ||
		    Query(con, "EXPLAIN SELECT * FROM drop_cross_owner WHERE s LIKE '%needle%'")
		            ->ToString()
		            .find("SEQ_SCAN") == string::npos) {
			throw std::runtime_error("missing rowid guard did not choose exact explicit/transparent fallback");
		}
		Check(con, "CREATE TABLE drop_cross_other(s VARCHAR)");
		Check(con, "CREATE INDEX " + guard + " ON drop_cross_other USING NGRAM_ROWID_GUARD(s)");
		ExpectError(con, "PRAGMA drop_ngram_index('drop_cross_owner', 's')", "different table");
		if (!HasRowIdGuard(con, "drop_cross_other") ||
		    ScalarInt64(con, "SELECT count(*) FROM ngram_main_drop_cross_owner.meta_s") != 1) {
			throw std::runtime_error("cross-table guard collision was not preserved by public drop");
		}
		Check(con, "DROP INDEX " + guard);
		Check(con, "PRAGMA drop_ngram_index('drop_cross_owner', 's')");

		Check(con, "CREATE TABLE overlap(a VARCHAR, b VARCHAR)");
		Check(con, "INSERT INTO overlap VALUES ('alpha needle', 'beta needle')");
		Check(con, "PRAGMA create_ngram_index('overlap', 'a')");
		Check(con, "PRAGMA create_ngram_index('overlap', 'b')");
		auto guard_a = ScalarString(con, "SELECT guard_name FROM ngram_main_overlap.meta_a");
		Check(con, "DROP INDEX " + guard_a);
		Check(con, "PRAGMA drop_ngram_index('overlap', 'a')");
		if (ScalarInt64(con, "SELECT count(*) FROM ngram_search('overlap', 'needle', col='b')") != 1) {
			throw std::runtime_error("dropping a missing per-index guard damaged an overlapping guard");
		}

		// Both directions of the drop preprocessor/execution race fail closed.
		Check(con, "CREATE TABLE drop_missing(s VARCHAR)");
		Check(con, "PRAGMA create_ngram_index('drop_missing', 's')");
		guard = ScalarString(con, "SELECT guard_name FROM ngram_main_drop_missing.meta_s");
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
		guard = ScalarString(con, "SELECT guard_name FROM ngram_main_drop_exact.meta_s");
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
		auto guard = ScalarString(con, "SELECT guard_name FROM ngram_main_drop_binding.meta_s");
		SetRowIdGuardBindState(con, "drop_binding", guard, IndexBindState::BINDING, true);
		ExpectError(con, "PRAGMA drop_ngram_index('drop_binding', 's')", "being bound");
		Rollback(con);
		if (!HasRowIdGuard(con, "drop_binding") ||
		    ScalarInt64(con, "SELECT count(*) FROM ngram_main_drop_binding.meta_s") != 1) {
			throw std::runtime_error("drop changed a guard while its binder owned the physical entry");
		}
		SetRowIdGuardBindState(con, "drop_binding", guard, IndexBindState::UNBOUND, true);
		auto drop = Expand(con, "PRAGMA drop_ngram_index('drop_binding', 's')");
		auto next = ExecuteThrough(con, drop, ngram::NGRAM_ROWID_GUARD_VALIDATE);
		if (!RowIdGuardIsBound(con, "drop_binding")) {
			throw std::runtime_error("drop validation succeeded before the exact guard became bound");
		}
		auto error = ExecuteRemainingForError(con, drop, next);
		if (!error.empty()) {
			throw std::runtime_error("bound rowid guard drop failed: " + error);
		}
	}
}

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

	// Guard mutation is deliberately conservative rather than transactional. If
	// a later UNIQUE index rejects a reused rowid after the guard sees it, no row
	// commits but the guard stays unsafe and exact search falls back until rebuild.
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
		bad_guard_name = ScalarString(con, "SELECT guard_name FROM ngram_main_bad_guard.meta_s");
		max_guard_name = ScalarString(con, "SELECT guard_name FROM ngram_main_max_guard.meta_s");
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
		    Query(con, "PRAGMA ngram_index_stats('max_guard')")
		            ->GetValue(12, 0)
		            .ToString()
		            .find("not observed") == string::npos) {
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
	// shadow digest and every scratch table to roll back before reuse.
	Check(con, "SET memory_limit='512MB'");
	Check(con, "CREATE TEMP TABLE cancel_split AS SELECT gram, segment_no, generation, "
	           "struct_extract(segment, 'postings') AS postings, "
	           "struct_extract(segment, 'rowid_count') AS rowid_count, "
	           "struct_extract(segment, 'min_rowid') AS min_rowid, "
	           "struct_extract(segment, 'max_rowid') AS max_rowid FROM ("
	           "SELECT gram, segment_no, r % 32 AS generation, ngram_pack_segment(r) AS segment "
	           "FROM ngram_unpack_postings((SELECT gram, segment_no, postings "
	           "FROM ngram_main_cancel_rows.segments_s)) GROUP BY gram, segment_no, generation)");
	Check(con, "DELETE FROM ngram_main_cancel_rows.segments_s");
	Check(con, "INSERT INTO ngram_main_cancel_rows.segments_s SELECT * FROM cancel_split "
	           "ORDER BY encode(gram), segment_no, generation");
	Check(con, "DELETE FROM ngram_main_cancel_rows.stats_s");
	Check(con, "INSERT INTO ngram_main_cancel_rows.stats_s SELECT decode(gram_key), sum(rowid_count)::BIGINT, "
	           "count(*)::BIGINT FROM (SELECT encode(gram) AS gram_key, rowid_count "
	           "FROM ngram_main_cancel_rows.segments_s) GROUP BY gram_key ORDER BY gram_key");
	Check(con, "DROP TABLE cancel_split");
	auto maintenance_digest = [&]() {
		return ScalarString(
		    con,
		    "SELECT concat("
		    "(SELECT count(*) || ':' || sum(hash(gram,segment_no,generation,postings,rowid_count,min_rowid,max_rowid)) "
		    "FROM ngram_main_cancel_rows.segments_s), '|', "
		    "(SELECT count(*) || ':' || sum(hash(gram,row_count,segment_count)) "
		    "FROM ngram_main_cancel_rows.stats_s), "
		    "'|', (SELECT hwm_rowid FROM ngram_main_cancel_rows.meta_s))");
	};
	auto before = maintenance_digest();
	for (auto &pragma : vector<string> {"PRAGMA ngram_compact('cancel_rows')",
	                                    "PRAGMA ngram_compact('cancel_rows', purge=true)"}) {
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
			throw std::runtime_error(pragma + " left partial shadow state or scratch tables after cancellation");
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
		if (argc != 2) {
			std::cerr << "usage: ngram_maintenance_checkpoint_gap DATABASE\n";
			return 2;
		}
		auto unique = string(argv[1]) + "." +
		              to_string(std::chrono::high_resolution_clock::now().time_since_epoch().count());
		TestCreationSchedules(unique + ".creation");
		TestProtectorsAndDrop(unique + ".protectors");
		TestVacuumAndConflict(unique + ".vacuum");
		TestWALAndStock(argv[0], unique + ".wal");
		TestUnboundCheckpointSeal(argv[0], unique + ".seal");
		TestCleanShutdownSeal(unique + ".clean");
		TestIncompatibleGuardQuarantine(unique + ".quarantine");
		TestQueryCancellation();
		RemoveDatabase(unique + ".creation");
		RemoveDatabase(unique + ".protectors");
		RemoveDatabase(unique + ".vacuum");
		RemoveDatabase(unique + ".wal");
		RemoveDatabase(unique + ".seal");
		RemoveDatabase(unique + ".clean");
		RemoveDatabase(unique + ".quarantine");
		return 0;
	} catch (std::exception &ex) {
		std::cerr << ex.what() << "\n";
		return 1;
	}
}
