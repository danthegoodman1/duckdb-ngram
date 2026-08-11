#include "duckdb.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/statement/transaction_statement.hpp"
#include "duckdb/storage/storage_manager.hpp"

#include <iostream>

using namespace duckdb;

static void Check(Connection &con, const string &sql) {
	auto result = con.Query(sql);
	if (result->HasError()) {
		throw std::runtime_error(result->GetError());
	}
}

static int64_t ScalarInt64(Connection &con, const string &sql) {
	auto result = con.Query(sql);
	if (result->HasError()) {
		throw std::runtime_error(result->GetError());
	}
	return result->GetValue(0, 0).GetValue<int64_t>();
}

static idx_t WALSize(Connection &con) {
	auto result = con.Query("SELECT current_database()");
	if (result->HasError()) {
		throw std::runtime_error(result->GetError());
	}
	auto &manager = DatabaseManager::Get(*con.context);
	auto database = manager.GetDatabase(result->GetValue(0, 0).ToString());
	return database->GetStorageManager().GetWALSize();
}

//! Deterministically forces the gap that a timing-only SQL test cannot: expand
//! and validate the pragma, checkpoint after its preprocessing transaction has
//! committed, then execute the already-generated body. Build this file against
//! the repo's libduckdb and run it with one fresh database path argument.
int main(int argc, char **argv) {
	if (argc != 2) {
		std::cerr << "usage: ngram_maintenance_checkpoint_gap DATABASE\n";
		return 2;
	}

	DuckDB db(argv[1]);
	Connection setup(db), maintenance(db), checkpoint(db);
	Check(setup, "LOAD ngram");
	Check(setup, "SET checkpoint_threshold='1TB'");
	Check(setup, "CREATE TABLE corpus AS SELECT i id, 'v' || i s FROM range(125000) t(i)");
	Check(setup, "PRAGMA create_ngram_index('corpus', 's')");
	Check(setup, "DELETE FROM corpus WHERE id < 122880");
	Check(setup,
	      "INSERT INTO corpus SELECT i, CASE WHEN i=249999 THEN 'phase10needle' ELSE 't' || i END "
	      "FROM range(125000, 250000) t(i)");

	// ParseStatements runs pragma expansion and its callback validation now.
	// The returned BEGIN/body/COMMIT statements have not executed yet.
	auto statements = maintenance.context->ParseStatements("PRAGMA ngram_refresh('corpus')");
	if (statements.empty() || statements.front()->type != StatementType::TRANSACTION_STATEMENT ||
	    statements.front()->Cast<TransactionStatement>().info->type != TransactionType::BEGIN_TRANSACTION) {
		std::cerr << "pragma expansion did not begin with its generated transaction\n";
		return 3;
	}
	if (ScalarInt64(checkpoint, "SELECT max(rowid) FROM corpus") != 249999) {
		std::cerr << "test setup did not produce the expected pre-checkpoint rowids\n";
		return 4;
	}
	Check(setup, "BEGIN");
	Check(setup,
	      "SELECT system.main.__ngram_maintenance_guard('ngram_refresh', current_database(), 'main', 'corpus', "
	      "'s', false, hwm_rowid, table_oid, schema_fingerprint, gram_size, case_insensitive) "
	      "FROM ngram_main_corpus.meta_s");
	Check(checkpoint, "FORCE CHECKPOINT");
	if (ScalarInt64(checkpoint, "SELECT max(rowid) FROM corpus") != 249999) {
		std::cerr << "checkpoint moved rowids while the maintenance fence was held\n";
		return 5;
	}
	Check(setup, "ROLLBACK");
	// The concurrent checkpoint persisted the old delete without moving rows.
	// Give the next checkpoint fresh full-row-group work and enough new tail to
	// keep max(rowid) above the HWM, so only witnesses expose the rowid move.
	Check(setup, "SET checkpoint_threshold='1TB'");
	Check(setup, "DELETE FROM corpus WHERE rowid >= 122880 AND rowid < 245760");
	Check(setup, "INSERT INTO corpus SELECT i, 'u' || i FROM range(250000, 375000) t(i)");
	Check(checkpoint, "FORCE CHECKPOINT");
	if (ScalarInt64(checkpoint, "SELECT max(rowid) FROM corpus") != 129239) {
		std::cerr << "FORCE CHECKPOINT did not move the expected rowids\n";
		return 6;
	}

	string maintenance_error;
	for (auto &statement : statements) {
		auto result = maintenance.Query(std::move(statement));
		if (result->HasError()) {
			maintenance_error = result->GetError();
			break;
		}
	}
	if (maintenance_error.find("cannot be maintained incrementally") == string::npos) {
		std::cerr << "maintenance unexpectedly published across the checkpoint: " << maintenance_error << "\n";
		return 7;
	}
	Check(maintenance, "ROLLBACK");

	// The failed generated transaction must leave both its HWM and its
	// vacuum locks or scratch state behind cleanly.
	Check(checkpoint, "FORCE CHECKPOINT");
	auto result = maintenance.Query(
	    "SELECT (SELECT hwm_rowid FROM ngram_main_corpus.meta_s), "
	    "(SELECT count(*) FROM duckdb_tables() WHERE database_name='temp' AND table_name LIKE '__ngram_%'), "
	    "(SELECT count(*) FROM corpus WHERE contains(lower(s), 'phase10needle'))");
	if (result->HasError() || result->GetValue(0, 0).GetValue<int64_t>() != 124999 ||
	    result->GetValue(1, 0).GetValue<int64_t>() != 0 || result->GetValue(2, 0).GetValue<int64_t>() != 1) {
		std::cerr << (result->HasError() ? result->GetError() : result->ToString()) << "\n";
		return 8;
	}
	Check(setup, "DELETE FROM corpus WHERE rowid < 122880");
	Check(setup, "INSERT INTO corpus SELECT i, 'w' || i FROM range(375000, 500000) t(i)");
	Check(checkpoint, "FORCE CHECKPOINT");
	if (ScalarInt64(checkpoint, "SELECT max(rowid) FROM corpus") != 131359) {
		std::cerr << "vacuum fence remained held after maintenance error rollback\n";
		return 9;
	}

	// The transaction's own vacuum lock is destroyed just before an automatic
	// checkpoint at commit. This second shape proves the connection state keeps
	// the fence through that checkpoint, then releases it after commit.
	Check(setup, "CREATE TABLE autocheckpoint AS SELECT i id, 'v' || i s FROM range(125000) t(i)");
	auto create_statements = maintenance.context->ParseStatements("PRAGMA create_ngram_index('autocheckpoint', 's')");
	if (create_statements.empty() || create_statements.front()->type != StatementType::TRANSACTION_STATEMENT ||
	    create_statements.front()->Cast<TransactionStatement>().info->type != TransactionType::BEGIN_TRANSACTION) {
		std::cerr << "CREATE expansion did not begin with its generated transaction\n";
		return 10;
	}
	Check(setup, "DELETE FROM autocheckpoint WHERE id < 122880");
	Check(setup,
	      "INSERT INTO autocheckpoint SELECT i, CASE WHEN i=249999 THEN 'autoneedle' ELSE 't' || i END "
	      "FROM range(125000, 250000) t(i)");
	Check(checkpoint, "FORCE CHECKPOINT");
	if (ScalarInt64(checkpoint, "SELECT max(rowid) FROM autocheckpoint") != 127119) {
		std::cerr << "CREATE gap checkpoint did not move the expected rowids\n";
		return 10;
	}
	for (auto &statement : create_statements) {
		auto create_result = maintenance.Query(std::move(statement));
		if (create_result->HasError()) {
			std::cerr << create_result->GetError() << "\n";
			return 11;
		}
	}
	result = maintenance.Query(
	    "SELECT (SELECT hwm_rowid FROM ngram_main_autocheckpoint.meta_s), "
	    "(SELECT count(*) FROM autocheckpoint WHERE contains(lower(s), 'autoneedle')), "
	    "(SELECT count(*) FROM ngram_search('autocheckpoint', 'autoneedle')), "
	    "(SELECT count(*) FROM duckdb_tables() WHERE database_name='temp' AND table_name LIKE '__ngram_%')");
	if (result->HasError() || result->GetValue(0, 0).GetValue<int64_t>() != 127119 ||
	    result->GetValue(1, 0).GetValue<int64_t>() != 1 || result->GetValue(2, 0).GetValue<int64_t>() != 1 ||
	    result->GetValue(3, 0).GetValue<int64_t>() != 0) {
		std::cerr << (result->HasError() ? result->GetError() : result->ToString()) << "\n";
		return 12;
	}
	auto wal_before = WALSize(setup);
	Check(setup, "SET checkpoint_threshold='1B'");
	Check(setup, "BEGIN");
	Check(setup, "DELETE FROM autocheckpoint WHERE rowid < 122880");
	Check(setup, "INSERT INTO autocheckpoint SELECT i, 'u' || i FROM range(250000, 375000) t(i)");
	Check(setup, "PRAGMA ngram_refresh('autocheckpoint')");
	Check(setup, "COMMIT");
	auto wal_after = WALSize(setup);
	if (wal_before <= 0 || wal_after >= wal_before) {
		std::cerr << "automatic checkpoint did not shrink WAL: " << wal_before << " -> " << wal_after << "\n";
		return 13;
	}
	result = setup.Query(
	    "SELECT (SELECT max(rowid) FROM autocheckpoint), "
	    "(SELECT hwm_rowid FROM ngram_main_autocheckpoint.meta_s), "
	    "(SELECT count(*) FROM autocheckpoint WHERE contains(lower(s), 'autoneedle')), "
	    "(SELECT count(*) FROM ngram_search('autocheckpoint', 'autoneedle'))");
	if (result->HasError() || result->GetValue(0, 0).GetValue<int64_t>() != 252119 ||
	    result->GetValue(1, 0).GetValue<int64_t>() != 127119 ||
	    result->GetValue(2, 0).GetValue<int64_t>() != 1 || result->GetValue(3, 0).GetValue<int64_t>() != 1) {
		std::cerr << (result->HasError() ? result->GetError() : result->ToString()) << "\n";
		return 14;
	}
	Check(setup, "SET checkpoint_threshold='1TB'");
	Check(setup, "DELETE FROM autocheckpoint WHERE rowid < 245760");
	Check(checkpoint, "FORCE CHECKPOINT");
	if (ScalarInt64(checkpoint, "SELECT max(rowid) FROM autocheckpoint") != 124999) {
		std::cerr << "vacuum fence remained held after maintenance commit\n";
		return 15;
	}
	return 0;
}
