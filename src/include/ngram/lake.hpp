//===----------------------------------------------------------------------===//
// ngram/lake.hpp: indexes on DuckLake tables: the registry and segments kept as lake tables, the maintenance scripts,
// and the candidate set a query's scan of the table is filtered by.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "ngram/catalog.hpp"

namespace duckdb {

class Connection;
class TableCatalogEntry;

namespace ngram {

//===----------------------------------------------------------------------===//
// An index on a DuckLake table is two lake tables in the lake's __ngram
// schema: one registry row in __ngram.registry and the postings in
// __ngram.segments_<id>, laid out as on a DuckDB table except that the
// 128-bit gram key is stored as two UBIGINT halves, because DuckDB's Parquet
// writer stores UHUGEINT as DOUBLE. Postings are keyed by DuckLake row ids,
// which DuckLake never reuses and keeps through updates and compaction, and
// the registry row records the snapshot whose rows they cover. Rows inserted
// after it are read from DuckLake's change feed, so every DuckLake client
// may write the table and none of them needs this extension. A query reads
// the index at its own transaction's snapshot and filters its scan of the
// table to the candidate row ids, which lets the scan skip every row group
// that holds none.
//===----------------------------------------------------------------------===//

constexpr const char *LAKE_CATALOG_TYPE = "ducklake";
constexpr int64_t LAKE_REGISTRY_VERSION = 1;
constexpr int64_t LAKE_FORMAT_VERSION = 1;

//! Whether the table lives in an attached DuckLake catalog.
bool IsLakeTable(TableCatalogEntry &table);
bool IsLakeCatalog(ClientContext &context, const string &catalog_name);

//! The snapshot the calling transaction reads `catalog_name` at.
int64_t LakeTransactionSnapshot(ClientContext &context, const string &catalog_name);

//! The schema version of that snapshot, as DuckLake reports it to DuckDB's
//! catalog. DuckLake raises it with every commit that creates, alters or drops
//! a schema, table, view or macro, and with some commits that only add
//! inlined-data tables; it never repeats.
int64_t LakeSchemaVersion(ClientContext &context, const string &catalog_name);

//! Whether the calling transaction has written `catalog_name`. Its own
//! inserts, updates and deletes are in no snapshot the index covers.
bool LakeWrittenByTransaction(ClientContext &context, const string &catalog_name);

//! The rows of a query the reader ran, materialized.
struct LakeRows {
	vector<vector<Value>> rows;

	idx_t RowCount() const {
		return rows.size();
	}
	const Value &GetValue(idx_t column, idx_t row) const {
		return rows[row][column];
	}
};

//! Queries one lake, through a connection of its own, at the snapshot of the
//! caller's transaction: every read names that snapshot with AT (VERSION =>
//! n), so what it returns is what the caller's transaction sees, except the
//! caller's own uncommitted writes. DuckDB holds the caller's context while it
//! plans a query, so planning reads the lake this way.
class LakeReader {
public:
	LakeReader(ClientContext &context, string catalog_name);
	~LakeReader();

	ClientContext &Context() const {
		return context;
	}
	const string &CatalogName() const {
		return catalog_name;
	}
	int64_t Snapshot() const {
		return snapshot;
	}
	//! ` AT (VERSION => <snapshot>)`, for a table reference.
	string At() const;
	//! The catalog-qualified name of a table in the __ngram schema.
	string IndexTable(const string &table) const;
	//! Runs `sql` and raises its error, if any, naming `what`.
	LakeRows Query(const string &sql, const char *what);
	//! Runs `sql`; false, with the error, when it fails.
	bool TryExecute(const string &sql, string &error);
	//! Query, or false with the error when `sql` fails.
	bool TryQuery(const string &sql, LakeRows &rows, string &error);

private:
	Connection &GetConnection();

	ClientContext &context;
	string catalog_name;
	int64_t snapshot;
	unique_ptr<Connection> connection;
};

//! One registry row: an index, or the lock row every maintenance call
//! deletes and re-inserts so that DuckLake lets one of two concurrent calls
//! commit.
struct LakeIndex {
	string index_ref;
	string schema_name, table_name, column_name;
	//! DuckLake's id of the table, which ALTER keeps and DROP retires.
	int64_t table_id = -1;
	int64_t format_version = -1;
	GramOptions options;
	//! Every row the table held at this snapshot is indexed.
	int64_t covered_snapshot = -1;
	//! The lake's schema version when the index was last maintained; equal to
	//! the reader's means no table has been altered since.
	int64_t schema_version = -1;
	//! Why the row is unusable (another format or registry version), or empty.
	string error;

	string Hex() const;
	string SegmentsTable() const;
};

//! Whether the lake has an ngram registry, in the calling transaction's
//! catalog: a lake with no index costs a query nothing.
bool LakeRegistryExists(ClientContext &context, const string &catalog_name);

//! The index rows of the lake, or of one table when `table_name` is given
//! (names match case-insensitively), as the reader's snapshot holds them;
//! empty when the lake has no registry.
vector<LakeIndex> ReadLakeRegistry(LakeReader &reader, const string &schema_name = string(),
                                   const string &table_name = string());

//! The rows of `schema_name`.`table_name` inserted after `covered_snapshot`
//! that the reader's snapshot holds, an update's new version among them, as a
//! relation of the table's columns and rowid. The change feed reads them from
//! the files committed since; once the snapshots it needs were expired, the
//! table itself is filtered on the insert snapshot every row carries.
string LakeTailSource(LakeReader &reader, const string &schema_name, const string &table_name,
                      int64_t covered_snapshot);

//! Empty when `index` still describes its table at the reader's snapshot;
//! otherwise why it does not: the table was altered or dropped after the
//! index's covered snapshot, or the row is of another format.
string LakeStaleReason(ClientContext &context, LakeReader &reader, const LakeIndex &index);

//===----------------------------------------------------------------------===//
// Maintenance: the scripts the pragmas expand to for a DuckLake table. Each
// script runs as one transaction and commits as one DuckLake snapshot. In
// auto-commit mode DuckDB expands a pragma in one transaction and runs the
// script in the next, so a script first checks again what its expansion read.
// Maintenance refuses a transaction that has already written the lake: it
// reads the lake's committed state.
//===----------------------------------------------------------------------===//

string LakeCreateIndexScript(ClientContext &context, const ResolvedTarget &target, const GramOptions &options);
string LakeRefreshScript(ClientContext &context, const ResolvedTarget &target, const string &only_column);
string LakeCompactScript(ClientContext &context, const ResolvedTarget &target, const string &only_column, bool purge);
string LakeDropIndexScript(ClientContext &context, const ResolvedTarget &target);
//! drop_ngram_index(index_ref, catalog => ...) for a DuckLake catalog.
string LakeDropIndexByReference(ClientContext &context, const string &catalog_name, const string &index_ref);
//! The SELECT ngram_index_stats returns for a DuckLake table.
string LakeIndexStatsSelect(ClientContext &context, const ResolvedTarget &target);

//! Every index of every attached DuckLake catalog, as ngram_indexes lists them.
struct ObservedIndex;
vector<ObservedIndex> ObserveLakeCatalogs(ClientContext &context);

//===----------------------------------------------------------------------===//
// Queries
//===----------------------------------------------------------------------===//

//! A literal a matching row's column contains: case-sensitively unless
//! `requires_ci`, which only a case-insensitive index can probe.
struct LakeNeedle {
	string text;
	bool requires_ci = false;
};

//! The rows of a lake table a query has to read, at the caller's snapshot.
struct LakeCandidates {
	//! The scan reads only `rowids`; otherwise it reads the whole table and
	//! `reason` says why.
	bool use_index = false;
	string reason;
	//! Sorted, distinct row ids: every row the index or the change feed
	//! proposes. Rows deleted since stay in, for the scan to drop.
	shared_ptr<const vector<int64_t>> rowids;
	//! For EXPLAIN: the index's options, the grams probed, how many row ids
	//! came from the postings and from rows inserted since the covered
	//! snapshot, and the share of row groups the candidates touch.
	string tokenizer;
	idx_t grams_probed = 0;
	idx_t posting_candidates = 0;
	idx_t tail_candidates = 0;
	double row_group_fraction = 0;
};

enum class LakeProbeMode : uint8_t {
	//! For a query's plan: candidates spread over more than
	//! ngram_lake_max_row_group_fraction of the row groups send it to a plain
	//! scan, and so does an index the reader cannot read.
	PLAN,
	//! For ngram_candidates: every candidate, and the reader's errors raised.
	CANDIDATES
};

//! The candidates for `needles` over the indexed `column_name` of `table`, or
//! why the scan reads the whole table.
LakeCandidates ProbeLakeIndex(ClientContext &context, TableCatalogEntry &table, const string &column_name,
                              const vector<LakeNeedle> &needles, LakeProbeMode mode = LakeProbeMode::PLAN);

//! A predicate on the rowid column that holds for `rowids` alone and, as a
//! pushed table filter, lets the scan skip every row group whose row ids
//! include none of them.
unique_ptr<Expression> LakeRowidFilter(const LakeCandidates &candidates, unique_ptr<Expression> rowid);

void RegisterLake(ExtensionLoader &loader);

} // namespace ngram
} // namespace duckdb
