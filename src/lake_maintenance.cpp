//===----------------------------------------------------------------------===//
// lake_maintenance.cpp: the create, refresh, compact and drop scripts of indexes on DuckLake tables, and the
// statistics and listing that report on them.
//===----------------------------------------------------------------------===//

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "ngram/build_sql.hpp"
#include "ngram/index_state.hpp"
#include "ngram/lake.hpp"

#include <algorithm>

namespace duckdb {
namespace ngram {

//! DuckLake numbers the rows a transaction inserts from here until it
//! commits; the index records only committed row ids.
constexpr int64_t LAKE_PROVISIONAL_ROWID = 1000000000000000000LL;

//! Segments-table row groups are capped by bytes, so that a common gram's
//! postings fill a row group of their own and a rare gram's lookup reads a
//! small one: on an 11.7 GB table, 1 MB row groups looked up fastest and read
//! least of the 2,048-row, 1 MB and 256 KB layouts measured.
constexpr const char *SEGMENTS_ROW_GROUP_BYTES = "1MB";
constexpr int64_t SEGMENTS_ROW_GROUP_ROWS = 100000000;

//! Rows sampled to estimate a build's grams per row.
constexpr idx_t LAKE_LENGTH_SAMPLE_ROWS = 4096;

static string RegistryTable(const string &catalog_name) {
	return Registry(catalog_name);
}

static string CatalogLit(const string &catalog_name) {
	return Lit(catalog_name);
}

//! A statement that stops the script with `message` unless `condition`
//! holds when it runs. Expansion and execution are separate transactions,
//! so everything the expansion read is checked again here.
static string CheckStatement(const string &condition, const string &message) {
	auto check = ScratchName("check");
	return "CREATE TEMP TABLE " + check + " AS SELECT CASE WHEN " + condition + " THEN true ELSE " +
	       SystemFunction("error") + "(" + Lit(message) + ") END AS ok;\nDROP TABLE " + check + ";\n";
}

//! Every maintenance call deletes the registry's lock row and inserts a new
//! one. DuckLake refuses to commit the second of two transactions that
//! delete the same row, so of two concurrent calls on one lake the later one
//! fails and can be rerun.
static string LockStatements(const string &catalog_name) {
	auto registry = RegistryTable(catalog_name);
	return "DELETE FROM " + registry + " WHERE kind = 'lock';\nINSERT INTO " + registry +
	       " (kind, lock_token) VALUES ('lock', " + Lit(UUID::ToString(UUID::GenerateRandomUUID())) + ");\n";
}

static string LockCheck(const string &catalog_name) {
	return "(SELECT " + SystemFunction("count") + "(*) FROM " + RegistryTable(catalog_name) +
	       " WHERE kind = 'lock') = 1";
}

static string IndexRowCheck(const string &catalog_name, const LakeIndex &index) {
	return "(SELECT " + SystemFunction("count") + "(*) FROM " + RegistryTable(catalog_name) +
	       " WHERE kind = 'index' AND index_id = " + Lit(index.index_ref) +
	       "::UUID AND covered_snapshot = " + to_string(index.covered_snapshot) + ") = 1";
}

static string ChangedMessage(const char *fn, const ResolvedTarget &target) {
	return string(fn) + ": the ngram registry of " + target.catalog_name +
	       " changed since this call was planned; run it again";
}

//! The DuckLake id of `table_name`, read when the script runs; DuckLake lists
//! tables by name alone, so a name two schemas share is refused.
static string TableIdExpression(const ResolvedTarget &target) {
	auto count = SystemFunction("count");
	auto prefix = "create_ngram_index: DuckLake catalog " + target.catalog_name;
	return "(SELECT CASE WHEN " + count + "(*) = 1 THEN " + SystemFunction("min") + "(table_id) WHEN " + count +
	       "(*) = 0 THEN " + SystemFunction("error") + "(" +
	       Lit(prefix + " holds no table named " + target.table_name) + ") ELSE " + SystemFunction("error") + "(" +
	       Lit(prefix + " holds several tables named " + target.table_name +
	           "; ngram indexes need a table name unique within the lake") +
	       ") END FROM " + SystemFunction("ducklake_table_info") + "(" + CatalogLit(target.catalog_name) +
	       ") WHERE table_name = " + Lit(target.table_name) + ")";
}

//! Maintenance reads the lake's committed state and writes on top of it, so
//! it runs in a transaction that has not written the lake otherwise.
static void RequireUnwrittenLake(ClientContext &context, const char *fn, const string &catalog_name) {
	if (LakeWrittenByTransaction(context, catalog_name)) {
		throw InvalidInputException("%s: this transaction has already written DuckLake catalog %s; run %s in a "
		                            "transaction of its own",
		                            fn, catalog_name, fn);
	}
}

static string CurrentSnapshotExpression(const string &catalog_name) {
	return "(SELECT id FROM " + SystemFunction("ducklake_current_snapshot") + "(" + CatalogLit(catalog_name) +
	       "))::BIGINT";
}

//! The registry row of an index, from expressions evaluated when the script
//! runs.
static string RegistryInsertStatement(const ResolvedTarget &target, const string &index_ref, const string &column_name,
                                      const GramOptions &options, const string &table_id, const string &covered,
                                      const string &schema_version) {
	return "INSERT INTO " + RegistryTable(target.catalog_name) + " SELECT 'index', " +
	       to_string(LAKE_REGISTRY_VERSION) + ", " + Lit(index_ref) + "::UUID, " + Lit(target.schema_name) + ", " +
	       Lit(target.table_name) + ", " + Lit(column_name) + ", " + table_id + ", " + to_string(LAKE_FORMAT_VERSION) +
	       ", " + Lit(TokenizerName(options.tokenizer)) + ", " + to_string(options.min_gram) + ", " +
	       to_string(options.max_gram) + ", " + (options.case_insensitive ? "true" : "false") + ", " + covered + ", " +
	       schema_version + ", NULL;\n";
}

static string DeleteIndexRow(const string &catalog_name, const LakeIndex &index) {
	return "DELETE FROM " + RegistryTable(catalog_name) +
	       " WHERE kind = 'index' AND index_id = " + Lit(index.index_ref) + "::UUID;\n";
}

static string KeyColumns(const string &gram_key) {
	return "(" + gram_key + " >> 64)::UBIGINT AS key_hi, (" + gram_key +
	       " & 18446744073709551615::UHUGEINT)::UBIGINT AS key_lo";
}

static string GramKeyExpression(const string &prefix = string()) {
	return "((" + prefix + "key_hi::UHUGEINT << 64) | " + prefix + "key_lo::UHUGEINT)";
}

//! The statements that write `packed` into the segments table as
//! `generation`. DuckLake sorts the insert by the table's sort key.
static string WritePackedStatement(const string &segments, const string &packed, const string &generation) {
	return "INSERT INTO " + segments + " SELECT " + KeyColumns("gram_key") + ", segment_no, " + generation +
	       ", postings, rowid_count, min_rowid, max_rowid FROM " + packed + ";\n";
}

static string GramKeysSelect(const string &column_name, const GramOptions &options) {
	return SystemFunction("unnest") + "(" + SystemFunction("ngram_gram_keys") + "(" + Ident(column_name) + ", " +
	       Lit(TokenizerName(options.tokenizer)) + ", " + to_string(options.min_gram) + ", " +
	       to_string(options.max_gram) + ", " + (options.case_insensitive ? "true" : "false") + ")) AS gram_key";
}

//! Segment-aligned committed row-id ranges covering [0, hi]; the last runs to
//! the provisional ids, so rows committed after the plan are packed too.
static vector<pair<int64_t, int64_t>> LakeRanges(ClientContext &context, int64_t lo, int64_t hi,
                                                 int64_t estimated_pairs) {
	auto ranges =
	    SegmentAlignedRanges(lo, MaxValue<int64_t>(hi, lo), BuildPartitionCount(context, estimated_pairs), false);
	ranges.back().second = LAKE_PROVISIONAL_ROWID - 1;
	return ranges;
}

static int64_t EstimatePairs(int64_t rows, double average_bytes, const GramOptions &options) {
	auto windows = MaxValue<double>(average_bytes - double(options.min_gram) + 1, 0);
	if (options.tokenizer == GramTokenizer::SPARSE) {
		windows *= 2;
	}
	return LossyNumericCast<int64_t>(windows * double(rows));
}

//! The pack statements over `source`, a relation of (rowid, column) pairs read
//! once per range.
static string PackLakeRanges(const string &packed, const string &source, const string &column_name,
                             const GramOptions &options, const vector<pair<int64_t, int64_t>> &ranges) {
	string script;
	for (idx_t i = 0; i < ranges.size(); i++) {
		script += PackPartitionStatement(packed, i == 0,
		                                 "SELECT rowid AS r, rowid >> " + to_string(SEGMENT_SHIFT) +
		                                     " AS segment_no, " + GramKeysSelect(column_name, options) + " FROM " +
		                                     source + " WHERE rowid >= " + to_string(ranges[i].first) +
		                                     " AND rowid <= " + to_string(ranges[i].second) + " AND " +
		                                     Ident(column_name) + " IS NOT NULL");
		script += ReleaseMemoryStatements();
	}
	return script;
}

//===----------------------------------------------------------------------===//
// Resolving the indexes a call works on
//===----------------------------------------------------------------------===//

static vector<LakeIndex> TableIndexes(LakeReader &reader, const ResolvedTarget &target) {
	return ReadLakeRegistry(reader, target.schema_name, target.table_name);
}

//! The indexes `fn` maintains: every index of the table, or the one on
//! `only_column`; each must still describe the table.
static vector<LakeIndex> MaintainedIndexes(ClientContext &context, LakeReader &reader, const char *fn,
                                           const ResolvedTarget &target, const string &only_column) {
	vector<LakeIndex> result;
	for (auto &index : TableIndexes(reader, target)) {
		if (!only_column.empty() && !StringUtil::CIEquals(index.column_name, only_column)) {
			continue;
		}
		auto reason = LakeStaleReason(context, reader, index);
		if (!reason.empty()) {
			throw InvalidInputException("%s: the ngram index on %s.%s cannot be maintained: %s; drop it and create "
			                            "it again",
			                            fn, target.table_name, index.column_name, reason);
		}
		result.push_back(std::move(index));
	}
	if (result.empty()) {
		if (only_column.empty()) {
			throw CatalogException("%s: no ngram index exists on %s", fn, target.table_name);
		}
		throw CatalogException("%s: no ngram index exists on %s.%s", fn, target.table_name, only_column);
	}
	return result;
}

//===----------------------------------------------------------------------===//
// create
//===----------------------------------------------------------------------===//

static constexpr const char *REGISTRY_COLUMNS =
    "kind VARCHAR NOT NULL, registry_version BIGINT, index_id UUID, schema_name VARCHAR, table_name VARCHAR, "
    "column_name VARCHAR, table_id BIGINT, format_version BIGINT, tokenizer VARCHAR, min_gram BIGINT, max_gram "
    "BIGINT, case_insensitive BOOLEAN, covered_snapshot BIGINT, schema_version BIGINT, lock_token VARCHAR";

//! The statements that create the lake's __ngram schema and registry, with
//! its lock row, for the first create in a lake. Of two concurrent first
//! creates DuckLake commits one: both create the schema.
static string BootstrapStatements(const string &catalog_name) {
	auto registry = RegistryTable(catalog_name);
	return "CREATE SCHEMA IF NOT EXISTS " + Ident(catalog_name) + "." + Ident(NGRAM_SCHEMA) +
	       ";\nCREATE TABLE IF NOT EXISTS " + registry + " (" + REGISTRY_COLUMNS + ");\nINSERT INTO " + registry +
	       " (kind, lock_token) SELECT 'lock', " + Lit(UUID::ToString(UUID::GenerateRandomUUID())) +
	       " WHERE NOT EXISTS (SELECT 1 FROM " + registry + " WHERE kind = 'lock');\n";
}

string LakeCreateIndexScript(ClientContext &context, const ResolvedTarget &target, const GramOptions &options) {
	RequireUnwrittenLake(context, "create_ngram_index", target.catalog_name);
	LakeReader reader(context, target.catalog_name);
	auto existing = TableIndexes(reader, target);
	for (auto &index : existing) {
		if (StringUtil::CIEquals(index.column_name, target.column_name)) {
			throw InvalidInputException("An ngram index already exists on %s.%s (%s); use drop_ngram_index first",
			                            target.table_name, target.column_name, index.index_ref);
		}
	}

	auto base = target.Qualified();
	auto column = Ident(target.column_name);
	auto at = reader.At();
	auto sizes =
	    reader.Query("SELECT " + SystemFunction("count") + "(*), coalesce(" + SystemFunction("max") +
	                     "(rowid), -1) FROM " + base + at + " WHERE rowid < " + to_string(LAKE_PROVISIONAL_ROWID),
	                 "the table's row ids");
	auto rows = sizes.GetValue(0, 0).GetValue<int64_t>();
	auto max_rowid = sizes.GetValue(1, 0).GetValue<int64_t>();
	auto lengths = reader.Query("SELECT coalesce(" + SystemFunction("avg") + "(" + SystemFunction("strlen") + "(" +
	                                column + ")), 0) FROM (SELECT " + column + " FROM " + base + at + " LIMIT " +
	                                to_string(LAKE_LENGTH_SAMPLE_ROWS) + ")",
	                            "a sample of the column");
	auto average_bytes = lengths.GetValue(0, 0).GetValue<double>();

	auto index_ref = UUID::ToString(UUID::GenerateRandomUUID());
	LakeIndex index;
	index.index_ref = index_ref;
	auto segments = StorageTable(target.catalog_name, index.SegmentsTable());
	auto registry = RegistryTable(target.catalog_name);
	auto packed = ScratchName("lake_packed");

	string script;
	if (!LakeRegistryExists(context, target.catalog_name)) {
		script += BootstrapStatements(target.catalog_name);
	}
	script += CheckStatement(LockCheck(target.catalog_name) + " AND NOT EXISTS (SELECT 1 FROM " + registry +
	                             " WHERE kind = 'index' AND lower(schema_name) = lower(" + Lit(target.schema_name) +
	                             ") AND lower(table_name) = lower(" + Lit(target.table_name) +
	                             ") AND lower(column_name) = lower(" + Lit(target.column_name) + "))",
	                         ChangedMessage("create_ngram_index", target));
	script += LockStatements(target.catalog_name);
	script += "CREATE TABLE " + segments +
	          " (key_hi UBIGINT, key_lo UBIGINT, segment_no BIGINT, generation INTEGER, postings BLOB, rowid_count "
	          "BIGINT, min_rowid BIGINT, max_rowid BIGINT) WITH (parquet_row_group_size = " +
	          to_string(SEGMENTS_ROW_GROUP_ROWS) + ", parquet_row_group_size_bytes = " + Lit(SEGMENTS_ROW_GROUP_BYTES) +
	          ");\n";
	script += "ALTER TABLE " + segments + " SET SORTED BY (key_hi, key_lo, segment_no);\n";
	script += PackLakeRanges(packed, base, target.column_name, options,
	                         LakeRanges(context, 0, max_rowid, EstimatePairs(rows, average_bytes, options)));
	script += WritePackedStatement(segments, packed, "0");
	// the index covers what the script's own transaction read. Its commit
	// raises the schema version by one, or more when DuckLake also adds
	// inlined-data tables; then queries check the snapshot history until a
	// refresh records the version
	auto current = CurrentSnapshotExpression(target.catalog_name);
	auto schema_version = "(SELECT schema_version FROM " + SystemFunction("ducklake_snapshots") + "(" +
	                      CatalogLit(target.catalog_name) + ") WHERE snapshot_id = " + current + ") + 1";
	script += RegistryInsertStatement(target, index_ref, target.column_name, options, TableIdExpression(target),
	                                  current, schema_version);
	script += "DROP TABLE " + packed + ";\n";
	return script;
}

//===----------------------------------------------------------------------===//
// refresh
//===----------------------------------------------------------------------===//

string LakeRefreshScript(ClientContext &context, const ResolvedTarget &target, const string &only_column) {
	RequireUnwrittenLake(context, "ngram_refresh", target.catalog_name);
	LakeReader reader(context, target.catalog_name);
	auto indexes = MaintainedIndexes(context, reader, "ngram_refresh", target, only_column);
	auto snapshot = reader.Snapshot();
	auto schema_version = LakeSchemaVersion(context, target.catalog_name);

	string script;
	for (auto &index : indexes) {
		script += CheckStatement(LockCheck(target.catalog_name) + " AND " + IndexRowCheck(target.catalog_name, index),
		                         ChangedMessage("ngram_refresh", target));
	}
	string body;
	for (auto &index : indexes) {
		if (index.covered_snapshot == snapshot) {
			continue;
		}
		// the change feed returns every row inserted after the covered
		// snapshot, an update's new version among them, and rows deleted
		// since too, whose postings only cost the recheck
		auto feed = LakeTailSource(reader, target.schema_name, target.table_name, index.covered_snapshot);
		auto sizes = reader.Query("SELECT " + SystemFunction("count") + "(*), coalesce(" + SystemFunction("min") +
		                              "(rowid), 0), coalesce(" + SystemFunction("max") + "(rowid), -1), coalesce(" +
		                              SystemFunction("avg") + "(" + SystemFunction("strlen") + "(" +
		                              Ident(index.column_name) + ")), 0) FROM " + feed,
		                          "the change feed");
		auto rows = sizes.GetValue(0, 0).GetValue<int64_t>();
		// with no new rows a refresh still advances the covered snapshot and
		// records the schema version: a staleness check reads the snapshot
		// history after the covered snapshot, which snapshot expiry removes
		auto segments = StorageTable(target.catalog_name, index.SegmentsTable());
		if (rows > 0) {
			auto packed = ScratchName("lake_packed");
			auto ranges = SegmentAlignedRanges(
			    sizes.GetValue(1, 0).GetValue<int64_t>(), sizes.GetValue(2, 0).GetValue<int64_t>(),
			    BuildPartitionCount(context,
			                        EstimatePairs(rows, sizes.GetValue(3, 0).GetValue<double>(), index.options)),
			    false);
			body += PackLakeRanges(packed, feed, index.column_name, index.options, ranges);
			body += WritePackedStatement(segments, packed,
			                             "(SELECT coalesce(" + SystemFunction("max") + "(generation), -1) + 1 FROM " +
			                                 segments + ")");
			body += "DROP TABLE " + packed + ";\n";
		}
		body += DeleteIndexRow(target.catalog_name, index);
		body += RegistryInsertStatement(target, index.index_ref, index.column_name, index.options,
		                                to_string(index.table_id), to_string(snapshot), to_string(schema_version));
	}
	if (!body.empty()) {
		script += LockStatements(target.catalog_name) + body;
	}
	return script;
}

//===----------------------------------------------------------------------===//
// compact
//===----------------------------------------------------------------------===//

string LakeCompactScript(ClientContext &context, const ResolvedTarget &target, const string &only_column, bool purge) {
	RequireUnwrittenLake(context, "ngram_compact", target.catalog_name);
	LakeReader reader(context, target.catalog_name);
	auto indexes = MaintainedIndexes(context, reader, "ngram_compact", target, only_column);
	auto schema_version = to_string(LakeSchemaVersion(context, target.catalog_name));
	auto base = target.Qualified();

	string script;
	for (auto &index : indexes) {
		script += CheckStatement(LockCheck(target.catalog_name) + " AND " + IndexRowCheck(target.catalog_name, index),
		                         ChangedMessage("ngram_compact", target));
	}
	string body;
	for (auto &index : indexes) {
		auto segments = StorageTable(target.catalog_name, index.SegmentsTable());
		auto shape = reader.Query("SELECT " + SystemFunction("count") + "(DISTINCT generation), coalesce(" +
		                              SystemFunction("sum") + "(rowid_count), 0), coalesce(" + SystemFunction("min") +
		                              "(segment_no), 0), coalesce(" + SystemFunction("max") +
		                              "(segment_no), -1) FROM " + segments + reader.At(),
		                          "the segments table");
		auto generations = shape.GetValue(0, 0).GetValue<int64_t>();
		if (generations <= 1 && !purge) {
			continue;
		}
		auto postings = shape.GetValue(1, 0).GetValue<int64_t>();
		auto first_segment = shape.GetValue(2, 0).GetValue<int64_t>();
		auto last_segment = shape.GetValue(3, 0).GetValue<int64_t>();
		auto packed = ScratchName("lake_packed");
		auto ranges = SegmentAlignedRanges(first_segment << SEGMENT_SHIFT, ((last_segment + 1) << SEGMENT_SHIFT) - 1,
		                                   BuildPartitionCount(context, postings), false);
		for (idx_t i = 0; i < ranges.size(); i++) {
			auto segment_lo = to_string(ranges[i].first >> SEGMENT_SHIFT);
			auto segment_hi = to_string(ranges[i].second >> SEGMENT_SHIFT);
			auto source = "SELECT gram_key, segment_no, r FROM " + SystemFunction("ngram_unpack_postings") +
			              "((SELECT " + GramKeyExpression() + " AS gram_key, segment_no, postings FROM " + segments +
			              " WHERE segment_no >= " + segment_lo + " AND segment_no <= " + segment_hi + "))";
			if (purge) {
				// a row id survives when the table still holds the row; DuckLake
				// never reuses a deleted row's id
				source += " WHERE r IN (SELECT rowid FROM " + base + " WHERE rowid >= " + to_string(ranges[i].first) +
				          " AND rowid <= " + to_string(ranges[i].second) + ")";
			}
			body += PackPartitionStatement(packed, i == 0, source);
		}
		body += "DELETE FROM " + segments + ";\n";
		body += WritePackedStatement(segments, packed, "0");
		body += "DROP TABLE " + packed + ";\n";
		body += DeleteIndexRow(target.catalog_name, index);
		body += RegistryInsertStatement(target, index.index_ref, index.column_name, index.options,
		                                to_string(index.table_id), to_string(index.covered_snapshot), schema_version);
	}
	if (!body.empty()) {
		script += LockStatements(target.catalog_name) + body;
	}
	return script;
}

//===----------------------------------------------------------------------===//
// drop
//===----------------------------------------------------------------------===//

static string DropScript(const string &catalog_name, const LakeIndex &index) {
	string script;
	script += CheckStatement(LockCheck(catalog_name) + " AND " + IndexRowCheck(catalog_name, index),
	                         "drop_ngram_index: the ngram registry of " + catalog_name +
	                             " changed since this call was planned; run it again");
	script += LockStatements(catalog_name);
	script += DeleteIndexRow(catalog_name, index);
	script += "DROP TABLE IF EXISTS " + StorageTable(catalog_name, index.SegmentsTable()) + ";\n";
	return script;
}

string LakeDropIndexScript(ClientContext &context, const ResolvedTarget &target) {
	RequireUnwrittenLake(context, "drop_ngram_index", target.catalog_name);
	LakeReader reader(context, target.catalog_name);
	for (auto &index : TableIndexes(reader, target)) {
		if (StringUtil::CIEquals(index.column_name, target.column_name)) {
			return DropScript(target.catalog_name, index);
		}
	}
	throw CatalogException("No ngram index exists on %s.%s", target.table_name, target.column_name);
}

string LakeDropIndexByReference(ClientContext &context, const string &catalog_name, const string &index_ref) {
	RequireUnwrittenLake(context, "drop_ngram_index", catalog_name);
	LakeReader reader(context, catalog_name);
	for (auto &index : ReadLakeRegistry(reader)) {
		if (index.index_ref == index_ref) {
			return DropScript(catalog_name, index);
		}
	}
	throw CatalogException("ngram: index %s does not exist in catalog %s", index_ref, catalog_name);
}

//===----------------------------------------------------------------------===//
// Statistics and listing
//===----------------------------------------------------------------------===//

string LakeIndexStatsSelect(ClientContext &context, const ResolvedTarget &target) {
	RequireUnwrittenLake(context, "ngram_index_stats", target.catalog_name);
	LakeReader reader(context, target.catalog_name);
	auto indexes = TableIndexes(reader, target);
	if (indexes.empty()) {
		throw CatalogException("No ngram indexes exist on %s", target.table_name);
	}
	std::sort(indexes.begin(), indexes.end(),
	          [](const LakeIndex &left, const LakeIndex &right) { return left.column_name < right.column_name; });
	auto count = SystemFunction("count");
	auto subquery = [](const string &select) {
		return "(SELECT " + select + ")";
	};
	string rows;
	for (auto &index : indexes) {
		auto segments = StorageTable(target.catalog_name, index.SegmentsTable());
		auto reason = LakeStaleReason(context, reader, index);
		if (!rows.empty()) {
			rows += " UNION ALL ";
		}
		// remaining_tail counts the live rows a query reads from the change
		// feed: those inserted, updated or rewritten after the covered snapshot
		rows += "SELECT m.column_name, m.tokenizer, m.min_gram, m.max_gram, m.case_insensitive, m.covered_snapshot, " +
		        CurrentSnapshotExpression(target.catalog_name) + " AS current_snapshot, " +
		        subquery(count + "(*) FROM " + target.Qualified() + " WHERE snapshot_id > m.covered_snapshot") +
		        " AS remaining_tail, " +
		        subquery(count + "(*) FROM (SELECT DISTINCT key_hi, key_lo FROM " + segments + ")") +
		        " AS distinct_grams, " + subquery(count + "(*) FROM " + segments) + " AS segments, " +
		        subquery(count + "(*) FROM (SELECT key_hi, key_lo, segment_no FROM " + segments +
		                 " GROUP BY ALL HAVING " + count + "(*) > 1)") +
		        " AS fragmented_keys, " + subquery(count + "(DISTINCT generation) FROM " + segments) +
		        " AS generations, " +
		        subquery("coalesce(" + SystemFunction("sum") + "(rowid_count), 0) FROM " + segments) +
		        " AS posting_entries, " +
		        subquery("coalesce(" + SystemFunction("sum") + "(" + SystemFunction("octet_length") +
		                 "(postings)), 0) FROM " + segments) +
		        " AS postings_bytes, " + (reason.empty() ? string("NULL::VARCHAR") : Lit(reason)) +
		        " AS stale_reason FROM " + RegistryTable(target.catalog_name) +
		        " m WHERE m.kind = 'index' AND m.index_id = " + Lit(index.index_ref) + "::UUID";
	}
	return "SELECT * FROM (" + rows + ") ORDER BY column_name";
}

vector<ObservedIndex> ObserveLakeCatalogs(ClientContext &context) {
	vector<ObservedIndex> result;
	for (auto &database : DatabaseManager::Get(context).GetDatabases(context)) {
		if (database->GetCatalog().GetCatalogType() != LAKE_CATALOG_TYPE) {
			continue;
		}
		auto catalog_name = database->GetName().GetIdentifierName();
		LakeReader reader(context, catalog_name);
		for (auto &index : ReadLakeRegistry(reader)) {
			ObservedIndex observed;
			observed.catalog_name = catalog_name;
			observed.schema_name = index.schema_name;
			observed.table_name = index.table_name;
			observed.location.index_ref = index.index_ref;
			observed.location.column_name = index.column_name;
			observed.format_version = index.format_version;
			if (!index.error.empty()) {
				observed.status = "MALFORMED";
				observed.reason = index.error;
			} else {
				observed.tokenizer = TokenizerSpec(index.options);
				observed.reason = LakeStaleReason(context, reader, index);
				observed.status = observed.reason.empty() ? "READY" : "SCAN_ONLY";
			}
			result.push_back(std::move(observed));
		}
	}
	return result;
}

} // namespace ngram
} // namespace duckdb
