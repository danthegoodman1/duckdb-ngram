#include "ngram/build_sql.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "ngram/fence.hpp"
#include "ngram/index_state.hpp"
#include "ngram/rowid_guard.hpp"
#include "ngram/settings.hpp"

namespace duckdb {
namespace ngram {

//===----------------------------------------------------------------------===//
// Partitioned packing (see ngram/build_sql.hpp for the design)
//===----------------------------------------------------------------------===//

//! Aggregate-state bytes one pair costs while its partition is being grouped:
//! eight for the rowid plus block slack and the copy each thread keeps of a
//! group before the hash tables are combined. Measured at ~28 B/pair (26.4 GB
//! peak RSS grouping 966 M pairs on 24 threads); rounded up for headroom.
constexpr int64_t PAIR_STATE_BYTES = 32;

//! Share of `memory_limit` the grouping pass may claim. The rest goes to the
//! scan and unnest feeding it, the hash table itself, and the packed output.
constexpr double PARTITION_MEMORY_FRACTION = 0.5;

//! Assumed memory budget when the session has no limit configured.
constexpr int64_t DEFAULT_PARTITION_BUDGET_BYTES = 8LL * 1024 * 1024 * 1024;

//! Rows read to estimate the average gram yield of a rowid range.
constexpr idx_t GRAM_ESTIMATE_SAMPLES = 512;

//! An index built on a machine where the estimate is far too low still has to
//! terminate: past this many partitions the estimate is not to be trusted and
//! the memory limit is simply too small for the corpus.
constexpr idx_t MAX_BUILD_PARTITIONS = 4096;

vector<pair<int64_t, int64_t>> SegmentAlignedRanges(int64_t lo, int64_t hi, idx_t partitions, bool open_ended) {
	vector<pair<int64_t, int64_t>> result;
	// the open end keeps the pair stream in step with the high-water mark the
	// script records: both cover everything committed when the script runs. A
	// bounded refresh closes it at hi and records a mark no higher.
	auto open_end = open_ended ? MAX_ROW_ID - 1 : hi;
	if (hi < lo || partitions <= 1) {
		result.emplace_back(lo, open_end);
		return result;
	}
	auto first_segment = lo >> SEGMENT_SHIFT;
	auto last_segment = hi >> SEGMENT_SHIFT;
	auto segments = last_segment - first_segment + 1;
	// rounded down, so a request that does not divide the segment count evenly
	// yields more and smaller partitions rather than fewer and larger ones: one
	// segment is the finest partition there is, and overshooting the memory
	// budget is the only failure mode worth avoiding
	auto per_partition = MaxValue<int64_t>(segments / NumericCast<int64_t>(partitions), 1);
	for (int64_t segment = first_segment; segment <= last_segment; segment += per_partition) {
		auto start = segment == first_segment ? lo : segment << SEGMENT_SHIFT;
		auto end_segment = segment + per_partition - 1;
		auto end = end_segment >= last_segment ? open_end : ((end_segment + 1) << SEGMENT_SHIFT) - 1;
		result.emplace_back(start, end);
	}
	return result;
}

string PackPartitionStatement(const string &packed, bool first, const string &pair_source) {
	string statement = first ? "CREATE TEMP TABLE " + packed + " AS " : "INSERT INTO " + packed + " ";
	return statement + "SELECT " + SystemFunction("decode") + "(gram_key) AS gram, segment_no, " +
	       SystemFunction("struct_extract") + "(segment, 'postings') AS postings, " + SystemFunction("struct_extract") +
	       "(segment, 'rowid_count') AS rowid_count, " + SystemFunction("struct_extract") +
	       "(segment, 'min_rowid') AS min_rowid, " + SystemFunction("struct_extract") +
	       "(segment, 'max_rowid') AS max_rowid FROM (" + "SELECT " + SystemFunction("encode") +
	       "(gram) AS gram_key, segment_no, " + SystemFunction("ngram_pack_segment") + "(r) AS segment FROM (" +
	       pair_source + ") GROUP BY gram_key, segment_no);\n";
}

string PackRangesStatements(const string &packed, const ResolvedTarget &target, const string &column_name,
                            const GramOptions &options, const vector<pair<int64_t, int64_t>> &ranges) {
	auto column = Ident(column_name);
	auto grams = SystemFunction("unnest") + "(" + SystemFunction("trigrams") + "(" + column + ", " +
	             to_string(options.gram_size) + ", " + (options.case_insensitive ? "true" : "false") + ")) AS gram";
	string script;
	for (idx_t i = 0; i < ranges.size(); i++) {
		script += PackPartitionStatement(
		    packed, i == 0,
		    "SELECT rowid AS r, rowid >> " + to_string(SEGMENT_SHIFT) + " AS segment_no, " + grams + " FROM " +
		        target.Qualified() + " WHERE rowid >= " + to_string(ranges[i].first) +
		        " AND rowid <= " + to_string(ranges[i].second) + " AND " + column + " IS NOT NULL");
	}
	return script;
}

idx_t BuildPartitionCount(ClientContext &context, int64_t estimated_pairs) {
	auto configured = ConfiguredBuildPartitions(context);
	if (configured > 0) {
		return MinValue<idx_t>(configured, MAX_BUILD_PARTITIONS);
	}
	auto limit = NumericCast<int64_t>(DBConfig::GetConfig(context).options.maximum_memory);
	if (limit <= 0) {
		limit = DEFAULT_PARTITION_BUDGET_BYTES;
	}
	auto pairs_per_partition =
	    MaxValue<int64_t>(LossyNumericCast<int64_t>(double(limit) * PARTITION_MEMORY_FRACTION) / PAIR_STATE_BYTES, 1);
	auto pairs = MaxValue<int64_t>(estimated_pairs, 0);
	auto partitions = pairs / pairs_per_partition + (pairs % pairs_per_partition != 0);
	return NumericCast<idx_t>(MinValue<int64_t>(MaxValue<int64_t>(partitions, 1), MAX_BUILD_PARTITIONS));
}

int64_t EstimateGramCount(ClientContext &context, TableCatalogEntry &table, const string &column, int64_t min_rowid,
                          int64_t max_rowid, idx_t gram_size) {
	if (min_rowid < 0 || max_rowid < min_rowid || !table.ColumnExists(column)) {
		return 0;
	}
	auto rowid_span = max_rowid - min_rowid + 1;
	auto samples = NumericCast<idx_t>(MinValue<int64_t>(NumericCast<int64_t>(GRAM_ESTIMATE_SAMPLES), rowid_span));
	auto &duck_table = table.Cast<DuckTableEntry>();
	auto &column_def = table.GetColumn(column);
	auto &transaction = DuckTransaction::Get(context, table.ParentCatalog());
	vector<StorageIndex> column_ids {duck_table.GetStorageIndex(ColumnIndex(column_def.Logical().index))};

	Vector row_ids(LogicalType::ROW_TYPE, samples);
	auto ids = FlatVector::GetData<row_t>(row_ids);
	for (idx_t i = 0; i < samples; i++) {
		auto offset = samples == 1 ? 0 : (rowid_span - 1) * NumericCast<int64_t>(i) / NumericCast<int64_t>(samples - 1);
		ids[i] = NumericCast<row_t>(min_rowid + offset);
	}
	DataChunk chunk;
	chunk.Initialize(Allocator::Get(context), vector<LogicalType> {column_def.Type()});
	ColumnFetchState fetch_state;
	duck_table.GetStorage().Fetch(transaction, chunk, column_ids, row_ids, samples, fetch_state);
	if (chunk.size() == 0) {
		return 0;
	}
	int64_t sampled_grams = 0;
	for (idx_t r = 0; r < chunk.size(); r++) {
		auto value = chunk.GetValue(0, r);
		if (value.IsNull()) {
			continue;
		}
		// byte length rather than character length: a row of multi-byte text
		// yields fewer grams than it has bytes, and overestimating only costs
		// partitions
		auto length = NumericCast<int64_t>(StringValue::Get(value).size());
		sampled_grams += MaxValue<int64_t>(length - NumericCast<int64_t>(gram_size) + 1, 0);
	}
	// rows deleted since they were written drop out of the fetch but still
	// count in the span, which biases the estimate upwards again
	return LossyNumericCast<int64_t>(double(sampled_grams) / double(chunk.size()) * double(rowid_span));
}

//! One rowid guard per table. The first index creates it behind the barrier,
//! covering every VARCHAR the table has at that moment, and records a fresh
//! name with an empty token for the barrier to fill in; later indexes record
//! the sibling indexes' guard and prove it covers their column.
static void ResolveCreateGuard(ClientContext &context, DuckTableEntry &table, const ResolvedTarget &target,
                               const vector<IndexLocation> &siblings, IndexLocation &location) {
	location.guard_name = GUARD_PREFIX + location.Hex();
	if (siblings.empty()) {
		return;
	}
	location.guard_name = siblings[0].guard_name;
	location.guard_token = siblings[0].guard_token;
	for (auto &sibling : siblings) {
		if (sibling.guard_name != location.guard_name || sibling.guard_token != location.guard_token) {
			throw InvalidInputException("create_ngram_index: the ngram indexes on %s record different rowid "
			                            "guards; drop them by id and rebuild them",
			                            target.table_name);
		}
	}
	MetaInfo shared;
	shared.column_name = location.column_name;
	shared.guard_name = location.guard_name;
	shared.guard_token = location.guard_token;
	auto reason = RowIdGuardReason(context, table, shared);
	if (!reason.empty()) {
		throw InvalidInputException("create_ngram_index: the rowid guard shared by the ngram indexes on %s cannot "
		                            "cover %s (%s); drop the table's ngram indexes and rebuild them",
		                            target.table_name, location.column_name, reason);
	}
}

//! The statements that install a table's first rowid guard behind the
//! creation barrier and record its token in the fence table.
static string FreshGuardStatements(ClientContext &context, DuckTableEntry &table, const ResolvedTarget &target,
                                   const IndexLocation &location, const string &fence) {
	auto base = target.Qualified();
	string script;
	// No registry row names a guard on this table, so every guard carrying the
	// generated prefix is a leftover of concurrent drops of the last two indexes
	// (each counted the other's row) and would block the DROP COLUMN below. The
	// preceding maintenance check fails the create if a sibling row appeared.
	for (auto &leftover : PrefixedGuardNames(context, table)) {
		script += "DROP INDEX IF EXISTS " + Ident(target.catalog_name) + "." + Ident(target.schema_name) + "." +
		          Ident(leftover) + ";\n";
	}
	// Replacing the physical table invalidates every snapshot that predates the
	// rowid guard. The temporary all-NULL column is dropped immediately; only
	// DuckDB's reservoir sample is intentionally discarded by this pair. ALTER
	// verification in DuckDB v1.5.5 does not reparse a quoted column name
	// containing '-', so the generated identifier stays unquoted-safe.
	auto epoch_name = "__ngram_epoch_" + location.Hex();
	script += "ALTER TABLE " + base + " ADD COLUMN " + Ident(epoch_name) + " BOOLEAN;\n";
	script += "ALTER TABLE " + base + " DROP COLUMN " + Ident(epoch_name) + ";\n";
	// This custom index has no postings and its build plan never scans the
	// base table. Its physical column dependency rewrites future updates of
	// every covered column into delete+insert, while its non-ART type disables
	// rowid-moving vacuum. Two persisted scalars detect reuse of a truncated
	// trailing range.
	string guard_columns;
	for (auto &definition : table.GetColumns().Physical()) {
		if (definition.Type().id() != LogicalTypeId::VARCHAR) {
			continue;
		}
		if (!guard_columns.empty()) {
			guard_columns += ", ";
		}
		guard_columns += Ident(definition.Name());
	}
	script += "CREATE INDEX " + Ident(location.guard_name) + " ON " + base + " USING " + NGRAM_ROWID_GUARD_TYPE + "(" +
	          guard_columns + ");\n";
	// Retain EXCLUSIVE through this scan-free CREATE. This one scalar proves
	// the fresh physical guard, captures its internal token, and releases the
	// fence before the postings build begins.
	script += "UPDATE " + fence + " SET guard_token = " + SystemFunction(NGRAM_CREATION_FINISH) + "(" +
	          Lit(target.catalog_name) + ", " + Lit(target.schema_name) + ", " + Lit(target.table_name) + ", " +
	          Lit(location.column_name) + ", " + Lit(location.guard_name) + ");\n";
	return script;
}

//! The registry row of a new index. Only committed rows are indexed: a
//! transaction-local rowid is reassigned at commit, so recording one would
//! leave the index pointing at a rowid that never exists. Uncommitted rows are
//! found by the tail scan instead, and land past the high-water mark when
//! they commit.
static string RegistryInsertStatement(const ResolvedTarget &target, const IndexLocation &location,
                                      const GramOptions &options, const string &fence) {
	auto committed_hwm = "(SELECT coalesce(" + SystemFunction("max") + "(rowid), -1) FROM " + target.Qualified() +
	                     " WHERE rowid < " + to_string(MAX_ROW_ID) + ")";
	return "INSERT INTO " + Registry(target.catalog_name) + " VALUES (" + to_string(REGISTRY_VERSION) + ", " +
	       Lit(location.index_ref) + "::UUID, " + SystemFunction("from_hex") + "(" +
	       Lit(Hex(OwnerKey(target.schema_name, target.table_name, location.column_name))) + "), " +
	       Lit(target.schema_name) + ", " + Lit(target.table_name) + ", " + Lit(location.column_name) + ", " +
	       to_string(NGRAM_FORMAT_VERSION) + ", " + to_string(options.gram_size) + ", " +
	       (options.case_insensitive ? "true" : "false") + ", " + committed_hwm + ", " + Lit(location.guard_name) +
	       ", (SELECT guard_token FROM " + fence + "));\n";
}

string CreateIndexScript(ClientContext &context, const ResolvedTarget &target, const GramOptions &options) {
	optional_ptr<TableCatalogEntry> entry = target.entry;
	auto &table = entry->Cast<DuckTableEntry>();
	auto registry = ReadRegistryForCreate(context, target.catalog_name);
	auto table_target = target;
	table_target.column_name.clear();
	auto siblings = Locations(registry, table_target, false);
	for (auto &sibling : siblings) {
		if (StringUtil::CIEquals(sibling.column_name, target.column_name)) {
			throw InvalidInputException("An ngram index already exists on %s.%s (%s); use drop_ngram_index first",
			                            target.table_name, target.column_name, sibling.index_ref);
		}
	}
	IndexLocation location;
	location.index_ref = UUID::ToString(UUID::GenerateRandomUUID());
	// generated names and the registry row use the catalog's spelling
	location.column_name = target.column_name;
	ResolveCreateGuard(context, table, target, siblings, location);

	auto segments = StorageTable(target.catalog_name, location.SegmentsTable());
	auto stats = StorageTable(target.catalog_name, location.StatsTable());
	auto total_rows = TableTotalRows(table);

	// No BEGIN/COMMIT here: the statement preprocessor wraps a multi-statement
	// pragma expansion in a transaction itself. The build runs through DuckDB's
	// engine, so it is parallel and can spill: one statement per rowid-range
	// partition groups that partition's (gram, segment_no, rowid) pairs into
	// segment rows with ngram_pack_segment, the partitions land in a temp table,
	// and that table is written to the segments table in gram order. Segments
	// are bucketed by rowid range, which lets a rowid-range partition hold whole
	// keys; duplicate (gram, rowid) pairs survive until the codec dedupes.
	string script;
	string fence = ScratchName("fence");
	string packed = ScratchName("build_packed");
	// This is the first executed statement. The volatile scalar acquires the
	// target transaction's vacuum fence before checking that the table planned
	// above is still the table the remaining script will scan.
	PreparedMaintenance prepared;
	prepared.kind = PreparedMaintenance::Kind::CREATE;
	prepared.fn = "create_ngram_index";
	prepared.target = target;
	prepared.location = location;
	prepared.location.registry_oid = registry.oid;
	prepared.bootstrap = !registry.oid;
	script += "CREATE TEMP TABLE " + fence + " AS SELECT " +
	          PreparedMaintenanceCall(context, NewMaintenanceGroup(context), std::move(prepared)) + " AS ignored, " +
	          (siblings.empty() ? string("NULL::VARCHAR") : Lit(location.guard_token)) + " AS guard_token;\n";
	if (siblings.empty()) {
		script += FreshGuardStatements(context, table, target, location, fence);
	}
	if (!registry.oid) {
		script += "CREATE SCHEMA IF NOT EXISTS " + Ident(target.catalog_name) + "." + Ident(NGRAM_SCHEMA) + ";\n";
		script += "CREATE TABLE " + Registry(target.catalog_name) +
		          "(registry_version INTEGER NOT NULL, index_id UUID PRIMARY KEY, owner_key BLOB UNIQUE NOT NULL, "
		          "schema_name VARCHAR NOT NULL, table_name VARCHAR NOT NULL, column_name VARCHAR NOT NULL, "
		          "format_version BIGINT NOT NULL, gram_size BIGINT NOT NULL, case_insensitive BOOLEAN NOT NULL, "
		          "hwm_rowid BIGINT NOT NULL, guard_name VARCHAR NOT NULL, guard_token VARCHAR NOT NULL);\n";
	}
	script += RegistryInsertStatement(target, location, options, fence);
	auto partitions = BuildPartitionCount(
	    context, EstimateGramCount(context, table, location.column_name, 0, total_rows - 1, options.gram_size));
	script += PackRangesStatements(packed, target, location.column_name, options,
	                               SegmentAlignedRanges(0, total_rows - 1, partitions));
	// gram order is what makes the probe's `gram = ?` filter prune row groups by
	// zone map, so the segments table is written sorted even though the
	// partitions produced their rows in rowid order
	script += "CREATE TABLE " + segments +
	          " AS "
	          "SELECT gram, segment_no, 0 AS generation, postings, rowid_count, min_rowid, max_rowid FROM " +
	          packed + " ORDER BY " + SystemFunction("encode") + "(gram), segment_no;\n";
	script += "CREATE TABLE " + stats +
	          " AS "
	          "SELECT " +
	          SystemFunction("decode") + "(gram_key) AS gram, " + SystemFunction("sum") +
	          "(rowid_count)::BIGINT AS row_count, " + SystemFunction("count") + "(*)::BIGINT AS segment_count FROM " +
	          "(SELECT " + SystemFunction("encode") + "(gram) AS gram_key, rowid_count FROM " + segments +
	          ") GROUP BY gram_key ORDER BY gram_key;\n";
	script += "DROP TABLE " + packed + ";\n";
	script += "DROP TABLE " + fence + ";\n";
	return script;
}

string DropIndexScript(ClientContext &context, const ObservedIndex &index) {
	auto &index_ref = index.location.index_ref;
	ResolvedTarget owner {index.catalog_name, index.schema_name, index.table_name, index.location.column_name, nullptr};
	auto table_target = owner;
	table_target.column_name.clear();
	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, index.table_name);
	auto base = Catalog::GetEntry(context, index.catalog_name, index.schema_name, lookup, OnEntryNotFound::RETURN_NULL);
	auto base_exists = base && base->type == CatalogType::TABLE_ENTRY && base->Cast<TableCatalogEntry>().IsDuckTable();

	auto guard_name = index.location.guard_name;
	auto guard_token = index.location.guard_token;
	vector<string> storage;
	if (index.legacy) {
		// Registry version 1 (format 3) kept meta, segments and stats in a schema
		// named by the index id and gave every index its own guard. This is the
		// only place that layout is known.
		auto hex = StringUtil::Replace(index_ref, "-", "_");
		auto schema = Ident(index.catalog_name) + "." + Ident("__ngram_idx_" + hex);
		guard_name = "__ngram_rowid_guard_" + hex;
		guard_token = LegacyGuardToken(context, index.catalog_name, "__ngram_idx_" + hex);
		for (auto part : {"meta", "segments", "stats"}) {
			storage.push_back("DROP TABLE " + schema + "." + part + ";\n");
		}
		storage.push_back("DROP SCHEMA " + schema + ";\n");
		if (ReadRegistry(context, index.catalog_name).rows.size() == 1) {
			storage.push_back("DROP TABLE " + Registry(index.catalog_name) + ";\n");
			storage.push_back("DROP SCHEMA " + Ident(index.catalog_name) + "." + Ident(NGRAM_SCHEMA) + ";\n");
		}
	} else {
		storage.push_back("DROP TABLE IF EXISTS " + StorageTable(index.catalog_name, index.location.SegmentsTable()) +
		                  ";\n");
		storage.push_back("DROP TABLE IF EXISTS " + StorageTable(index.catalog_name, index.location.StatsTable()) +
		                  ";\n");
	}
	// The guard goes with the last index that records it, once the exact
	// incarnation (name, type, table, token) is proven at execution time. An
	// absent guard is fine; a same-named guard of another incarnation blocks.
	auto drop_guard = base_exists && !guard_name.empty() &&
	                  (index.legacy || OtherGuardReferences(context, table_target, guard_name, index_ref) == 0);

	string script;
	auto fence = ScratchName("fence");
	PreparedMaintenance prepared;
	prepared.kind = PreparedMaintenance::Kind::DROP;
	prepared.fn = "drop_ngram_index_by_id";
	prepared.target = owner;
	prepared.location = index.location;
	prepared.location.guard_name = guard_name;
	prepared.location.guard_token = guard_token;
	prepared.drop_guard = drop_guard;
	script += "CREATE TEMP TABLE " + fence + " AS SELECT " +
	          PreparedMaintenanceCall(context, NewMaintenanceGroup(context), std::move(prepared)) + " AS ignored;\n";
	if (drop_guard) {
		script += "DROP INDEX IF EXISTS " + Ident(index.catalog_name) + "." + Ident(index.schema_name) + "." +
		          Ident(guard_name) + ";\n";
	}
	script += "DELETE FROM " + Registry(index.catalog_name) + " WHERE index_id = " + Lit(index_ref) + "::UUID;\n";
	for (auto &statement : storage) {
		script += statement;
	}
	script += "DROP TABLE " + fence + ";\n";
	return script;
}

} // namespace ngram
} // namespace duckdb
