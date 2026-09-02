#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/catalog_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "ngram/index_pragmas.hpp"
#include "ngram/maintenance.hpp"
#include "ngram/rowid_guard.hpp"
#include "ngram/search_core.hpp"

#include <algorithm>
#include <array>

namespace duckdb {
namespace ngram {

static constexpr const char *REGISTRY_TABLE = "registry";
static constexpr int32_t REGISTRY_VERSION = 2;
static constexpr const char *GUARD_PREFIX = "__ngram_guard_";

//! One registry row: the index's identity, the metadata every read path
//! validates, and the reason the row is unusable when it is.
struct RegistryRow {
	string index_ref, owner_key, schema_name, table_name, column_name;
	int64_t format_version = -1;
	MetaInfo meta;
	string error;
};

struct RegistrySnapshot {
	idx_t oid = 0;
	//! Registry version 1 (format 3) had only the six identity columns. Its
	//! rows are listed and drop-only; nothing else reads that layout.
	bool legacy_shape = false;
	vector<RegistryRow> rows;
};

static string OwnerKey(const string &schema, const string &table, const string &column) {
	string result;
	result.reserve(schema.size() + table.size() + column.size() + 24);
	for (auto &part : {schema, table, column}) {
		auto length = NumericCast<uint64_t>(part.size());
		for (idx_t shift = 0; shift < 8; shift++) {
			result.push_back(static_cast<char>(static_cast<unsigned char>(length >> ((7 - shift) * 8))));
		}
		result += StringUtil::Lower(part);
	}
	return result;
}

static string Hex(const string &input) {
	static constexpr char DIGITS[] = "0123456789abcdef";
	string result;
	result.reserve(input.size() * 2);
	for (auto byte : input) {
		auto value = static_cast<unsigned char>(byte);
		result.push_back(DIGITS[value >> 4]);
		result.push_back(DIGITS[value & 15]);
	}
	return result;
}

static bool IsCanonicalUUID(const string &value) {
	if (value.size() != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-') {
		return false;
	}
	try {
		return value[14] == '4' && (value[19] == '8' || value[19] == '9' || value[19] == 'a' || value[19] == 'b') &&
		       Value::UUID(value).ToString() == value;
	} catch (...) {
		return false;
	}
}

string Ident(const string &name) {
	return KeywordHelper::WriteOptionallyQuoted(name);
}

string Lit(const string &value) {
	return KeywordHelper::WriteQuoted(value);
}

string SystemFunction(const string &name) {
	return Ident("system") + "." + Ident("main") + "." + Ident(name);
}

static string StorageTable(const string &catalog_name, const string &table) {
	return Ident(catalog_name) + "." + Ident(NGRAM_SCHEMA) + "." + Ident(table);
}

static string Registry(const string &catalog_name) {
	return StorageTable(catalog_name, REGISTRY_TABLE);
}

int64_t TableTotalRows(TableCatalogEntry &table) {
	return NumericCast<int64_t>(table.Cast<DuckTableEntry>().GetStorage().GetTotalRows());
}

static const array<const char *, 12> REGISTRY_COLUMNS = {
    "registry_version", "index_id",  "owner_key",        "schema_name", "table_name", "column_name",
    "format_version",   "gram_size", "case_insensitive", "hwm_rowid",   "guard_name", "guard_token"};
static const array<LogicalType, 12> REGISTRY_TYPES = {LogicalType::INTEGER, LogicalType::UUID,    LogicalType::BLOB,
                                                      LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
                                                      LogicalType::BIGINT,  LogicalType::BIGINT,  LogicalType::BOOLEAN,
                                                      LogicalType::BIGINT,  LogicalType::VARCHAR, LogicalType::VARCHAR};
static constexpr idx_t IDENTITY_COLUMNS = 6;

static RegistrySnapshot ReadRegistry(ClientContext &context, const string &catalog_name) {
	RegistrySnapshot result;
	// EntryLookupInfo stores the name by reference.
	string registry_table = REGISTRY_TABLE;
	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, registry_table);
	auto entry = Catalog::GetEntry(context, catalog_name, NGRAM_SCHEMA, lookup, OnEntryNotFound::RETURN_NULL);
	if (!entry) {
		return result;
	}
	if (entry->name != REGISTRY_TABLE || entry->ParentSchema().name != NGRAM_SCHEMA ||
	    entry->type != CatalogType::TABLE_ENTRY || !entry->Cast<TableCatalogEntry>().IsDuckTable()) {
		throw InvalidInputException("ngram: registry is not an ordinary DuckDB table");
	}
	auto &table = entry->Cast<DuckTableEntry>();
	auto columns = table.GetColumns().Logical();
	result.legacy_shape = columns.Size() == IDENTITY_COLUMNS;
	if (columns.Size() != REGISTRY_COLUMNS.size() && !result.legacy_shape) {
		throw InvalidInputException("ngram: registry has %llu columns; expected %llu", columns.Size(),
		                            REGISTRY_COLUMNS.size());
	}
	vector<StorageIndex> column_ids;
	vector<LogicalType> types;
	idx_t i = 0;
	for (auto &column : columns) {
		if (column.Name() != REGISTRY_COLUMNS[i] || column.Type() != REGISTRY_TYPES[i] || column.Generated()) {
			throw InvalidInputException("ngram: registry column %llu has the wrong name or type", i + 1);
		}
		column_ids.push_back(table.GetStorageIndex(ColumnIndex(column.Logical().index)));
		types.push_back(column.Type());
		i++;
	}
	result.oid = table.oid;
	auto &transaction = DuckTransaction::Get(context, table.ParentCatalog());
	TableScanState state;
	InitializeExhaustiveScan(context, transaction, table.GetStorage(), state, column_ids, nullptr);
	DataChunk chunk;
	chunk.Initialize(Allocator::Get(context), types);
	while (true) {
		if (context.interrupted.load(std::memory_order_relaxed)) {
			throw InterruptException();
		}
		chunk.Reset();
		table.GetStorage().Scan(transaction, chunk, state);
		if (chunk.size() == 0) {
			break;
		}
		for (idx_t r = 0; r < chunk.size(); r++) {
			for (idx_t c = 0; c < chunk.ColumnCount(); c++) {
				if (chunk.GetValue(c, r).IsNull()) {
					throw InvalidInputException("ngram: registry contains NULLs");
				}
			}
			auto version = chunk.GetValue(0, r).GetValue<int32_t>();
			RegistryRow row;
			row.index_ref = UUID::ToString(chunk.GetValue(1, r).GetValue<hugeint_t>());
			row.owner_key = StringValue::Get(chunk.GetValue(2, r));
			row.schema_name = StringValue::Get(chunk.GetValue(3, r));
			row.table_name = StringValue::Get(chunk.GetValue(4, r));
			row.column_name = StringValue::Get(chunk.GetValue(5, r));
			row.meta.column_name = row.column_name;
			if (!IsCanonicalUUID(row.index_ref)) {
				throw InvalidInputException("ngram: registry row has a noncanonical ID");
			}
			if (result.legacy_shape) {
				row.format_version = 3;
				row.error =
				    StringUtil::Format("index format 3 (registry version %d) predates format %lld; drop it with "
				                       "drop_ngram_index_by_id and rebuild it",
				                       version, NGRAM_FORMAT_VERSION);
				result.rows.push_back(std::move(row));
				continue;
			}
			row.format_version = chunk.GetValue(6, r).GetValue<int64_t>();
			auto gram_size = chunk.GetValue(7, r).GetValue<int64_t>();
			row.meta.options.case_insensitive = chunk.GetValue(8, r).GetValue<bool>();
			row.meta.hwm_rowid = chunk.GetValue(9, r).GetValue<int64_t>();
			row.meta.guard_name = StringValue::Get(chunk.GetValue(10, r));
			row.meta.guard_token = StringValue::Get(chunk.GetValue(11, r));
			if (version != REGISTRY_VERSION) {
				row.error = StringUtil::Format("unsupported registry row version %d", version);
			} else if (row.owner_key != OwnerKey(row.schema_name, row.table_name, row.column_name)) {
				row.error = "registry row owner key does not match its owner";
			} else if (row.format_version != NGRAM_FORMAT_VERSION) {
				row.error = StringUtil::Format("index format %lld is not readable by this extension, which uses format "
				                               "%lld; drop it with drop_ngram_index_by_id and rebuild it",
				                               row.format_version, NGRAM_FORMAT_VERSION);
			} else if (gram_size < 1) {
				row.error = StringUtil::Format("registry row records gram_size %lld", gram_size);
			} else if (row.meta.hwm_rowid < -1 || row.meta.hwm_rowid >= LOCAL_ROWID_START) {
				row.error = StringUtil::Format("registry row records hwm_rowid %lld", row.meta.hwm_rowid);
			} else {
				row.meta.options.gram_size = NumericCast<idx_t>(gram_size);
			}
			result.rows.push_back(std::move(row));
		}
	}
	return result;
}

static bool RowBelongsTo(const RegistryRow &row, const ResolvedTarget &target) {
	if (!target.column_name.empty()) {
		return row.owner_key == OwnerKey(target.schema_name, target.table_name, target.column_name);
	}
	return StringUtil::CIEquals(row.schema_name, target.schema_name) &&
	       StringUtil::CIEquals(row.table_name, target.table_name);
}

static IndexLocation LocationOf(const RegistryRow &row, idx_t registry_oid) {
	IndexLocation location;
	location.index_ref = row.index_ref;
	location.column_name = row.column_name;
	location.registry_oid = registry_oid;
	location.guard_name = row.meta.guard_name;
	location.guard_token = row.meta.guard_token;
	return location;
}

static vector<IndexLocation> Locations(const RegistrySnapshot &registry, const ResolvedTarget &target, bool lenient) {
	vector<IndexLocation> result;
	for (auto &row : registry.rows) {
		if (!RowBelongsTo(row, target)) {
			continue;
		}
		if (!row.error.empty()) {
			if (lenient) {
				continue;
			}
			throw InvalidInputException("ngram: the index on %s.%s (%s) is unusable: %s", target.table_name,
			                            row.column_name, row.index_ref, row.error);
		}
		result.push_back(LocationOf(row, registry.oid));
	}
	return result;
}

vector<IndexLocation> ExistingIndexes(ClientContext &context, const ResolvedTarget &target, bool lenient) {
	RegistrySnapshot registry;
	try {
		registry = ReadRegistry(context, target.catalog_name);
	} catch (CatalogException &) {
		if (lenient) {
			return {};
		}
		throw;
	} catch (InvalidInputException &) {
		if (lenient) {
			return {};
		}
		throw;
	}
	return Locations(registry, target, lenient);
}

void RequireUniqueIndexColumns(const vector<IndexLocation> &indexes) {
	unordered_set<string> seen;
	for (auto &index : indexes) {
		if (!seen.insert(StringUtil::Lower(index.column_name)).second) {
			throw InvalidInputException("ngram: multiple allocations claim column %s", index.column_name);
		}
	}
}

idx_t OtherGuardReferences(ClientContext &context, const ResolvedTarget &target, const string &guard_name,
                           const string &except_ref) {
	idx_t references = 0;
	for (auto &row : ReadRegistry(context, target.catalog_name).rows) {
		references += row.index_ref != except_ref && StringUtil::CIEquals(row.schema_name, target.schema_name) &&
		              StringUtil::CIEquals(row.table_name, target.table_name) && row.meta.guard_name == guard_name;
	}
	return references;
}

//! The NGRAM_ROWID_GUARD indexes on `table` whose names carry the generated
//! guard prefix, in the calling transaction's catalog snapshot.
static vector<string> PrefixedGuardNames(ClientContext &context, DuckTableEntry &table) {
	vector<string> names;
	auto info = table.GetStorage().GetDataTableInfo().get();
	table.ParentSchema().Scan(context, CatalogType::INDEX_ENTRY, [&](CatalogEntry &entry) {
		auto &index = entry.Cast<DuckIndexEntry>();
		if (index.index_type == NGRAM_ROWID_GUARD_TYPE && StringUtil::StartsWith(index.name, GUARD_PREFIX) &&
		    index.info && index.info->info.get() == info) {
			names.push_back(index.name);
		}
	});
	std::sort(names.begin(), names.end());
	return names;
}

bool IndexLocationAvailable(ClientContext &context, const ResolvedTarget &target, const IndexLocation &location,
                            bool changed_is_absent) {
	// Every identity change below is a plan that outlived the row it was bound
	// to. Maintenance and candidate callers must not act on the replacement, so
	// they raise; the exhaustive read paths scan instead.
	auto replaced = [&](const string &message) -> bool {
		if (changed_is_absent) {
			return false;
		}
		throw InvalidInputException(message);
	};
	RegistrySnapshot registry;
	try {
		registry = ReadRegistry(context, target.catalog_name);
	} catch (CatalogException &ex) {
		return replaced(ex.what());
	} catch (InvalidInputException &ex) {
		return replaced(ex.what());
	}
	if (!registry.oid) {
		return false;
	}
	if (registry.oid != location.registry_oid) {
		return replaced("ngram: registry changed after the index operation was prepared");
	}
	for (auto &row : registry.rows) {
		if (row.index_ref != location.index_ref) {
			continue;
		}
		if (!StringUtil::CIEquals(row.schema_name, target.schema_name) ||
		    !StringUtil::CIEquals(row.table_name, target.table_name) ||
		    !StringUtil::CIEquals(row.column_name, location.column_name)) {
			return replaced(StringUtil::Format("ngram: registry row %s changed after the index operation was prepared",
			                                   location.index_ref));
		}
		return true;
	}
	return false;
}

MetaInfo ReadMeta(ClientContext &context, const string &catalog_name, const IndexLocation &location) {
	for (auto &row : ReadRegistry(context, catalog_name).rows) {
		if (row.index_ref != location.index_ref) {
			continue;
		}
		if (!row.error.empty()) {
			throw InvalidInputException("ngram: the index on %s.%s (%s) is unusable: %s", row.table_name,
			                            row.column_name, row.index_ref, row.error);
		}
		return row.meta;
	}
	throw CatalogException("ngram: index %s no longer exists; was it dropped after binding?", location.index_ref);
}

//! The registry as create_ngram_index needs it: absent (bootstrap) or in the
//! current shape. A registry version 1 database must be emptied by id first.
static RegistrySnapshot ReadRegistryForCreate(ClientContext &context, const string &catalog_name) {
	auto registry = ReadRegistry(context, catalog_name);
	if (registry.legacy_shape) {
		throw InvalidInputException("create_ngram_index: the ngram registry in %s predates format %lld; drop each "
		                            "listed index with drop_ngram_index_by_id and rebuild it",
		                            catalog_name, NGRAM_FORMAT_VERSION);
	}
	return registry;
}

void ValidateRegistryForCreate(ClientContext &context, const string &catalog_name, idx_t expected_registry_oid,
                               bool expected_bootstrap) {
	auto registry = ReadRegistryForCreate(context, catalog_name);
	if (!registry.oid != expected_bootstrap || registry.oid != expected_registry_oid) {
		throw InvalidInputException("create_ngram_index: registry changed after the operation was prepared");
	}
}

//===----------------------------------------------------------------------===//
// Lifecycle observation (PRAGMA ngram_indexes / ngram_index_status)
//===----------------------------------------------------------------------===//

struct ObservedIndex {
	string catalog_name;
	string schema_name, table_name;
	string status;
	string reason;
	IndexLocation location;
	int64_t format_version = -1;
	bool legacy = false;
};

//! The index id named by a storage table (segments_<hex> or stats_<hex>).
static bool ParseStorageName(const string &name, string &index_ref, bool &segments) {
	auto lower = StringUtil::Lower(name);
	string hex;
	if (StringUtil::StartsWith(lower, "segments_")) {
		hex = lower.substr(strlen("segments_"));
		segments = true;
	} else if (StringUtil::StartsWith(lower, "stats_")) {
		hex = lower.substr(strlen("stats_"));
		segments = false;
	} else {
		return false;
	}
	if (hex.size() != 32) {
		return false;
	}
	index_ref = hex.substr(0, 8) + "-" + hex.substr(8, 4) + "-" + hex.substr(12, 4) + "-" + hex.substr(16, 4) + "-" +
	            hex.substr(20);
	return IsCanonicalUUID(index_ref);
}

static void ClassifyBase(ClientContext &context, ObservedIndex &observed, const MetaInfo &meta) {
	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, observed.table_name);
	auto base =
	    Catalog::GetEntry(context, observed.catalog_name, observed.schema_name, lookup, OnEntryNotFound::RETURN_NULL);
	if (!base || base->type != CatalogType::TABLE_ENTRY || !base->Cast<TableCatalogEntry>().IsDuckTable()) {
		observed.status = "ORPHAN";
		observed.reason = "base table is missing or no longer an ordinary DuckDB table";
		return;
	}
	auto &table = base->Cast<DuckTableEntry>();
	if (!table.ColumnExists(observed.location.column_name)) {
		observed.status = "ORPHAN";
		observed.reason = "indexed column is missing";
		return;
	}
	auto guard = RowIdGuardReason(context, table, meta);
	if (!guard.empty()) {
		observed.status = "SCAN_ONLY";
		observed.reason = "exact rowid guard is unavailable: " + guard;
		return;
	}
	observed.status = "READY";
}

static void RethrowFatalObservation(std::exception &ex) {
	auto type = ErrorData(ex).Type();
	if (type != ExceptionType::CATALOG && type != ExceptionType::INVALID_INPUT) {
		throw;
	}
}

static vector<ObservedIndex> ObserveCatalog(ClientContext &context, const string &catalog_name) {
	vector<ObservedIndex> result;
	RegistrySnapshot registry;
	string registry_error;
	try {
		registry = ReadRegistry(context, catalog_name);
	} catch (std::exception &ex) {
		RethrowFatalObservation(ex);
		registry_error = ErrorData(ex).Message();
	}
	// Storage tables present in the schema, by index id: (segments, stats).
	unordered_map<string, pair<bool, bool>> storage;
	vector<ObservedIndex> foreign;
	auto schema = Catalog::GetSchema(context, catalog_name, NGRAM_SCHEMA, OnEntryNotFound::RETURN_NULL);
	if (schema) {
		schema->Scan(context, CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
			string index_ref;
			bool segments = false;
			bool table = entry.type == CatalogType::TABLE_ENTRY && entry.Cast<TableCatalogEntry>().IsDuckTable();
			if (table && StringUtil::CIEquals(entry.name, REGISTRY_TABLE)) {
				return;
			}
			if (!table || !ParseStorageName(entry.name, index_ref, segments)) {
				ObservedIndex observed;
				observed.catalog_name = catalog_name;
				observed.location.index_ref = entry.name;
				observed.status = "MALFORMED";
				observed.reason =
				    StringUtil::Format("%s.%s is not a storage table of this extension", NGRAM_SCHEMA, entry.name);
				foreign.push_back(std::move(observed));
				return;
			}
			(segments ? storage[index_ref].first : storage[index_ref].second) = true;
		});
	}
	for (auto &row : registry.rows) {
		ObservedIndex observed;
		observed.catalog_name = catalog_name;
		observed.schema_name = row.schema_name;
		observed.table_name = row.table_name;
		observed.location = LocationOf(row, registry.oid);
		observed.format_version = row.format_version;
		observed.legacy = registry.legacy_shape;
		// The row consumes its storage entry; whatever remains in storage
		// afterwards has no registry row. Read the entry before erasing it.
		bool storage_complete = false;
		auto tables = storage.find(row.index_ref);
		if (tables != storage.end()) {
			storage_complete = tables->second.first && tables->second.second;
			storage.erase(tables);
		}
		if (!row.error.empty()) {
			observed.status = "MALFORMED";
			observed.reason = row.error;
		} else if (!storage_complete) {
			observed.status = "MALFORMED";
			observed.reason = "a storage table is missing";
		} else {
			ClassifyBase(context, observed, row.meta);
		}
		result.push_back(std::move(observed));
	}
	for (auto &orphaned : storage) {
		ObservedIndex observed;
		observed.catalog_name = catalog_name;
		observed.location.index_ref = orphaned.first;
		observed.status = "MALFORMED";
		observed.reason = registry_error.empty() ? "storage has no registry row" : registry_error;
		result.push_back(std::move(observed));
	}
	result.insert(result.end(), std::make_move_iterator(foreign.begin()), std::make_move_iterator(foreign.end()));
	return result;
}

static ObservedIndex FindObserved(ClientContext &context, const string &catalog_name, const string &index_ref) {
	if (!IsCanonicalUUID(index_ref)) {
		throw InvalidInputException("ngram: index reference must be a canonical lowercase UUID");
	}
	auto database = DatabaseManager::Get(context).GetDatabase(context, catalog_name);
	if (!database || !database->GetCatalog().IsDuckCatalog() || !database->HasStorageManager()) {
		throw CatalogException("ngram: %s is not an attached DuckDB catalog", catalog_name);
	}
	for (auto &observed : ObserveCatalog(context, database->GetName())) {
		if (observed.location.index_ref == index_ref) {
			return observed;
		}
	}
	throw CatalogException("ngram: index %s does not exist in catalog %s", index_ref, catalog_name);
}

ResolvedTarget ResolveTarget(ClientContext &context, const string &table_input, const string &column_name,
                             bool require_column) {
	auto qname = QualifiedName::Parse(table_input);
	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, qname.name);
	auto entry = Catalog::GetEntry(context, qname.catalog, qname.schema, lookup, OnEntryNotFound::THROW_EXCEPTION);
	// a TABLE_ENTRY lookup can also return a view (they share a catalog set)
	if (entry->type != CatalogType::TABLE_ENTRY) {
		if (entry->type == CatalogType::VIEW_ENTRY) {
			throw BinderException("%s is a view, not a table; ngram indexes require a base table", table_input);
		}
		throw BinderException("%s is not a table; ngram indexes require a base table", table_input);
	}
	auto &table_entry = entry->Cast<TableCatalogEntry>();
	if (table_entry.ParentCatalog().IsTemporaryCatalog()) {
		throw BinderException("%s is a temporary table; ngram indexes on temporary tables are not supported",
		                      table_input);
	}
	if (require_column) {
		// a user column named rowid (identifiers match case-insensitively) shadows
		// the pseudo-column the build reads as the row identifier, so the build
		// would silently index that column's values instead of row ids
		for (auto &col : table_entry.GetColumns().Logical()) {
			if (StringUtil::Lower(col.Name()) == "rowid") {
				throw BinderException("cannot build an ngram index on %s: its column \"%s\" shadows the rowid "
				                      "pseudo-column used as the row identifier",
				                      table_input, col.Name());
			}
		}
		if (!table_entry.ColumnExists(column_name)) {
			throw CatalogException("Table %s does not have a column named %s", table_input, column_name);
		}
		auto &column = table_entry.GetColumn(column_name);
		if (column.Generated()) {
			throw BinderException("ngram indexes require a physical VARCHAR column; %s.%s is generated", table_input,
			                      column_name);
		}
		if (column.Type().id() != LogicalTypeId::VARCHAR) {
			throw BinderException("ngram indexes require a VARCHAR column; %s.%s is %s", table_input, column_name,
			                      column.Type().ToString());
		}
	}
	ResolvedTarget target;
	target.catalog_name = table_entry.ParentCatalog().GetName();
	target.schema_name = table_entry.ParentSchema().name;
	target.table_name = table_entry.name;
	// store the catalog's spelling of the column: lookups are case-insensitive,
	// but owner keys and name comparisons are not. When the column no longer
	// exists on the base table (e.g. dropping an orphaned index after the
	// column was removed), the user's spelling passes through.
	target.column_name = column_name;
	if (!column_name.empty() && table_entry.ColumnExists(column_name)) {
		target.column_name = table_entry.GetColumn(column_name).Name();
	}
	target.entry = &table_entry;
	return target;
}

//! Evaluate a guard by inserting into an invocation-scoped temp table. A bare
//! `SELECT <guard>` works but prints a NULL row per guard; the INSERT raises the
//! same error without producing a result row.
static string SilentGuard(const string &guard_table, const string &guard_expression) {
	return "INSERT INTO " + guard_table + " SELECT " + guard_expression + ";\n";
}

static string ScratchName(const char *purpose) {
	return Ident(string("__ngram_") + purpose + "_" + UUID::ToString(UUID::GenerateRandomUUID()));
}

//! The execution-time re-validation every generated script starts with. For a
//! create, `guard_token` is empty when the script installs a fresh guard behind
//! the barrier and the shared guard's token otherwise. For a drop, `guard_name`
//! names the guard the script drops, or is empty.
static string MaintenanceGuardCall(const char *fn, const ResolvedTarget &target, const string &column_name,
                                   bool creating, const string &guard_name, const string &guard_token,
                                   const string &index_ref, idx_t registry_oid, bool bootstrap) {
	return SystemFunction(NGRAM_MAINTENANCE_GUARD) + "(" + Lit(fn) + ", " + Lit(target.catalog_name) + ", " +
	       Lit(target.schema_name) + ", " + Lit(target.table_name) + ", " + Lit(column_name) + ", " +
	       (creating ? "true" : "false") + ", -1, 0, false, " + Lit(guard_name) + ", " + Lit(guard_token) + ", " +
	       Lit(index_ref) + ", " + to_string(registry_oid) + ", " + (bootstrap ? "true" : "false") + ")";
}

//===----------------------------------------------------------------------===//
// Partitioned packing (see ngram/index_pragmas.hpp for the design)
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
	auto open_end = open_ended ? LOCAL_ROWID_START - 1 : hi;
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

idx_t BuildPartitionCount(ClientContext &context, int64_t estimated_pairs) {
	Value value;
	if (context.TryGetCurrentSetting("ngram_build_partitions", value) && !value.IsNull()) {
		auto configured = value.GetValue<int64_t>();
		if (configured < 0) {
			throw InvalidInputException("ngram_build_partitions cannot be negative, got %lld", configured);
		}
		if (configured > 0) {
			return NumericCast<idx_t>(MinValue<int64_t>(configured, NumericCast<int64_t>(MAX_BUILD_PARTITIONS)));
		}
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

static string CreateNgramIndexQuery(ClientContext &context, const FunctionParameters &parameters) {
	auto table_input = parameters.values[0].ToString();
	auto column_name = parameters.values[1].ToString();

	int32_t gram_size = 3;
	bool case_insensitive = true;
	for (auto &entry : parameters.named_parameters) {
		if (entry.second.IsNull()) {
			throw BinderException("create_ngram_index: parameter %s cannot be NULL", entry.first);
		}
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
	if (!target.entry->IsDuckTable()) {
		// the index reads rowids and row storage directly; a table living in a
		// foreign catalog (sqlite, postgres, ...) has neither
		throw BinderException("create_ngram_index: %s is not a DuckDB base table", table_input);
	}
	// generated names and the registry row must use the catalog's spelling
	column_name = target.column_name;
	auto &table = target.entry->Cast<DuckTableEntry>();
	auto registry = ReadRegistryForCreate(context, target.catalog_name);
	auto table_target = target;
	table_target.column_name.clear();
	auto siblings = Locations(registry, table_target, false);
	for (auto &sibling : siblings) {
		if (StringUtil::CIEquals(sibling.column_name, column_name)) {
			throw InvalidInputException("An ngram index already exists on %s.%s (%s); use drop_ngram_index first",
			                            target.table_name, column_name, sibling.index_ref);
		}
	}
	IndexLocation location;
	location.index_ref = UUID::ToString(UUID::GenerateRandomUUID());
	location.column_name = column_name;
	// One rowid guard per table. The first index creates it behind the barrier
	// below, covering every VARCHAR the table has at that moment; later indexes
	// record the same guard and prove it covers their column.
	string guard_name = GUARD_PREFIX + location.Hex();
	string guard_token;
	if (!siblings.empty()) {
		guard_name = siblings[0].guard_name;
		guard_token = siblings[0].guard_token;
		for (auto &sibling : siblings) {
			if (sibling.guard_name != guard_name || sibling.guard_token != guard_token) {
				throw InvalidInputException("create_ngram_index: the ngram indexes on %s record different rowid "
				                            "guards; drop them by id and rebuild them",
				                            target.table_name);
			}
		}
		MetaInfo shared;
		shared.column_name = column_name;
		shared.guard_name = guard_name;
		shared.guard_token = guard_token;
		auto reason = RowIdGuardReason(context, table, shared);
		if (!reason.empty()) {
			throw InvalidInputException("create_ngram_index: the rowid guard shared by the ngram indexes on %s cannot "
			                            "cover %s (%s); drop the table's ngram indexes and rebuild them",
			                            target.table_name, column_name, reason);
		}
	}

	auto base = Ident(target.catalog_name) + "." + Ident(target.schema_name) + "." + Ident(target.table_name);
	auto segments = StorageTable(target.catalog_name, location.SegmentsTable());
	auto stats = StorageTable(target.catalog_name, location.StatsTable());
	auto column = Ident(column_name);
	auto gram_str = to_string(gram_size);
	auto ci_str = case_insensitive ? "true" : "false";
	// ALTER verification in DuckDB v1.5.5 does not reparse a quoted column
	// name containing '-', so keep generated physical identifiers unquoted-safe.
	auto epoch_name = "__ngram_epoch_" + location.Hex();
	auto total_rows = TableTotalRows(table);

	// No BEGIN/COMMIT here: DuckDB's statement preprocessor wraps a multi-statement
	// pragma expansion in a transaction itself (statement_preprocessor.cpp).
	//
	// The build runs through DuckDB's own engine so it is parallel and can spill.
	// One statement per rowid-range partition groups that partition's (gram,
	// segment_no, rowid) pairs into segment rows with the ngram_pack_segment
	// aggregate; the partitions land in a temp table, which is then written to
	// the segments table in gram order. Segments are bucketed by rowid range
	// (not count), which is what lets a rowid-range partition hold whole keys.
	// Duplicate (gram, rowid) instances survive until the codec, which sorts and
	// dedupes.
	string script;
	string guard = ScratchName("guard");
	string packed = ScratchName("build_packed");
	// This is the first executed statement. The volatile scalar acquires the
	// target transaction's vacuum fence before checking that the table planned
	// above is still the table the remaining script will scan.
	script += "CREATE TEMP TABLE " + guard + " AS SELECT " +
	          MaintenanceGuardCall("create_ngram_index", target, column_name, true, guard_name, guard_token, "",
	                               registry.oid, !registry.oid) +
	          " AS ignored, " + (siblings.empty() ? string("NULL::VARCHAR") : Lit(guard_token)) + " AS guard_token;\n";
	if (siblings.empty()) {
		// No registry row names a guard on this table, so every guard carrying
		// the generated prefix is a leftover: concurrent drops of the table's
		// last two indexes each counted the other's row and both kept the guard.
		// A leftover blocks the DROP COLUMN below. The maintenance guard above
		// fails this create if a sibling row appeared, so no referenced guard is
		// dropped here.
		for (auto &leftover : PrefixedGuardNames(context, table)) {
			script += "DROP INDEX IF EXISTS " + Ident(target.catalog_name) + "." + Ident(target.schema_name) + "." +
			          Ident(leftover) + ";\n";
		}
		// Replacing the physical table invalidates every snapshot that predates the
		// rowid guard. The temporary all-NULL column is dropped immediately; only
		// DuckDB's reservoir sample is intentionally discarded by this pair.
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
		script += "CREATE INDEX " + Ident(guard_name) + " ON " + base + " USING " + NGRAM_ROWID_GUARD_TYPE + "(" +
		          guard_columns + ");\n";
		// Retain EXCLUSIVE through this scan-free CREATE. This one scalar proves
		// the fresh physical guard, captures its internal token, and releases the
		// fence before the postings build begins.
		script += "UPDATE " + guard + " SET guard_token = " + SystemFunction(NGRAM_CREATION_FINISH) + "(" +
		          Lit(target.catalog_name) + ", " + Lit(target.schema_name) + ", " + Lit(target.table_name) + ", " +
		          Lit(column_name) + ", " + Lit(guard_name) + ");\n";
	}
	if (!registry.oid) {
		script += "CREATE SCHEMA IF NOT EXISTS " + Ident(target.catalog_name) + "." + Ident(NGRAM_SCHEMA) + ";\n";
		script += "CREATE TABLE " + Registry(target.catalog_name) +
		          "(registry_version INTEGER NOT NULL, index_id UUID PRIMARY KEY, owner_key BLOB UNIQUE NOT NULL, "
		          "schema_name VARCHAR NOT NULL, table_name VARCHAR NOT NULL, column_name VARCHAR NOT NULL, "
		          "format_version BIGINT NOT NULL, gram_size BIGINT NOT NULL, case_insensitive BOOLEAN NOT NULL, "
		          "hwm_rowid BIGINT NOT NULL, guard_name VARCHAR NOT NULL, guard_token VARCHAR NOT NULL);\n";
	}
	// Only committed rows are indexed. A transaction-local rowid is reassigned
	// at commit, so recording one would leave the index pointing at a rowid
	// that never exists. Uncommitted rows are found by the tail scan instead,
	// and land past the high-water mark when they commit.
	auto committed_only = "rowid < " + to_string(LOCAL_ROWID_START);
	auto committed_hwm =
	    "(SELECT coalesce(" + SystemFunction("max") + "(rowid), -1) FROM " + base + " WHERE " + committed_only + ")";
	script += "INSERT INTO " + Registry(target.catalog_name) + " VALUES (" + to_string(REGISTRY_VERSION) + ", " +
	          Lit(location.index_ref) + "::UUID, " + SystemFunction("from_hex") + "(" +
	          Lit(Hex(OwnerKey(target.schema_name, target.table_name, column_name))) + "), " + Lit(target.schema_name) +
	          ", " + Lit(target.table_name) + ", " + Lit(column_name) + ", " + to_string(NGRAM_FORMAT_VERSION) + ", " +
	          gram_str + ", " + ci_str + ", " + committed_hwm + ", " + Lit(guard_name) + ", (SELECT guard_token FROM " +
	          guard + "));\n";
	auto partitions = BuildPartitionCount(
	    context, EstimateGramCount(context, table, column_name, 0, total_rows - 1, NumericCast<idx_t>(gram_size)));
	auto ranges = SegmentAlignedRanges(0, total_rows - 1, partitions);
	for (idx_t i = 0; i < ranges.size(); i++) {
		script += PackPartitionStatement(
		    packed, i == 0,
		    "SELECT rowid AS r, rowid >> " + to_string(SEGMENT_SHIFT) + " AS segment_no, " + SystemFunction("unnest") +
		        "(" + SystemFunction("trigrams") + "(" + column + ", " + gram_str + ", " + ci_str + ")) AS gram FROM " +
		        base + " WHERE rowid >= " + to_string(ranges[i].first) +
		        " AND rowid <= " + to_string(ranges[i].second) + " AND " + column + " IS NOT NULL");
	}
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
	script += "DROP TABLE " + guard + ";\n";
	return script;
}

static string DropNgramIndexByIdQuery(ClientContext &context, const FunctionParameters &parameters);

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

	// A stats run is also the place a user looks to decide whether to refresh
	// or compact, so it reports the table facts the maintenance pragmas
	// compare against: how far the index lags the table, how fragmented the
	// segments are, and whether the guard already knows the index is dead.
	// The guard verdict and the table's rowid count are read here, in the
	// pragma callback, and embedded as literals.
	auto total_rows = TableTotalRows(*target.entry);
	auto base = Ident(target.catalog_name) + "." + Ident(target.schema_name) + "." + Ident(target.table_name);
	string query;
	std::sort(indexes.begin(), indexes.end(), [](const IndexLocation &left, const IndexLocation &right) {
		return left.column_name < right.column_name;
	});
	for (auto &location : indexes) {
		auto segments = StorageTable(target.catalog_name, location.SegmentsTable());
		auto stats = StorageTable(target.catalog_name, location.StatsTable());
		auto info = ReadMeta(context, target.catalog_name, location);
		auto staleness = RowIdGuardReason(context, target.entry->Cast<DuckTableEntry>(), info);
		if (!query.empty()) {
			query += "UNION ALL ";
		}
		// remaining_tail is what a bounded-refresh loop watches from outside the
		// call: the committed rows the index does not cover yet, which every
		// query is currently answering with a tail scan. Counted against the
		// registry row's own mark, so it is exact rather than derived from
		// table_max_rowid (deletes leave rowid gaps).
		query += "SELECT m.column_name, m.gram_size, m.case_insensitive, m.hwm_rowid, " + to_string(total_rows - 1) +
		         "::BIGINT AS table_max_rowid, "
		         "(SELECT " +
		         SystemFunction("count") + "(*) FROM " + base + " WHERE rowid > m.hwm_rowid AND rowid < " +
		         to_string(LOCAL_ROWID_START) +
		         ") AS remaining_tail, "
		         "(SELECT " +
		         SystemFunction("count") + "(DISTINCT " + SystemFunction("encode") + "(gram)) FROM " + stats +
		         ") AS distinct_grams, "
		         "(SELECT " +
		         SystemFunction("count") + "(*) FROM " + segments +
		         ") AS segments, "
		         "(SELECT " +
		         SystemFunction("count") + "(*) FROM (SELECT " + SystemFunction("encode") +
		         "(gram) AS gram_key, segment_no FROM " + segments + " GROUP BY " + SystemFunction("encode") +
		         "(gram), segment_no HAVING " + SystemFunction("count") +
		         "(*) > 1)) AS fragmented_keys, "
		         "(SELECT " +
		         SystemFunction("count") + "(DISTINCT generation) FROM " + segments +
		         ") AS generations, "
		         "(SELECT coalesce(" +
		         SystemFunction("sum") + "(rowid_count), 0) FROM " + segments +
		         ") AS posting_entries, "
		         "(SELECT coalesce(" +
		         SystemFunction("sum") + "(" + SystemFunction("octet_length") + "(postings)), 0) FROM " + segments +
		         ") AS postings_bytes, " + (staleness.empty() ? string("NULL::VARCHAR") : Lit(staleness)) +
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
	ResolvedTarget owner {index.catalog_name, index.schema_name, index.table_name, index.location.column_name, nullptr};
	auto table_target = owner;
	table_target.column_name.clear();
	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, index.table_name);
	auto base = Catalog::GetEntry(context, index.catalog_name, index.schema_name, lookup, OnEntryNotFound::RETURN_NULL);
	auto base_exists = base && base->type == CatalogType::TABLE_ENTRY && base->Cast<TableCatalogEntry>().IsDuckTable();

	auto guard_name = index.location.guard_name;
	auto guard_token = Lit(index.location.guard_token);
	vector<string> storage;
	if (index.legacy) {
		// Registry version 1 (format 3) kept meta, segments and stats in a schema
		// named by the index id and gave every index its own guard. This is the
		// only place that layout is known.
		auto hex = StringUtil::Replace(index_ref, "-", "_");
		auto schema = Ident(index.catalog_name) + "." + Ident("__ngram_idx_" + hex);
		guard_name = "__ngram_rowid_guard_" + hex;
		guard_token = "(SELECT guard_token FROM " + schema + ".meta)";
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
	auto guard = ScratchName("guard");
	script +=
	    "CREATE TEMP TABLE " + guard + " AS SELECT " +
	    MaintenanceGuardCall("drop_ngram_index_by_id", owner, index.location.column_name, false,
	                         drop_guard ? guard_name : string(), "", index_ref, index.location.registry_oid, false) +
	    " AS ignored;\n";
	if (drop_guard) {
		script += SilentGuard(guard, SystemFunction(NGRAM_ROWID_GUARD_VALIDATE) + "(" + Lit(index.catalog_name) + ", " +
		                                 Lit(index.schema_name) + ", " + Lit(index.table_name) + ", " +
		                                 Lit(guard_name) + ", " + guard_token + ")");
		script += "DROP INDEX IF EXISTS " + Ident(index.catalog_name) + "." + Ident(index.schema_name) + "." +
		          Ident(guard_name) + ";\n";
	}
	script += "DELETE FROM " + Registry(index.catalog_name) + " WHERE index_id = " + Lit(index_ref) + "::UUID;\n";
	for (auto &statement : storage) {
		script += statement;
	}
	script += "DROP TABLE " + guard + ";\n";
	return script;
}

void RegisterIndexPragmas(ExtensionLoader &loader) {
	DBConfig::GetConfig(loader.GetDatabaseInstance())
	    .AddExtensionOption("ngram_build_partitions",
	                        "how many rowid-range partitions index build, refresh and compact split their packing "
	                        "pass into; 0 sizes it from memory_limit",
	                        LogicalType::BIGINT, Value::BIGINT(0));

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
}

} // namespace ngram
} // namespace duckdb
