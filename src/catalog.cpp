#include "ngram/catalog.hpp"

#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "ngram/rowid_guard.hpp"
#include "ngram/search_core.hpp"

#include <algorithm>
#include <array>

namespace duckdb {
namespace ngram {

string OwnerKey(const string &schema, const string &table, const string &column) {
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

string Hex(const string &input) {
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

bool IsCanonicalUUID(const string &value) {
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

string ResolvedTarget::Qualified() const {
	return Ident(catalog_name) + "." + Ident(schema_name) + "." + Ident(table_name);
}

string StorageTable(const string &catalog_name, const string &table) {
	return Ident(catalog_name) + "." + Ident(NGRAM_SCHEMA) + "." + Ident(table);
}

string Registry(const string &catalog_name) {
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

RegistrySnapshot ReadRegistry(ClientContext &context, const string &catalog_name) {
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
		ThrowIfInterrupted(context);
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
			} else if (row.meta.hwm_rowid < -1 || row.meta.hwm_rowid >= MAX_ROW_ID) {
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

IndexLocation LocationOf(const RegistryRow &row, idx_t registry_oid) {
	IndexLocation location;
	location.index_ref = row.index_ref;
	location.column_name = row.column_name;
	location.registry_oid = registry_oid;
	location.guard_name = row.meta.guard_name;
	location.guard_token = row.meta.guard_token;
	return location;
}

vector<IndexLocation> Locations(const RegistrySnapshot &registry, const ResolvedTarget &target, bool lenient) {
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

vector<string> PrefixedGuardNames(ClientContext &context, DuckTableEntry &table) {
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

RegistrySnapshot ReadRegistryForCreate(ClientContext &context, const string &catalog_name) {
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

bool ParseStorageName(const string &name, string &index_ref, bool &segments) {
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

string ScratchName(const char *purpose) {
	return Ident(string("__ngram_") + purpose + "_" + UUID::ToString(UUID::GenerateRandomUUID()));
}

string LegacyGuardToken(ClientContext &context, const string &catalog_name, const string &schema_name) {
	string meta_name = "meta";
	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, meta_name);
	auto entry = Catalog::GetEntry(context, catalog_name, schema_name, lookup, OnEntryNotFound::RETURN_NULL);
	if (!entry || entry->type != CatalogType::TABLE_ENTRY || !entry->Cast<TableCatalogEntry>().IsDuckTable() ||
	    !entry->Cast<TableCatalogEntry>().ColumnExists("guard_token")) {
		return string();
	}
	auto &table = entry->Cast<DuckTableEntry>();
	auto &column = table.GetColumn("guard_token");
	if (column.Type().id() != LogicalTypeId::VARCHAR) {
		return string();
	}
	auto &transaction = DuckTransaction::Get(context, table.ParentCatalog());
	vector<StorageIndex> column_ids {table.GetStorageIndex(ColumnIndex(column.Logical().index))};
	TableScanState state;
	InitializeExhaustiveScan(context, transaction, table.GetStorage(), state, column_ids, nullptr);
	DataChunk chunk;
	chunk.Initialize(Allocator::Get(context), {LogicalType::VARCHAR});
	table.GetStorage().Scan(transaction, chunk, state);
	if (chunk.size() == 0 || chunk.GetValue(0, 0).IsNull()) {
		return string();
	}
	return StringValue::Get(chunk.GetValue(0, 0));
}

} // namespace ngram
} // namespace duckdb
