#include "duckdb/catalog/catalog.hpp"
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
#include "duckdb/parser/constraints/list.hpp"
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

//! Pre-registry indexes used these name-derived helpers. New allocations use
//! one UUID-derived opaque schema with fixed table names; legacy discovery and
//! drop retain these functions during the format transition.

string ShadowSchemaName(const string &schema, const string &table) {
	return "ngram_" + schema + "_" + table;
}

string MetaTableName(const string &column) {
	return "meta_" + column;
}

string SegmentsTableName(const string &column) {
	return "segments_" + column;
}

string StatsTableName(const string &column) {
	return "stats_" + column;
}

static constexpr const char *REGISTRY_SCHEMA = "__ngram";
static constexpr const char *REGISTRY_TABLE = "registry";
static constexpr const char *REGISTERED_PREFIX = "__ngram_idx_";
static constexpr int32_t REGISTRY_VERSION = 1;
static constexpr const char *REGISTERED_META = "meta";
static constexpr const char *REGISTERED_SEGMENTS = "segments";
static constexpr const char *REGISTERED_STATS = "stats";

struct RegistryRow {
	string index_ref, owner_key, schema_name, table_name, column_name;
};

struct RegistrySnapshot {
	idx_t oid = 0;
	vector<RegistryRow> rows;
	string error;
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

static string LegacyIndexRef(const string &schema, const string &meta) {
	return "legacy:" + Hex(schema) + ":" + Hex(meta);
}

static bool DecodeHex(const string &text, string &result) {
	if (text.empty() || text.size() % 2 != 0) {
		return false;
	}
	result.clear();
	result.reserve(text.size() / 2);
	for (idx_t i = 0; i < text.size(); i += 2) {
		auto digit = [](char value) -> int {
			if (value >= '0' && value <= '9') {
				return value - '0';
			}
			if (value >= 'a' && value <= 'f') {
				return value - 'a' + 10;
			}
			return -1;
		};
		auto high = digit(text[i]);
		auto low = digit(text[i + 1]);
		if (high < 0 || low < 0) {
			return false;
		}
		result.push_back(static_cast<char>((high << 4) | low));
	}
	return result.find('\0') == string::npos;
}

static bool ParseLegacyIndexRef(const string &ref, string &schema, string &meta) {
	if (ref.rfind("legacy:", 0) != 0) {
		return false;
	}
	auto separator = ref.find(':', strlen("legacy:"));
	return separator != string::npos && ref.find(':', separator + 1) == string::npos &&
	       DecodeHex(ref.substr(strlen("legacy:"), separator - strlen("legacy:")), schema) &&
	       DecodeHex(ref.substr(separator + 1), meta) && StringUtil::CIStartsWith(meta, "meta_") &&
	       ref == LegacyIndexRef(schema, meta);
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

static string RegisteredStorageSchema(const string &index_ref) {
	if (!IsCanonicalUUID(index_ref)) {
		throw InvalidInputException("ngram: registered index reference is not a canonical lowercase UUID: %s",
		                            index_ref);
	}
	return string(REGISTERED_PREFIX) + StringUtil::Replace(index_ref, "-", "_");
}

static void ValidateRegistryShape(DuckTableEntry &table) {
	static const array<const char *, 6> NAMES = {"registry_version", "index_id",   "owner_key",
	                                             "schema_name",      "table_name", "column_name"};
	static const array<LogicalType, 6> TYPES = {LogicalType::INTEGER, LogicalType::UUID, LogicalType::BLOB,
	                                             LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
	auto columns = table.GetColumns().Logical();
	if (columns.Size() != NAMES.size()) {
		throw InvalidInputException("ngram: registry must have exactly six columns");
	}
	idx_t i = 0;
	for (auto &column : columns) {
		if (column.Name() != NAMES[i] || column.Type() != TYPES[i] || column.Generated()) {
			throw InvalidInputException("ngram: registry column %llu has the wrong name or type", i + 1);
		}
		i++;
	}
	array<idx_t, 6> not_null {};
	idx_t primary_keys = 0, owner_uniques = 0, invalid = 0;
	for (auto &constraint : table.GetConstraints()) {
		if (constraint->type == ConstraintType::NOT_NULL) {
			auto index = constraint->Cast<NotNullConstraint>().index.index;
			index < not_null.size() ? not_null[index]++ : invalid++;
		} else if (constraint->type == ConstraintType::UNIQUE) {
			auto &unique = constraint->Cast<UniqueConstraint>();
			auto indexes = unique.GetLogicalIndexes(table.GetColumns());
			auto primary = indexes.size() == 1 && indexes[0].index == 1 && unique.IsPrimaryKey();
			auto owner = indexes.size() == 1 && indexes[0].index == 2 && !unique.IsPrimaryKey();
			primary_keys += primary;
			owner_uniques += owner;
			invalid += !primary && !owner;
		} else {
			invalid++;
		}
	}
	for (auto count : not_null) {
		invalid += count != 1;
	}
	if (primary_keys != 1 || owner_uniques != 1 || invalid || table.GetConstraints().size() != 8) {
		throw InvalidInputException("ngram: registry constraints are malformed");
	}
}

static RegistrySnapshot ReadRegistry(ClientContext &context, const string &catalog_name,
                                     bool retain_identifiable_corruption = false) {
	RegistrySnapshot result;
	// EntryLookupInfo stores the name by reference.
	string registry_table = REGISTRY_TABLE;
	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, registry_table);
	auto entry = Catalog::GetEntry(context, catalog_name, REGISTRY_SCHEMA, lookup, OnEntryNotFound::RETURN_NULL);
	if (!entry) {
		return result;
	}
	if (entry->name != REGISTRY_TABLE || entry->ParentSchema().name != REGISTRY_SCHEMA ||
	    entry->type != CatalogType::TABLE_ENTRY || !entry->Cast<TableCatalogEntry>().IsDuckTable()) {
		throw InvalidInputException("ngram: registry is not an ordinary DuckDB table");
	}
	auto &table = entry->Cast<DuckTableEntry>();
	ValidateRegistryShape(table);
	result.oid = table.oid;
	vector<StorageIndex> column_ids;
	vector<LogicalType> types;
	for (auto &column : table.GetColumns().Logical()) {
		column_ids.push_back(table.GetStorageIndex(ColumnIndex(column.Logical().index)));
		types.push_back(column.Type());
	}
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
		for (idx_t row = 0; row < chunk.size(); row++) {
			for (idx_t col = 0; col < chunk.ColumnCount(); col++) {
				if (chunk.GetValue(col, row).IsNull()) {
					throw InvalidInputException("ngram: registry contains NULLs");
				}
			}
			auto version = chunk.GetValue(0, row).GetValue<int32_t>();
			RegistryRow registry_row;
			registry_row.index_ref = UUID::ToString(chunk.GetValue(1, row).GetValue<hugeint_t>());
			registry_row.owner_key = StringValue::Get(chunk.GetValue(2, row));
			registry_row.schema_name = StringValue::Get(chunk.GetValue(3, row));
			registry_row.table_name = StringValue::Get(chunk.GetValue(4, row));
			registry_row.column_name = StringValue::Get(chunk.GetValue(5, row));
			string error;
			if (version != REGISTRY_VERSION) {
				error = StringUtil::Format("ngram: unsupported registry row version %d", version);
			} else if (!IsCanonicalUUID(registry_row.index_ref) ||
			           registry_row.owner_key != OwnerKey(registry_row.schema_name, registry_row.table_name,
			                                              registry_row.column_name)) {
				error = "ngram: registry row has a noncanonical ID or owner key";
			}
			if (!error.empty()) {
				if (!retain_identifiable_corruption) {
					throw InvalidInputException(error);
				}
				if (result.error.empty()) {
					result.error = error;
				}
				if (!IsCanonicalUUID(registry_row.index_ref)) {
					continue;
				}
			}
			result.rows.push_back(std::move(registry_row));
		}
	}
	return result;
}

struct RegistryCreateState {
	bool bootstrap = false;
	idx_t registry_oid = 0;
};

static idx_t ExistingTableOid(ClientContext &context, const string &catalog, const string &schema,
                              const string &table);
static idx_t ExistingSchemaOid(ClientContext &context, const string &catalog, const string &schema);

static idx_t ForeignSchemaEntries(ClientContext &context, SchemaCatalogEntry &schema,
                                  const vector<string> &allowed_tables) {
	static const CatalogType TYPES[] = {
	    CatalogType::TABLE_ENTRY,           CatalogType::INDEX_ENTRY,
	    CatalogType::SEQUENCE_ENTRY,        CatalogType::MACRO_ENTRY,
	    CatalogType::TABLE_MACRO_ENTRY,     CatalogType::TYPE_ENTRY,
	    CatalogType::COLLATION_ENTRY,       CatalogType::COPY_FUNCTION_ENTRY,
	    CatalogType::PRAGMA_FUNCTION_ENTRY, CatalogType::COORDINATE_SYSTEM_ENTRY};
	idx_t foreign = 0;
	for (auto type : TYPES) {
		schema.Scan(context, type, [&](CatalogEntry &entry) {
			bool allowed = type == CatalogType::TABLE_ENTRY;
			if (allowed) {
				allowed = false;
				for (auto &name : allowed_tables) {
					allowed |= StringUtil::CIEquals(entry.name, name);
				}
			}
			foreign += !allowed;
		});
	}
	return foreign;
}

struct ObservedIndex {
	string catalog_name;
	string schema_name, table_name;
	string status;
	string reason;
	IndexLocation location;
	int64_t format_version = -1;
};

static bool ParseOpaqueSchema(const string &schema, string &index_ref) {
	auto suffix = schema.substr(strlen(REGISTERED_PREFIX));
	index_ref = StringUtil::Replace(suffix, "_", "-");
	return suffix.size() == 36 && IsCanonicalUUID(index_ref) && schema == RegisteredStorageSchema(index_ref);
}

static RegistryCreateState InspectRegistryForCreate(ClientContext &context, const string &catalog_name) {
	RegistryCreateState result;
	auto registry = ReadRegistry(context, catalog_name);
	auto registry_schema =
	    Catalog::GetSchema(context, catalog_name, REGISTRY_SCHEMA, OnEntryNotFound::RETURN_NULL);
	unordered_set<string> opaque;
	for (auto &schema : Catalog::GetSchemas(context, catalog_name)) {
		if (StringUtil::CIStartsWith(schema.get().name, REGISTERED_PREFIX)) {
			opaque.insert(schema.get().name);
		}
	}
	if (!registry.oid) {
		if (registry_schema || !opaque.empty()) {
			throw InvalidInputException(
			    "create_ngram_index: reserved registry/storage objects exist without a valid %s.%s table; "
			    "inspect them with PRAGMA ngram_indexes and repair or remove the corruption",
			    REGISTRY_SCHEMA, REGISTRY_TABLE);
		}
		result.bootstrap = true;
		return result;
	}
	result.registry_oid = registry.oid;
	unordered_set<string> missing;
	for (auto &row : registry.rows) {
		auto schema = RegisteredStorageSchema(row.index_ref);
		if (!opaque.erase(schema)) {
			missing.insert(schema);
			continue;
		}
		auto entry = Catalog::GetSchema(context, catalog_name, schema, OnEntryNotFound::THROW_EXCEPTION);
		if (!ExistingTableOid(context, catalog_name, schema, REGISTERED_META) ||
		    !ExistingTableOid(context, catalog_name, schema, REGISTERED_SEGMENTS) ||
		    !ExistingTableOid(context, catalog_name, schema, REGISTERED_STATS) ||
		    ForeignSchemaEntries(context, *entry, {REGISTERED_META, REGISTERED_SEGMENTS, REGISTERED_STATS})) {
			throw InvalidInputException("create_ngram_index: registered storage schema %s is malformed", schema);
		}
		auto &meta = ResolveExistingTable(context, catalog_name, schema, REGISTERED_META,
		                                  "ngram index meta table");
		ShadowTarget owner {row.schema_name, row.table_name, row.column_name, schema};
		auto header = ReadMetaHeader(context, DuckTransaction::Get(context, meta.ParentCatalog()), meta, owner);
		if (header.format_version != NGRAM_FORMAT_VERSION) {
			throw InvalidInputException("create_ngram_index: registered storage schema %s uses unsupported meta format",
			                            schema);
		}
		if (!StringUtil::CIEquals(header.schema_name, row.schema_name) ||
		    !StringUtil::CIEquals(header.table_name, row.table_name) ||
		    !StringUtil::CIEquals(header.column_name, row.column_name)) {
			throw InvalidInputException("create_ngram_index: registry and meta owners differ in storage schema %s",
			                            schema);
		}
	}
	if (!opaque.empty()) {
		throw InvalidInputException("create_ngram_index: unregistered or malformed opaque storage schema %s exists; "
		                            "inspect it with PRAGMA ngram_indexes before creating another index",
		                            *opaque.begin());
	}
	if (!missing.empty()) {
		throw InvalidInputException("create_ngram_index: registry row has no matching opaque storage schema %s",
		                            *missing.begin());
	}
	return result;
}

void ValidateRegistryForCreate(ClientContext &context, const string &catalog_name, idx_t expected_registry_oid,
                               bool expected_bootstrap) {
	auto actual = InspectRegistryForCreate(context, catalog_name);
	if (actual.bootstrap != expected_bootstrap || actual.registry_oid != expected_registry_oid) {
		throw InvalidInputException("create_ngram_index: registry changed after the operation was prepared");
	}
}

static void ClassifyRegisteredBase(ClientContext &context, ObservedIndex &observed, const MetaInfo &info) {
	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, observed.table_name);
	auto base = Catalog::GetEntry(context, observed.catalog_name, observed.schema_name, lookup,
	                              OnEntryNotFound::RETURN_NULL);
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
	auto fingerprint = ComputeTableFingerprint(context, table);
	auto stale = CertainStaleReason(info, fingerprint);
	if (!stale.empty()) {
		observed.status = !info.instance_id.empty() && info.instance_id == fingerprint.instance_id &&
		                          info.catalog_oid == fingerprint.catalog_oid && info.table_oid != fingerprint.table_oid
		                      ? "REPLACED"
		                      : "SCAN_ONLY";
		observed.reason = stale;
		return;
	}
	auto guard = RowIdGuardReason(context, table, info);
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

static vector<ObservedIndex> ObserveCatalog(ClientContext &context, const string &catalog_name,
                                            const string &filter_ref = string()) {
	vector<ObservedIndex> result;
	string filter_schema;
	string filter_meta;
	bool filter_registered = !filter_ref.empty() && IsCanonicalUUID(filter_ref);
	if (!filter_ref.empty() && !filter_registered && !ParseLegacyIndexRef(filter_ref, filter_schema, filter_meta)) {
		throw InvalidInputException("ngram: index reference must be a canonical lowercase UUID or legacy locator");
	}
	if (filter_registered) {
		filter_schema = RegisteredStorageSchema(filter_ref);
	}
	RegistrySnapshot registry;
	string registry_error;
	if (filter_meta.empty()) {
		try {
			registry = ReadRegistry(context, catalog_name, true);
			registry_error = registry.error;
		} catch (std::exception &ex) {
			RethrowFatalObservation(ex);
		registry_error = ErrorData(ex).Message();
		}
	}
	unordered_map<string, RegistryRow> rows;
	for (auto &row : registry.rows) {
		rows[row.index_ref] = row;
	}
	unordered_set<string> seen;
	vector<reference<SchemaCatalogEntry>> schemas;
	if (filter_schema.empty()) {
		schemas = Catalog::GetSchemas(context, catalog_name);
	} else {
		auto schema = Catalog::GetSchema(context, catalog_name, filter_schema, OnEntryNotFound::RETURN_NULL);
		if (schema && schema->name == filter_schema) {
			schemas.push_back(*schema);
		}
	}
	for (auto &schema_ref : schemas) {
		auto &schema = schema_ref.get();
		if (!filter_meta.empty() || !StringUtil::CIStartsWith(schema.name, REGISTERED_PREFIX)) {
			continue;
		}
		ObservedIndex observed;
		observed.catalog_name = catalog_name;
		observed.location.shadow_schema = schema.name;
		observed.location.schema_oid = schema.oid;
		if (!ParseOpaqueSchema(schema.name, observed.location.index_ref)) {
			observed.location.index_ref = schema.name;
			observed.status = "MALFORMED";
			observed.reason = "opaque schema name is not a canonical UUID derivation";
			result.push_back(std::move(observed));
			continue;
		}
		seen.insert(observed.location.index_ref);
		auto row = rows.find(observed.location.index_ref);
		if (row != rows.end()) {
			observed.location.registry_oid = registry.oid;
			observed.schema_name = row->second.schema_name;
			observed.table_name = row->second.table_name;
			observed.location.column_name = row->second.column_name;
		}
		try {
			observed.location.meta_oid = ExistingTableOid(context, catalog_name, schema.name, REGISTERED_META);
			observed.location.segments_oid = ExistingTableOid(context, catalog_name, schema.name, REGISTERED_SEGMENTS);
			observed.location.stats_oid = ExistingTableOid(context, catalog_name, schema.name, REGISTERED_STATS);
			if (!observed.location.meta_oid || !observed.location.segments_oid || !observed.location.stats_oid) {
				throw InvalidInputException("one or more exact storage tables are missing");
			}
			if (ForeignSchemaEntries(context, schema,
			                         {REGISTERED_META, REGISTERED_SEGMENTS, REGISTERED_STATS}) != 0) {
				throw InvalidInputException("opaque storage schema contains foreign objects; ID drop is blocked");
			}
			auto &meta = ResolveExistingTable(context, catalog_name, schema.name, REGISTERED_META,
			                                  "ngram index meta table");
			auto &tx = DuckTransaction::Get(context, meta.ParentCatalog());
			ShadowTarget target {observed.schema_name, observed.table_name, observed.location.column_name, schema.name};
			auto header = ReadMetaHeader(context, tx, meta, target);
			if (observed.location.registry_oid) {
				if (!StringUtil::CIEquals(header.schema_name, observed.schema_name) ||
				    !StringUtil::CIEquals(header.table_name, observed.table_name) ||
				    !StringUtil::CIEquals(header.column_name, observed.location.column_name)) {
					// Preserve the durable storage owner for mutation-safety checks;
					// the row/meta disagreement still makes this allocation MALFORMED.
					observed.schema_name = header.schema_name;
					observed.table_name = header.table_name;
					observed.location.column_name = header.column_name;
					throw InvalidInputException("registry and meta owners differ");
				}
			} else {
				// When the registry row is gone, meta is the only durable owner source.
				observed.schema_name = header.schema_name;
				observed.table_name = header.table_name;
				observed.location.column_name = header.column_name;
				target = {header.schema_name, header.table_name, header.column_name, schema.name};
			}
			observed.format_version = header.format_version;
			unique_ptr<MetaInfo> meta_info;
			if (observed.format_version == NGRAM_FORMAT_VERSION) {
				auto info = ReadMeta(context, tx, meta, target);
				meta_info = make_uniq<MetaInfo>(std::move(info));
			}
			if (!registry_error.empty()) {
				observed.status = "MALFORMED";
				observed.reason = registry_error;
			} else if (!meta_info) {
				observed.status = "MALFORMED";
				observed.reason = "registered storage uses unsupported meta format";
			} else if (!observed.location.registry_oid) {
				observed.status = "UNREGISTERED";
				observed.reason = "registry row is missing";
			} else {
				ClassifyRegisteredBase(context, observed, *meta_info);
			}
		} catch (std::exception &ex) {
			RethrowFatalObservation(ex);
			observed.status = "MALFORMED";
			observed.reason = ErrorData(ex).Message();
		}
		result.push_back(std::move(observed));
	}
	if (filter_meta.empty()) {
		for (auto &row : registry.rows) {
			if (!filter_ref.empty() && row.index_ref != filter_ref) {
				continue;
			}
			if (seen.find(row.index_ref) != seen.end()) {
				continue;
			}
			ObservedIndex observed;
			observed.catalog_name = catalog_name;
			observed.location.index_ref = row.index_ref;
			observed.location.shadow_schema = RegisteredStorageSchema(row.index_ref);
			observed.schema_name = row.schema_name;
			observed.table_name = row.table_name;
			observed.location.column_name = row.column_name;
			observed.location.registry_oid = registry.oid;
			observed.status = registry_error.empty() ? "MISSING_STORAGE" : "MALFORMED";
			observed.reason = registry_error.empty() ? "registered opaque storage schema is missing" : registry_error;
			result.push_back(std::move(observed));
		}
	}
	for (auto &schema_ref : schemas) {
		auto &schema = schema_ref.get();
		if (filter_registered || !StringUtil::CIStartsWith(schema.name, "ngram_") ||
		    StringUtil::CIStartsWith(schema.name, REGISTERED_PREFIX)) {
			continue;
		}
		vector<string> metas;
		if (!filter_meta.empty()) {
			EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, filter_meta);
			auto entry = Catalog::GetEntry(context, catalog_name, schema.name, lookup, OnEntryNotFound::RETURN_NULL);
			if (entry && entry->name == filter_meta) {
				metas.push_back(filter_meta);
			}
		} else {
			schema.Scan(context, CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
				if (StringUtil::CIStartsWith(entry.name, "meta_")) {
					metas.push_back(entry.name);
				}
			});
		}
		for (auto &meta_name : metas) {
			ObservedIndex observed;
			observed.catalog_name = catalog_name;
			observed.location.index_ref = LegacyIndexRef(schema.name, meta_name);
			observed.location.shadow_schema = schema.name;
			observed.location.schema_oid = schema.oid;
			try {
				auto &meta = ResolveExistingTable(context, catalog_name, schema.name, meta_name,
				                                  "ngram index meta table");
				auto &tx = DuckTransaction::Get(context, meta.ParentCatalog());
				ShadowTarget target {string(), string(), string(), schema.name};
				auto header = ReadMetaHeader(context, tx, meta, target);
				if (meta_name != MetaTableName(header.column_name)) {
					throw InvalidInputException("legacy meta table name does not match its recorded column owner");
				}
				if (schema.name != ShadowSchemaName(header.schema_name, header.table_name)) {
					throw InvalidInputException("legacy storage schema does not match its recorded owner");
				}
				observed.schema_name = header.schema_name;
				observed.table_name = header.table_name;
				observed.location.column_name = header.column_name;
				observed.location.meta_oid = meta.oid;
				observed.location.segments_oid = ExistingTableOid(context, catalog_name, schema.name,
				                                             SegmentsTableName(header.column_name));
				observed.location.stats_oid = ExistingTableOid(context, catalog_name, schema.name,
				                                                  StatsTableName(header.column_name));
				if (!observed.location.segments_oid || !observed.location.stats_oid) {
					throw InvalidInputException("one or more legacy storage tables are missing");
				}
				target = {header.schema_name, header.table_name, header.column_name, schema.name};
				observed.format_version = header.format_version;
				if (observed.format_version == NGRAM_FORMAT_VERSION) {
					auto info = ReadMeta(context, tx, meta, target);
					ClassifyRegisteredBase(context, observed, info);
				} else if (observed.format_version == 2 && !meta.ColumnExists("guard_name") &&
				           !meta.ColumnExists("guard_token")) {
					observed.status = "LEGACY_REBUILD";
					observed.reason = "v2 storage is drop-only";
				} else {
					throw InvalidInputException("unsupported legacy meta format");
				}
			} catch (std::exception &ex) {
				RethrowFatalObservation(ex);
				observed.status = "MALFORMED";
				observed.reason = ErrorData(ex).Message();
			}
			result.push_back(std::move(observed));
		}
	}
	return result;
}

static ObservedIndex FindObserved(ClientContext &context, const string &catalog_name, const string &index_ref) {
	auto database = DatabaseManager::Get(context).GetDatabase(context, catalog_name);
	if (!database || !database->GetCatalog().IsDuckCatalog() || !database->HasStorageManager()) {
		throw CatalogException("ngram: %s is not an attached DuckDB catalog", catalog_name);
	}
	for (auto &observed : ObserveCatalog(context, database->GetName(), index_ref)) {
		if (observed.location.index_ref == index_ref) {
			return observed;
		}
	}
	throw CatalogException("ngram: index %s does not exist in catalog %s", index_ref, catalog_name);
}

void RequireExclusiveOwnerAllocation(ClientContext &context, const ResolvedTarget &target,
                                     const string &index_ref) {
	// Preserve the hot resolver's strict legacy spelling/ownership checks, then
	// supplement it with lifecycle observation for registry-row-lost storage.
	RequireUniqueIndexColumns(ExistingIndexes(context, target));
	auto expected_owner = OwnerKey(target.schema_name, target.table_name, target.column_name);
	bool found = false;
	for (auto &observed : ObserveCatalog(context, target.catalog_name)) {
		auto same_owner = OwnerKey(observed.schema_name, observed.table_name, observed.location.column_name) ==
		                  expected_owner;
		if (observed.location.index_ref != index_ref) {
			if (same_owner) {
				throw InvalidInputException("ngram: multiple allocations claim column %s", target.column_name);
			}
			continue;
		}
		if (found || !same_owner || observed.status == "MALFORMED" || observed.status == "MISSING_STORAGE") {
			throw InvalidInputException("ngram: index allocation changed after the operation was prepared");
		}
		found = true;
	}
	if (!found) {
		throw InvalidInputException("ngram: index allocation was removed after the operation was prepared");
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
	// but generated shadow-table names and name comparisons are not. When the
	// column no longer exists on the base table (e.g. dropping an orphaned
	// index after the column was removed), the user's spelling passes through.
	target.column_name = column_name;
	if (!column_name.empty() && table_entry.ColumnExists(column_name)) {
		target.column_name = table_entry.GetColumn(column_name).Name();
	}
	target.shadow_schema = ShadowSchemaName(target.schema_name, target.table_name);
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

struct CreationProtector {
	CreationProtector() = default;
	CreationProtector(string kind_p, string detail_p, string name_p, string identity_p, transaction_t timestamp_p)
	    : kind(std::move(kind_p)), detail(std::move(detail_p)), name(std::move(name_p)),
	      identity(std::move(identity_p)), timestamp(timestamp_p) {
	}

	string kind;
	string detail;
	string name;
	string identity;
	transaction_t timestamp = 0;
};

static string MaintenanceGuardCall(const ResolvedTarget &target, const string &column_name,
                                   const TableFingerprint &fingerprint, const char *fn,
                                   const CreationProtector &protector, const string &new_guard_name,
                                   const RegistryCreateState &registry) {
	return SystemFunction(NGRAM_MAINTENANCE_GUARD) + "(" + Lit(fn) + ", " + Lit(target.catalog_name) + ", " +
	       Lit(target.schema_name) + ", " + Lit(target.table_name) + ", " + Lit(column_name) + ", true, " +
	       "-1, " + to_string(fingerprint.table_oid) + ", " +
	       Lit(fingerprint.schema_fingerprint) + ", 0, false, " + Lit(protector.kind) + ", " +
	       Lit(protector.detail) + ", " + Lit(protector.name) + ", " + Lit(protector.identity) + ", " +
	       to_string(protector.timestamp) + ", " + Lit(new_guard_name) + ", " +
	       to_string(registry.registry_oid) + ", " + (registry.bootstrap ? "true" : "false") +
	       ", '', '', '', 0, 0, 0, 0, 0)";
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
	return statement +
	       "SELECT " + SystemFunction("decode") + "(gram_key) AS gram, segment_no, " +
	       SystemFunction("struct_extract") + "(segment, 'postings') AS postings, " +
	       SystemFunction("struct_extract") + "(segment, 'rowid_count') AS rowid_count, " +
	       SystemFunction("struct_extract") + "(segment, 'min_rowid') AS min_rowid, " +
	       SystemFunction("struct_extract") + "(segment, 'max_rowid') AS max_rowid FROM (" +
	       "SELECT " + SystemFunction("encode") + "(gram) AS gram_key, segment_no, " +
	       SystemFunction("ngram_pack_segment") + "(r) AS segment FROM (" + pair_source + ") GROUP BY gram_key, segment_no);\n";
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

static vector<string> ExistingMetaTables(ClientContext &context, const ResolvedTarget &target) {
	vector<string> result;
	auto schema_entry =
	    Catalog::GetSchema(context, target.catalog_name, target.shadow_schema, OnEntryNotFound::RETURN_NULL);
	if (schema_entry) {
		schema_entry->Scan(context, CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
			if (StringUtil::CIStartsWith(entry.name, "meta_")) {
				result.push_back(entry.name);
			}
		});
	}
	return result;
}

static idx_t ExistingTableOid(ClientContext &context, const string &catalog, const string &schema,
                              const string &table) {
	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, table);
	auto entry = Catalog::GetEntry(context, catalog, schema, lookup, OnEntryNotFound::RETURN_NULL);
	if (!entry) {
		return 0;
	}
	if (entry->name != table || entry->type != CatalogType::TABLE_ENTRY ||
	    !entry->Cast<TableCatalogEntry>().IsDuckTable()) {
		throw InvalidInputException("ngram: storage object %s.%s is not an ordinary DuckDB table", schema, table);
	}
	return entry->oid;
}

static idx_t ExistingSchemaOid(ClientContext &context, const string &catalog, const string &schema) {
	auto entry = Catalog::GetSchema(context, catalog, schema, OnEntryNotFound::RETURN_NULL);
	if (entry && entry->name != schema) {
		throw InvalidInputException("ngram: storage schema name is not canonical");
	}
	return entry ? entry->oid : 0;
}

vector<IndexLocation> ExistingIndexes(ClientContext &context, const ResolvedTarget &target,
                                      bool ignore_registry_corruption) {
	vector<IndexLocation> result;
	RegistrySnapshot registry;
	try {
		registry = ReadRegistry(context, target.catalog_name);
	} catch (CatalogException &) {
		if (ignore_registry_corruption) return result;
		throw;
	} catch (InvalidInputException &) {
		if (ignore_registry_corruption) return result;
		throw;
	}
	for (auto &row : registry.rows) {
		if (!target.column_name.empty()) {
			if (row.owner_key != OwnerKey(target.schema_name, target.table_name, target.column_name)) {
				continue;
			}
		} else if (!StringUtil::CIEquals(row.schema_name, target.schema_name) ||
		           !StringUtil::CIEquals(row.table_name, target.table_name)) {
			continue;
		}
		IndexLocation location;
		location.index_ref = row.index_ref;
		location.shadow_schema = RegisteredStorageSchema(row.index_ref);
		location.column_name = row.column_name;
		location.registry_oid = registry.oid;
		location.schema_oid = ExistingSchemaOid(context, target.catalog_name, location.shadow_schema);
		location.meta_oid =
		    ExistingTableOid(context, target.catalog_name, location.shadow_schema, location.MetaTable());
		location.segments_oid =
		    ExistingTableOid(context, target.catalog_name, location.shadow_schema, location.SegmentsTable());
		location.stats_oid =
		    ExistingTableOid(context, target.catalog_name, location.shadow_schema, location.StatsTable());
		result.push_back(std::move(location));
	}
	for (auto &meta_name : ExistingMetaTables(context, target)) {
		auto column_name = meta_name.substr(strlen("meta_"));
		if (!target.column_name.empty() && !StringUtil::CIEquals(column_name, target.column_name)) {
			continue;
		}
		IndexLocation location;
		location.index_ref = LegacyIndexRef(target.shadow_schema, meta_name);
		location.shadow_schema = target.shadow_schema;
		location.column_name = column_name;
		location.meta_oid = ExistingTableOid(context, target.catalog_name, location.shadow_schema, meta_name);
		auto &meta = ResolveExistingTable(context, target.catalog_name, location.shadow_schema, meta_name,
		                                  "ngram index meta table");
		ShadowTarget owner {target.schema_name, target.table_name, location.column_name, location.shadow_schema};
		auto header = ReadMetaHeader(context, DuckTransaction::Get(context, meta.ParentCatalog()), meta, owner);
		if (!StringUtil::CIEquals(header.schema_name, target.schema_name) ||
		    !StringUtil::CIEquals(header.table_name, target.table_name)) {
			continue;
		}
		if (location.shadow_schema != ShadowSchemaName(header.schema_name, header.table_name)) {
			throw InvalidInputException("ngram: legacy storage schema does not match its recorded owner");
		}
		if (meta_name != MetaTableName(header.column_name)) {
			throw InvalidInputException("ngram: legacy meta table name does not match its recorded column owner");
		}
		location.schema_oid = ExistingSchemaOid(context, target.catalog_name, location.shadow_schema);
		location.segments_oid =
		    ExistingTableOid(context, target.catalog_name, location.shadow_schema, location.SegmentsTable());
		location.stats_oid = ExistingTableOid(context, target.catalog_name, location.shadow_schema, location.StatsTable());
		result.push_back(std::move(location));
	}
	return result;
}

void RequireUniqueIndexColumns(const vector<IndexLocation> &indexes) {
	unordered_set<string> seen;
	for (auto &index : indexes) {
		if (!seen.insert(StringUtil::Lower(index.column_name)).second) {
			throw InvalidInputException("ngram: multiple allocations claim column %s", index.column_name);
		}
	}
}

bool IndexLocationAvailable(ClientContext &context, const ResolvedTarget &target, const IndexLocation &location,
                            bool changed_is_absent) {
	// Every identity change below is a plan that outlived the objects it was
	// bound to. Maintenance and candidate callers must not act on the
	// replacement, so they raise; the exhaustive read paths scan instead.
	auto replaced = [&](const string &message) -> bool {
		if (changed_is_absent) {
			return false;
		}
		throw InvalidInputException(message);
	};
	auto current_schema = ExistingSchemaOid(context, target.catalog_name, location.shadow_schema);
	auto current_meta = ExistingTableOid(context, target.catalog_name, location.shadow_schema, location.MetaTable());
	auto current_segments =
	    ExistingTableOid(context, target.catalog_name, location.shadow_schema, location.SegmentsTable());
	auto current_stats = ExistingTableOid(context, target.catalog_name, location.shadow_schema, location.StatsTable());
	auto changed = [](idx_t expected, idx_t current) { return current && current != expected; };
	if (changed(location.schema_oid, current_schema) || changed(location.meta_oid, current_meta) ||
	    changed(location.segments_oid, current_segments) || changed(location.stats_oid, current_stats)) {
		return replaced("ngram: storage schema or tables changed after the index operation was prepared");
	}
	auto absent = !current_schema || !current_meta || !current_segments || !current_stats;
	if (!location.Registered()) {
		return !absent;
	}
	RegistrySnapshot registry;
	try {
		registry = ReadRegistry(context, target.catalog_name);
	} catch (CatalogException &) {
		if (absent && !current_schema && !current_meta && !current_segments && !current_stats) {
			return false;
		}
		throw;
	} catch (InvalidInputException &) {
		if (absent && !current_schema && !current_meta && !current_segments && !current_stats) {
			return false;
		}
		throw;
	}
	if (!location.registry_oid) {
		for (auto &row : registry.rows) {
			if (row.index_ref == location.index_ref) {
				return replaced(StringUtil::Format(
				    "ngram: registry row %s appeared after the index operation was prepared", location.index_ref));
			}
		}
		return !absent;
	}
	if (!registry.oid || registry.oid != location.registry_oid) {
		if (!current_schema && !current_meta && !current_segments && !current_stats) {
			return false;
		}
		return replaced("ngram: registry changed after the index operation was prepared");
	}
	idx_t matches = 0;
	for (auto &row : registry.rows) {
		if (row.index_ref != location.index_ref) {
			continue;
		}
		matches++;
		if (row.owner_key != OwnerKey(target.schema_name, target.table_name, location.column_name) ||
		    location.shadow_schema != RegisteredStorageSchema(row.index_ref)) {
			return replaced(StringUtil::Format(
			    "ngram: registry row %s changed after the index operation was prepared", location.index_ref));
		}
	}
	if (matches != 1) {
		if (!current_schema && !current_meta && !current_segments && !current_stats && matches == 0) {
			return false;
		}
		return replaced(StringUtil::Format(
		    "ngram: registry row %s was removed after the index operation was prepared", location.index_ref));
	}
	return !absent;
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
	// generated names and the meta row must use the catalog's spelling
	column_name = target.column_name;
	auto registry_state = InspectRegistryForCreate(context, target.catalog_name);
	auto index_ref = UUID::ToString(UUID::GenerateRandomUUID());
	auto storage_schema = RegisteredStorageSchema(index_ref);

	auto base = Ident(target.catalog_name) + "." + Ident(target.schema_name) + "." + Ident(target.table_name);
	auto shadow = Ident(target.catalog_name) + "." + Ident(storage_schema);
	auto meta = shadow + "." + Ident(REGISTERED_META);
	auto segments = shadow + "." + Ident(REGISTERED_SEGMENTS);
	auto stats = shadow + "." + Ident(REGISTERED_STATS);
	auto column = Ident(column_name);
	auto gram_str = to_string(gram_size);
	auto ci_str = case_insensitive ? "true" : "false";
	// ALTER verification in DuckDB v1.5.5 does not reparse a quoted column
	// name containing '-', so keep generated physical identifiers unquoted-safe.
	auto incarnation = StringUtil::Replace(index_ref, "-", "_");
	auto epoch_name = "__ngram_epoch_" + incarnation;
	auto fresh_guard_name = "__ngram_rowid_guard_" + incarnation;

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
	auto fingerprint = ComputeTableFingerprint(context, *target.entry);
	CreationProtector protector;
	vector<string> protector_columns;
	auto table_target = target;
	table_target.column_name.clear();
	auto existing_indexes = ExistingIndexes(context, table_target);
	for (auto &location : existing_indexes) {
		if (!location.Registered()) {
			throw InvalidInputException(
			    "create_ngram_index: a legacy ngram index still exists on %s; drop every legacy allocation by ID "
			    "and rebuild it before creating registered indexes",
			    target.table_name);
		}
	}
	if (!existing_indexes.empty()) {
		auto &transaction = DuckTransaction::Get(context, target.entry->ParentCatalog());
		for (auto &location : existing_indexes) {
			auto indexed_column = location.column_name;
			auto &meta_entry = ResolveExistingTable(context, target.catalog_name, location.shadow_schema,
				                                        location.MetaTable(),
			                                        "ngram index meta table");
			ShadowTarget existing {target.schema_name, target.table_name, indexed_column, location.shadow_schema};
			auto info = ReadMeta(context, transaction, meta_entry, existing);
			if (StringUtil::CIEquals(indexed_column, column_name)) {
				throw InvalidInputException("An ngram index already exists on %s.%s (%s); use drop_ngram_index first",
				                            target.table_name, column_name, location.index_ref);
			}
			vector<string> covered_columns;
			auto reason = RowIdGuardProtectionReason(target.entry->Cast<DuckTableEntry>(), info, column_name,
			                                         covered_columns);
			if (reason.empty() && protector.kind.empty()) {
				// A v3 guard was created behind this phase's ADD/DROP barrier,
				// so its broad column dependency proves every older snapshot was
				// invalidated. It can bridge a later per-column guard install.
				protector = {"ngram_v3", indexed_column, info.guard_name, info.guard_token, 0};
				protector_columns = std::move(covered_columns);
			}
		}
	}
	if (protector.kind.empty()) {
		NativeUpdateProtector native;
		if (FindNativeUpdateProtector(context, target.entry->Cast<DuckTableEntry>(), column_name, native)) {
			protector = {"native_art", "", native.name, to_string(native.oid), native.timestamp};
		}
	}
	if (protector.kind.empty() && !existing_indexes.empty()) {
		throw InvalidInputException(
		    "create_ngram_index: no existing v3 rowid guard safely covers %s; drop and rebuild the table's ngram "
		    "indexes before indexing a VARCHAR column added after their creation barriers",
		    column_name);
	}
	auto guard_name = fresh_guard_name;
	string guard_columns;
	if (protector.kind.empty()) {
		for (auto &definition : target.entry->GetColumns().Physical()) {
			if (definition.Type().id() != LogicalTypeId::VARCHAR) {
				continue;
			}
			if (!guard_columns.empty()) {
				guard_columns += ", ";
			}
			guard_columns += Ident(definition.Name());
		}
	} else if (protector.kind == "ngram_v3") {
		for (auto &protected_column : protector_columns) {
			if (!guard_columns.empty()) {
				guard_columns += ", ";
			}
			guard_columns += Ident(protected_column);
		}
	} else {
		// The existing protector proves only the target's update history. A
		// fresh per-index guard is therefore deliberately target-only here.
		guard_columns = column;
	}
	string script;
	string guard = ScratchName("guard");
	string packed = ScratchName("build_packed");
	// This is the first executed statement. The volatile scalar acquires the
	// target transaction's vacuum fence before checking that the table planned
	// above is still the table the remaining script will scan.
	script += "CREATE TEMP TABLE " + guard + " AS SELECT " +
	          MaintenanceGuardCall(target, column_name, fingerprint, "create_ngram_index", protector, guard_name,
	                               registry_state) +
	          " AS ignored, NULL::VARCHAR AS guard_token;\n";
	// Replacing the physical table invalidates every snapshot that predates the
	// rowid guard. The temporary all-NULL column is dropped immediately; only
	// DuckDB's reservoir sample is intentionally discarded by this pair.
	if (protector.kind.empty()) {
		script += "ALTER TABLE " + base + " ADD COLUMN " + Ident(epoch_name) + " BOOLEAN;\n";
		script += "ALTER TABLE " + base + " DROP COLUMN " + Ident(epoch_name) + ";\n";
	}
	// Every ngram index owns a separate physical guard. The ordinary barrier
	// creates a broad guard for all current VARCHARs; a protector-backed build
	// creates only the target dependency its proof covers.
	script += "CREATE INDEX " + Ident(guard_name) + " ON " + base + " USING " + NGRAM_ROWID_GUARD_TYPE + "(" +
	          guard_columns + ");\n";
	// Retain EXCLUSIVE through this scan-free CREATE in every mode. This one
	// scalar proves the fresh physical guard, captures its internal token, and
	// releases the fence before the postings build begins.
	auto finish = SystemFunction(NGRAM_CREATION_FINISH) + "(" + Lit(target.catalog_name) + ", " +
	              Lit(target.schema_name) + ", " + Lit(target.table_name) + ", " + Lit(column_name) + ", " +
	              Lit(guard_name) + ")";
	script += "UPDATE " + guard + " SET guard_token = " + finish + ";\n";
	if (registry_state.bootstrap) {
		script += "CREATE SCHEMA " + Ident(target.catalog_name) + "." + Ident(REGISTRY_SCHEMA) + ";\n";
		script += "CREATE TABLE " + Ident(target.catalog_name) + "." + Ident(REGISTRY_SCHEMA) + "." +
		          Ident(REGISTRY_TABLE) +
		          "(registry_version INTEGER NOT NULL, index_id UUID PRIMARY KEY, owner_key BLOB UNIQUE NOT NULL, "
		          "schema_name VARCHAR NOT NULL, table_name VARCHAR NOT NULL, column_name VARCHAR NOT NULL);\n";
	}
	script += "INSERT INTO " + Ident(target.catalog_name) + "." + Ident(REGISTRY_SCHEMA) + "." +
	          Ident(REGISTRY_TABLE) + " VALUES (" + to_string(REGISTRY_VERSION) + ", " + Lit(index_ref) +
	          "::UUID, " + SystemFunction("from_hex") + "(" + Lit(Hex(OwnerKey(target.schema_name, target.table_name,
	                                                                           column_name))) +
	          "), " + Lit(target.schema_name) + ", " + Lit(target.table_name) + ", " + Lit(column_name) + ");\n";
	// This custom index has no postings and its build plan never scans the base
	// table. Its physical column dependency rewrites future indexed-column
	// updates to delete+insert, while its non-ART type disables rowid-moving
	// vacuum. Two persisted scalars detect reuse of a truncated trailing range.
	// Only committed rows are indexed. A transaction-local rowid is reassigned
	// at commit, so recording one would leave the index pointing at a rowid
	// that never exists (and, before this filter, made ngram_search fetch a
	// vanished local row and take the database down with an internal error).
	// Uncommitted rows are found by the tail scan instead, and land past the
	// high-water mark when they commit.
	auto committed_only = "rowid < " + to_string(LOCAL_ROWID_START);
	auto committed_hwm = "(SELECT coalesce(" + SystemFunction("max") + "(rowid), -1) FROM " + base + " WHERE " +
	                     committed_only + ")";
	script += "CREATE SCHEMA " + shadow + ";\n";
	script += "CREATE TABLE " + meta + " AS SELECT " + to_string(NGRAM_FORMAT_VERSION) + " AS format_version, " +
	          Lit(target.schema_name) + " AS schema_name, " + Lit(target.table_name) + " AS table_name, " +
	          Lit(column_name) + " AS column_name, " + gram_str + " AS gram_size, " + ci_str +
	          " AS case_insensitive, " +
	          committed_hwm + " AS hwm_rowid, " + Lit(fingerprint.schema_fingerprint) +
	          " AS schema_fingerprint, " + Lit(fingerprint.ColumnType(column_name)) + " AS column_type, " +
	          to_string(fingerprint.table_oid) + "::BIGINT AS table_oid, " + to_string(fingerprint.catalog_oid) +
	          "::BIGINT AS catalog_oid, " + Lit(fingerprint.instance_id) + " AS instance_id, " +
	          Lit(guard_name) + " AS guard_name, (SELECT guard_token FROM " + guard + ") AS guard_token;\n";
	auto partitions =
	    BuildPartitionCount(context, EstimateGramCount(context, *target.entry, column_name, 0,
	                                                   fingerprint.total_rows - 1, NumericCast<idx_t>(gram_size)));
	auto ranges = SegmentAlignedRanges(0, fingerprint.total_rows - 1, partitions);
	for (idx_t i = 0; i < ranges.size(); i++) {
		script += PackPartitionStatement(
		    packed, i == 0,
		    "SELECT rowid AS r, rowid >> " + to_string(SEGMENT_SHIFT) + " AS segment_no, " +
		        SystemFunction("unnest") + "(" +
		        SystemFunction("trigrams") + "(" + column +
		        ", " + gram_str + ", " + ci_str + ")) AS gram FROM " + base +
		        " WHERE rowid >= " + to_string(ranges[i].first) + " AND rowid <= " + to_string(ranges[i].second) +
		        " AND " + column + " IS NOT NULL");
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
	          "SELECT " + SystemFunction("decode") + "(gram_key) AS gram, " + SystemFunction("sum") +
	          "(rowid_count)::BIGINT AS row_count, " +
	          SystemFunction("count") + "(*)::BIGINT AS segment_count FROM " +
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
	// segments are, and whether a detector already knows the index is dead.
	// The staleness verdict and the table's rowid count are read here, in the
	// pragma callback, and embedded as literals.
	auto fingerprint = ComputeTableFingerprint(context, *target.entry);
	auto &transaction = DuckTransaction::Get(context, target.entry->ParentCatalog());

	auto base = Ident(target.catalog_name) + "." + Ident(target.schema_name) + "." + Ident(target.table_name);
	string query;
	std::sort(indexes.begin(), indexes.end(), [](const IndexLocation &left, const IndexLocation &right) {
		return left.column_name < right.column_name;
	});
	for (auto &location : indexes) {
		auto &indexed_column = location.column_name;
		if (!IndexLocationAvailable(context, target, location)) {
			throw InvalidInputException("ngram_index_stats: index storage is unavailable");
		}
		auto shadow = Ident(target.catalog_name) + "." + Ident(location.shadow_schema);
		auto meta = shadow + "." + Ident(location.MetaTable());
		auto segments = shadow + "." + Ident(location.SegmentsTable());
		auto stats = shadow + "." + Ident(location.StatsTable());
		string staleness;
		{
			auto &meta_entry = ResolveExistingTable(context, target.catalog_name, location.shadow_schema,
			                                        location.MetaTable(), "ngram index meta table");
			ShadowTarget shadow_target {target.schema_name, target.table_name, indexed_column, location.shadow_schema};
			// ReadMeta raises the collision error itself when this meta row
			// names a different base table, which is what the pragma should do
			// rather than silently reporting nothing
			auto info = ReadMeta(context, transaction, meta_entry, shadow_target);
			staleness = CertainStaleReason(info, fingerprint);
			if (staleness.empty()) {
				staleness = RowIdGuardReason(context, target.entry->Cast<DuckTableEntry>(), info);
			}
		}
		if (!query.empty()) {
			query += "UNION ALL ";
		}
		// remaining_tail is what a bounded-refresh loop watches from outside the
		// call: the committed rows the index does not cover yet, which every
		// query is currently answering with a tail scan. Counted against the
		// meta row's own mark, so it is exact rather than derived from
		// table_max_rowid (deletes leave rowid gaps).
		query += "SELECT m.column_name, m.gram_size, m.case_insensitive, m.hwm_rowid, " +
		         to_string(fingerprint.total_rows - 1) +
		         "::BIGINT AS table_max_rowid, "
		         "(SELECT " + SystemFunction("count") + "(*) FROM " +
		         base + " WHERE rowid > m.hwm_rowid AND rowid < " + to_string(LOCAL_ROWID_START) +
		         ") AS remaining_tail, "
		         "(SELECT " + SystemFunction("count") + "(DISTINCT " + SystemFunction("encode") + "(gram)) FROM " +
		         stats +
		         ") AS distinct_grams, "
		         "(SELECT " + SystemFunction("count") + "(*) FROM " +
		         segments +
		         ") AS segments, "
		         "(SELECT " + SystemFunction("count") + "(*) FROM (SELECT " + SystemFunction("encode") +
		         "(gram) AS gram_key, segment_no FROM " +
		         segments +
		         " GROUP BY " + SystemFunction("encode") + "(gram), segment_no HAVING " +
		         SystemFunction("count") + "(*) > 1)) AS fragmented_keys, "
		         "(SELECT " + SystemFunction("count") + "(DISTINCT generation) FROM " +
		         segments +
		         ") AS generations, "
		         "(SELECT coalesce(" + SystemFunction("sum") + "(rowid_count), 0) FROM " +
		         segments +
		         ") AS posting_entries, "
		         "(SELECT coalesce(" + SystemFunction("sum") + "(" + SystemFunction("octet_length") +
		         "(postings)), 0) FROM " +
		         segments + ") AS postings_bytes, " + (staleness.empty() ? string("NULL::VARCHAR") : Lit(staleness)) +
		         " AS stale_reason FROM " + meta + " m ";
	}
	query += "ORDER BY column_name;";
	return query;
}

static string ValuesRow(const ObservedIndex &index) {
	auto value = [](const string &text) { return text.empty() ? string("NULL::VARCHAR") : Lit(text); };
	return "(" + Lit(index.catalog_name) + ", " + Lit(index.location.Registered() ? "registered" : "legacy") +
	       ", " + Lit(index.location.index_ref) + ", " + value(index.schema_name) + ", " + value(index.table_name) +
	       ", " + value(index.location.column_name) + ", " + Lit(index.location.shadow_schema) + ", " +
	       (index.format_version < 0 ? "NULL::BIGINT" : to_string(index.format_version) + "::BIGINT") + ", " + Lit(index.status) +
	       ", " + value(index.reason) + ")";
}

static string NgramIndexesQuery(ClientContext &context, const FunctionParameters &) {
	vector<ObservedIndex> indexes;
	for (auto &database : DatabaseManager::Get(context).GetDatabases(context)) {
		if (!database->HasStorageManager() || !database->GetCatalog().IsDuckCatalog()) {
			continue;
		}
		auto observed = ObserveCatalog(context, database->GetName());
		indexes.insert(indexes.end(), std::make_move_iterator(observed.begin()), std::make_move_iterator(observed.end()));
	}
	string query = "SELECT * FROM (VALUES ";
	if (indexes.empty()) {
		query += "(NULL::VARCHAR,NULL::VARCHAR,NULL::VARCHAR,NULL::VARCHAR,NULL::VARCHAR,NULL::VARCHAR,NULL::VARCHAR,"
		         "NULL::BIGINT,NULL::VARCHAR,NULL::VARCHAR)";
	} else {
		for (idx_t i = 0; i < indexes.size(); i++) {
			if (i) {
				query += ",";
			}
			query += ValuesRow(indexes[i]);
		}
	}
	query += ") v(database_name,kind,index_ref,schema_name,table_name,column_name,storage_schema,format_version,status,reason)";
	if (indexes.empty()) {
		query += " WHERE false";
	}
	query += " ORDER BY database_name,index_ref";
	return query;
}

static string NgramIndexStatusQuery(ClientContext &context, const FunctionParameters &parameters) {
	auto index = FindObserved(context, parameters.values[0].ToString(), parameters.values[1].ToString());
	return "SELECT * FROM (VALUES " + ValuesRow(index) +
	       ") v(database_name,kind,index_ref,schema_name,table_name,column_name,storage_schema,format_version,status,reason)";
}

static string DropNgramIndexByIdQuery(ClientContext &context, const FunctionParameters &parameters) {
	auto catalog_name = parameters.values[0].ToString();
	auto index_ref = parameters.values[1].ToString();
	auto database = DatabaseManager::Get(context).GetDatabase(context, catalog_name);
	if (!database || database->IsReadOnly()) {
		throw InvalidInputException("drop_ngram_index_by_id: catalog %s is missing or read-only", catalog_name);
	}
	auto index = FindObserved(context, catalog_name, index_ref);
	if (index.status == "MALFORMED" || index.status == "MISSING_STORAGE") {
		throw InvalidInputException("drop_ngram_index_by_id: index %s is %s: %s; repair the catalog manually",
		                            index_ref, index.status, index.reason);
	}
	ResolvedTarget owner {index.catalog_name, index.schema_name, index.table_name, index.location.column_name,
	                      ShadowSchemaName(index.schema_name, index.table_name), nullptr};
	RequireExclusiveOwnerAllocation(context, owner, index_ref);
	string meta_table = REGISTERED_META;
	string segments_table = REGISTERED_SEGMENTS;
	string stats_table = REGISTERED_STATS;
	if (!index.location.Registered()) {
		string decoded_schema;
		if (!ParseLegacyIndexRef(index_ref, decoded_schema, meta_table) ||
		    decoded_schema != index.location.shadow_schema) {
			throw InvalidInputException("drop_ngram_index_by_id: malformed legacy locator");
		}
		segments_table = index.location.SegmentsTable();
		stats_table = index.location.StatsTable();
	}
	auto shadow = Ident(index.catalog_name) + "." + Ident(index.location.shadow_schema);
	string script;
	auto guard = ScratchName("guard");
	script += "CREATE TEMP TABLE " + guard + " AS SELECT " + SystemFunction(NGRAM_MAINTENANCE_GUARD) +
	          "('drop_ngram_index_by_id', " + Lit(index.catalog_name) + ", " + Lit(index.schema_name) + ", " +
	          Lit(index.table_name) + ", " + Lit(index.location.column_name) +
	          ", false, -1, 0, '', 0, false, '', '', '', '', 0, '', 0, false, " + Lit(index.location.index_ref) +
	          ", " + Lit(index.location.shadow_schema) + ", " + Lit(meta_table) + ", " +
	          to_string(index.location.registry_oid) + ", " + to_string(index.location.schema_oid) + ", " +
	          to_string(index.location.meta_oid) + ", " + to_string(index.location.segments_oid) + ", " +
	          to_string(index.location.stats_oid) +
	          ") AS ignored;\n";
	if (index.format_version == 2 || index.format_version == NGRAM_FORMAT_VERSION) {
		EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, index.table_name);
		auto base = Catalog::GetEntry(context, index.catalog_name, index.schema_name, lookup,
		                              OnEntryNotFound::RETURN_NULL);
		if (base && base->type == CatalogType::TABLE_ENTRY && base->Cast<TableCatalogEntry>().IsDuckTable()) {
			auto &table = base->Cast<DuckTableEntry>();
			auto &meta = ResolveExistingTable(context, index.catalog_name, index.location.shadow_schema, meta_table,
			                                  "ngram index meta table");
			ShadowTarget target {index.schema_name, index.table_name, index.location.column_name,
			                     index.location.shadow_schema};
			string guard_name;
			string guard_token;
			if (index.format_version == NGRAM_FORMAT_VERSION) {
				auto info = ReadMeta(context, DuckTransaction::Get(context, table.ParentCatalog()), meta, target);
				guard_name = info.guard_name;
				guard_token = info.guard_token;
			}
			script += SilentGuard(guard, SystemFunction(NGRAM_ROWID_GUARD_VALIDATE) + "(" +
			                                Lit(index.catalog_name) + ", " + Lit(index.schema_name) + ", " +
			                                Lit(index.table_name) + ", " + Lit(index.location.column_name) + ", " +
			                                Lit(index.location.shadow_schema) + ", " +
			                                to_string(index.format_version) + ", " +
			                                to_string(index.location.meta_oid) + ", " + Lit(guard_name) + ", " +
			                                Lit(guard_token) + ", " + Lit(meta_table) + ")");
			if (index.format_version == NGRAM_FORMAT_VERSION) {
				script += "DROP INDEX IF EXISTS " + Ident(index.catalog_name) + "." + Ident(index.schema_name) + "." +
				          Ident(guard_name) + ";\n";
			}
		}
	}
	if (index.location.registry_oid) {
		script += "DELETE FROM " + Ident(index.catalog_name) + "." + Ident(REGISTRY_SCHEMA) + "." +
		          Ident(REGISTRY_TABLE) + " WHERE index_id = " + Lit(index_ref) + "::UUID;\n";
	}
	script += "DROP TABLE " + shadow + "." + Ident(meta_table) + ";\n";
	script += "DROP TABLE " + shadow + "." + Ident(segments_table) + ";\n";
	script += "DROP TABLE " + shadow + "." + Ident(stats_table) + ";\n";
	bool drop_schema = index.location.Registered();
	if (!index.location.Registered()) {
		auto schema = Catalog::GetSchema(context, index.catalog_name, index.location.shadow_schema,
		                                 OnEntryNotFound::THROW_EXCEPTION);
		drop_schema = ForeignSchemaEntries(context, *schema, {meta_table, segments_table, stats_table}) == 0;
	}
	if (drop_schema) {
		script += "DROP SCHEMA " + shadow + ";\n";
	}
	if (index.location.Registered() && !index.location.registry_oid) {
		auto registry = ReadRegistry(context, index.catalog_name);
		auto reserved = Catalog::GetSchema(context, index.catalog_name, REGISTRY_SCHEMA,
		                                   OnEntryNotFound::RETURN_NULL);
		if (!registry.oid && reserved && ForeignSchemaEntries(context, *reserved, {}) == 0) {
			// A whole-registry deletion leaves this empty reserved schema behind.
			// Plain DROP (never CASCADE) makes concurrent object creation roll back.
			script += "DROP SCHEMA " + Ident(index.catalog_name) + "." + Ident(REGISTRY_SCHEMA) + ";\n";
		}
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
