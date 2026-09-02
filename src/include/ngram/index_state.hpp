//===----------------------------------------------------------------------===//
// ngram/index_state.hpp: what a read path establishes before touching postings: the rowid guard verdict, registry-row
// validation, and lifecycle classification.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "ngram/catalog.hpp"

namespace duckdb {

class DuckTableEntry;

namespace ngram {

enum class IndexAvailability : uint8_t {
	//! The registry row the operation was prepared against is still there.
	AVAILABLE,
	//! The row, or the registry table, is gone.
	ABSENT,
	//! The registry table was replaced, its row now names another owner, or the
	//! registry cannot be read; `reason` says which.
	CHANGED
};

//! What every read path establishes before it touches postings.
struct IndexVerdict {
	IndexAvailability availability = IndexAvailability::ABSENT;
	//! CHANGED: what changed. AVAILABLE: the rowid guard's objection, empty when
	//! the postings still describe the table's rows exactly.
	string reason;
	//! AVAILABLE: the row's metadata.
	MetaInfo meta;
};

//! The registry part of the verdict alone: whether the row is still there. The
//! drop path uses it, whose row may be unusable and whose table may be gone.
IndexVerdict LocateIndex(ClientContext &context, const ResolvedTarget &target, const IndexLocation &location);

//! LocateIndex plus the rowid guard verdict against `target.entry`. A row this
//! extension cannot read (other format, corrupt values) raises with its reason.
//! Callers keep their own policy on a non-empty reason: scan, error, or refuse.
IndexVerdict ValidateIndex(ClientContext &context, const ResolvedTarget &target, const IndexLocation &location);

//! An indexed column of a base table, resolved against the registry, with its
//! metadata already read and its guard verdict already passed.
struct MaintenanceColumn {
	string column_name;
	IndexLocation location;
	MetaInfo meta;
};

//! Read and validate one indexed column for `fn` (ngram_refresh or
//! ngram_compact): the column exists, the row is still there, and the guard
//! proves the index maintainable; anything else raises with the remedy. Used
//! while expanding the pragma for early errors and again behind the fence at
//! execution time, where it is the correctness check.
MaintenanceColumn ResolveMaintenanceColumn(ClientContext &context, const char *fn, const ResolvedTarget &target,
                                           const IndexLocation &location);

//! Empty when the exact guard recorded by meta proves the indexed rowid
//! prefix safe; otherwise a reason that makes callers scan or reject.
string RowIdGuardReason(ClientContext &context, DuckTableEntry &table, const MetaInfo &meta);

//! Empty when the guard named by a registry row is absent or is exactly the
//! recorded incarnation (name, type, table, token), binding it first when it
//! is unbound; otherwise why a drop must not touch the index carrying that
//! name.
string RowIdGuardDropReason(ClientContext &context, DuckTableEntry &table, const string &guard_name,
                            const string &guard_token, bool bind_unbound = true);

//! Return the internally minted token of an exact, freshly installed guard.
//! Throws before the creation fence is released when the physical guard does
//! not match the planned table, name, type, or target-column dependency.
string InstalledRowIdGuardToken(DuckTableEntry &table, const string &column_name, const string &guard_name);

//! One index as PRAGMA ngram_indexes lists it: its owner, its status (READY,
//! SCAN_ONLY, ORPHAN, MALFORMED) and the reason behind any other status.
struct ObservedIndex {
	string catalog_name;
	string schema_name, table_name;
	string status;
	string reason;
	IndexLocation location;
	int64_t format_version = -1;
	bool legacy = false;
};

//! Every index and stray storage object of one DuckDB catalog.
vector<ObservedIndex> ObserveCatalog(ClientContext &context, const string &catalog_name);

//! The index `index_ref` of `catalog_name`; raises when the catalog is not an
//! attached DuckDB database or lists no such index.
ObservedIndex FindObserved(ClientContext &context, const string &catalog_name, const string &index_ref);

} // namespace ngram
} // namespace duckdb
