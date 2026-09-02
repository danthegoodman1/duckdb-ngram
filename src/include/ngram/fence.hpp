//===----------------------------------------------------------------------===//
// ngram/fence.hpp: the vacuum fence and creation barrier generated scripts hold, and the execution-time scalars that
// re-check what the pragma callback planned.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "ngram/catalog.hpp"

namespace duckdb {

class DuckTransactionManager;

namespace ngram {

//! Internal execution-time functions used by generated maintenance scripts.
constexpr const char *NGRAM_MAINTENANCE_GUARD = "__ngram_maintenance_guard";
constexpr const char *NGRAM_CREATION_FINISH = "__ngram_creation_finish";

//! The checks a generated script repeats at execution time, prepared by the
//! pragma callback and handed to __ngram_maintenance_guard through a handle.
struct PreparedMaintenance {
	enum class Kind : uint8_t { CREATE, DROP, MAINTAIN };
	Kind kind = Kind::MAINTAIN;
	//! The pragma, for messages.
	string fn;
	//! The base table; `entry` is resolved again at execution time.
	ResolvedTarget target;
	//! The registry row the script was generated from. CREATE: the guard the
	//! script installs (empty token) or shares, and the registry oid it expects.
	IndexLocation location;
	//! MAINTAIN: the metadata the script was generated from.
	MetaInfo meta;
	//! CREATE: the registry did not exist when the script was generated.
	bool bootstrap = false;
	//! DROP: this row is the guard's last reference, so the script drops it.
	bool drop_guard = false;
};

//! A fresh group for the handles of one generated script. A script's handles
//! (one per column of a refresh or compact) run in one transaction, and
//! consuming any of them binds the rest to that transaction.
uint64_t NewMaintenanceGroup(ClientContext &context);

//! Store `prepared` on the connection under `group` and return the scalar call
//! that consumes it, the first executed statement of every generated script. A
//! handle waits through the expanding transaction's commit and through
//! unrelated statements until its script runs. The end of the transaction that
//! consumed a handle of its group drops the group's unconsumed handles, and a
//! pragma expanded in a later transaction drops the handles of every earlier
//! expansion.
string PreparedMaintenanceCall(ClientContext &context, uint64_t group, PreparedMaintenance prepared);

//! True while this context owns the creation EXCLUSIVE for `manager`. Seal
//! readers can then use that lock instead of trying to nest a shared lock.
bool ContextOwnsCreationBarrier(ClientContext &context, DuckTransactionManager &manager);

//! Registers the execution-time scalars.
void RegisterFence(ExtensionLoader &loader);

} // namespace ngram
} // namespace duckdb
