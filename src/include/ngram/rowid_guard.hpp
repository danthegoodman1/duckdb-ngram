//===----------------------------------------------------------------------===//
// ngram/rowid_guard.hpp: the NGRAM_ROWID_GUARD index type: a zero-posting DuckDB index that keeps rowids stable and
// records reuse of a vacuumed trailing range.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/execution/index/bound_index.hpp"

namespace duckdb {

class AttachedDatabase;
class DuckTableEntry;
class Index;

namespace ngram {

constexpr const char *NGRAM_ROWID_GUARD_TYPE = "NGRAM_ROWID_GUARD";
//! The persisted storage option holding a guard's incarnation token.
constexpr const char *NGRAM_GUARD_TOKEN_OPTION = "ngram_guard_token";

//! Record the runtime reported by the host's built-in pragma_version(). A
//! mismatch latches fail-closed for this process but does not prevent loading
//! the extension to inspect or drop an existing guard.
void InitializeRowIdGuardHostRuntime(ExtensionLoader &loader);

//! Register the custom non-ART index type and the bind-at-load callbacks.
void RegisterRowIdGuard(ExtensionLoader &loader);

//! True when the host is the pinned DuckDB build; the guard's checkpoint
//! internals are only trusted then.
bool RowIdGuardRuntimeCompatible();
void RequirePinnedRuntime();
string HostRuntimeMismatchReason();

//! The header checkpoint iteration of `db` (0 for an in-memory database).
uint64_t CheckpointIteration(AttachedDatabase &db);

//! What a guard persists in its index storage options, as read back.
struct StoredGuardState {
	string token;
	int64_t max_seen = -1;
	bool unsafe_reuse = true;
	bool protection_compatible = false;
	optional_idx checkpoint_iteration;
};
StoredGuardState ReadStoredGuardState(const IndexStorageInfo &storage);

//! The state a read path needs from one guard.
struct RowIdGuardState {
	string token;
	int64_t max_seen;
	bool unsafe_reuse;
	bool protection_compatible;
	vector<column_t> column_ids;
};

//! The live state of a bound guard, applying its pending persisted seal
//! against `current_iteration` when one is given.
RowIdGuardState ReadBoundGuardState(Index &index, optional_idx current_iteration);

//! Whether every unbound guard on `table` is well-formed enough to bind.
//! DuckDB's v1.5.5 bind state stays poisoned by a malformed expression, so
//! callers screen first.
bool CanBindRowIdGuards(DuckTableEntry &table, bool require_compatible);

} // namespace ngram
} // namespace duckdb
