//===----------------------------------------------------------------------===//
//                         ngram
//
// ngram/rowid_guard.hpp
//
// A zero-posting DuckDB index used only to keep rowids stable and to detect
// reuse of a vacuumed trailing rowid range.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {

class DuckTableEntry;

namespace ngram {

struct MetaInfo;

constexpr const char *NGRAM_ROWID_GUARD_TYPE = "NGRAM_ROWID_GUARD";
constexpr const char *NGRAM_ROWID_GUARD_VALIDATE = "__ngram_rowid_guard_validate";

//! Record the runtime reported by the host's built-in pragma_version(). A
//! mismatch latches fail-closed for this process but does not prevent loading
//! the extension to inspect or drop an existing guard.
void InitializeRowIdGuardHostRuntime(ExtensionLoader &loader);

//! Register the custom non-ART index type.
void RegisterRowIdGuard(ExtensionLoader &loader);

//! Return the internally minted token of an exact, freshly installed guard.
//! Throws before the creation fence is released when the physical guard does
//! not match the planned table, name, type, or target-column dependency.
string InstalledRowIdGuardToken(DuckTableEntry &table, const string &column_name, const string &guard_name);

//! Empty when the exact guard recorded by meta proves the indexed rowid
//! prefix safe; otherwise a reason that makes callers scan or reject.
string RowIdGuardReason(ClientContext &context, DuckTableEntry &table, const MetaInfo &meta);

} // namespace ngram
} // namespace duckdb
