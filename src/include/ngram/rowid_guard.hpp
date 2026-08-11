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

struct NativeUpdateProtector {
	NativeUpdateProtector() = default;
	NativeUpdateProtector(string name_p, idx_t oid_p, transaction_t timestamp_p)
	    : name(std::move(name_p)), oid(oid_p), timestamp(timestamp_p) {
	}

	string name;
	idx_t oid = 0;
	transaction_t timestamp = 0;
};

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

//! Protection-only validation for creating a fresh snapshot. Ignores the old
//! index's max/unsafe state, but pins its exact guard incarnation and returns
//! the physical column names that guard protects.
string RowIdGuardProtectionReason(DuckTableEntry &table, const MetaInfo &meta, const string &column_name,
                                  vector<string> &protected_columns);

//! Find a committed, bound native ART whose dependency already forces updates
//! of `column_name` through delete+insert. The creation barrier pins and
//! revalidates this exact catalog incarnation before relying on it.
bool FindNativeUpdateProtector(ClientContext &context, DuckTableEntry &table, const string &column_name,
                               NativeUpdateProtector &result);

//! Empty when the exact native protector is still bound to the same table and
//! column with the pinned catalog identity and creation timestamp.
string NativeUpdateProtectorReason(ClientContext &context, DuckTableEntry &table, const string &column_name,
                                   const NativeUpdateProtector &expected);

} // namespace ngram
} // namespace duckdb
