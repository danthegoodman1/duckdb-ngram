//===----------------------------------------------------------------------===//
// ngram/gram.hpp: normalization, gram extraction, needle decomposition, and trigrams().
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/unordered_set.hpp"

namespace duckdb {
namespace ngram {

//! Normalization (case folding) is defined once, here: index build and
//! query-time needle decomposition both go through NormalizeString, so the two
//! sides cannot disagree, which is the one bug class that silently drops rows.
struct GramOptions {
	idx_t gram_size = 3;
	bool case_insensitive = true;
};

//! Writes the normalized copy of (data, len) into `normalized` and the byte offset of
//! every codepoint boundary (including the end) into `offsets`. Normalization is
//! per-codepoint lowercase via utf8proc, matching DuckDB's lower().
void NormalizeString(const char *data, idx_t len, const GramOptions &options, string &normalized,
                     vector<idx_t> &offsets);

//! Calls emit(gram_ptr, gram_byte_len) for every window of gram_size codepoints, in
//! order, duplicates included. Strings with fewer than gram_size codepoints emit
//! nothing. `scratch` and `offsets` are reusable buffers; the emitted pointers are
//! valid into `scratch` until the next call.
template <class CALLBACK>
void ExtractGrams(const char *data, idx_t len, const GramOptions &options, string &scratch, vector<idx_t> &offsets,
                  CALLBACK &&emit) {
	NormalizeString(data, len, options, scratch, offsets);
	// offsets holds codepoint_count + 1 entries
	idx_t codepoints = offsets.size() - 1;
	if (codepoints < options.gram_size) {
		return;
	}
	for (idx_t i = 0; i + options.gram_size <= codepoints; i++) {
		emit(scratch.data() + offsets[i], offsets[i + options.gram_size] - offsets[i]);
	}
}

//! The bits a gram key keeps. Every key is ANDed with the process-wide mask
//! before it is stored or probed. The default keeps every bit; the C++
//! harness narrows it to force distinct grams of one size onto one key and
//! prove that recheck removes the widened candidates. Set it before an index
//! is built and keep it for that index's lifetime: a key derived under one
//! mask is only found under the same mask.
struct GramKeyMask {
	GramKeyMask() = default;
	GramKeyMask(uint64_t upper_p, uint64_t lower_p) : upper(upper_p), lower(lower_p) {
	}

	uint64_t upper = ~uint64_t(0);
	uint64_t lower = ~uint64_t(0);
};

//! The current mask, read once per chunk or needle rather than per gram.
GramKeyMask CurrentGramKeyMask();

//! The collision seam for tests; production code never calls this.
void SetGramKeyMask(GramKeyMask mask);

//! The storage key of a normalized gram: the leading sorted column of the
//! segments table, so a probe's `gram_key = ?` is a native fixed-width filter.
//! Grams of at most 16 bytes (every 3- and 4-gram, since a codepoint is at
//! most 4 bytes) are byte-packed big-endian into the high bytes with zero
//! padding below, which preserves byte order; longer grams hash to 64 bits in
//! the low half under an all-ones high half. The result is ANDed with `mask`.
//!
//! Collisions only widen. Within one index every gram has gram_size
//! codepoints, so two distinct byte-packed grams share a key only if one is
//! the other plus trailing NUL codepoints, which changes the codepoint count;
//! no byte-packed key of valid UTF-8 starts with eight 0xFF bytes, so it never
//! meets a hashed key; two hashed grams collide with probability 2^-64. A
//! collision merges the grams' postings under one key, so each gram's
//! candidate set becomes a superset and recheck removes the excess. Build and
//! query derive keys with this one function, so a query gram always finds the
//! rows the build wrote for it. The harness's `TestGramKeyCollisions` forces
//! same-size collisions of every packing shape through the mask.
uhugeint_t GramKey(const char *data, idx_t len, const GramKeyMask &mask);

struct GramKeyHash {
	size_t operator()(const uhugeint_t &key) const {
		return Hash(key);
	}
};

//! The keys of a query's distinct grams in first-occurrence order. Order
//! breaks ties when the rarest grams are selected, so it is deterministic.
class NeedleKeys {
public:
	//! Adds a key unless it is present; false when adding would exceed
	//! `max_keys` distinct keys.
	bool Add(uhugeint_t key, idx_t max_keys);

	vector<uhugeint_t> keys;

private:
	unordered_set<uhugeint_t, GramKeyHash> seen;
};

enum class NeedleShape : uint8_t {
	//! At least one gram; its keys were added.
	PROBEABLE,
	//! Fewer than gram_size codepoints: the index cannot be probed and the
	//! caller falls back to a full scan, which is still exhaustive.
	TOO_SHORT,
	//! The needle's bytes or distinct grams exceed `max_keys`, the count the
	//! query memory budget admits; the caller declines the probe.
	OVER_BUDGET
};

//! Add the keys of the needle's distinct grams to `keys`, in one pass with a
//! cancellation check every few thousand grams.
NeedleShape DecomposeNeedle(ClientContext &context, const char *data, idx_t len, const GramOptions &options,
                            idx_t max_keys, NeedleKeys &keys);

void RegisterGram(ExtensionLoader &loader);

} // namespace ngram
} // namespace duckdb
