//===----------------------------------------------------------------------===//
//                         ngram
//
// ngram/trigram.hpp
//
// Shared gram extraction and needle decomposition. Normalization (case
// folding) is defined here and nowhere else: index build and query-time
// needle decomposition both go through NormalizeString, so the two sides can
// never disagree — a mismatch is the one bug class that silently drops rows.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace ngram {

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

struct NeedleDecomposition {
	//! Deduplicated grams in first-occurrence order.
	vector<string> grams;
	//! Needle has fewer than gram_size codepoints: the index cannot be probed and the
	//! caller must fall back to a full scan (which is still exhaustive).
	bool too_short = false;
};

NeedleDecomposition DecomposeNeedle(const char *data, idx_t len, const GramOptions &options);

void RegisterTrigramsFunction(ExtensionLoader &loader);

} // namespace ngram
} // namespace duckdb
