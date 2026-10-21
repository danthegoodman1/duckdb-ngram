//===----------------------------------------------------------------------===//
// ngram/gram.hpp: normalization, gram extraction, needle decomposition, and trigrams().
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/unordered_set.hpp"

namespace duckdb {
namespace ngram {

//! How an index cuts text into grams.
enum class GramTokenizer : uint8_t {
	//! Every window of exactly min_gram codepoints.
	NGRAM,
	//! Every sparse gram of min_gram to max_gram codepoints: the windows whose
	//! inner (min_gram - 1)-codepoint windows all weigh less than both of its
	//! border windows (docs/sparse-grams.md). Every min_gram window is one, so
	//! a sparse index holds everything an ngram index of min_gram does.
	SPARSE
};

//! Normalization (case folding) and tokenizing are defined once, here: index
//! build and query-time needle decomposition both go through ExtractGramRanges,
//! so the two sides cannot disagree, which is the one bug class that silently
//! drops rows.
struct GramOptions {
	GramTokenizer tokenizer = GramTokenizer::NGRAM;
	//! The shortest gram, in codepoints; an ngram index's only gram length.
	idx_t min_gram = 3;
	//! The longest gram, in codepoints; equal to min_gram for an ngram index.
	idx_t max_gram = 3;
	bool case_insensitive = true;
};

//! The longest sparse gram an index may hold, in codepoints.
constexpr idx_t MAX_SPARSE_GRAM = 64;
//! A sparse index's gram lengths unless the index names its own.
constexpr idx_t DEFAULT_SPARSE_MIN_GRAM = 3;
constexpr idx_t DEFAULT_SPARSE_MAX_GRAM = 8;

//! The tokenizer's SQL name: ngram or sparse.
const char *TokenizerName(GramTokenizer tokenizer);
//! The tokenizer named `name`, case-insensitively; throws naming `fn` for any other name.
GramTokenizer ParseTokenizer(const string &name, const char *fn);
//! Raises naming `fn` unless `options` describe an index this extension can build.
void ValidateGramOptions(const GramOptions &options, const char *fn);
//! The options as a reader would write them: ngram(3), sparse(3, 16).
string TokenizerSpec(const GramOptions &options);

//! Writes the normalized copy of (data, len) into `normalized` and the byte offset of
//! every codepoint boundary (including the end) into `offsets`. Normalization is
//! per-codepoint lowercase via utf8proc, matching DuckDB's lower().
void NormalizeString(const char *data, idx_t len, const GramOptions &options, string &normalized,
                     vector<idx_t> &offsets);

struct SparseWindow {
	uint64_t weight;
	idx_t position;
};

//! Reusable buffers for gram extraction. `normalized` and `offsets` hold the
//! last input's normalized text and codepoint boundaries, which the emitted
//! ranges index.
struct GramScratch {
	string normalized;
	vector<idx_t> offsets;
	vector<SparseWindow> windows;
};

//! MurmurHash3's 64-bit finalizer, a bijection.
inline uint64_t Fmix64(uint64_t value) {
	value ^= value >> 33;
	value *= 0xff51afd7ed558ccdULL;
	value ^= value >> 33;
	value *= 0xc4ceb9fe1a85ec53ULL;
	value ^= value >> 33;
	return value;
}

//! The sparse weight of the normalized window of `count` (at least two)
//! codepoints starting at codepoint `first`. Each codepoint enters as its
//! UTF-8 bytes packed big-endian, which differ for different codepoints; a
//! two-codepoint window, a sparse(3, *) index's, mixes the pair through one
//! bijection, so distinct windows never tie. The function is part of the
//! storage format.
inline uint64_t SparseWindowWeight(const GramScratch &scratch, idx_t first, idx_t count) {
	auto packed = [&](idx_t codepoint) {
		uint64_t value = 0;
		for (auto i = scratch.offsets[codepoint]; i < scratch.offsets[codepoint + 1]; i++) {
			value = (value << 8) | static_cast<uint8_t>(scratch.normalized[i]);
		}
		return value;
	};
	auto weight = Fmix64((packed(first) << 32) | packed(first + 1));
	for (idx_t i = 2; i < count; i++) {
		weight = Fmix64(weight ^ packed(first + i));
	}
	return weight;
}

//! Calls emit(first, end) with the codepoint range [first, end) of every gram
//! of the normalized input, duplicates included. An ngram index emits its
//! windows left to right. A sparse index emits in one pass over a stack of
//! windows whose weights strictly decrease: a window pops every lighter one,
//! pairing with each, then pairs with the heavier one left on top, then
//! retires the top's equals, so each pair it emits has only lighter windows
//! between its borders. A border too far away for max_gram clears the stack,
//! since every window beneath it lies further still. Strings with fewer than
//! min_gram codepoints emit nothing.
template <class CALLBACK>
void ExtractGramRanges(const char *data, idx_t len, const GramOptions &options, GramScratch &scratch, CALLBACK &&emit) {
	NormalizeString(data, len, options, scratch.normalized, scratch.offsets);
	// offsets holds codepoint_count + 1 entries
	idx_t codepoints = scratch.offsets.size() - 1;
	if (codepoints < options.min_gram) {
		return;
	}
	if (options.tokenizer == GramTokenizer::NGRAM) {
		for (idx_t i = 0; i + options.min_gram <= codepoints; i++) {
			emit(i, i + options.min_gram);
		}
		return;
	}
	auto span = options.min_gram - 1;
	auto &stack = scratch.windows;
	stack.clear();
	for (idx_t right = 0; right + span <= codepoints; right++) {
		auto weight = SparseWindowWeight(scratch, right, span);
		auto end = right + span;
		while (!stack.empty() && stack.back().weight < weight) {
			if (end - stack.back().position > options.max_gram) {
				stack.clear();
				break;
			}
			emit(stack.back().position, end);
			stack.pop_back();
		}
		if (!stack.empty() && end - stack.back().position <= options.max_gram) {
			emit(stack.back().position, end);
		}
		while (!stack.empty() && stack.back().weight == weight) {
			stack.pop_back();
		}
		stack.push_back(SparseWindow {weight, right});
	}
}

//! ExtractGramRanges, calling emit(gram_ptr, gram_byte_len) with each gram's
//! normalized bytes, which stay valid until the scratch's next use.
template <class CALLBACK>
void ExtractGrams(const char *data, idx_t len, const GramOptions &options, GramScratch &scratch, CALLBACK &&emit) {
	ExtractGramRanges(data, len, options, scratch, [&](idx_t first, idx_t end) {
		auto begin = scratch.offsets[first];
		emit(scratch.normalized.data() + begin, scratch.offsets[end] - begin);
	});
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
//! Collisions only widen. Two distinct byte-packed grams share a key only if
//! one is the other plus trailing NUL codepoints, which no index holds
//! together, since every gram an index byte-packs has min_gram codepoints; no
//! byte-packed key of valid UTF-8 starts with eight 0xFF bytes, so it never
//! meets a hashed key; two hashed grams collide with probability 2^-64. A
//! collision merges the grams' postings under one key, so each gram's
//! candidate set becomes a superset and recheck removes the excess. Build and
//! query derive keys with this one function, so a query gram always finds the
//! rows the build wrote for it. The harness's `TestGramKeyCollisions` forces
//! same-size collisions of every packing shape through the mask.
uhugeint_t GramKey(const char *data, idx_t len, const GramKeyMask &mask);

//! The upper half of every sparse bucket key. Byte-packed keys of valid UTF-8
//! never start with 0xFE or 0xFF and hashed keys carry all ones, so bucket
//! keys meet neither.
constexpr uint64_t SPARSE_BUCKET_KEY_UPPER = 0xFFFFFFFFFFFFFFFEULL;

//! A sparse index hashes each gram longer than min_gram into one of
//! 2^SPARSE_BUCKET_BITS buckets. Most long grams occur once, and a segment row
//! per distinct one made a sparse(3, 16) index of enwik9 84 million rows,
//! 60 million of them holding one posting; bucketing bounds the rows a
//! segment can hold. A bucket's postings are its grams' union, a superset the
//! intersection with the needle's other grams and the recheck narrow: on
//! enwik9 the candidates stayed within 1% of exact keys while the index shrank
//! from 4.6 to 2.9 GB (docs/sparse-grams.md).
constexpr idx_t SPARSE_BUCKET_BITS = 20;

//! The key of the gram [first, end) of `scratch`'s normalized text under
//! `options`: GramKey for every gram of an ngram index and for a sparse
//! index's min_gram grams, so a sparse index holds an ngram index's exact
//! keys, and SPARSE_BUCKET_KEY_UPPER over the gram's bucket for a longer
//! sparse gram. Build and query both key through here.
uhugeint_t TokenizedGramKey(const GramOptions &options, const GramScratch &scratch, idx_t first, idx_t end,
                            const GramKeyMask &mask);

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
	//! Fewer than min_gram codepoints: the index cannot be probed and the
	//! caller falls back to a full scan, which is still exhaustive.
	TOO_SHORT,
	//! The needle's bytes or distinct grams exceed `max_keys`, the count the
	//! query memory budget admits; the caller declines the probe.
	OVER_BUDGET
};

//! Add the keys of the grams a probe for the needle reads to `keys`, in one
//! pass with a cancellation check every few thousand grams: every gram for an
//! ngram index, and for a sparse index the covering grams, those no other
//! gram of the needle contains. A row holding a gram holds every gram inside
//! it, so a contained gram's postings include the container's and add
//! nothing to the intersection.
NeedleShape DecomposeNeedle(ClientContext &context, const char *data, idx_t len, const GramOptions &options,
                            idx_t max_keys, NeedleKeys &keys);

//! The normalized text of the first gram among `needles` that `options` key
//! to each of `keys`, quoted as SQL literals, for EXPLAIN ANALYZE; "?" for a
//! key none of them yields.
vector<string> GramTexts(const GramOptions &options, const vector<string> &needles, const vector<uhugeint_t> &keys);

void RegisterGram(ExtensionLoader &loader);

} // namespace ngram
} // namespace duckdb
