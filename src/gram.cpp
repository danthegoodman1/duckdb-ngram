#include "ngram/gram.hpp"

#include "duckdb/common/vector/list_vector.hpp"
#include "utf8proc_wrapper.hpp"

#include <algorithm>
#include <atomic>
#include <limits>

namespace duckdb {
namespace ngram {

//! Grams up to this many bytes are byte-packed; longer ones are hashed.
constexpr idx_t PACKED_KEY_BYTES = 16;

static std::atomic<uint64_t> key_mask_upper {~uint64_t(0)};
static std::atomic<uint64_t> key_mask_lower {~uint64_t(0)};

GramKeyMask CurrentGramKeyMask() {
	GramKeyMask mask;
	mask.upper = key_mask_upper.load(std::memory_order_relaxed);
	mask.lower = key_mask_lower.load(std::memory_order_relaxed);
	return mask;
}

void SetGramKeyMask(GramKeyMask mask) {
	key_mask_upper.store(mask.upper, std::memory_order_relaxed);
	key_mask_lower.store(mask.lower, std::memory_order_relaxed);
}

uhugeint_t GramKey(const char *data, idx_t len, const GramKeyMask &mask) {
	if (len <= PACKED_KEY_BYTES) {
		uint64_t upper = 0;
		uint64_t lower = 0;
		for (idx_t i = 0; i < len; i++) {
			auto byte = static_cast<uint64_t>(static_cast<uint8_t>(data[i]));
			if (i < 8) {
				upper |= byte << ((7 - i) * 8);
			} else {
				lower |= byte << ((15 - i) * 8);
			}
		}
		return uhugeint_t(upper & mask.upper, lower & mask.lower);
	}
	// FNV-1a; any fixed function works, since collisions only widen and the
	// key never leaves this extension's storage
	uint64_t hash = 14695981039346656037ULL;
	for (idx_t i = 0; i < len; i++) {
		hash ^= static_cast<uint64_t>(static_cast<uint8_t>(data[i]));
		hash *= 1099511628211ULL;
	}
	return uhugeint_t(std::numeric_limits<uint64_t>::max() & mask.upper, hash & mask.lower);
}

uhugeint_t TokenizedGramKey(const GramOptions &options, const GramScratch &scratch, idx_t first, idx_t end,
                            const GramKeyMask &mask) {
	auto begin = scratch.offsets[first];
	auto data = scratch.normalized.data() + begin;
	auto len = scratch.offsets[end] - begin;
	if (options.tokenizer == GramTokenizer::NGRAM || end - first == options.min_gram) {
		return GramKey(data, len, mask);
	}
	uint64_t hash = 14695981039346656037ULL;
	for (idx_t i = 0; i < len; i++) {
		hash ^= static_cast<uint64_t>(static_cast<uint8_t>(data[i]));
		hash *= 1099511628211ULL;
	}
	auto bucket = Fmix64(hash) >> (64 - SPARSE_BUCKET_BITS);
	return uhugeint_t(SPARSE_BUCKET_KEY_UPPER & mask.upper, bucket & mask.lower);
}

void NormalizeString(const char *data, idx_t len, const GramOptions &options, string &normalized,
                     vector<idx_t> &offsets) {
	normalized.clear();
	offsets.clear();
	for (idx_t i = 0; i < len;) {
		offsets.push_back(normalized.size());
		auto byte = static_cast<uint8_t>(data[i]);
		if (byte < 0x80) {
			auto c = options.case_insensitive ? StringUtil::ASCII_TO_LOWER_MAP[byte] : byte;
			normalized.push_back(static_cast<char>(c));
			i++;
			continue;
		}
		// DuckDB guarantees VARCHARs hold valid UTF-8; on invalid input
		// UTF8ToCodepoint raises an engine exception rather than returning,
		// so sz is always the positive length of the decoded codepoint here
		int sz = 0;
		auto codepoint = Utf8Proc::UTF8ToCodepoint(data + i, sz, len - i);
		auto folded = options.case_insensitive ? Utf8Proc::CodepointToLower(codepoint) : codepoint;
		char utf8_bytes[4];
		int utf8_sz = 0;
		if (Utf8Proc::CodepointToUtf8(folded, utf8_sz, utf8_bytes)) {
			normalized.append(utf8_bytes, static_cast<idx_t>(utf8_sz));
		} else {
			normalized.append(data + i, static_cast<idx_t>(sz));
		}
		i += static_cast<idx_t>(sz);
	}
	offsets.push_back(normalized.size());
}

const char *TokenizerName(GramTokenizer tokenizer) {
	return tokenizer == GramTokenizer::SPARSE ? "sparse" : "ngram";
}

GramTokenizer ParseTokenizer(const string &name, const char *fn) {
	if (StringUtil::CIEquals(name, "ngram")) {
		return GramTokenizer::NGRAM;
	}
	if (StringUtil::CIEquals(name, "sparse")) {
		return GramTokenizer::SPARSE;
	}
	throw InvalidInputException("%s: tokenizer must be 'ngram' or 'sparse', got '%s'", fn, name);
}

void ValidateGramOptions(const GramOptions &options, const char *fn) {
	if (options.tokenizer == GramTokenizer::NGRAM) {
		if (options.min_gram < 1 || options.max_gram != options.min_gram) {
			throw InvalidInputException("%s: an ngram tokenizer needs one gram size of at least 1", fn);
		}
		return;
	}
	if (options.min_gram < 3) {
		throw InvalidInputException("%s: min_gram must be at least 3 for the sparse tokenizer, got %llu", fn,
		                            options.min_gram);
	}
	if (options.max_gram < options.min_gram || options.max_gram > MAX_SPARSE_GRAM) {
		throw InvalidInputException("%s: max_gram must lie between min_gram (%llu) and %llu, got %llu", fn,
		                            options.min_gram, MAX_SPARSE_GRAM, options.max_gram);
	}
}

string TokenizerSpec(const GramOptions &options) {
	if (options.tokenizer == GramTokenizer::NGRAM) {
		return StringUtil::Format("ngram(%llu)", options.min_gram);
	}
	return StringUtil::Format("sparse(%llu, %llu)", options.min_gram, options.max_gram);
}

bool NeedleKeys::Add(uhugeint_t key, idx_t max_keys) {
	if (seen.find(key) != seen.end()) {
		return true;
	}
	if (keys.size() >= max_keys) {
		return false;
	}
	seen.insert(key);
	keys.push_back(key);
	return true;
}

static constexpr idx_t GRAMS_PER_INTERRUPT_CHECK = 4096;

//! Needle bytes admitted per key of the probe budget. The normalized copy and
//! its codepoint offsets take about nine bytes per needle byte; a sparse
//! needle adds a window stack and a covering stack of up to sixteen bytes per
//! codepoint each. Sixteen and four bytes per key keep either inside the
//! per-key allowance the budget was derived from, whatever the needle's grams.
static constexpr idx_t NGRAM_NEEDLE_BYTES_PER_KEY = 16;
static constexpr idx_t SPARSE_NEEDLE_BYTES_PER_KEY = 4;

NeedleShape DecomposeNeedle(ClientContext &context, const char *data, idx_t len, const GramOptions &options,
                            idx_t max_keys, NeedleKeys &keys) {
	auto bytes_per_key =
	    options.tokenizer == GramTokenizer::SPARSE ? SPARSE_NEEDLE_BYTES_PER_KEY : NGRAM_NEEDLE_BYTES_PER_KEY;
	if (len > bytes_per_key * max_keys) {
		return NeedleShape::OVER_BUDGET;
	}
	GramScratch scratch;
	idx_t grams_since_check = 0;
	auto check_interrupt = [&]() {
		if (++grams_since_check == GRAMS_PER_INTERRUPT_CHECK) {
			grams_since_check = 0;
			if (context.IsInterrupted()) {
				throw InterruptException();
			}
		}
	};
	auto mask = CurrentGramKeyMask();
	if (options.tokenizer == GramTokenizer::NGRAM) {
		bool emitted = false;
		bool over_budget = false;
		ExtractGrams(data, len, options, scratch, [&](const char *gram, idx_t gram_len) {
			emitted = true;
			if (over_budget) {
				return;
			}
			check_interrupt();
			over_budget = !keys.Add(GramKey(gram, gram_len, mask), max_keys);
		});
		if (over_budget) {
			return NeedleShape::OVER_BUDGET;
		}
		return emitted ? NeedleShape::PROBEABLE : NeedleShape::TOO_SHORT;
	}
	// Grams arrive in order of their end, and a gram ending with or after
	// another contains it exactly when it starts no later, so each arrival
	// retires the kept grams starting at or after it. What stays are the
	// covering grams, by first codepoint.
	vector<std::pair<idx_t, idx_t>> covering;
	ExtractGramRanges(data, len, options, scratch, [&](idx_t first, idx_t end) {
		check_interrupt();
		while (!covering.empty() && covering.back().first >= first) {
			covering.pop_back();
		}
		covering.emplace_back(first, end);
	});
	if (covering.empty()) {
		return NeedleShape::TOO_SHORT;
	}
	for (auto &range : covering) {
		if (!keys.Add(TokenizedGramKey(options, scratch, range.first, range.second, mask), max_keys)) {
			return NeedleShape::OVER_BUDGET;
		}
	}
	return NeedleShape::PROBEABLE;
}

vector<string> GramTexts(const GramOptions &options, const vector<string> &needles, const vector<uhugeint_t> &keys) {
	vector<string> texts(keys.size(), "?");
	GramScratch scratch;
	auto mask = CurrentGramKeyMask();
	for (auto &needle : needles) {
		ExtractGramRanges(needle.data(), needle.size(), options, scratch, [&](idx_t first, idx_t end) {
			auto key = TokenizedGramKey(options, scratch, first, end, mask);
			for (idx_t i = 0; i < keys.size(); i++) {
				if (keys[i] == key && texts[i] == "?") {
					auto begin = scratch.offsets[first];
					auto gram = scratch.normalized.substr(begin, scratch.offsets[end] - begin);
					texts[i] = "'" + StringUtil::Replace(gram, "'", "''") + "'";
				}
			}
		});
	}
	return texts;
}

//! trigrams(text[, gram_size[, case_insensitive]]) -> LIST(VARCHAR)
//! Returns every gram_size-codepoint window of the (normalized) input, in order,
//! duplicates included. Defaults: gram_size 3, case_insensitive true.
static void TrigramsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();

	UnifiedVectorFormat input_format;
	args.data[0].ToUnifiedFormat(input_format);
	auto input_strings = UnifiedVectorFormat::GetData<string_t>(input_format);

	UnifiedVectorFormat gram_format;
	const int32_t *gram_sizes = nullptr;
	if (args.ColumnCount() > 1) {
		args.data[1].ToUnifiedFormat(gram_format);
		gram_sizes = UnifiedVectorFormat::GetData<int32_t>(gram_format);
	}

	UnifiedVectorFormat ci_format;
	const bool *case_insensitive_flags = nullptr;
	if (args.ColumnCount() > 2) {
		args.data[2].ToUnifiedFormat(ci_format);
		case_insensitive_flags = UnifiedVectorFormat::GetData<bool>(ci_format);
	}

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto list_entries = FlatVector::GetDataMutable<list_entry_t>(result);
	auto &result_validity = FlatVector::ValidityMutable(result);
	ListVector::SetListSize(result, 0);

	idx_t total_grams = 0;
	GramScratch scratch;
	for (idx_t row = 0; row < count; row++) {
		auto input_idx = input_format.sel->get_index(row);
		bool is_null = !input_format.validity.RowIsValid(input_idx);

		GramOptions options;
		if (gram_sizes) {
			auto gram_idx = gram_format.sel->get_index(row);
			if (!gram_format.validity.RowIsValid(gram_idx)) {
				is_null = true;
			} else {
				auto gram_size = gram_sizes[gram_idx];
				if (gram_size < 1) {
					throw InvalidInputException("trigrams: gram_size must be at least 1, got %d", gram_size);
				}
				options.min_gram = options.max_gram = static_cast<idx_t>(gram_size);
			}
		}
		if (case_insensitive_flags) {
			auto ci_idx = ci_format.sel->get_index(row);
			if (!ci_format.validity.RowIsValid(ci_idx)) {
				is_null = true;
			} else {
				options.case_insensitive = case_insensitive_flags[ci_idx];
			}
		}

		if (is_null) {
			result_validity.SetInvalid(row);
			list_entries[row] = list_entry_t(total_grams, 0);
			continue;
		}

		auto &input = input_strings[input_idx];
		idx_t row_start = total_grams;
		ExtractGrams(input.GetData(), input.GetSize(), options, scratch, [&](const char *gram, idx_t gram_len) {
			if (total_grams >= ListVector::GetListCapacity(result)) {
				ListVector::SetListSize(result, total_grams);
				ListVector::Reserve(result, 2 * ListVector::GetListCapacity(result));
			}
			auto &child = ListVector::GetChildMutable(result);
			auto child_strings = FlatVector::GetDataMutable<string_t>(child);
			child_strings[total_grams] = StringVector::AddString(child, gram, gram_len);
			total_grams++;
		});
		list_entries[row] = list_entry_t(row_start, total_grams - row_start);
	}
	ListVector::SetListSize(result, total_grams);

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//! sparse_grams(text[, min_gram, max_gram[, case_insensitive]]) -> LIST(VARCHAR)
//! Returns every sparse gram of the (normalized) input that a sparse index
//! with these options holds, by first codepoint and then length, duplicates
//! included. Defaults: lengths 3 to 8, case_insensitive true.
static void SparseGramsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();
	UnifiedVectorFormat input_format, min_format, max_format, ci_format;
	args.data[0].ToUnifiedFormat(input_format);
	auto input_strings = UnifiedVectorFormat::GetData<string_t>(input_format);
	const int32_t *min_grams = nullptr;
	const int32_t *max_grams = nullptr;
	if (args.ColumnCount() > 1) {
		args.data[1].ToUnifiedFormat(min_format);
		args.data[2].ToUnifiedFormat(max_format);
		min_grams = UnifiedVectorFormat::GetData<int32_t>(min_format);
		max_grams = UnifiedVectorFormat::GetData<int32_t>(max_format);
	}
	const bool *case_insensitive_flags = nullptr;
	if (args.ColumnCount() > 3) {
		args.data[3].ToUnifiedFormat(ci_format);
		case_insensitive_flags = UnifiedVectorFormat::GetData<bool>(ci_format);
	}

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto list_entries = FlatVector::GetDataMutable<list_entry_t>(result);
	auto &result_validity = FlatVector::ValidityMutable(result);
	ListVector::SetListSize(result, 0);

	idx_t total_grams = 0;
	GramScratch scratch;
	vector<std::pair<idx_t, idx_t>> ranges;
	for (idx_t row = 0; row < count; row++) {
		auto input_idx = input_format.sel->get_index(row);
		bool is_null = !input_format.validity.RowIsValid(input_idx);
		GramOptions options;
		options.tokenizer = GramTokenizer::SPARSE;
		options.min_gram = DEFAULT_SPARSE_MIN_GRAM;
		options.max_gram = DEFAULT_SPARSE_MAX_GRAM;
		if (min_grams) {
			auto min_idx = min_format.sel->get_index(row);
			auto max_idx = max_format.sel->get_index(row);
			if (!min_format.validity.RowIsValid(min_idx) || !max_format.validity.RowIsValid(max_idx)) {
				is_null = true;
			} else {
				options.min_gram = NumericCast<idx_t>(MaxValue<int32_t>(min_grams[min_idx], 0));
				options.max_gram = NumericCast<idx_t>(MaxValue<int32_t>(max_grams[max_idx], 0));
			}
		}
		if (case_insensitive_flags) {
			auto ci_idx = ci_format.sel->get_index(row);
			if (!ci_format.validity.RowIsValid(ci_idx)) {
				is_null = true;
			} else {
				options.case_insensitive = case_insensitive_flags[ci_idx];
			}
		}
		if (is_null) {
			result_validity.SetInvalid(row);
			list_entries[row] = list_entry_t(total_grams, 0);
			continue;
		}
		ValidateGramOptions(options, "sparse_grams");
		auto &input = input_strings[input_idx];
		ranges.clear();
		ExtractGramRanges(input.GetData(), input.GetSize(), options, scratch,
		                  [&](idx_t first, idx_t end) { ranges.emplace_back(first, end); });
		std::sort(ranges.begin(), ranges.end());
		if (total_grams + ranges.size() > ListVector::GetListCapacity(result)) {
			ListVector::SetListSize(result, total_grams);
			ListVector::Reserve(result, NextPowerOfTwo(total_grams + ranges.size()));
		}
		auto &child = ListVector::GetChildMutable(result);
		auto child_strings = FlatVector::GetDataMutable<string_t>(child);
		for (idx_t i = 0; i < ranges.size(); i++) {
			auto begin = scratch.offsets[ranges[i].first];
			child_strings[total_grams + i] = StringVector::AddString(child, scratch.normalized.data() + begin,
			                                                         scratch.offsets[ranges[i].second] - begin);
		}
		list_entries[row] = list_entry_t(total_grams, ranges.size());
		total_grams += ranges.size();
	}
	ListVector::SetListSize(result, total_grams);

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//! ngram_gram_key(gram) -> UHUGEINT: the storage key of one already
//! normalized gram, for tests and inspection of the segments table.
static void GramKeyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto mask = CurrentGramKeyMask();
	UnaryExecutor::Execute<string_t, uhugeint_t>(args.data[0], result, args.size(), [&](string_t gram) {
		return GramKey(gram.GetData(), gram.GetSize(), mask);
	});
}

//! Keys of two common shapes order like 64-bit integers: a gram of at most
//! four bytes keeps only its top 32 bits, and a sparse bucket key has a fixed
//! upper half above every such gram's and a bucket below 2^32. The codes are
//! the gram's top 32 bits and 2^32 plus the bucket, which order the same way.
static inline bool SortCodeFits(const uhugeint_t &key) {
	return (key.lower == 0 && (key.upper & 0xFFFFFFFFULL) == 0) ||
	       (key.upper == SPARSE_BUCKET_KEY_UPPER && key.lower <= 0xFFFFFFFFULL);
}

static inline uint64_t SortCode(const uhugeint_t &key) {
	return key.upper == SPARSE_BUCKET_KEY_UPPER ? (uint64_t(1) << 32) + key.lower : key.upper >> 32;
}

static inline uhugeint_t FromSortCode(uint64_t code) {
	return code >> 32 ? uhugeint_t(SPARSE_BUCKET_KEY_UPPER, code - (uint64_t(1) << 32)) : uhugeint_t(code << 32, 0);
}

//! Keys a chunk reserves before its first row, at most: 16 MiB of list.
static constexpr idx_t MAX_RESERVED_CHUNK_KEYS = idx_t(1) << 20;

//! ngram_gram_keys(text, tokenizer, min_gram, max_gram, case_insensitive) ->
//! LIST(UHUGEINT): the keys of the distinct grams of `text`, ascending. The
//! build pipeline unnests this into its (gram_key, segment_no, rowid) pair
//! stream; deduplicating here keeps repeated grams of one row out of the
//! grouping pass.
static void GramKeysFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();
	UnifiedVectorFormat input_format, tokenizer_format, min_format, max_format, ci_format;
	args.data[0].ToUnifiedFormat(input_format);
	args.data[1].ToUnifiedFormat(tokenizer_format);
	args.data[2].ToUnifiedFormat(min_format);
	args.data[3].ToUnifiedFormat(max_format);
	args.data[4].ToUnifiedFormat(ci_format);
	auto input_strings = UnifiedVectorFormat::GetData<string_t>(input_format);
	auto tokenizers = UnifiedVectorFormat::GetData<string_t>(tokenizer_format);
	auto min_grams = UnifiedVectorFormat::GetData<int32_t>(min_format);
	auto max_grams = UnifiedVectorFormat::GetData<int32_t>(max_format);
	auto case_insensitive_flags = UnifiedVectorFormat::GetData<bool>(ci_format);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto list_entries = FlatVector::GetDataMutable<list_entry_t>(result);
	auto &result_validity = FlatVector::ValidityMutable(result);
	ListVector::SetListSize(result, 0);

	// a row yields at most one gram per byte, two for a sparse tokenizer, so
	// the chunk's bytes bound its keys; reserving that bound, capped, spares
	// the list from regrowing
	idx_t input_bytes = 0;
	for (idx_t row = 0; row < count; row++) {
		auto input_idx = input_format.sel->get_index(row);
		if (input_format.validity.RowIsValid(input_idx)) {
			input_bytes += 2 * input_strings[input_idx].GetSize();
		}
	}
	ListVector::Reserve(result, MinValue<idx_t>(input_bytes, MAX_RESERVED_CHUNK_KEYS));

	idx_t total = 0;
	GramScratch scratch;
	vector<uhugeint_t> keys;
	vector<uint64_t> codes;
	auto mask = CurrentGramKeyMask();
	GramOptions options;
	bool options_valid = false;
	string_t tokenizer_name;
	GramTokenizer tokenizer = GramTokenizer::NGRAM;
	bool tokenizer_valid = false;
	for (idx_t row = 0; row < count; row++) {
		auto input_idx = input_format.sel->get_index(row);
		auto tokenizer_idx = tokenizer_format.sel->get_index(row);
		auto min_idx = min_format.sel->get_index(row);
		auto max_idx = max_format.sel->get_index(row);
		auto ci_idx = ci_format.sel->get_index(row);
		if (!input_format.validity.RowIsValid(input_idx) || !tokenizer_format.validity.RowIsValid(tokenizer_idx) ||
		    !min_format.validity.RowIsValid(min_idx) || !max_format.validity.RowIsValid(max_idx) ||
		    !ci_format.validity.RowIsValid(ci_idx)) {
			result_validity.SetInvalid(row);
			list_entries[row] = list_entry_t(total, 0);
			continue;
		}
		// the build passes constants, so the options are parsed once
		if (!tokenizer_valid || !(tokenizers[tokenizer_idx] == tokenizer_name)) {
			tokenizer = ParseTokenizer(tokenizers[tokenizer_idx].GetString(), "ngram_gram_keys");
			tokenizer_name = tokenizers[tokenizer_idx];
			tokenizer_valid = true;
		}
		GramOptions row_options;
		row_options.tokenizer = tokenizer;
		row_options.min_gram = NumericCast<idx_t>(MaxValue<int32_t>(min_grams[min_idx], 0));
		row_options.max_gram = NumericCast<idx_t>(MaxValue<int32_t>(max_grams[max_idx], 0));
		row_options.case_insensitive = case_insensitive_flags[ci_idx];
		if (!options_valid || row_options.tokenizer != options.tokenizer || row_options.min_gram != options.min_gram ||
		    row_options.max_gram != options.max_gram || row_options.case_insensitive != options.case_insensitive) {
			ValidateGramOptions(row_options, "ngram_gram_keys");
			options = row_options;
			options_valid = true;
		}
		auto &input = input_strings[input_idx];
		keys.clear();
		bool coded = true;
		ExtractGramRanges(input.GetData(), input.GetSize(), options, scratch, [&](idx_t first, idx_t end) {
			auto key = TokenizedGramKey(options, scratch, first, end, mask);
			coded = coded && SortCodeFits(key);
			keys.push_back(key);
		});
		idx_t distinct;
		if (coded) {
			// every key has a 64-bit sort code (an ASCII trigram, say, or a
			// sparse bucket), so the keys sort as integers
			codes.resize(keys.size());
			for (idx_t i = 0; i < keys.size(); i++) {
				codes[i] = SortCode(keys[i]);
			}
			std::sort(codes.begin(), codes.end());
			distinct = NumericCast<idx_t>(std::unique(codes.begin(), codes.end()) - codes.begin());
			for (idx_t i = 0; i < distinct; i++) {
				keys[i] = FromSortCode(codes[i]);
			}
		} else {
			// uhugeint_t's own comparisons are out of line; these inline ones
			// order the same way
			std::sort(keys.begin(), keys.end(), [](const uhugeint_t &a, const uhugeint_t &b) {
				return a.upper < b.upper || (a.upper == b.upper && a.lower < b.lower);
			});
			distinct = NumericCast<idx_t>(std::unique(keys.begin(), keys.end(),
			                                          [](const uhugeint_t &a, const uhugeint_t &b) {
				                                          return a.upper == b.upper && a.lower == b.lower;
			                                          }) -
			                              keys.begin());
		}
		if (total + distinct > ListVector::GetListCapacity(result)) {
			ListVector::SetListSize(result, total);
			ListVector::Reserve(result, NextPowerOfTwo(total + distinct));
		}
		auto child_keys = FlatVector::GetDataMutable<uhugeint_t>(ListVector::GetChildMutable(result));
		for (idx_t i = 0; i < distinct; i++) {
			child_keys[total + i] = keys[i];
		}
		list_entries[row] = list_entry_t(total, distinct);
		total += distinct;
	}
	ListVector::SetListSize(result, total);

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

void RegisterGram(ExtensionLoader &loader) {
	auto list_type = LogicalType::LIST(LogicalType::VARCHAR);
	ScalarFunctionSet trigrams("trigrams");
	// every overload rejects a gram size below one
	for (auto &arguments :
	     vector<vector<LogicalType>> {{LogicalType::VARCHAR},
	                                  {LogicalType::VARCHAR, LogicalType::INTEGER},
	                                  {LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::BOOLEAN}}) {
		ScalarFunction overload(arguments, list_type, TrigramsFunction);
		overload.SetFallible();
		trigrams.AddFunction(overload);
	}
	loader.RegisterFunction(trigrams);

	ScalarFunctionSet sparse_grams("sparse_grams");
	// every overload rejects lengths a sparse index refuses
	for (auto &arguments : vector<vector<LogicalType>> {
	         {LogicalType::VARCHAR},
	         {LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::INTEGER},
	         {LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::BOOLEAN}}) {
		ScalarFunction overload(arguments, list_type, SparseGramsFunction);
		overload.SetFallible();
		sparse_grams.AddFunction(overload);
	}
	loader.RegisterFunction(sparse_grams);

	loader.RegisterFunction(
	    ScalarFunction("ngram_gram_key", {LogicalType::VARCHAR}, LogicalType::UHUGEINT, GramKeyFunction));
	auto gram_keys = ScalarFunction(
	    "ngram_gram_keys",
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::BOOLEAN},
	    LogicalType::LIST(LogicalType::UHUGEINT), GramKeysFunction);
	gram_keys.SetFallible();
	loader.RegisterFunction(gram_keys);
}

} // namespace ngram
} // namespace duckdb
