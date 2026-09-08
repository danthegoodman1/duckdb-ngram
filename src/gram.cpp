#include "ngram/gram.hpp"

#include "utf8proc_wrapper.hpp"

#include <algorithm>
#include <limits>

namespace duckdb {
namespace ngram {

//! Grams up to this many bytes are byte-packed; longer ones are hashed.
constexpr idx_t PACKED_KEY_BYTES = 16;

uhugeint_t GramKey(const char *data, idx_t len) {
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
		return uhugeint_t(upper, lower);
	}
	// FNV-1a; any fixed function works, since collisions only widen and the
	// key never leaves this extension's storage
	uint64_t hash = 14695981039346656037ULL;
	for (idx_t i = 0; i < len; i++) {
		hash ^= static_cast<uint64_t>(static_cast<uint8_t>(data[i]));
		hash *= 1099511628211ULL;
	}
	return uhugeint_t(std::numeric_limits<uint64_t>::max(), hash);
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
		auto codepoint = Utf8Proc::UTF8ToCodepoint(data + i, sz);
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

void MergeKeys(vector<uhugeint_t> &target, const vector<uhugeint_t> &keys) {
	for (auto &key : keys) {
		if (std::find(target.begin(), target.end(), key) == target.end()) {
			target.push_back(key);
		}
	}
}

NeedleDecomposition DecomposeNeedle(const char *data, idx_t len, const GramOptions &options) {
	NeedleDecomposition result;
	string scratch;
	vector<idx_t> offsets;
	bool emitted = false;
	ExtractGrams(data, len, options, scratch, offsets, [&](const char *gram, idx_t gram_len) {
		emitted = true;
		auto key = GramKey(gram, gram_len);
		if (std::find(result.keys.begin(), result.keys.end(), key) == result.keys.end()) {
			result.keys.push_back(key);
		}
	});
	result.too_short = !emitted;
	return result;
}

//! trigrams(text[, gram_size[, case_insensitive]]) -> LIST(VARCHAR)
//! Returns every gram_size-codepoint window of the (normalized) input, in order,
//! duplicates included. Defaults: gram_size 3, case_insensitive true.
static void TrigramsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();

	UnifiedVectorFormat input_format;
	args.data[0].ToUnifiedFormat(count, input_format);
	auto input_strings = UnifiedVectorFormat::GetData<string_t>(input_format);

	UnifiedVectorFormat gram_format;
	const int32_t *gram_sizes = nullptr;
	if (args.ColumnCount() > 1) {
		args.data[1].ToUnifiedFormat(count, gram_format);
		gram_sizes = UnifiedVectorFormat::GetData<int32_t>(gram_format);
	}

	UnifiedVectorFormat ci_format;
	const bool *case_insensitive_flags = nullptr;
	if (args.ColumnCount() > 2) {
		args.data[2].ToUnifiedFormat(count, ci_format);
		case_insensitive_flags = UnifiedVectorFormat::GetData<bool>(ci_format);
	}

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto list_entries = FlatVector::GetData<list_entry_t>(result);
	auto &result_validity = FlatVector::Validity(result);
	ListVector::SetListSize(result, 0);

	idx_t total_grams = 0;
	string scratch;
	vector<idx_t> offsets;
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
				options.gram_size = static_cast<idx_t>(gram_size);
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
		ExtractGrams(input.GetData(), input.GetSize(), options, scratch, offsets,
		             [&](const char *gram, idx_t gram_len) {
			             if (total_grams >= ListVector::GetListCapacity(result)) {
				             ListVector::SetListSize(result, total_grams);
				             ListVector::Reserve(result, 2 * ListVector::GetListCapacity(result));
			             }
			             auto &child = ListVector::GetEntry(result);
			             auto child_strings = FlatVector::GetData<string_t>(child);
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

//! ngram_gram_key(gram) -> UHUGEINT: the storage key of one already
//! normalized gram, for tests and inspection of the segments table.
static void GramKeyFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::Execute<string_t, uhugeint_t>(args.data[0], result, args.size(),
	                                             [](string_t gram) { return GramKey(gram.GetData(), gram.GetSize()); });
}

//! ngram_gram_keys(text, gram_size, case_insensitive) -> LIST(UHUGEINT): the
//! keys of the distinct grams of `text`, ascending. The build pipeline unnests
//! this into its (gram_key, segment_no, rowid) pair stream; deduplicating
//! here keeps repeated grams of one row out of the grouping pass.
static void GramKeysFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();
	UnifiedVectorFormat input_format, gram_format, ci_format;
	args.data[0].ToUnifiedFormat(count, input_format);
	args.data[1].ToUnifiedFormat(count, gram_format);
	args.data[2].ToUnifiedFormat(count, ci_format);
	auto input_strings = UnifiedVectorFormat::GetData<string_t>(input_format);
	auto gram_sizes = UnifiedVectorFormat::GetData<int32_t>(gram_format);
	auto case_insensitive_flags = UnifiedVectorFormat::GetData<bool>(ci_format);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto list_entries = FlatVector::GetData<list_entry_t>(result);
	auto &result_validity = FlatVector::Validity(result);
	ListVector::SetListSize(result, 0);

	idx_t total = 0;
	string scratch;
	vector<idx_t> offsets;
	vector<uhugeint_t> keys;
	for (idx_t row = 0; row < count; row++) {
		auto input_idx = input_format.sel->get_index(row);
		auto gram_idx = gram_format.sel->get_index(row);
		auto ci_idx = ci_format.sel->get_index(row);
		if (!input_format.validity.RowIsValid(input_idx) || !gram_format.validity.RowIsValid(gram_idx) ||
		    !ci_format.validity.RowIsValid(ci_idx)) {
			result_validity.SetInvalid(row);
			list_entries[row] = list_entry_t(total, 0);
			continue;
		}
		if (gram_sizes[gram_idx] < 1) {
			throw InvalidInputException("ngram_gram_keys: gram_size must be at least 1, got %d", gram_sizes[gram_idx]);
		}
		GramOptions options;
		options.gram_size = static_cast<idx_t>(gram_sizes[gram_idx]);
		options.case_insensitive = case_insensitive_flags[ci_idx];
		auto &input = input_strings[input_idx];
		keys.clear();
		ExtractGrams(input.GetData(), input.GetSize(), options, scratch, offsets,
		             [&](const char *gram, idx_t gram_len) { keys.push_back(GramKey(gram, gram_len)); });
		std::sort(keys.begin(), keys.end());
		keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
		if (total + keys.size() > ListVector::GetListCapacity(result)) {
			ListVector::SetListSize(result, total);
			ListVector::Reserve(result, NextPowerOfTwo(total + keys.size()));
		}
		auto child_keys = FlatVector::GetData<uhugeint_t>(ListVector::GetEntry(result));
		for (idx_t i = 0; i < keys.size(); i++) {
			child_keys[total + i] = keys[i];
		}
		list_entries[row] = list_entry_t(total, keys.size());
		total += keys.size();
	}
	ListVector::SetListSize(result, total);

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

void RegisterGram(ExtensionLoader &loader) {
	auto list_type = LogicalType::LIST(LogicalType::VARCHAR);
	ScalarFunctionSet trigrams("trigrams");
	trigrams.AddFunction(ScalarFunction({LogicalType::VARCHAR}, list_type, TrigramsFunction));
	trigrams.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::INTEGER}, list_type, TrigramsFunction));
	trigrams.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::BOOLEAN}, list_type,
	                                    TrigramsFunction));
	loader.RegisterFunction(trigrams);

	loader.RegisterFunction(
	    ScalarFunction("ngram_gram_key", {LogicalType::VARCHAR}, LogicalType::UHUGEINT, GramKeyFunction));
	loader.RegisterFunction(ScalarFunction("ngram_gram_keys",
	                                       {LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::BOOLEAN},
	                                       LogicalType::LIST(LogicalType::UHUGEINT), GramKeysFunction));
}

} // namespace ngram
} // namespace duckdb
