#include "ngram/trigram.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/scalar_function.hpp"

namespace duckdb {
namespace ngram {

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

void RegisterTrigramsFunction(ExtensionLoader &loader) {
	auto list_type = LogicalType::LIST(LogicalType::VARCHAR);
	ScalarFunctionSet trigrams("trigrams");
	trigrams.AddFunction(ScalarFunction({LogicalType::VARCHAR}, list_type, TrigramsFunction));
	trigrams.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::INTEGER}, list_type, TrigramsFunction));
	trigrams.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::BOOLEAN}, list_type,
	                                    TrigramsFunction));
	loader.RegisterFunction(trigrams);
}

} // namespace ngram
} // namespace duckdb
