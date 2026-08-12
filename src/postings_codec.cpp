#include "ngram/postings_codec.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/scalar_function.hpp"

#include <algorithm>
#include <limits>

namespace duckdb {
namespace ngram {

static void AppendVarint(string &blob, uint64_t value) {
	while (value >= 0x80) {
		blob.push_back(static_cast<char>((value & 0x7F) | 0x80));
		value >>= 7;
	}
	blob.push_back(static_cast<char>(value));
}

static uint64_t ReadVarint(const char *data, idx_t size, idx_t &pos) {
	uint64_t value = 0;
	idx_t shift = 0;
	while (true) {
		if (pos >= size || shift > 63) {
			throw InvalidInputException("ngram: malformed postings blob");
		}
		auto byte = static_cast<uint8_t>(data[pos++]);
		// at shift 63 only the lowest payload bit is in range; anything above
		// would silently wrap out of uint64
		if (shift == 63 && (byte & 0x7E) != 0) {
			throw InvalidInputException("ngram: malformed postings blob");
		}
		value |= static_cast<uint64_t>(byte & 0x7F) << shift;
		if (!(byte & 0x80)) {
			// counts and rowid deltas both live in [0, INT64_MAX]
			if (value > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
				throw InvalidInputException("ngram: malformed postings blob");
			}
			return value;
		}
		shift += 7;
	}
}

string EncodePostings(vector<int64_t> &rowids) {
	for (auto rowid : rowids) {
		if (rowid < 0) {
			throw InvalidInputException("ngram: postings cannot contain negative rowids");
		}
	}
	std::sort(rowids.begin(), rowids.end());
	rowids.erase(std::unique(rowids.begin(), rowids.end()), rowids.end());

	string blob;
	blob.push_back(static_cast<char>(POSTINGS_FORMAT_VERSION));
	AppendVarint(blob, rowids.size());
	uint64_t previous = 0;
	for (idx_t i = 0; i < rowids.size(); i++) {
		auto current = static_cast<uint64_t>(rowids[i]);
		AppendVarint(blob, i == 0 ? current : current - previous);
		previous = current;
	}
	return blob;
}

void DecodePostings(const char *data, idx_t size, vector<int64_t> &result) {
	if (size == 0 || static_cast<uint8_t>(data[0]) != POSTINGS_FORMAT_VERSION) {
		throw InvalidInputException("ngram: unknown postings blob format");
	}
	idx_t pos = 1;
	auto count = ReadVarint(data, size, pos);
	uint64_t current = 0;
	for (uint64_t i = 0; i < count; i++) {
		auto delta = ReadVarint(data, size, pos);
		// deltas between sorted unique rowids are >= 1 (the first entry is an
		// absolute rowid and may be 0); the sum must stay within int64
		if (i > 0 && delta == 0) {
			throw InvalidInputException("ngram: malformed postings blob");
		}
		if (delta > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) - current) {
			throw InvalidInputException("ngram: malformed postings blob");
		}
		current += delta;
		result.push_back(static_cast<int64_t>(current));
	}
	if (pos != size) {
		throw InvalidInputException("ngram: malformed postings blob");
	}
}

idx_t PostingsCount(const char *data, idx_t size) {
	if (size == 0 || static_cast<uint8_t>(data[0]) != POSTINGS_FORMAT_VERSION) {
		throw InvalidInputException("ngram: unknown postings blob format");
	}
	idx_t pos = 1;
	return NumericCast<idx_t>(ReadVarint(data, size, pos));
}

static void EncodePostingsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();

	UnifiedVectorFormat list_format;
	args.data[0].ToUnifiedFormat(count, list_format);
	auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(list_format);

	auto &child = ListVector::GetEntry(args.data[0]);
	auto child_size = ListVector::GetListSize(args.data[0]);
	UnifiedVectorFormat child_format;
	child.ToUnifiedFormat(child_size, child_format);
	auto child_values = UnifiedVectorFormat::GetData<int64_t>(child_format);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_strings = FlatVector::GetData<string_t>(result);
	auto &result_validity = FlatVector::Validity(result);

	vector<int64_t> rowids;
	for (idx_t row = 0; row < count; row++) {
		auto list_idx = list_format.sel->get_index(row);
		if (!list_format.validity.RowIsValid(list_idx)) {
			result_validity.SetInvalid(row);
			continue;
		}
		auto entry = list_entries[list_idx];
		rowids.clear();
		for (idx_t i = 0; i < entry.length; i++) {
			auto child_idx = child_format.sel->get_index(entry.offset + i);
			if (!child_format.validity.RowIsValid(child_idx)) {
				throw InvalidInputException("ngram: postings cannot contain NULL rowids");
			}
			rowids.push_back(child_values[child_idx]);
		}
		auto blob = EncodePostings(rowids);
		result_strings[row] = StringVector::AddStringOrBlob(result, blob);
	}

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static void DecodePostingsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();

	UnifiedVectorFormat blob_format;
	args.data[0].ToUnifiedFormat(count, blob_format);
	auto blobs = UnifiedVectorFormat::GetData<string_t>(blob_format);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto list_entries = FlatVector::GetData<list_entry_t>(result);
	auto &result_validity = FlatVector::Validity(result);
	ListVector::SetListSize(result, 0);

	idx_t total = 0;
	vector<int64_t> rowids;
	for (idx_t row = 0; row < count; row++) {
		auto blob_idx = blob_format.sel->get_index(row);
		if (!blob_format.validity.RowIsValid(blob_idx)) {
			result_validity.SetInvalid(row);
			list_entries[row] = list_entry_t(total, 0);
			continue;
		}
		rowids.clear();
		DecodePostings(blobs[blob_idx].GetData(), blobs[blob_idx].GetSize(), rowids);

		if (total + rowids.size() > ListVector::GetListCapacity(result)) {
			ListVector::SetListSize(result, total);
			ListVector::Reserve(result, NextPowerOfTwo(total + rowids.size()));
		}
		auto &child = ListVector::GetEntry(result);
		auto child_values = FlatVector::GetData<int64_t>(child);
		for (idx_t i = 0; i < rowids.size(); i++) {
			child_values[total + i] = rowids[i];
		}
		list_entries[row] = list_entry_t(total, rowids.size());
		total += rowids.size();
	}
	ListVector::SetListSize(result, total);

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

void RegisterPostingsCodec(ExtensionLoader &loader) {
	auto rowid_list = LogicalType::LIST(LogicalType::BIGINT);
	loader.RegisterFunction(
	    ScalarFunction("ngram_encode_postings", {rowid_list}, LogicalType::BLOB, EncodePostingsFunction));
	loader.RegisterFunction(
	    ScalarFunction("ngram_decode_postings", {LogicalType::BLOB}, rowid_list, DecodePostingsFunction));
}

} // namespace ngram
} // namespace duckdb
