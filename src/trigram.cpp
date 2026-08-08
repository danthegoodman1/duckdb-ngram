#include "ngram/trigram.hpp"

#include "duckdb/common/string_util.hpp"
#include "utf8proc_wrapper.hpp"

namespace duckdb {
namespace ngram {

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
		int sz = 0;
		auto codepoint = Utf8Proc::UTF8ToCodepoint(data + i, sz);
		if (sz <= 0) {
			// DuckDB guarantees valid UTF-8 in VARCHAR; treat a stray byte as one
			// codepoint so we cannot loop forever on unexpected input
			normalized.push_back(data[i]);
			i++;
			continue;
		}
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

NeedleDecomposition DecomposeNeedle(const char *data, idx_t len, const GramOptions &options) {
	NeedleDecomposition result;
	string scratch;
	vector<idx_t> offsets;
	bool emitted = false;
	ExtractGrams(data, len, options, scratch, offsets, [&](const char *gram, idx_t gram_len) {
		emitted = true;
		string gram_str(gram, gram_len);
		for (auto &existing : result.grams) {
			if (existing == gram_str) {
				return;
			}
		}
		result.grams.push_back(std::move(gram_str));
	});
	result.too_short = !emitted;
	return result;
}

} // namespace ngram
} // namespace duckdb
