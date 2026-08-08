//===----------------------------------------------------------------------===//
//                         ngram
//
// ngram/postings_codec.hpp
//
// Posting-segment blob codec. Format v1:
//   [0]        format version byte (1)
//   varint     rowid count
//   varint     first rowid
//   varint...  deltas between consecutive sorted unique rowids (all >= 1)
// Varints are LEB128 on unsigned 64-bit values; rowids are non-negative.
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace ngram {

constexpr uint8_t POSTINGS_FORMAT_VERSION = 1;

//! Encode rowids into a postings blob. The input is sorted and deduplicated in
//! place; negative rowids throw.
string EncodePostings(vector<int64_t> &rowids);

//! Append the rowids stored in blob (data, size) to result. Throws on a
//! malformed or unknown-version blob.
void DecodePostings(const char *data, idx_t size, vector<int64_t> &result);

void RegisterPostingsCodec(ExtensionLoader &loader);

} // namespace ngram
} // namespace duckdb
