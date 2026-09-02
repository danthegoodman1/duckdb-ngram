//===----------------------------------------------------------------------===//
// ngram/postings.hpp: the posting-blob codec, ngram_pack_segment, ngram_unpack_postings, and the encode/decode scalars.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace ngram {

//! Blob format v1:
//!   [0]        format version byte (1)
//!   varint     rowid count
//!   varint     first rowid
//!   varint...  deltas between consecutive sorted unique rowids (all >= 1)
//! Varints are LEB128 on unsigned 64-bit values; rowids are non-negative.
constexpr uint8_t POSTINGS_FORMAT_VERSION = 1;

//! Append the rowids stored in blob (data, size) to result. Throws on a
//! malformed or unknown-version blob.
void DecodePostings(const char *data, idx_t size, vector<int64_t> &result);

//! Read and validate the format byte and encoded row count without decoding
//! rowids. Callers use this to admit memory before growing a result vector.
idx_t PostingsCount(const char *data, idx_t size);

void RegisterPostings(ExtensionLoader &loader);

} // namespace ngram
} // namespace duckdb
