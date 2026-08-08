#define DUCKDB_EXTENSION_MAIN

#include "ngram_extension.hpp"
#include "duckdb.hpp"
#include "ngram/index_pragmas.hpp"
#include "ngram/postings_codec.hpp"
#include "ngram/trigram.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	ngram::RegisterTrigramsFunction(loader);
	ngram::RegisterPostingsCodec(loader);
	ngram::RegisterIndexPragmas(loader);
}

void NgramExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string NgramExtension::Name() {
	return "ngram";
}

std::string NgramExtension::Version() const {
#ifdef EXT_VERSION_NGRAM
	return EXT_VERSION_NGRAM;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(ngram, loader) {
	duckdb::LoadInternal(loader);
}
}
