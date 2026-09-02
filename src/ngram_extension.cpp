#define DUCKDB_EXTENSION_MAIN

#include "ngram_extension.hpp"
#include "duckdb.hpp"
#include "ngram/fence.hpp"
#include "ngram/gram.hpp"
#include "ngram/postings.hpp"
#include "ngram/rowid_guard.hpp"
#include "ngram/settings.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	ngram::InitializeRowIdGuardHostRuntime(loader);
	ngram::RegisterSettings(loader);
	ngram::RegisterRowIdGuard(loader);
	ngram::RegisterGram(loader);
	ngram::RegisterPostings(loader);
	ngram::RegisterFence(loader);
	ngram::RegisterPragmas(loader);
	ngram::RegisterSearchFunctions(loader);
	ngram::RegisterRewrite(loader);
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
