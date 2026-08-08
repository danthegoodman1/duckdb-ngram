#define DUCKDB_EXTENSION_MAIN

#include "ngram_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace duckdb {

inline void NgramScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "...........🦆 " + name.GetString());
	});
}

static void LoadInternal(ExtensionLoader &loader) {
	auto ngram_scalar_function =
	    ScalarFunction("ngram", {LogicalType::VARCHAR}, LogicalType::VARCHAR, NgramScalarFun);

	loader.RegisterFunction(ngram_scalar_function);
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
