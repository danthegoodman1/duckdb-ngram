#pragma once

#include "duckdb.hpp"

namespace duckdb {

class NgramExtension : public Extension {
public:
	void Load(ExtensionLoader &db) override;
	std::string Name() override;
	std::string Version() const override;
};

namespace ngram {

//! Registration entry points of the modules without a header of their own.
void RegisterPragmas(ExtensionLoader &loader);
void RegisterSearchFunctions(ExtensionLoader &loader);
void RegisterRewrite(ExtensionLoader &loader);

} // namespace ngram
} // namespace duckdb
