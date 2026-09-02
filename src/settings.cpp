#include "ngram/settings.hpp"

#include <cmath>

namespace duckdb {
namespace ngram {

//! Probing more of the needle's grams shrinks the candidate set but costs one
//! more posting-list decode each time, and rarest-first means every additional
//! gram is denser than the last. Measured across four indexes (1/10/100 GB of
//! natural language, plus a bigram index whose grams are deliberately dense),
//! total query time is a shallow U with its floor at 2-4 and a steep right
//! arm: at 100 GB a rare needle costs 0.465 s at K=2, 0.640 s at K=3 and
//! 1.750 s at K=8. Three sits at the floor everywhere while keeping a genuine
//! three-way intersection, which K=2 does not: on the dense-gram index K=2
//! leaves 0.89% of rows as candidates against K=3's 0.28%, close enough to
//! ngram_max_candidate_fraction to risk giving up the index entirely.
//! Lowering K is always safe for correctness — fewer grams can only widen the
//! candidate set, never drop a match (benchmarks/RESULTS.md).
static constexpr idx_t DEFAULT_MAX_GRAMS_PER_QUERY = 3;

//! Fetch costs 250-300 ns per candidate at every scale, while a parallel scan
//! of the whole table costs about 0.04 s at 1 GB, 0.35 s at 10 GB and 3.5 s
//! at 100 GB, which puts the break-even at 1.6%, 1.3% and 1.1% of rows. One
//! percent is that crossover rounded toward scanning (benchmarks/RESULTS.md).
static constexpr double DEFAULT_MAX_CANDIDATE_FRACTION = 0.01;
static constexpr int64_t DEFAULT_MAX_PROBE_ROWIDS = 100000000;
static constexpr idx_t MAX_PROBE_MEMORY_BYTES = 256ULL * 1024ULL * 1024ULL;

template <class T>
static bool TryGetSetting(ClientContext &context, const char *name, T &result) {
	Value value;
	if (!context.TryGetCurrentSetting(name, value) || value.IsNull()) {
		return false;
	}
	result = value.GetValue<T>();
	return true;
}

idx_t MaxGramsPerQuery(ClientContext &context) {
	int64_t k;
	if (!TryGetSetting(context, "ngram_max_grams_per_query", k)) {
		return DEFAULT_MAX_GRAMS_PER_QUERY;
	}
	if (k < 1) {
		throw InvalidInputException("ngram_max_grams_per_query must be at least 1, got %lld", k);
	}
	return static_cast<idx_t>(k);
}

double MaxCandidateFraction(ClientContext &context) {
	double fraction;
	if (!TryGetSetting(context, "ngram_max_candidate_fraction", fraction)) {
		return DEFAULT_MAX_CANDIDATE_FRACTION;
	}
	if (std::isnan(fraction) || fraction < 0) {
		throw InvalidInputException("ngram_max_candidate_fraction must be a non-negative fraction, got %s",
		                            to_string(fraction));
	}
	return fraction;
}

idx_t MaxProbeRowids(ClientContext &context) {
	int64_t count;
	if (!TryGetSetting(context, "ngram_max_probe_rowids", count)) {
		return NumericCast<idx_t>(DEFAULT_MAX_PROBE_ROWIDS);
	}
	if (count < 1) {
		throw InvalidInputException("ngram_max_probe_rowids must be at least 1, got %lld", count);
	}
	return NumericCast<idx_t>(count);
}

idx_t ProbeMemoryBudget(ClientContext &context) {
	auto limit = DBConfig::GetConfig(context).options.maximum_memory;
	if (limit == DConstants::INVALID_INDEX) {
		limit = 8ULL * 1024ULL * 1024ULL * 1024ULL;
	}
	return MinValue<idx_t>(limit / 4, MAX_PROBE_MEMORY_BYTES);
}

bool AutoAccelerateEnabled(ClientContext &context) {
	bool enabled;
	return TryGetSetting(context, "ngram_auto_accelerate", enabled) && enabled;
}

idx_t ConfiguredBuildPartitions(ClientContext &context) {
	int64_t configured;
	if (!TryGetSetting(context, "ngram_build_partitions", configured)) {
		return 0;
	}
	if (configured < 0) {
		throw InvalidInputException("ngram_build_partitions cannot be negative, got %lld", configured);
	}
	return NumericCast<idx_t>(configured);
}

void RegisterSettings(ExtensionLoader &loader) {
	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	config.AddExtensionOption("ngram_max_grams_per_query",
	                          "ngram index queries probe at most this many of the needle's rarest grams",
	                          LogicalType::BIGINT, Value::BIGINT(DEFAULT_MAX_GRAMS_PER_QUERY));
	config.AddExtensionOption("ngram_max_candidate_fraction",
	                          "full-result ngram queries scan when the candidate upper bound exceeds this fraction",
	                          LogicalType::DOUBLE, Value::DOUBLE(DEFAULT_MAX_CANDIDATE_FRACTION));
	config.AddExtensionOption("ngram_max_probe_rowids",
	                          "hard limit on posting rowids an ngram query may decode before scanning or erroring",
	                          LogicalType::BIGINT, Value::BIGINT(DEFAULT_MAX_PROBE_ROWIDS));
	config.AddExtensionOption("ngram_build_partitions",
	                          "how many rowid-range partitions index build, refresh and compact split their packing "
	                          "pass into; 0 sizes it from memory_limit",
	                          LogicalType::BIGINT, Value::BIGINT(0));
	config.AddExtensionOption("ngram_auto_accelerate",
	                          "rewrite contains/LIKE/ILIKE filters over ngram-indexed columns into index scans "
	                          "(opt-in; exact queries fall back when probe resources or density are too high)",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false));
}

} // namespace ngram
} // namespace duckdb
