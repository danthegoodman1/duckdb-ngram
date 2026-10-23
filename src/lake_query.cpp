//===----------------------------------------------------------------------===//
// lake_query.cpp: the candidate row ids of a query over an indexed DuckLake table, and the rowid predicate that
// confines the table's scan to them.
//===----------------------------------------------------------------------===//

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/types/value_map.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/storage/statistics/numeric_stats.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "ngram/gram.hpp"
#include "ngram/lake.hpp"
#include "ngram/postings.hpp"
#include "ngram/probe.hpp"
#include "ngram/settings.hpp"

#include <algorithm>

namespace duckdb {
namespace ngram {

//! DuckDB's Parquet writer starts a row group every 122,880 rows unless a
//! DuckLake option sets another size.
constexpr int64_t DEFAULT_LAKE_ROW_GROUP_ROWS = 122880;

static string KeyHi(uhugeint_t key) {
	return to_string(key.upper);
}

static string KeyLo(uhugeint_t key) {
	return to_string(key.lower);
}

//! A filter that keeps the rows of `keys` and prunes row groups on each half.
static string KeyFilter(const vector<uhugeint_t> &keys) {
	string upper, lower;
	for (idx_t i = 0; i < keys.size(); i++) {
		upper += (i ? ", " : "") + KeyHi(keys[i]);
		lower += (i ? ", " : "") + KeyLo(keys[i]);
	}
	return "key_hi IN (" + upper + ") AND key_lo IN (" + lower + ")";
}

//! The rows a DuckLake table's Parquet writer puts in a row group: the
//! table's own option, its schema's, the lake's, or the writer's default.
static int64_t LakeRowGroupRows(LakeReader &reader, const string &schema_name, const string &table_name) {
	auto rows = reader.Query("SELECT scope, scope_entry, value FROM " + SystemFunction("ducklake_options") + "(" +
	                             Lit(reader.CatalogName()) + ") WHERE option_name = 'parquet_row_group_size'",
	                         "the lake's options");
	int64_t table_value = -1, schema_value = -1, global_value = -1;
	for (idx_t row = 0; row < rows.RowCount(); row++) {
		auto scope = rows.GetValue(0, row).ToString();
		auto entry = rows.GetValue(1, row);
		auto value = rows.GetValue(2, row).DefaultCastAs(LogicalType::BIGINT).GetValue<int64_t>();
		if (scope == "TABLE" && !entry.IsNull() &&
		    StringUtil::CIEquals(entry.ToString(), schema_name + "." + table_name)) {
			table_value = value;
		} else if (scope == "SCHEMA" && !entry.IsNull() && StringUtil::CIEquals(entry.ToString(), schema_name)) {
			schema_value = value;
		} else if (scope == "GLOBAL") {
			global_value = value;
		}
	}
	for (auto value : {table_value, schema_value, global_value}) {
		if (value > 0) {
			return value;
		}
	}
	return DEFAULT_LAKE_ROW_GROUP_ROWS;
}

//! Row ids of `postings` rows, one segment at a time: each chosen key's
//! generations united, then the keys intersected, rarest first.
static void IntersectPostings(const LakeRows &rows, const vector<uhugeint_t> &chosen, vector<int64_t> &result) {
	// segment -> chosen key -> postings rows
	map<int64_t, vector<vector<idx_t>>> segments;
	for (idx_t row = 0; row < rows.RowCount(); row++) {
		uhugeint_t key(rows.GetValue(0, row).GetValue<uint64_t>());
		key = (key << 64) | uhugeint_t(rows.GetValue(1, row).GetValue<uint64_t>());
		auto position = std::find(chosen.begin(), chosen.end(), key);
		if (position == chosen.end()) {
			continue;
		}
		auto &by_key = segments[rows.GetValue(2, row).GetValue<int64_t>()];
		by_key.resize(chosen.size());
		by_key[NumericCast<idx_t>(position - chosen.begin())].push_back(row);
	}
	vector<int64_t> current, next, decoded;
	for (auto &segment : segments) {
		auto &by_key = segment.second;
		if (std::any_of(by_key.begin(), by_key.end(), [](const vector<idx_t> &key_rows) { return key_rows.empty(); })) {
			continue;
		}
		// the smallest key first, so each intersection shrinks the set
		vector<pair<idx_t, idx_t>> order;
		for (idx_t key = 0; key < by_key.size(); key++) {
			idx_t count = 0;
			for (auto row : by_key[key]) {
				count += rows.GetValue(4, row).GetValue<int64_t>();
			}
			order.emplace_back(count, key);
		}
		std::sort(order.begin(), order.end());
		for (idx_t step = 0; step < order.size(); step++) {
			decoded.clear();
			for (auto row : by_key[order[step].second]) {
				auto blob = rows.GetValue(3, row);
				auto &bytes = StringValue::Get(blob);
				DecodePostings(bytes.data(), bytes.size(), decoded);
			}
			std::sort(decoded.begin(), decoded.end());
			decoded.erase(std::unique(decoded.begin(), decoded.end()), decoded.end());
			if (step == 0) {
				current.swap(decoded);
				continue;
			}
			next.clear();
			std::set_intersection(current.begin(), current.end(), decoded.begin(), decoded.end(),
			                      std::back_inserter(next));
			current.swap(next);
			if (current.empty()) {
				break;
			}
		}
		result.insert(result.end(), current.begin(), current.end());
	}
}

//! The settings one probe reads, checked before it reads the lake.
struct LakeProbeLimits {
	idx_t max_keys;
	idx_t max_grams;
	idx_t max_rowids;
	//! Negative when the gate is off.
	double max_row_group_fraction;
};

//! The probe proper: reads the registry, postings and change feed at the
//! caller's snapshot into `result`.
static void ProbeAtSnapshot(ClientContext &context, TableCatalogEntry &table, const string &column_name,
                            const vector<LakeNeedle> &needles, const LakeProbeLimits &limits, LakeCandidates &result) {
	auto catalog_name = table.ParentCatalog().GetName().GetIdentifierName();
	auto schema_name = table.ParentSchema().name.GetIdentifierName();
	auto table_name = table.name.GetIdentifierName();
	LakeReader reader(context, catalog_name);
	optional_ptr<LakeIndex> index;
	auto indexes = ReadLakeRegistry(reader, schema_name, table_name);
	for (auto &candidate : indexes) {
		if (StringUtil::CIEquals(candidate.column_name, column_name)) {
			index = &candidate;
		}
	}
	if (!index) {
		result.reason = "no ngram index on " + column_name;
		return;
	}
	auto stale = LakeStaleReason(context, reader, *index);
	if (!stale.empty()) {
		result.reason = stale;
		return;
	}
	auto &options = index->options;
	result.tokenizer = TokenizerSpec(options);

	// every needle narrows the rows a match can be, so the union of their keys
	// probes one intersection
	NeedleKeys keys;
	vector<string> probed_needles;
	for (auto &needle : needles) {
		if (needle.requires_ci && !options.case_insensitive) {
			continue;
		}
		NeedleKeys own;
		if (DecomposeNeedle(context, needle.text.data(), needle.text.size(), options, limits.max_keys, own) !=
		    NeedleShape::PROBEABLE) {
			continue;
		}
		probed_needles.push_back(needle.text);
		for (auto &key : own.keys) {
			if (!keys.Add(key, limits.max_keys)) {
				break;
			}
		}
	}
	if (keys.keys.empty()) {
		result.reason = "no needle the index can probe";
		return;
	}

	auto segments = reader.IndexTable(index->SegmentsTable()) + reader.At();
	// the rarest keys, ties broken by their order in the needles
	auto chosen = keys.keys;
	if (chosen.size() > limits.max_grams) {
		auto totals_rows = reader.Query("SELECT key_hi, key_lo, " + SystemFunction("sum") + "(rowid_count) FROM " +
		                                    segments + " WHERE " + KeyFilter(chosen) + " GROUP BY ALL",
		                                "the index manifest");
		vector<pair<int64_t, idx_t>> totals;
		for (idx_t key = 0; key < chosen.size(); key++) {
			totals.emplace_back(0, key);
		}
		for (idx_t row = 0; row < totals_rows.RowCount(); row++) {
			uhugeint_t key(totals_rows.GetValue(0, row).GetValue<uint64_t>());
			key = (key << 64) | uhugeint_t(totals_rows.GetValue(1, row).GetValue<uint64_t>());
			auto position = std::find(chosen.begin(), chosen.end(), key);
			if (position != chosen.end()) {
				totals[NumericCast<idx_t>(position - chosen.begin())].first =
				    totals_rows.GetValue(2, row).DefaultCastAs(LogicalType::BIGINT).GetValue<int64_t>();
			}
		}
		std::stable_sort(totals.begin(), totals.end(),
		                 [](const pair<int64_t, idx_t> &left, const pair<int64_t, idx_t> &right) {
			                 return left.first < right.first;
		                 });
		vector<uhugeint_t> rarest;
		for (idx_t i = 0; i < limits.max_grams; i++) {
			rarest.push_back(chosen[totals[i].second]);
		}
		chosen = std::move(rarest);
	}
	result.grams_probed = chosen.size();

	auto postings = reader.Query("SELECT key_hi, key_lo, segment_no, postings, rowid_count FROM " + segments +
	                                 " WHERE " + KeyFilter(chosen),
	                             "the index postings");
	idx_t posting_rowids = 0;
	for (idx_t row = 0; row < postings.RowCount(); row++) {
		posting_rowids += NumericCast<idx_t>(postings.GetValue(4, row).GetValue<int64_t>());
	}
	if (posting_rowids > limits.max_rowids) {
		result.reason = "decoded-rowid work budget exceeded";
		return;
	}
	vector<int64_t> rowids;
	IntersectPostings(postings, chosen, rowids);
	result.posting_candidates = rowids.size();

	// rows inserted after the covered snapshot, an update's new version
	// among them, come from the change feed; any row matching the query
	// contains every needle once case is folded
	if (index->covered_snapshot < reader.Snapshot()) {
		string filter;
		for (auto &needle : probed_needles) {
			filter += string(filter.empty() ? "" : " AND ") + SystemFunction("contains") + "(" +
			          SystemFunction("lower") + "(" + Ident(column_name) + "), " + SystemFunction("lower") + "(" +
			          Lit(needle) + "))";
		}
		auto tail = reader.Query("SELECT rowid FROM " +
		                             LakeTailSource(reader, schema_name, table_name, index->covered_snapshot) +
		                             " WHERE " + filter,
		                         "the rows inserted since the index's last refresh");
		for (idx_t row = 0; row < tail.RowCount(); row++) {
			rowids.push_back(tail.GetValue(0, row).GetValue<int64_t>());
		}
		result.tail_candidates = tail.RowCount();
	}
	std::sort(rowids.begin(), rowids.end());
	rowids.erase(std::unique(rowids.begin(), rowids.end()), rowids.end());

	// The scan reads every row group a candidate falls in, so once the
	// candidates spread over most of them a plain scan reads about as much
	// and skips the per-candidate filter. Row ids run consecutively through
	// a file, so dividing them by the row-group size counts the row groups.
	auto group_rows = LakeRowGroupRows(reader, schema_name, table_name);
	idx_t groups_hit = 0;
	int64_t last_group = -1;
	for (auto rowid : rowids) {
		auto group = rowid / group_rows;
		if (group != last_group) {
			groups_hit++;
			last_group = group;
		}
	}
	// The table's row groups: its live rows, which DuckLake counts from its
	// statistics, fill at least this many, and its row ids span at least to
	// the highest candidate. Deleted rows leave gaps in the row ids, so after
	// many deletes the span is the larger.
	auto cardinality = table.GetStorageInfo(context).cardinality;
	auto table_rows = cardinality.IsValid() ? NumericCast<int64_t>(cardinality.GetIndex()) : int64_t(0);
	auto span_groups = rowids.empty() ? int64_t(1) : rowids.back() / group_rows + 1;
	auto table_groups = MaxValue<int64_t>((table_rows + group_rows - 1) / group_rows, span_groups);
	result.row_group_fraction = double(groups_hit) / double(table_groups);
	if (limits.max_row_group_fraction >= 0 && result.row_group_fraction > limits.max_row_group_fraction) {
		result.reason =
		    StringUtil::Format("candidates fall in %.0f%% of the row groups", 100 * result.row_group_fraction);
		return;
	}
	result.rowids = make_shared_ptr<const vector<int64_t>>(std::move(rowids));
	result.use_index = true;
}

LakeCandidates ProbeLakeIndex(ClientContext &context, TableCatalogEntry &table, const string &column_name,
                              const vector<LakeNeedle> &needles, LakeProbeMode mode) {
	LakeCandidates result;
	LakeProbeLimits limits {MaxProbeKeys(context), MaxLakeGramsPerQuery(context), MaxProbeRowids(context),
	                        mode == LakeProbeMode::PLAN ? MaxLakeRowGroupFraction(context) : -1.0};
	auto catalog_name = table.ParentCatalog().GetName().GetIdentifierName();
	// the transaction's own writes are in no snapshot: an update keeps its
	// row's id and changes the row with no record the index could read
	if (LakeWrittenByTransaction(context, catalog_name)) {
		result.reason = "the transaction has written the lake";
		return result;
	}
	if (!LakeRegistryExists(context, catalog_name)) {
		result.reason = "no ngram index";
		return result;
	}
	if (mode == LakeProbeMode::CANDIDATES) {
		ProbeAtSnapshot(context, table, column_name, needles, limits, result);
		return result;
	}
	try {
		ProbeAtSnapshot(context, table, column_name, needles, limits, result);
	} catch (std::exception &ex) {
		// the scan answers alone when the index cannot be read at the
		// statement's snapshot, for instance after the snapshot expired
		ErrorData error(ex);
		if (error.Type() == ExceptionType::INTERRUPT || error.Type() == ExceptionType::INTERNAL ||
		    error.Type() == ExceptionType::FATAL) {
			throw;
		}
		result = LakeCandidates();
		result.reason = "the index could not be read: " + error.RawMessage();
	}
	return result;
}

//===----------------------------------------------------------------------===//
// The rowid predicate
//===----------------------------------------------------------------------===//

//! __ngram_lake_rowids(rowid): whether the row id is a candidate. The
//! extension builds it, with its candidates, while it plans a query; it has
//! no SQL spelling.
static constexpr const char *LAKE_ROWIDS_FUNCTION = "__ngram_lake_rowids";

struct LakeRowidsData : public FunctionData {
	explicit LakeRowidsData(shared_ptr<const vector<int64_t>> rowids_p) : rowids(std::move(rowids_p)) {
	}
	shared_ptr<const vector<int64_t>> rowids;

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<LakeRowidsData>(rowids);
	}
	bool Equals(const FunctionData &other) const override {
		return rowids == other.Cast<LakeRowidsData>().rowids;
	}
};

static void LakeRowidsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &rowids = *state.expr.Cast<BoundFunctionExpression>().BindInfo()->Cast<LakeRowidsData>().rowids;
	auto input = args.data[0].Values<int64_t>();
	auto writer = FlatVector::Writer<bool>(result, args.size());
	// a scan hands row ids out in ascending runs, so each search starts where
	// the last one ended unless the row id went back
	auto cursor = rowids.begin();
	for (auto entry : input) {
		if (!entry.IsValid()) {
			writer.WriteNull();
			continue;
		}
		auto rowid = entry.GetValue();
		if (cursor != rowids.begin() && *(cursor - 1) >= rowid) {
			cursor = rowids.begin();
		}
		cursor = std::lower_bound(cursor, rowids.end(), rowid);
		writer.WriteValue(cursor != rowids.end() && *cursor == rowid);
	}
}

//! A row group is skipped when no candidate lies between its least and
//! greatest row id.
static FilterPropagateResult LakeRowidsPrune(const FunctionStatisticsPruneInput &input) {
	if (!input.bind_data || input.child_stats.empty() || !input.child_stats[0] ||
	    !NumericStats::HasMinMax(*input.child_stats[0])) {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}
	auto &rowids = *input.bind_data->Cast<LakeRowidsData>().rowids;
	auto &stats = *input.child_stats[0];
	auto low = NumericStats::Min(stats).GetValue<int64_t>();
	auto high = NumericStats::Max(stats).GetValue<int64_t>();
	auto first = std::lower_bound(rowids.begin(), rowids.end(), low);
	if (first == rowids.end() || *first > high) {
		return FilterPropagateResult::FILTER_ALWAYS_FALSE;
	}
	return FilterPropagateResult::NO_PRUNING_POSSIBLE;
}

static unique_ptr<FunctionData> LakeRowidsBind(BindScalarFunctionInput &input) {
	throw BinderException("%s is internal to the ngram extension", LAKE_ROWIDS_FUNCTION);
}

static ScalarFunction LakeRowidsScalar() {
	ScalarFunction function(LAKE_ROWIDS_FUNCTION, {LogicalType::BIGINT}, LogicalType::BOOLEAN, LakeRowidsFunction,
	                        LakeRowidsBind);
	function.SetFilterPruneCallback(LakeRowidsPrune);
	return function;
}

unique_ptr<Expression> LakeRowidFilter(const LakeCandidates &candidates, unique_ptr<Expression> rowid) {
	vector<unique_ptr<Expression>> children;
	children.push_back(std::move(rowid));
	return make_uniq<BoundFunctionExpression>(BoundScalarFunction(LakeRowidsScalar()), std::move(children),
	                                          make_uniq<LakeRowidsData>(candidates.rowids));
}

//===----------------------------------------------------------------------===//
// ngram_search's predicate
//===----------------------------------------------------------------------===//

//! __ngram_search_match(column, needle, case_insensitive): whether the column
//! contains the needle, both folded as an index folds them when
//! case_insensitive holds. ngram_search over a DuckLake table filters its scan
//! with it, which also tells the optimizer extension to use the index.
static void SearchMatchFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto haystacks = args.data[0].Values<string_t>();
	auto needles = args.data[1].Values<string_t>();
	auto folds = args.data[2].Values<bool>();
	auto writer = FlatVector::Writer<bool>(result, args.size());
	GramOptions folding;
	string folded_haystack, folded_needle, last_needle;
	vector<idx_t> offsets;
	bool last_fold = false;
	for (idx_t row = 0; row < args.size(); row++) {
		auto haystack = haystacks[row];
		auto needle = needles[row];
		auto fold = folds[row];
		if (!haystack.IsValid() || !needle.IsValid() || !fold.IsValid()) {
			writer.WriteNull();
			continue;
		}
		auto needle_text = needle.GetValue().GetString();
		if (!fold.GetValue()) {
			writer.WriteValue(haystack.GetValue().GetString().find(needle_text) != string::npos);
			continue;
		}
		if (needle_text != last_needle || !last_fold) {
			folding.case_insensitive = true;
			NormalizeString(needle_text.data(), needle_text.size(), folding, folded_needle, offsets);
			last_needle = needle_text;
			last_fold = true;
		}
		auto text = haystack.GetValue();
		NormalizeString(text.GetData(), text.GetSize(), folding, folded_haystack, offsets);
		writer.WriteValue(folded_haystack.find(folded_needle) != string::npos);
	}
}

void RegisterLake(ExtensionLoader &loader) {
	loader.RegisterFunction(LakeRowidsScalar());
	loader.RegisterFunction(ScalarFunction("__ngram_search_match",
	                                       {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BOOLEAN},
	                                       LogicalType::BOOLEAN, SearchMatchFunction));
}

} // namespace ngram
} // namespace duckdb
