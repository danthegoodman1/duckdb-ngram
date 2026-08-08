#include "duckdb/common/exception.hpp"
#include "duckdb/function/table_function.hpp"
#include "ngram/postings_codec.hpp"

namespace duckdb {
namespace ngram {

//! ngram_pack_postings((SELECT gram, segment_no, rowid ... ORDER BY gram, segment_no))
//! streams key-ordered (gram, segment_no, rowid) rows and emits one postings row per
//! (gram, segment_no) run, holding only the current run in memory. This replaces a
//! grouped list() aggregate, which cannot run under a memory limit at scale (the
//! sort feeding this function spills; list() states do not). The operator runs in
//! parallel: each thread packs the runs of its own slice of the sorted stream, so a
//! key can yield one partial segment per thread. Readers must union all segments of
//! a gram, which the query path does anyway; rowids within a run need no ordering
//! (the codec sorts), only the (gram, segment_no) run structure matters.

struct PackPostingsLocalState : LocalTableFunctionState {
	bool has_current = false;
	string current_gram;
	int64_t current_segment = 0;
	vector<int64_t> rowids;
	//! resume position inside the current input chunk after a full output chunk
	idx_t input_offset = 0;
};

static unique_ptr<FunctionData> PackPostingsBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	auto &types = input.input_table_types;
	if (types.size() != 3 || types[0].id() != LogicalTypeId::VARCHAR || types[1].id() != LogicalTypeId::BIGINT ||
	    types[2].id() != LogicalTypeId::BIGINT) {
		throw BinderException("ngram_pack_postings expects a table of (gram VARCHAR, segment_no BIGINT, rowid BIGINT)");
	}
	return_types = {LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BLOB,
	                LogicalType::BIGINT,  LogicalType::BIGINT, LogicalType::BIGINT};
	names = {"gram", "segment_no", "postings", "rowid_count", "min_rowid", "max_rowid"};
	return make_uniq<TableFunctionData>();
}

static unique_ptr<GlobalTableFunctionState> PackPostingsInitGlobal(ClientContext &context,
                                                                   TableFunctionInitInput &input) {
	return make_uniq<GlobalTableFunctionState>();
}

static unique_ptr<LocalTableFunctionState> PackPostingsInitLocal(ExecutionContext &context,
                                                                 TableFunctionInitInput &input,
                                                                 GlobalTableFunctionState *global_state) {
	return make_uniq<PackPostingsLocalState>();
}

static void EmitRun(PackPostingsLocalState &state, DataChunk &output, idx_t out_idx) {
	// EncodePostings sorts and dedupes state.rowids in place, so count/min/max are
	// read after encoding
	auto blob = EncodePostings(state.rowids);
	FlatVector::GetData<string_t>(output.data[0])[out_idx] =
	    StringVector::AddString(output.data[0], state.current_gram);
	FlatVector::GetData<int64_t>(output.data[1])[out_idx] = state.current_segment;
	FlatVector::GetData<string_t>(output.data[2])[out_idx] = StringVector::AddStringOrBlob(output.data[2], blob);
	FlatVector::GetData<int64_t>(output.data[3])[out_idx] = static_cast<int64_t>(state.rowids.size());
	FlatVector::GetData<int64_t>(output.data[4])[out_idx] = state.rowids.front();
	FlatVector::GetData<int64_t>(output.data[5])[out_idx] = state.rowids.back();
}

static OperatorResultType PackPostingsFunction(ExecutionContext &context, TableFunctionInput &data_p, DataChunk &input,
                                               DataChunk &output) {
	auto &state = data_p.local_state->Cast<PackPostingsLocalState>();

	UnifiedVectorFormat gram_format, segment_format, rowid_format;
	input.data[0].ToUnifiedFormat(input.size(), gram_format);
	input.data[1].ToUnifiedFormat(input.size(), segment_format);
	input.data[2].ToUnifiedFormat(input.size(), rowid_format);
	auto grams = UnifiedVectorFormat::GetData<string_t>(gram_format);
	auto segments = UnifiedVectorFormat::GetData<int64_t>(segment_format);
	auto rowids = UnifiedVectorFormat::GetData<int64_t>(rowid_format);

	idx_t out_count = 0;
	while (state.input_offset < input.size()) {
		auto row = state.input_offset;
		auto gram_idx = gram_format.sel->get_index(row);
		auto segment_idx = segment_format.sel->get_index(row);
		auto rowid_idx = rowid_format.sel->get_index(row);
		if (!gram_format.validity.RowIsValid(gram_idx) || !segment_format.validity.RowIsValid(segment_idx) ||
		    !rowid_format.validity.RowIsValid(rowid_idx)) {
			throw InvalidInputException("ngram_pack_postings: input must not contain NULLs");
		}
		auto &gram = grams[gram_idx];
		auto segment_no = segments[segment_idx];

		bool boundary = !state.has_current || segment_no != state.current_segment ||
		                gram != string_t(state.current_gram.data(), state.current_gram.size());
		if (boundary) {
			if (state.has_current) {
				if (out_count >= STANDARD_VECTOR_SIZE) {
					// Defensive only: a call emits at most one run per input row, so
					// while the output chunk's capacity is >= the input chunk's this
					// path cannot be reached (and no test exercises it). Kept so a
					// future capacity change degrades to resumption, not corruption.
					output.SetCardinality(out_count);
					return OperatorResultType::HAVE_MORE_OUTPUT;
				}
				EmitRun(state, output, out_count++);
			}
			state.has_current = true;
			state.current_gram.assign(gram.GetData(), gram.GetSize());
			state.current_segment = segment_no;
			state.rowids.clear();
		}
		state.rowids.push_back(rowids[rowid_idx]);
		state.input_offset++;
	}
	state.input_offset = 0;
	output.SetCardinality(out_count);
	return OperatorResultType::NEED_MORE_INPUT;
}

static OperatorFinalizeResultType PackPostingsFinal(ExecutionContext &context, TableFunctionInput &data_p,
                                                    DataChunk &output) {
	auto &state = data_p.local_state->Cast<PackPostingsLocalState>();
	idx_t out_count = 0;
	if (state.has_current) {
		EmitRun(state, output, out_count++);
		state.has_current = false;
		state.rowids.clear();
	}
	output.SetCardinality(out_count);
	return OperatorFinalizeResultType::FINISHED;
}

void RegisterPackPostings(ExtensionLoader &loader) {
	TableFunction pack("ngram_pack_postings", {LogicalType::TABLE}, nullptr, PackPostingsBind, PackPostingsInitGlobal,
	                   PackPostingsInitLocal);
	pack.in_out_function = PackPostingsFunction;
	pack.in_out_function_final = PackPostingsFinal;
	loader.RegisterFunction(pack);
}

} // namespace ngram
} // namespace duckdb
