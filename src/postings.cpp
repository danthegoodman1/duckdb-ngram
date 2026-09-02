#include "ngram/postings.hpp"

#include "ngram/catalog.hpp"

#include <algorithm>
#include <limits>

namespace duckdb {
namespace ngram {

static void AppendVarint(string &blob, uint64_t value) {
	while (value >= 0x80) {
		blob.push_back(static_cast<char>((value & 0x7F) | 0x80));
		value >>= 7;
	}
	blob.push_back(static_cast<char>(value));
}

static uint64_t ReadVarint(const char *data, idx_t size, idx_t &pos) {
	uint64_t value = 0;
	idx_t shift = 0;
	while (true) {
		if (pos >= size || shift > 63) {
			throw InvalidInputException("ngram: malformed postings blob");
		}
		auto byte = static_cast<uint8_t>(data[pos++]);
		// at shift 63 only the lowest payload bit is in range; anything above
		// would silently wrap out of uint64
		if (shift == 63 && (byte & 0x7E) != 0) {
			throw InvalidInputException("ngram: malformed postings blob");
		}
		value |= static_cast<uint64_t>(byte & 0x7F) << shift;
		if (!(byte & 0x80)) {
			// counts and rowid deltas both live in [0, INT64_MAX]
			if (value > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
				throw InvalidInputException("ngram: malformed postings blob");
			}
			return value;
		}
		shift += 7;
	}
}

//! Encode rowids into a postings blob. The input is sorted and deduplicated in
//! place; negative rowids throw.
static string EncodePostings(vector<int64_t> &rowids) {
	for (auto rowid : rowids) {
		if (rowid < 0) {
			throw InvalidInputException("ngram: postings cannot contain negative rowids");
		}
	}
	std::sort(rowids.begin(), rowids.end());
	rowids.erase(std::unique(rowids.begin(), rowids.end()), rowids.end());

	string blob;
	blob.push_back(static_cast<char>(POSTINGS_FORMAT_VERSION));
	AppendVarint(blob, rowids.size());
	uint64_t previous = 0;
	for (idx_t i = 0; i < rowids.size(); i++) {
		auto current = static_cast<uint64_t>(rowids[i]);
		AppendVarint(blob, i == 0 ? current : current - previous);
		previous = current;
	}
	return blob;
}

void DecodePostings(const char *data, idx_t size, vector<int64_t> &result) {
	if (size == 0 || static_cast<uint8_t>(data[0]) != POSTINGS_FORMAT_VERSION) {
		throw InvalidInputException("ngram: unknown postings blob format");
	}
	idx_t pos = 1;
	auto count = ReadVarint(data, size, pos);
	uint64_t current = 0;
	for (uint64_t i = 0; i < count; i++) {
		auto delta = ReadVarint(data, size, pos);
		// deltas between sorted unique rowids are >= 1 (the first entry is an
		// absolute rowid and may be 0); the sum must stay within int64
		if (i > 0 && delta == 0) {
			throw InvalidInputException("ngram: malformed postings blob");
		}
		if (delta > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) - current) {
			throw InvalidInputException("ngram: malformed postings blob");
		}
		current += delta;
		result.push_back(static_cast<int64_t>(current));
	}
	if (pos != size) {
		throw InvalidInputException("ngram: malformed postings blob");
	}
}

idx_t PostingsCount(const char *data, idx_t size) {
	if (size == 0 || static_cast<uint8_t>(data[0]) != POSTINGS_FORMAT_VERSION) {
		throw InvalidInputException("ngram: unknown postings blob format");
	}
	idx_t pos = 1;
	return NumericCast<idx_t>(ReadVarint(data, size, pos));
}

static void EncodePostingsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();

	UnifiedVectorFormat list_format;
	args.data[0].ToUnifiedFormat(count, list_format);
	auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(list_format);

	auto &child = ListVector::GetEntry(args.data[0]);
	auto child_size = ListVector::GetListSize(args.data[0]);
	UnifiedVectorFormat child_format;
	child.ToUnifiedFormat(child_size, child_format);
	auto child_values = UnifiedVectorFormat::GetData<int64_t>(child_format);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_strings = FlatVector::GetData<string_t>(result);
	auto &result_validity = FlatVector::Validity(result);

	vector<int64_t> rowids;
	for (idx_t row = 0; row < count; row++) {
		auto list_idx = list_format.sel->get_index(row);
		if (!list_format.validity.RowIsValid(list_idx)) {
			result_validity.SetInvalid(row);
			continue;
		}
		auto entry = list_entries[list_idx];
		rowids.clear();
		for (idx_t i = 0; i < entry.length; i++) {
			auto child_idx = child_format.sel->get_index(entry.offset + i);
			if (!child_format.validity.RowIsValid(child_idx)) {
				throw InvalidInputException("ngram: postings cannot contain NULL rowids");
			}
			rowids.push_back(child_values[child_idx]);
		}
		auto blob = EncodePostings(rowids);
		result_strings[row] = StringVector::AddStringOrBlob(result, blob);
	}

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

static void DecodePostingsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto count = args.size();

	UnifiedVectorFormat blob_format;
	args.data[0].ToUnifiedFormat(count, blob_format);
	auto blobs = UnifiedVectorFormat::GetData<string_t>(blob_format);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto list_entries = FlatVector::GetData<list_entry_t>(result);
	auto &result_validity = FlatVector::Validity(result);
	ListVector::SetListSize(result, 0);

	idx_t total = 0;
	vector<int64_t> rowids;
	for (idx_t row = 0; row < count; row++) {
		auto blob_idx = blob_format.sel->get_index(row);
		if (!blob_format.validity.RowIsValid(blob_idx)) {
			result_validity.SetInvalid(row);
			list_entries[row] = list_entry_t(total, 0);
			continue;
		}
		rowids.clear();
		DecodePostings(blobs[blob_idx].GetData(), blobs[blob_idx].GetSize(), rowids);

		if (total + rowids.size() > ListVector::GetListCapacity(result)) {
			ListVector::SetListSize(result, total);
			ListVector::Reserve(result, NextPowerOfTwo(total + rowids.size()));
		}
		auto &child = ListVector::GetEntry(result);
		auto child_values = FlatVector::GetData<int64_t>(child);
		for (idx_t i = 0; i < rowids.size(); i++) {
			child_values[total + i] = rowids[i];
		}
		list_entries[row] = list_entry_t(total, rowids.size());
		total += rowids.size();
	}
	ListVector::SetListSize(result, total);

	if (args.AllConstant()) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

//! ngram_unpack_postings((SELECT gram, segment_no, postings FROM segments))
//! reverses the packing: it streams one (gram, segment_no, rowid) row per
//! posting. Compaction groups its output back through ngram_pack_segment to
//! merge the segment rows that share a key. Its eight-column form additionally
//! checks all persisted descriptor fields and the index high-water mark before
//! compaction can filter a corrupt posting as dead. It emits at most one output
//! chunk per call and resumes where it stopped, so a single blob holding more
//! rowids than fit in a chunk is spread over several calls rather than buffered.

struct UnpackPostingsLocalState : LocalTableFunctionState {
	//! resume position inside the current input chunk
	idx_t input_offset = 0;
	//! decoded postings of the row at input_offset, and how many were emitted
	vector<int64_t> rowids;
	idx_t rowid_offset = 0;
	bool row_decoded = false;
};

static unique_ptr<FunctionData> UnpackPostingsBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	auto &types = input.input_table_types;
	if ((types.size() != 3 && types.size() != 8) || types[0].id() != LogicalTypeId::VARCHAR ||
	    types[1].id() != LogicalTypeId::BIGINT || types[2].id() != LogicalTypeId::BLOB ||
	    (types.size() == 8 && (types[3].id() != LogicalTypeId::BIGINT || types[4].id() != LogicalTypeId::BIGINT ||
	                           types[5].id() != LogicalTypeId::BIGINT || types[6].id() != LogicalTypeId::BIGINT ||
	                           types[7].id() != LogicalTypeId::BIGINT))) {
		throw BinderException(
		    "ngram_unpack_postings expects a table of (gram VARCHAR, segment_no BIGINT, postings BLOB) or its "
		    "checked eight-column form");
	}
	return_types = {LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT};
	names = {"gram", "segment_no", "r"};
	return make_uniq<TableFunctionData>();
}

static unique_ptr<GlobalTableFunctionState> UnpackPostingsInitGlobal(ClientContext &context,
                                                                     TableFunctionInitInput &input) {
	return make_uniq<GlobalTableFunctionState>();
}

static unique_ptr<LocalTableFunctionState> UnpackPostingsInitLocal(ExecutionContext &context,
                                                                   TableFunctionInitInput &input,
                                                                   GlobalTableFunctionState *global_state) {
	return make_uniq<UnpackPostingsLocalState>();
}

static OperatorResultType UnpackPostingsFunction(ExecutionContext &context, TableFunctionInput &data_p,
                                                 DataChunk &input, DataChunk &output) {
	auto &state = data_p.local_state->Cast<UnpackPostingsLocalState>();

	UnifiedVectorFormat gram_format, segment_format, blob_format, count_format, min_format, max_format,
	    generation_format, hwm_format;
	input.data[0].ToUnifiedFormat(input.size(), gram_format);
	input.data[1].ToUnifiedFormat(input.size(), segment_format);
	input.data[2].ToUnifiedFormat(input.size(), blob_format);
	auto checked = input.ColumnCount() == 8;
	if (checked) {
		input.data[3].ToUnifiedFormat(input.size(), count_format);
		input.data[4].ToUnifiedFormat(input.size(), min_format);
		input.data[5].ToUnifiedFormat(input.size(), max_format);
		input.data[6].ToUnifiedFormat(input.size(), generation_format);
		input.data[7].ToUnifiedFormat(input.size(), hwm_format);
	}
	auto grams = UnifiedVectorFormat::GetData<string_t>(gram_format);
	auto segments = UnifiedVectorFormat::GetData<int64_t>(segment_format);
	auto blobs = UnifiedVectorFormat::GetData<string_t>(blob_format);
	auto counts = checked ? UnifiedVectorFormat::GetData<int64_t>(count_format) : nullptr;
	auto minima = checked ? UnifiedVectorFormat::GetData<int64_t>(min_format) : nullptr;
	auto maxima = checked ? UnifiedVectorFormat::GetData<int64_t>(max_format) : nullptr;
	auto generations = checked ? UnifiedVectorFormat::GetData<int64_t>(generation_format) : nullptr;
	auto hwms = checked ? UnifiedVectorFormat::GetData<int64_t>(hwm_format) : nullptr;

	auto out_gram = FlatVector::GetData<string_t>(output.data[0]);
	auto out_segment = FlatVector::GetData<int64_t>(output.data[1]);
	auto out_rowid = FlatVector::GetData<int64_t>(output.data[2]);

	idx_t out_count = 0;
	while (state.input_offset < input.size()) {
		auto row = state.input_offset;
		auto gram_idx = gram_format.sel->get_index(row);
		auto segment_idx = segment_format.sel->get_index(row);
		auto blob_idx = blob_format.sel->get_index(row);
		auto count_idx = checked ? count_format.sel->get_index(row) : 0;
		auto min_idx = checked ? min_format.sel->get_index(row) : 0;
		auto max_idx = checked ? max_format.sel->get_index(row) : 0;
		auto generation_idx = checked ? generation_format.sel->get_index(row) : 0;
		auto hwm_idx = checked ? hwm_format.sel->get_index(row) : 0;
		if (!gram_format.validity.RowIsValid(gram_idx) || !segment_format.validity.RowIsValid(segment_idx) ||
		    !blob_format.validity.RowIsValid(blob_idx) ||
		    (checked &&
		     (!count_format.validity.RowIsValid(count_idx) || !min_format.validity.RowIsValid(min_idx) ||
		      !max_format.validity.RowIsValid(max_idx) || !generation_format.validity.RowIsValid(generation_idx) ||
		      !hwm_format.validity.RowIsValid(hwm_idx)))) {
			throw InvalidInputException("ngram_unpack_postings: input must not contain NULLs");
		}
		if (!state.row_decoded) {
			state.rowids.clear();
			state.rowid_offset = 0;
			DecodePostings(blobs[blob_idx].GetData(), blobs[blob_idx].GetSize(), state.rowids);
			if (checked) {
				for (auto rowid : state.rowids) {
					if (rowid < 0 || segments[segment_idx] < 0 || (rowid >> SEGMENT_SHIFT) != segments[segment_idx] ||
					    rowid > hwms[hwm_idx]) {
						throw InvalidInputException(
						    "ngram_unpack_postings: posting rowid lies outside its declared segment or indexed range");
					}
				}
				if (counts[count_idx] <= 0 || NumericCast<idx_t>(counts[count_idx]) != state.rowids.size() ||
				    state.rowids.empty() || minima[min_idx] != state.rowids.front() ||
				    maxima[max_idx] != state.rowids.back() || generations[generation_idx] < 0) {
					throw InvalidInputException(
					    "ngram_unpack_postings: segment descriptor disagrees with its postings blob");
				}
			}
			state.row_decoded = true;
		}
		while (state.rowid_offset < state.rowids.size()) {
			if (out_count >= STANDARD_VECTOR_SIZE) {
				output.SetCardinality(out_count);
				return OperatorResultType::HAVE_MORE_OUTPUT;
			}
			out_gram[out_count] = StringVector::AddString(output.data[0], grams[gram_idx]);
			out_segment[out_count] = segments[segment_idx];
			out_rowid[out_count] = state.rowids[state.rowid_offset++];
			out_count++;
		}
		state.row_decoded = false;
		state.input_offset++;
	}
	state.input_offset = 0;
	output.SetCardinality(out_count);
	return OperatorResultType::NEED_MORE_INPUT;
}

//! ngram_pack_segment(rowid) turns the rowids of one (gram, segment_no) group
//! into that key's segment row: the encoded postings blob and the count, min
//! and max the probe prunes with. Build, refresh and compact all run it under
//! a plain GROUP BY, which puts the work on DuckDB's radix-partitioned hash
//! aggregate (the partitioning design is in ngram/build_sql.hpp). The payload
//! is a function of the group's rowid set alone, because EncodePostings sorts
//! and deduplicates before encoding: arrival order and repeated rowids cannot
//! change a byte of it.
//!
//! Rowids accumulate in a linked list of blocks taken from the aggregate's
//! arena, which is buffer-manager backed, so the memory is accounted and a
//! partition too large for `memory_limit` raises OutOfMemory rather than
//! taking the process down.

namespace {

//! One block of a state's rowid buffer. Header and payload come from a single
//! arena allocation, so appending a block is one bump.
struct RowidBlock {
	RowidBlock *next;
	idx_t count;
	idx_t capacity;
	int64_t *data;
};

struct EncodePostingsState {
	RowidBlock *first;
	RowidBlock *last;
	idx_t total;
};

//! Block sizes grow geometrically so that the millions of tiny groups (a rare
//! gram in one segment) waste a few entries while a dense gram pays a handful
//! of allocations, and are capped so the largest groups do not reserve far
//! past what they use. Unused capacity is bounded by the data itself.
constexpr idx_t FIRST_BLOCK_ROWIDS = 16;
constexpr idx_t MAX_BLOCK_ROWIDS = 65536;

struct EncodePostingsOp {
	template <class STATE>
	static void Initialize(STATE &state) {
		state.first = nullptr;
		state.last = nullptr;
		state.total = 0;
	}
};

void AppendRowid(ArenaAllocator &allocator, EncodePostingsState &state, int64_t rowid) {
	if (!state.last || state.last->count == state.last->capacity) {
		auto capacity =
		    state.last ? MinValue<idx_t>(state.last->capacity * 2, MAX_BLOCK_ROWIDS) : idx_t(FIRST_BLOCK_ROWIDS);
		auto memory = allocator.AllocateAligned(sizeof(RowidBlock) + capacity * sizeof(int64_t));
		auto block = reinterpret_cast<RowidBlock *>(memory);
		block->next = nullptr;
		block->count = 0;
		block->capacity = capacity;
		block->data = reinterpret_cast<int64_t *>(memory + sizeof(RowidBlock));
		(state.last ? state.last->next : state.first) = block;
		state.last = block;
	}
	state.last->data[state.last->count++] = rowid;
	state.total++;
}

void EncodePostingsUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count, Vector &state_vector,
                          idx_t count) {
	D_ASSERT(input_count == 1);
	UnifiedVectorFormat rowid_format;
	inputs[0].ToUnifiedFormat(count, rowid_format);
	auto rowids = UnifiedVectorFormat::GetData<int64_t>(rowid_format);

	UnifiedVectorFormat states_format;
	state_vector.ToUnifiedFormat(count, states_format);
	auto states = UnifiedVectorFormat::GetData<EncodePostingsState *>(states_format);

	for (idx_t i = 0; i < count; i++) {
		auto rowid_idx = rowid_format.sel->get_index(i);
		if (!rowid_format.validity.RowIsValid(rowid_idx)) {
			throw InvalidInputException("ngram_pack_segment: input must not contain NULLs");
		}
		AppendRowid(aggr_input_data.allocator, *states[states_format.sel->get_index(i)], rowids[rowid_idx]);
	}
}

void EncodePostingsCombine(Vector &state_vector, Vector &combined, AggregateInputData &aggr_input_data, idx_t count) {
	UnifiedVectorFormat states_format;
	state_vector.ToUnifiedFormat(count, states_format);
	auto states = UnifiedVectorFormat::GetData<EncodePostingsState *>(states_format);
	auto targets = FlatVector::GetData<EncodePostingsState *>(combined);

	for (idx_t i = 0; i < count; i++) {
		auto &source = *states[states_format.sel->get_index(i)];
		auto &target = *targets[i];
		if (source.total == 0) {
			continue;
		}
		if (aggr_input_data.combine_type == AggregateCombineType::ALLOW_DESTRUCTIVE) {
			// the source's blocks live in an arena the hash table keeps alive, so
			// the target adopts them whole; the source is emptied so that a later
			// combine or finalize of it cannot count them twice
			(target.last ? target.last->next : target.first) = source.first;
			target.last = source.last;
			target.total += source.total;
			EncodePostingsOp::Initialize(source);
			continue;
		}
		for (auto block = source.first; block; block = block->next) {
			for (idx_t r = 0; r < block->count; r++) {
				AppendRowid(aggr_input_data.allocator, target, block->data[r]);
			}
		}
	}
}

void EncodePostingsFinalize(Vector &state_vector, AggregateInputData &aggr_input_data, Vector &result, idx_t count,
                            idx_t offset) {
	UnifiedVectorFormat states_format;
	state_vector.ToUnifiedFormat(count, states_format);
	auto states = UnifiedVectorFormat::GetData<EncodePostingsState *>(states_format);

	auto &children = StructVector::GetEntries(result);
	auto &postings_child = *children[0];
	auto postings = FlatVector::GetData<string_t>(postings_child);
	auto rowid_count = FlatVector::GetData<int64_t>(*children[1]);
	auto min_rowid = FlatVector::GetData<int64_t>(*children[2]);
	auto max_rowid = FlatVector::GetData<int64_t>(*children[3]);
	auto &mask = FlatVector::Validity(result);

	vector<int64_t> rowids;
	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[states_format.sel->get_index(i)];
		auto row = i + offset;
		if (state.total == 0) {
			// no rows reached this group (only possible through a FILTER clause);
			// there is no segment to describe
			mask.SetInvalid(row);
			for (auto &child : children) {
				FlatVector::Validity(*child).SetInvalid(row);
			}
			continue;
		}
		rowids.clear();
		rowids.reserve(state.total);
		for (auto block = state.first; block; block = block->next) {
			rowids.insert(rowids.end(), block->data, block->data + block->count);
		}
		// EncodePostings sorts and dedupes rowids in place, so count/min/max are
		// read after encoding — the same order the streaming packer uses
		auto blob = EncodePostings(rowids);
		postings[row] = StringVector::AddStringOrBlob(postings_child, blob);
		rowid_count[row] = NumericCast<int64_t>(rowids.size());
		min_rowid[row] = rowids.front();
		max_rowid[row] = rowids.back();
	}
}

} // namespace

void RegisterPostings(ExtensionLoader &loader) {
	auto rowid_list = LogicalType::LIST(LogicalType::BIGINT);
	loader.RegisterFunction(
	    ScalarFunction("ngram_encode_postings", {rowid_list}, LogicalType::BLOB, EncodePostingsFunction));
	loader.RegisterFunction(
	    ScalarFunction("ngram_decode_postings", {LogicalType::BLOB}, rowid_list, DecodePostingsFunction));

	TableFunction unpack("ngram_unpack_postings", {LogicalType::TABLE}, nullptr, UnpackPostingsBind,
	                     UnpackPostingsInitGlobal, UnpackPostingsInitLocal);
	unpack.in_out_function = UnpackPostingsFunction;
	loader.RegisterFunction(unpack);

	child_list_t<LogicalType> segment_fields;
	segment_fields.emplace_back("postings", LogicalType::BLOB);
	segment_fields.emplace_back("rowid_count", LogicalType::BIGINT);
	segment_fields.emplace_back("min_rowid", LogicalType::BIGINT);
	segment_fields.emplace_back("max_rowid", LogicalType::BIGINT);
	AggregateFunction encode("ngram_pack_segment", {LogicalType::BIGINT}, LogicalType::STRUCT(segment_fields),
	                         AggregateFunction::StateSize<EncodePostingsState>,
	                         AggregateFunction::StateInitialize<EncodePostingsState, EncodePostingsOp>,
	                         EncodePostingsUpdate, EncodePostingsCombine, EncodePostingsFinalize,
	                         FunctionNullHandling::DEFAULT_NULL_HANDLING);
	// the payload is a function of the group's rowid set alone: EncodePostings
	// sorts and dedupes, so neither input order nor repeated rowids change it
	encode.SetOrderDependent(AggregateOrderDependent::NOT_ORDER_DEPENDENT);
	encode.SetDistinctDependent(AggregateDistinctDependent::NOT_DISTINCT_DEPENDENT);
	loader.RegisterFunction(encode);
}

} // namespace ngram
} // namespace duckdb
