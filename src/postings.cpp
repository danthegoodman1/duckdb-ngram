#include "ngram/postings.hpp"

#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
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
	args.data[0].ToUnifiedFormat(list_format);
	auto list_entries = UnifiedVectorFormat::GetData<list_entry_t>(list_format);

	auto &child = ListVector::GetChild(args.data[0]);
	UnifiedVectorFormat child_format;
	child.ToUnifiedFormat(child_format);
	auto child_values = UnifiedVectorFormat::GetData<int64_t>(child_format);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_strings = FlatVector::GetDataMutable<string_t>(result);
	auto &result_validity = FlatVector::ValidityMutable(result);

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
	args.data[0].ToUnifiedFormat(blob_format);
	auto blobs = UnifiedVectorFormat::GetData<string_t>(blob_format);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto list_entries = FlatVector::GetDataMutable<list_entry_t>(result);
	auto &result_validity = FlatVector::ValidityMutable(result);
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
		auto &child = ListVector::GetChildMutable(result);
		auto child_values = FlatVector::GetDataMutable<int64_t>(child);
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

//! ngram_unpack_postings((SELECT gram_key, segment_no, postings FROM segments))
//! reverses the packing: it streams one (gram_key, segment_no, rowid) row per
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
                                                   vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto &types = input.input_table_types;
	if ((types.size() != 3 && types.size() != 8) || types[0].id() != LogicalTypeId::UHUGEINT ||
	    types[1].id() != LogicalTypeId::BIGINT || types[2].id() != LogicalTypeId::BLOB ||
	    (types.size() == 8 && (types[3].id() != LogicalTypeId::BIGINT || types[4].id() != LogicalTypeId::BIGINT ||
	                           types[5].id() != LogicalTypeId::BIGINT || types[6].id() != LogicalTypeId::BIGINT ||
	                           types[7].id() != LogicalTypeId::BIGINT))) {
		throw BinderException(
		    "ngram_unpack_postings expects a table of (gram_key UHUGEINT, segment_no BIGINT, postings BLOB) or its "
		    "checked eight-column form");
	}
	return_types = {LogicalType::UHUGEINT, LogicalType::BIGINT, LogicalType::BIGINT};
	names = {"gram_key", "segment_no", "r"};
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

	UnifiedVectorFormat key_format, segment_format, blob_format, count_format, min_format, max_format,
	    generation_format, hwm_format;
	input.data[0].ToUnifiedFormat(key_format);
	input.data[1].ToUnifiedFormat(segment_format);
	input.data[2].ToUnifiedFormat(blob_format);
	auto checked = input.ColumnCount() == 8;
	if (checked) {
		input.data[3].ToUnifiedFormat(count_format);
		input.data[4].ToUnifiedFormat(min_format);
		input.data[5].ToUnifiedFormat(max_format);
		input.data[6].ToUnifiedFormat(generation_format);
		input.data[7].ToUnifiedFormat(hwm_format);
	}
	auto keys = UnifiedVectorFormat::GetData<uhugeint_t>(key_format);
	auto segments = UnifiedVectorFormat::GetData<int64_t>(segment_format);
	auto blobs = UnifiedVectorFormat::GetData<string_t>(blob_format);
	auto counts = checked ? UnifiedVectorFormat::GetData<int64_t>(count_format) : nullptr;
	auto minima = checked ? UnifiedVectorFormat::GetData<int64_t>(min_format) : nullptr;
	auto maxima = checked ? UnifiedVectorFormat::GetData<int64_t>(max_format) : nullptr;
	auto generations = checked ? UnifiedVectorFormat::GetData<int64_t>(generation_format) : nullptr;
	auto hwms = checked ? UnifiedVectorFormat::GetData<int64_t>(hwm_format) : nullptr;

	auto out_key = FlatVector::GetDataMutable<uhugeint_t>(output.data[0]);
	auto out_segment = FlatVector::GetDataMutable<int64_t>(output.data[1]);
	auto out_rowid = FlatVector::GetDataMutable<int64_t>(output.data[2]);

	idx_t out_count = 0;
	while (state.input_offset < input.size()) {
		auto row = state.input_offset;
		auto key_idx = key_format.sel->get_index(row);
		auto segment_idx = segment_format.sel->get_index(row);
		auto blob_idx = blob_format.sel->get_index(row);
		auto count_idx = checked ? count_format.sel->get_index(row) : 0;
		auto min_idx = checked ? min_format.sel->get_index(row) : 0;
		auto max_idx = checked ? max_format.sel->get_index(row) : 0;
		auto generation_idx = checked ? generation_format.sel->get_index(row) : 0;
		auto hwm_idx = checked ? hwm_format.sel->get_index(row) : 0;
		if (!key_format.validity.RowIsValid(key_idx) || !segment_format.validity.RowIsValid(segment_idx) ||
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
				output.SetChildCardinality(out_count);
				return OperatorResultType::HAVE_MORE_OUTPUT;
			}
			out_key[out_count] = keys[key_idx];
			out_segment[out_count] = segments[segment_idx];
			out_rowid[out_count] = state.rowids[state.rowid_offset++];
			out_count++;
		}
		state.row_decoded = false;
		state.input_offset++;
	}
	state.input_offset = 0;
	output.SetChildCardinality(out_count);
	return OperatorResultType::NEED_MORE_INPUT;
}

//! ngram_pack_segment(rowid) turns the rowids of one (gram_key, segment_no) group
//! into that key's segment row: the encoded postings blob and the count, min
//! and max the probe prunes with. Build, refresh and compact all run it under
//! a plain GROUP BY, which puts the work on DuckDB's radix-partitioned hash
//! aggregate (the partitioning design is in ngram/build_sql.hpp). The payload
//! is a function of the group's rowid set alone, because EncodePostings sorts
//! and deduplicates before encoding: arrival order and repeated rowids cannot
//! change a byte of it.
//!
//! A group's rowids accumulate as offsets within its segment, since every
//! rowid of a group shares segment_no = rowid >> SEGMENT_SHIFT by construction
//! of the pack statement and an offset is below 2^20. The offsets are stored
//! as a stream of varint deltas: the rowids a thread feeds one state ascend in
//! scan order, so the dense groups that hold most postings spend one or two
//! bytes on each. The first bytes of a state's stream live in the state itself
//! and the rest in a linked list of blocks taken from the aggregate's arena,
//! which is buffer-manager backed, so the memory is accounted and a partition
//! too large for `memory_limit` raises OutOfMemory rather than taking the
//! process down.
//!
//! Most states are small: each thread's hash table abandons its groups when it
//! fills, so a common gram leaves one partial state per thread per table
//! generation. In the first partition of an enwik9 sparse(3, 8) build, half of
//! the 44 million states held one rowid and nine in ten held at most sixteen,
//! against 5.8 million groups.

namespace {

static void WriteVarint(data_ptr_t &out, uint64_t value) {
	while (value >= 0x80) {
		*out++ = static_cast<data_t>((value & 0x7F) | 0x80);
		value >>= 7;
	}
	*out++ = static_cast<data_t>(value);
}

idx_t VarintSize(uint32_t value) {
	return value < (1U << 7) ? 1 : value < (1U << 14) ? 2 : value < (1U << 21) ? 3 : value < (1U << 28) ? 4 : 5;
}

//! One block of a state's stream. The bytes follow the header in the same
//! arena allocation, so appending a block is one bump. The alignment pads the
//! header to 16 bytes where pointers take 4.
struct alignas(8) RowidBlock {
	RowidBlock *next;
	uint32_t size;
	uint32_t capacity;

	data_ptr_t Data() {
		return reinterpret_cast<data_ptr_t>(this + 1);
	}
};
static_assert(sizeof(RowidBlock) == 16, "the stream follows a 16-byte header");

struct RowidBlockList {
	RowidBlock *first;
	RowidBlock *last;
};

//! Stream bytes a state holds before it allocates blocks.
constexpr idx_t INLINE_BYTES = 16;

//! The inline size of a state whose stream has moved to blocks.
constexpr uint32_t IN_BLOCKS = std::numeric_limits<uint32_t>::max();

//! Each stretch of a stream (the inline bytes, or one block) starts from
//! offset zero, so a stretch decodes on its own and a block list moves between
//! states whole. Within a stretch each offset is coded as its delta from the
//! last plus one; the code zero restarts from offset zero, which lets offsets
//! arrive in any order, as they do when combine appends one state to another.
struct EncodePostingsState {
	//! The group's segment start, taken from its first rowid.
	int64_t base;
	//! The last offset appended, which the next delta is taken from.
	uint32_t last;
	//! Stream bytes held inline, or IN_BLOCKS.
	uint32_t inline_size;
	union {
		data_t bytes[INLINE_BYTES];
		RowidBlockList blocks;
	} storage;

	bool Empty() const {
		return inline_size == 0;
	}
	bool InBlocks() const {
		return inline_size == IN_BLOCKS;
	}
};
static_assert(sizeof(EncodePostingsState) == 32, "the inline stream shares the block list's space");

//! Block capacities double from FIRST_BLOCK_BYTES up to MAX_BLOCK_BYTES, so a
//! state reserves at most one block past what it uses. The sizes make 64-byte
//! to 1 KiB allocations.
constexpr idx_t FIRST_BLOCK_BYTES = 48;
constexpr idx_t MAX_BLOCK_BYTES = 1008;

struct EncodePostingsOp {
	template <class STATE>
	static void Initialize(STATE &state) {
		state.base = 0;
		state.last = 0;
		state.inline_size = 0;
	}
};

RowidBlock *NewBlock(ArenaAllocator &allocator, idx_t capacity) {
	auto block = reinterpret_cast<RowidBlock *>(allocator.AllocateAligned(sizeof(RowidBlock) + capacity));
	block->next = nullptr;
	block->size = 0;
	block->capacity = NumericCast<uint32_t>(capacity);
	return block;
}

//! The code of `offset` after `last`: the delta plus one, after a zero byte
//! that restarts the stretch from offset zero when `offset` descends.
void WriteOffsetCode(data_ptr_t &out, uint32_t last, uint32_t offset) {
	if (offset < last) {
		*out++ = 0;
		last = 0;
	}
	WriteVarint(out, offset - last + 1);
}

idx_t OffsetCodeSize(uint32_t last, uint32_t offset) {
	return offset < last ? 1 + VarintSize(offset + 1) : VarintSize(offset - last + 1);
}

//! Calls `emit(offset)` for each offset in a stretch of `size` stream bytes.
template <class EMIT>
void DecodeStretch(const_data_ptr_t data, idx_t size, EMIT &&emit) {
	uint32_t offset = 0;
	for (idx_t pos = 0; pos < size;) {
		uint32_t code = 0;
		idx_t shift = 0;
		data_t byte;
		do {
			byte = data[pos++];
			code |= static_cast<uint32_t>(byte & 0x7F) << shift;
			shift += 7;
		} while (byte & 0x80);
		if (code == 0) {
			offset = 0;
			continue;
		}
		offset += code - 1;
		emit(offset);
	}
}

//! Calls `emit(offset)` for each offset of the state, a stretch at a time.
template <class EMIT>
void ForEachOffset(EncodePostingsState &state, EMIT &&emit) {
	if (!state.InBlocks()) {
		DecodeStretch(state.storage.bytes, state.inline_size, emit);
		return;
	}
	for (auto block = state.storage.blocks.first; block; block = block->next) {
		DecodeStretch(block->Data(), block->size, emit);
	}
}

void AppendOffset(ArenaAllocator &allocator, EncodePostingsState &state, uint32_t offset) {
	if (!state.InBlocks()) {
		if (state.inline_size + OffsetCodeSize(state.last, offset) <= INLINE_BYTES) {
			auto out = state.storage.bytes + state.inline_size;
			WriteOffsetCode(out, state.last, offset);
			state.inline_size = NumericCast<uint32_t>(out - state.storage.bytes);
			state.last = offset;
			return;
		}
		// the inline stream moves into the first block before the list overwrites it
		auto block = NewBlock(allocator, FIRST_BLOCK_BYTES);
		memcpy(block->Data(), state.storage.bytes, state.inline_size);
		block->size = state.inline_size;
		state.storage.blocks = {block, block};
		state.inline_size = IN_BLOCKS;
	}
	auto &tail = state.storage.blocks.last;
	if (tail->size + OffsetCodeSize(state.last, offset) > tail->capacity) {
		auto block = NewBlock(allocator, MinValue<idx_t>(idx_t(tail->capacity) * 2, MAX_BLOCK_BYTES));
		tail->next = block;
		tail = block;
		// a block's stretch starts from offset zero
		state.last = 0;
	}
	auto out = tail->Data() + tail->size;
	WriteOffsetCode(out, state.last, offset);
	tail->size = NumericCast<uint32_t>(out - tail->Data());
	state.last = offset;
}

void AppendRowid(ArenaAllocator &allocator, EncodePostingsState &state, int64_t rowid) {
	if (rowid < 0) {
		throw InvalidInputException("ngram_pack_segment: rowids must not be negative");
	}
	auto base = (rowid >> SEGMENT_SHIFT) << SEGMENT_SHIFT;
	if (state.Empty()) {
		state.base = base;
	} else if (base != state.base) {
		throw InvalidInputException("ngram_pack_segment: the rowids of one group must lie in one segment");
	}
	AppendOffset(allocator, state, static_cast<uint32_t>(rowid - base));
}

void EncodePostingsUpdate(Vector inputs[], AggregateInputData &aggr_input_data, idx_t input_count, Vector &state_vector,
                          idx_t count) {
	D_ASSERT(input_count == 1);
	UnifiedVectorFormat rowid_format;
	inputs[0].ToUnifiedFormat(rowid_format);
	auto rowids = UnifiedVectorFormat::GetData<int64_t>(rowid_format);

	UnifiedVectorFormat states_format;
	state_vector.ToUnifiedFormat(states_format);
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
	state_vector.ToUnifiedFormat(states_format);
	auto states = UnifiedVectorFormat::GetData<EncodePostingsState *>(states_format);
	auto targets = FlatVector::GetData<EncodePostingsState *>(combined);

	for (idx_t i = 0; i < count; i++) {
		auto &source = *states[states_format.sel->get_index(i)];
		auto &target = *targets[i];
		if (source.Empty()) {
			continue;
		}
		if (target.Empty()) {
			target.base = source.base;
		} else if (target.base != source.base) {
			throw InvalidInputException("ngram_pack_segment: the rowids of one group must lie in one segment");
		}
		auto &allocator = aggr_input_data.allocator;
		auto append = [&](uint32_t offset) {
			AppendOffset(allocator, target, offset);
		};
		if (source.InBlocks() && aggr_input_data.combine_type == AggregateCombineType::ALLOW_DESTRUCTIVE) {
			// the source's blocks live in an arena the hash table keeps alive, so
			// the target adopts them whole, appending after the source's last
			// offset; the source is emptied so that a later combine or finalize
			// of it cannot count them twice
			if (target.InBlocks()) {
				target.storage.blocks.last->next = source.storage.blocks.first;
				target.storage.blocks.last = source.storage.blocks.last;
				target.last = source.last;
			} else {
				// the target's few inline offsets join the adopted list
				auto inline_state = target;
				target.storage = source.storage;
				target.inline_size = IN_BLOCKS;
				target.last = source.last;
				ForEachOffset(inline_state, append);
			}
			EncodePostingsOp::Initialize(source);
			continue;
		}
		ForEachOffset(source, append);
	}
}

//! Sort a group's segment offsets and drop repeats. Each thread that updated
//! the group appended its rowids in scan order, so the offsets arrive as a
//! few ascending runs, which merge in O(n log runs).
static void SortOffsets(vector<uint32_t> &offsets, vector<uint32_t> &scratch, vector<idx_t> &runs) {
	runs.clear();
	runs.push_back(0);
	for (idx_t i = 1; i < offsets.size(); i++) {
		if (offsets[i] < offsets[i - 1]) {
			runs.push_back(i);
		}
	}
	runs.push_back(offsets.size());
	while (runs.size() > 2) {
		scratch.resize(offsets.size());
		idx_t merged = 0;
		for (idx_t run = 0; run + 1 < runs.size(); run += 2) {
			auto begin = offsets.begin() + NumericCast<int64_t>(runs[run]);
			auto middle = offsets.begin() + NumericCast<int64_t>(runs[run + 1]);
			auto end = run + 2 < runs.size() ? offsets.begin() + NumericCast<int64_t>(runs[run + 2]) : middle;
			std::merge(begin, middle, middle, end, scratch.begin() + NumericCast<int64_t>(runs[run]));
			runs[merged++] = runs[run];
		}
		runs[merged++] = offsets.size();
		runs.resize(merged);
		offsets.swap(scratch);
	}
	offsets.erase(std::unique(offsets.begin(), offsets.end()), offsets.end());
}

//! The v1 blob of the sorted, unique rowids base + offsets[i], into `blob`:
//! the bytes EncodePostings writes for the same rowids.
static string_t EncodeSortedOffsets(int64_t base, const vector<uint32_t> &offsets, vector<data_t> &blob) {
	// a format byte, two varints of at most ten bytes, and deltas below 2^20,
	// three bytes each at most
	blob.resize(1 + 10 + 10 + 3 * offsets.size());
	auto out = blob.data();
	*out++ = POSTINGS_FORMAT_VERSION;
	WriteVarint(out, offsets.size());
	WriteVarint(out, static_cast<uint64_t>(base) + offsets[0]);
	for (idx_t i = 1; i < offsets.size(); i++) {
		WriteVarint(out, offsets[i] - offsets[i - 1]);
	}
	return string_t(const_char_ptr_cast(blob.data()), NumericCast<uint32_t>(out - blob.data()));
}

void EncodePostingsFinalize(Vector &state_vector, AggregateFinalizeInputData &aggr_input_data, Vector &result,
                            idx_t count, idx_t offset) {
	UnifiedVectorFormat states_format;
	state_vector.ToUnifiedFormat(states_format);
	auto states = UnifiedVectorFormat::GetData<EncodePostingsState *>(states_format);

	auto &children = StructVector::GetEntries(result);
	auto &postings_child = children[0];
	auto postings = FlatVector::GetDataMutable<string_t>(postings_child);
	auto rowid_count = FlatVector::GetDataMutable<int64_t>(children[1]);
	auto min_rowid = FlatVector::GetDataMutable<int64_t>(children[2]);
	auto max_rowid = FlatVector::GetDataMutable<int64_t>(children[3]);
	auto &mask = FlatVector::ValidityMutable(result);

	vector<uint32_t> offsets;
	vector<uint32_t> scratch;
	vector<idx_t> runs;
	vector<data_t> blob;
	for (idx_t i = 0; i < count; i++) {
		auto &state = *states[states_format.sel->get_index(i)];
		auto row = i + offset;
		if (state.Empty()) {
			// no rows reached this group (only possible through a FILTER clause);
			// there is no segment to describe
			mask.SetInvalid(row);
			for (auto &child : children) {
				FlatVector::ValidityMutable(child).SetInvalid(row);
			}
			continue;
		}
		offsets.clear();
		ForEachOffset(state, [&](uint32_t value) { offsets.push_back(value); });
		// count, min and max describe the deduplicated set the blob encodes
		SortOffsets(offsets, scratch, runs);
		postings[row] = StringVector::AddStringOrBlob(postings_child, EncodeSortedOffsets(state.base, offsets, blob));
		rowid_count[row] = NumericCast<int64_t>(offsets.size());
		min_rowid[row] = state.base + offsets.front();
		max_rowid[row] = state.base + offsets.back();
	}
}

} // namespace

void RegisterPostings(ExtensionLoader &loader) {
	auto rowid_list = LogicalType::LIST(LogicalType::BIGINT);
	// both reject malformed input
	auto encode_postings =
	    ScalarFunction("ngram_encode_postings", {rowid_list}, LogicalType::BLOB, EncodePostingsFunction);
	encode_postings.SetFallible();
	loader.RegisterFunction(encode_postings);
	auto decode_postings =
	    ScalarFunction("ngram_decode_postings", {LogicalType::BLOB}, rowid_list, DecodePostingsFunction);
	decode_postings.SetFallible();
	loader.RegisterFunction(decode_postings);

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
