#include "ngram/probe.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/table_filter_set.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/transaction/local_storage.hpp"
#include "ngram/catalog.hpp"
#include "ngram/postings.hpp"
#include "ngram/search_core.hpp"
#include "ngram/settings.hpp"

#include <algorithm>
#include <limits>

namespace duckdb {
namespace ngram {

ProbeMemoryReservation::ProbeMemoryReservation(BufferManager &manager_p, idx_t size_p)
    : manager(manager_p), size(size_p) {
	manager.ReserveMemory(size);
}

ProbeMemoryReservation::~ProbeMemoryReservation() {
	manager.FreeReservedMemory(size.load());
}

void ProbeMemoryReservation::Grow(idx_t extra) {
	manager.ReserveMemory(extra);
	size.fetch_add(extra);
}

void ProbeMemoryReservation::Shrink(idx_t released) {
	D_ASSERT(released <= size.load());
	manager.FreeReservedMemory(released);
	size.fetch_sub(released);
}

void ProbeDecodeTracker::Add(idx_t bytes) {
	auto now = live.fetch_add(bytes) + bytes;
	auto seen = peak.load();
	while (now > seen && !peak.compare_exchange_weak(seen, now)) {
	}
}

void ProbeDecodeTracker::Release(idx_t bytes) {
	D_ASSERT(bytes <= live.load());
	live.fetch_sub(bytes);
}

static idx_t ProbeThreads(ClientContext &context) {
	return MaxValue<idx_t>(NumericCast<idx_t>(TaskScheduler::GetScheduler(context).NumberOfThreads()), 1);
}

static bool CheckedAdd(idx_t &target, idx_t value) {
	if (value > std::numeric_limits<idx_t>::max() - target) {
		return false;
	}
	target += value;
	return true;
}

static bool CheckedMultiply(idx_t left, idx_t right, idx_t &result) {
	if (left != 0 && right > std::numeric_limits<idx_t>::max() / left) {
		return false;
	}
	result = left * right;
	return true;
}

//! Every count the planner adds up is bounded by rows that exist, so leaving
//! idx_t means a corrupt count somewhere.
static void ThrowProbeOverflow() {
	throw InvalidInputException("ngram: probe arithmetic overflow");
}

//! Memory the planner needs before it reads a segments-table row: per-key
//! scratch (the decomposition's key and set entry, descriptor vectors,
//! totals, sort indexes, filter values) for the whole needle, without
//! depending on STL layouts, plus 256 KiB for each manifest worker's scan
//! chunk and state. Ordinary DuckDB allocator buffers are not charged to
//! BufferManager reservations.
static constexpr idx_t PREFLIGHT_FIXED_BYTES = 4096;
static constexpr idx_t PREFLIGHT_BYTES_PER_KEY = 256;
static constexpr idx_t PREFLIGHT_BYTES_PER_WORKER = 256 * 1024;

static idx_t PreflightProbeBytes(idx_t key_count, idx_t workers) {
	idx_t preflight_bytes = PREFLIGHT_FIXED_BYTES;
	idx_t per_key_bytes;
	idx_t worker_bytes;
	if (!CheckedMultiply(key_count, PREFLIGHT_BYTES_PER_KEY, per_key_bytes) ||
	    !CheckedMultiply(workers, PREFLIGHT_BYTES_PER_WORKER, worker_bytes) ||
	    !CheckedAdd(preflight_bytes, per_key_bytes) || !CheckedAdd(preflight_bytes, worker_bytes)) {
		ThrowProbeOverflow();
	}
	return preflight_bytes;
}

idx_t MaxProbeKeys(ClientContext &context) {
	auto budget = ProbeMemoryBudget(context);
	auto fixed = PreflightProbeBytes(0, 1);
	return budget > fixed ? (budget - fixed) / PREFLIGHT_BYTES_PER_KEY : 0;
}

//! Bytes the manifest occupies per descriptor row: the descriptor, the
//! segment slot it may open, and slack for the per-gram scratch.
static constexpr idx_t MANIFEST_BYTES_PER_ROW = sizeof(ProbeDescriptor) + sizeof(ProbeSegment) + 8;

//! The segments-table rows of one gram, as collected by its manifest scan.
struct GramRows {
	vector<ProbeDescriptor> descriptors;
	idx_t row_count = 0;
};

//! The vector-aligned, disjoint row spans of one row group whose key-column
//! segments may hold the filter's key, appended to `spans` as [start, end).
//! A column segment's zone map is exact for the fixed-width key, and the rows
//! of one key are contiguous within each generation's key-ordered run, so a
//! key admits about one segment per run. Alignment keeps two spans from
//! sharing a vector, so no row is scanned twice; the key filter drops the
//! rows of other keys inside a span. v1.5.5's own filtered scan skips only
//! the first excluded segment of a row group (RowGroup::CheckZonemapSegments
//! takes a segment's relative start for its vector index), so positioned by
//! the host it reads the whole row group: measured 108,000 rows per key on a
//! two-row-group table (docs/review/2026-09-09/format_observations.json).
static void AdmittedKeySpans(SegmentNode<RowGroup> &row_group, const StorageIndex &key_column, TableFilter &filter,
                             vector<std::pair<idx_t, idx_t>> &spans) {
	auto &column = row_group.GetNode().GetRawColumnData(key_column);
	if (column.CheckZonemap(nullptr, key_column, filter) == FilterPropagateResult::FILTER_ALWAYS_FALSE) {
		return;
	}
	auto row_group_start = row_group.GetRowStart();
	auto row_group_end = row_group.GetRowEnd();
	auto &tree = column.GetSegmentTree();
	for (auto segment = tree.GetRootSegment(); segment; segment = tree.GetNextSegment(*segment)) {
		if (segment->GetCount() == 0) {
			continue;
		}
		// the host's check reads the segment's statistics under their lock
		// and declines to prune a column with updates
		ColumnScanState segment_state(nullptr);
		segment_state.current = segment;
		segment_state.segment_tree = &tree;
		optional_ptr<SegmentNode<ColumnSegment>> checked_segment;
		if (column.CheckZonemap(segment_state, filter, checked_segment) == FilterPropagateResult::FILTER_ALWAYS_FALSE) {
			continue;
		}
		auto offset = segment->GetRowStart();
		auto start = row_group_start + offset - offset % STANDARD_VECTOR_SIZE;
		auto last = offset + segment->GetCount();
		auto end = MinValue<idx_t>(row_group_start +
		                               (last + STANDARD_VECTOR_SIZE - 1) / STANDARD_VECTOR_SIZE * STANDARD_VECTOR_SIZE,
		                           row_group_end);
		if (end <= start) {
			continue;
		}
		if (!spans.empty() && spans.back().second >= start) {
			spans.back().second = MaxValue(spans.back().second, end);
		} else {
			spans.emplace_back(start, end);
		}
	}
}

//! Read every visible segments-table row of every needle key, one key per
//! unit across the scheduler's threads. The key is the table's leading
//! sorted column, so a key's rows are read by bounded scans over the column
//! segments whose zone maps admit it, with the equality filter evaluated
//! natively on the fixed-width column, followed by this transaction's local
//! rows. Rows are validated against the high-water mark and the segment
//! capacity as they arrive, charged to the reservation chunk by chunk, and
//! their total is bounded by `max_rows`; past it the scans stop and the plan
//! declines. Returns false on that decline.
static bool CollectGramRows(ClientContext &context, DuckTransaction &tx, DuckTableEntry &segments_entry,
                            const vector<uhugeint_t> &keys, int64_t hwm, idx_t workers, idx_t max_rows,
                            ProbeMemoryReservation &reservation, vector<GramRows> &per_key, idx_t &rows_scanned,
                            idx_t &rows_visited) {
	vector<StorageIndex> column_ids;
	vector<LogicalType> types;
	AddShadowColumn(segments_entry, "gram_key", LogicalTypeId::UHUGEINT, column_ids, types);
	AddShadowColumn(segments_entry, "segment_no", LogicalTypeId::BIGINT, column_ids, types);
	AddShadowColumn(segments_entry, "rowid_count", LogicalTypeId::BIGINT, column_ids, types);
	AddShadowColumn(segments_entry, "min_rowid", LogicalTypeId::BIGINT, column_ids, types);
	AddShadowColumn(segments_entry, "max_rowid", LogicalTypeId::BIGINT, column_ids, types);
	column_ids.emplace_back(StorageIndex(COLUMN_IDENTIFIER_ROW_ID));
	types.emplace_back(LogicalType::ROW_TYPE);
	auto &storage = segments_entry.GetStorage();
	auto max_segment = hwm < 0 ? int64_t(-1) : hwm >> SEGMENT_SHIFT;

	atomic<idx_t> total_rows {0};
	atomic<idx_t> total_visited {0};
	atomic<bool> declined {false};
	ParallelForEachUnit(context, keys.size(), workers, [&](idx_t key_index) {
		auto &rows = per_key[key_index];
		TableFilterSet filters;
		filters.PushFilter(ProjectionIndex(0),
		                   ConstantComparisonFilter(ExpressionType::COMPARE_EQUAL, Value::UHUGEINT(keys[key_index])));
		auto &filter = filters.GetFilterByColumnIndexMutable(ProjectionIndex(0));
		DataChunk chunk;
		chunk.Initialize(Allocator::Get(context), types);
		// validates and appends the chunk's rows; false once the plan declines
		auto consume = [&]() {
			if (total_rows.fetch_add(chunk.size()) + chunk.size() > max_rows) {
				declined.store(true);
				return false;
			}
			// charged before the descriptors that hold them are appended
			reservation.Grow(chunk.size() * MANIFEST_BYTES_PER_ROW);
			UnifiedVectorFormat formats[6];
			for (idx_t c = 0; c < 6; c++) {
				chunk.data[c].ToUnifiedFormat(formats[c]);
			}
			auto key_data = UnifiedVectorFormat::GetData<uhugeint_t>(formats[0]);
			auto segment_data = UnifiedVectorFormat::GetData<int64_t>(formats[1]);
			auto count_data = UnifiedVectorFormat::GetData<int64_t>(formats[2]);
			auto min_data = UnifiedVectorFormat::GetData<int64_t>(formats[3]);
			auto max_data = UnifiedVectorFormat::GetData<int64_t>(formats[4]);
			auto rowid_data = UnifiedVectorFormat::GetData<row_t>(formats[5]);
			for (idx_t r = 0; r < chunk.size(); r++) {
				idx_t idx[6];
				for (idx_t c = 0; c < 6; c++) {
					idx[c] = formats[c].sel->get_index(r);
					if (!formats[c].validity.RowIsValid(idx[c])) {
						throw InvalidInputException("ngram: segments table contains NULLs; the index is malformed");
					}
				}
				auto segment_no = segment_data[idx[1]];
				if (key_data[idx[0]] != keys[key_index] || segment_no < 0 || segment_no > max_segment ||
				    count_data[idx[2]] <= 0) {
					throw InvalidInputException("ngram: invalid segments-table descriptor; the index is malformed");
				}
				auto count = NumericCast<idx_t>(count_data[idx[2]]);
				if (count > (idx_t(1) << SEGMENT_SHIFT)) {
					throw InvalidInputException(
					    "ngram: segment row_count exceeds its rowid range; the index is malformed");
				}
				auto segment_start = segment_no << SEGMENT_SHIFT;
				auto min_rowid = min_data[idx[3]];
				auto max_rowid = max_data[idx[4]];
				if (min_rowid < segment_start || max_rowid < min_rowid ||
				    max_rowid >= segment_start + (int64_t(1) << SEGMENT_SHIFT) || max_rowid > hwm) {
					throw InvalidInputException(
					    "ngram: segment rowid span lies outside its segment or past the high-water mark; the index "
					    "is malformed");
				}
				if (count_data[idx[2]] > max_rowid - min_rowid + 1) {
					// the same class as the per-segment check below, met before a
					// corrupt span can leave the segment out
					throw InvalidInputException(
					    "ngram: gram posting count exceeds its segment rowid range; the index is malformed");
				}
				if (!CheckedAdd(rows.row_count, count)) {
					ThrowProbeOverflow();
				}
				rows.descriptors.emplace_back(segment_no, key_index, rowid_data[idx[5]], count, min_rowid, max_rowid);
			}
			return true;
		};
		if (storage.GetTotalRows() > 0) {
			auto &collection = *storage.GetRowGroupCollection();
			auto row_groups = collection.GetRowGroups();
			vector<std::pair<idx_t, idx_t>> spans;
			for (auto row_group = row_groups->GetRootSegment(); row_group;
			     row_group = row_groups->GetNextSegment(*row_group)) {
				AdmittedKeySpans(*row_group, column_ids[0], filter, spans);
			}
			for (auto &span : spans) {
				ThrowIfInterrupted(context);
				if (declined.load()) {
					return;
				}
				TableScanState state;
				total_visited.fetch_add(
				    InitializeBoundedScan(context, storage, state, column_ids, &filters, span.first, span.second));
				while (true) {
					chunk.Reset();
					state.table_state.Scan(tx, chunk);
					if (chunk.size() == 0) {
						break;
					}
					if (!consume()) {
						return;
					}
				}
			}
		}
		// rows this transaction appended, a refresh in progress, are local
		TableScanState local;
		local.Initialize(column_ids, &context, &filters);
		auto &local_storage = LocalStorage::Get(tx);
		local_storage.InitializeScan(storage, local.local_state, &filters);
		while (!declined.load()) {
			ThrowIfInterrupted(context);
			chunk.Reset();
			local_storage.Scan(local.local_state, column_ids, chunk);
			if (chunk.size() == 0) {
				break;
			}
			if (!consume()) {
				return;
			}
		}
	});
	rows_scanned = total_rows.load();
	rows_visited = total_visited.load();
	return !declined.load();
}

//! Move the `max_grams` rarest grams by posting total, stable on ties, into
//! the plan as its keys and manifest, ordered by segment, gram and posting
//! rowid.
static void SelectRarestGrams(const vector<uhugeint_t> &keys, vector<GramRows> &per_key, idx_t max_grams,
                              ProbePlan &plan) {
	vector<idx_t> order(keys.size());
	for (idx_t i = 0; i < order.size(); i++) {
		order[i] = i;
	}
	std::stable_sort(order.begin(), order.end(),
	                 [&](idx_t a, idx_t b) { return per_key[a].row_count < per_key[b].row_count; });
	order.resize(MinValue<idx_t>(order.size(), max_grams));
	idx_t descriptor_count = 0;
	for (auto index : order) {
		descriptor_count += per_key[index].descriptors.size();
	}
	plan.keys.reserve(order.size());
	plan.descriptors.reserve(descriptor_count);
	for (idx_t gram_index = 0; gram_index < order.size(); gram_index++) {
		auto &rows = per_key[order[gram_index]];
		plan.keys.push_back(keys[order[gram_index]]);
		for (auto &descriptor : rows.descriptors) {
			plan.descriptors.push_back(descriptor);
			plan.descriptors.back().gram_index = gram_index;
		}
		vector<ProbeDescriptor>().swap(rows.descriptors);
	}
	std::sort(plan.descriptors.begin(), plan.descriptors.end(), [](const ProbeDescriptor &a, const ProbeDescriptor &b) {
		if (a.segment_no != b.segment_no) {
			return a.segment_no < b.segment_no;
		}
		if (a.gram_index != b.gram_index) {
			return a.gram_index < b.gram_index;
		}
		return a.posting_rowid < b.posting_rowid;
	});
}

//! Bytes one worker needs for a segment whose grams have these posting
//! counts: at the peak intersection step the current candidates, the next
//! posting list and the intersection output coexist, plus fetch scratch.
static idx_t SegmentWorkerBytes(const vector<idx_t> &counts, idx_t segment_capacity) {
	idx_t peak_rows = counts[0];
	if (counts.size() > 1) {
		idx_t current_bound = MinValue(segment_capacity, counts[0]);
		for (idx_t gram = 1; gram < counts.size(); gram++) {
			auto result_bound = MinValue(current_bound, MinValue(segment_capacity, counts[gram]));
			idx_t step_peak = current_bound;
			if (!CheckedAdd(step_peak, counts[gram]) || !CheckedAdd(step_peak, result_bound)) {
				ThrowProbeOverflow();
			}
			peak_rows = MaxValue(peak_rows, step_peak);
			current_bound = result_bound;
		}
	}
	idx_t peak_bytes;
	if (!CheckedMultiply(peak_rows, sizeof(row_t), peak_bytes) || !CheckedAdd(peak_bytes, idx_t(256 * 1024))) {
		ThrowProbeOverflow();
	}
	return peak_bytes;
}

//! Group the manifest by rowid segment and admit every segment that carries
//! all selected grams, accounting the decoded work, the peak per-worker
//! decode bytes and the peak candidate bytes a published segment holds, until
//! the work budget is exceeded, which declines the plan. The structural
//! checks continue past that point, so a decline in one segment cannot hide
//! corruption in a later one. A segment whose grams' rowid spans share no
//! row can hold no candidate and is left out; otherwise the candidates lie
//! within the spans' intersection, which bounds them beside the smallest
//! posting count and prices them for the admission gate. The published
//! candidate vector keeps the capacity of the first intersection, the
//! smallest posting count, so the memory model charges that bound.
static void AdmitSegments(ProbePlan &plan, int64_t hwm, idx_t hard_work_limit, idx_t &estimated_decoded_rowids,
                          idx_t &peak_worker_bytes, idx_t &peak_candidate_bytes) {
	auto &descriptors = plan.descriptors;
	vector<idx_t> counts;
	vector<row_t> lows;
	vector<row_t> highs;
	counts.reserve(plan.keys.size());
	for (idx_t begin = 0; begin < descriptors.size();) {
		idx_t end = begin + 1;
		while (end < descriptors.size() && descriptors[end].segment_no == descriptors[begin].segment_no) {
			end++;
		}
		counts.clear();
		lows.clear();
		highs.clear();
		idx_t current_gram = DConstants::INVALID_INDEX;
		for (idx_t i = begin; i < end; i++) {
			if (descriptors[i].gram_index != current_gram) {
				current_gram = descriptors[i].gram_index;
				counts.push_back(0);
				lows.push_back(descriptors[i].min_rowid);
				highs.push_back(descriptors[i].max_rowid);
			}
			if (!CheckedAdd(counts.back(), descriptors[i].posting_count)) {
				ThrowProbeOverflow();
			}
			lows.back() = MinValue(lows.back(), descriptors[i].min_rowid);
			highs.back() = MaxValue(highs.back(), descriptors[i].max_rowid);
		}
		auto segment_start = NumericCast<idx_t>(descriptors[begin].segment_no) << SEGMENT_SHIFT;
		auto segment_capacity =
		    MinValue<idx_t>((idx_t(1) << SEGMENT_SHIFT), NumericCast<idx_t>(hwm) - segment_start + 1);
		for (auto count : counts) {
			if (count > segment_capacity) {
				throw InvalidInputException(
				    "ngram: gram posting count exceeds its segment rowid range; the index is malformed");
			}
		}
		auto segment_begin = begin;
		begin = end;
		if (counts.size() != plan.keys.size()) {
			continue;
		}
		row_t span_low = lows[0];
		row_t span_high = highs[0];
		for (idx_t gram = 1; gram < counts.size(); gram++) {
			span_low = MaxValue(span_low, lows[gram]);
			span_high = MinValue(span_high, highs[gram]);
		}
		if (span_low > span_high) {
			continue;
		}
		auto span_rows = NumericCast<idx_t>(span_high - span_low) + 1;
		idx_t vector_bound = segment_capacity;
		for (auto count : counts) {
			vector_bound = MinValue(vector_bound, count);
		}
		auto candidate_bound = MinValue(vector_bound, span_rows);
		plan.segments.push_back(ProbeSegment {descriptors[segment_begin].segment_no, segment_begin, end});
		// smallest posting list first: every later intersection is bounded by
		// the smallest list decoded so far
		auto &gram_order = plan.segments.back().gram_order;
		gram_order.resize(counts.size());
		for (idx_t gram = 0; gram < counts.size(); gram++) {
			gram_order[gram] = gram;
		}
		std::stable_sort(gram_order.begin(), gram_order.end(), [&](idx_t a, idx_t b) { return counts[a] < counts[b]; });
		if (!plan.decline_reason.empty()) {
			continue;
		}
		for (auto count : counts) {
			if (!CheckedAdd(estimated_decoded_rowids, count)) {
				ThrowProbeOverflow();
			}
		}
		if (estimated_decoded_rowids > hard_work_limit) {
			plan.decline_reason = "decoded-rowid work budget exceeded";
			continue;
		}
		if (!CheckedAdd(plan.candidate_upper_bound, candidate_bound)) {
			ThrowProbeOverflow();
		}
		auto range_equivalent = (span_rows + RANGE_ROWS_PER_FETCH - 1) / RANGE_ROWS_PER_FETCH;
		if (!CheckedAdd(plan.admission_rows, MinValue(candidate_bound, range_equivalent))) {
			ThrowProbeOverflow();
		}
		peak_worker_bytes = MaxValue(peak_worker_bytes, SegmentWorkerBytes(counts, segment_capacity));
		peak_candidate_bytes = MaxValue(peak_candidate_bytes, vector_bound * sizeof(row_t));
	}
}

unique_ptr<ProbePlan> PlanIndexProbe(ClientContext &context, DuckTransaction &tx, DuckTableEntry &segments_entry,
                                     const vector<uhugeint_t> &keys, idx_t max_grams, int64_t hwm,
                                     double candidate_fraction, idx_t worker_cap, idx_t extra_columns) {
	D_ASSERT(!keys.empty());
	D_ASSERT(worker_cap > 0);
	auto plan = make_uniq<ProbePlan>();
	plan->segments_entry = &segments_entry;
	plan->hwm = hwm;
	plan->tracker = make_shared_ptr<ProbeDecodeTracker>();

	// Both callers supply distinct keys. Account the whole needle before
	// reading a row, collect every key's segment rows, then keep the rarest K.
	// Manifest workers are as many as the budget leaves room for after the
	// needle's keys, one at least.
	auto memory_budget = ProbeMemoryBudget(context);
	auto key_bytes = PreflightProbeBytes(keys.size(), 0);
	if (key_bytes + PREFLIGHT_BYTES_PER_WORKER > memory_budget) {
		plan->decline_reason = "query grams exceed query memory budget";
		return plan;
	}
	auto workers = MinValue<idx_t>(ProbeThreads(context), keys.size());
	workers = MinValue<idx_t>(workers, (memory_budget - key_bytes) / PREFLIGHT_BYTES_PER_WORKER);
	auto preflight_bytes = PreflightProbeBytes(keys.size(), workers);
	plan->memory_reservation =
	    make_uniq<ProbeMemoryReservation>(BufferManager::GetBufferManager(context), preflight_bytes);
	auto max_manifest_rows = (memory_budget - preflight_bytes) / MANIFEST_BYTES_PER_ROW;
	vector<GramRows> per_key(keys.size());
	if (!CollectGramRows(context, tx, segments_entry, keys, hwm, workers, max_manifest_rows, *plan->memory_reservation,
	                     per_key, plan->manifest_rows_scanned, plan->manifest_rows_visited)) {
		plan->decline_reason = "segment manifest exceeds query memory budget";
		return plan;
	}
	SelectRarestGrams(keys, per_key, max_grams, *plan);
	// the unselected grams' rows are released with their charge
	vector<GramRows>().swap(per_key);
	auto manifest_bytes = plan->descriptors.size() * MANIFEST_BYTES_PER_ROW;
	plan->memory_reservation->Shrink((plan->manifest_rows_scanned - plan->descriptors.size()) * MANIFEST_BYTES_PER_ROW);
	auto reserved_bytes = preflight_bytes + manifest_bytes;

	idx_t estimated_decoded_rowids = 0;
	idx_t peak_worker_bytes = 0;
	idx_t peak_candidate_bytes = 0;
	AdmitSegments(*plan, hwm, MaxProbeRowids(context), estimated_decoded_rowids, peak_worker_bytes,
	              peak_candidate_bytes);
	if (!plan->decline_reason.empty()) {
		return plan;
	}
	if (estimated_decoded_rowids > std::numeric_limits<idx_t>::max() / sizeof(row_t)) {
		ThrowProbeOverflow();
	}
	// Every projected column beyond the recheck's is one more fetch per kept
	// row, charged per candidate: measured against the searched column's
	// fetch, a bit-packed integer costs a tenth, a DOUBLE about one, a short
	// FSST string nine (docs/review/2026-09-09). The probe stands in for a
	// scan of the rows the index covers; both paths scan the rows past the
	// mark.
	idx_t projection_weight = 1;
	if (!CheckedAdd(projection_weight, extra_columns) ||
	    !CheckedMultiply(plan->admission_rows, projection_weight, plan->admission_rows)) {
		ThrowProbeOverflow();
	}
	auto indexed_rows = hwm < 0 ? idx_t(0) : NumericCast<idx_t>(hwm) + 1;
	if (candidate_fraction >= 0 &&
	    static_cast<double>(plan->admission_rows) > candidate_fraction * static_cast<double>(indexed_rows)) {
		plan->decline_reason = "candidate fraction exceeded";
		return plan;
	}
	if (plan->segments.empty()) {
		plan->admitted = true;
		return plan;
	}
	if (peak_worker_bytes > memory_budget - reserved_bytes) {
		plan->decline_reason = "one posting segment exceeds query memory budget";
		return plan;
	}
	// With M fetch workers the candidate queue keeps at most M segments
	// decoding, pending or published at once, and a worker fetching a popped
	// segment holds one more; when the queue is full two workers are
	// empty-handed, so at most 2M - 2 candidate vectors are alive beside M
	// decode peaks. A decode peak already holds the candidates it produces,
	// so a single worker is charged its peak alone, and M workers are charged
	// M peaks plus M - 2 candidate vectors. Workers are capped by the fetch
	// batches the candidates fill, so a plan with few segments and many
	// candidates still fetches in parallel.
	auto available_bytes = memory_budget - reserved_bytes;
	auto per_worker_bytes = peak_worker_bytes;
	if (!CheckedAdd(per_worker_bytes, peak_candidate_bytes)) {
		ThrowProbeOverflow();
	}
	auto possible_workers = (available_bytes + 2 * peak_candidate_bytes) / per_worker_bytes;
	auto fetch_units =
	    MaxValue<idx_t>(plan->segments.size(), (plan->candidate_upper_bound + FETCH_BATCH_ROWS - 1) / FETCH_BATCH_ROWS);
	plan->max_threads = MinValue<idx_t>(
	    fetch_units, MinValue<idx_t>(worker_cap, MinValue<idx_t>(ProbeThreads(context), possible_workers)));
	if (plan->max_threads <= 1) {
		plan->max_threads = 1;
		plan->workspace_bytes = peak_worker_bytes;
	} else {
		idx_t alive_candidate_bytes;
		if (!CheckedMultiply(peak_worker_bytes, plan->max_threads, plan->workspace_bytes) ||
		    !CheckedMultiply(peak_candidate_bytes, plan->max_threads - 2, alive_candidate_bytes) ||
		    !CheckedAdd(plan->workspace_bytes, alive_candidate_bytes)) {
			ThrowProbeOverflow();
		}
	}
	D_ASSERT(plan->workspace_bytes <= available_bytes);
	plan->memory_reservation->Grow(plan->workspace_bytes);
	AddShadowColumn(segments_entry, "gram_key", LogicalTypeId::UHUGEINT, plan->decode_column_ids, plan->decode_types);
	AddShadowColumn(segments_entry, "segment_no", LogicalTypeId::BIGINT, plan->decode_column_ids, plan->decode_types);
	AddShadowColumn(segments_entry, "postings", LogicalTypeId::BLOB, plan->decode_column_ids, plan->decode_types);
	AddShadowColumn(segments_entry, "rowid_count", LogicalTypeId::BIGINT, plan->decode_column_ids, plan->decode_types);
	plan->admitted = true;
	return plan;
}

//! Leave `buffer` empty with room for exactly `rows`, releasing a larger
//! allocation a previous segment left behind, so a worker's buffers never
//! exceed the peak the plan modeled for the segment it is decoding.
static void ReserveExactly(vector<row_t> &buffer, idx_t rows) {
	buffer.clear();
	if (buffer.capacity() != rows) {
		// release before reserving: a growing reserve would hold both
		// allocations at once
		vector<row_t>().swap(buffer);
		buffer.reserve(rows);
	}
}

//! Charge the difference between the worker's current rowid buffers and what
//! it last charged.
static void TrackDecodeBuffers(ProbePlan &plan, ProbeDecodeScratch &scratch, const vector<row_t> &candidates) {
	auto bytes =
	    (candidates.capacity() + scratch.postings.capacity() + scratch.intersection.capacity()) * sizeof(row_t);
	if (bytes > scratch.tracked_bytes) {
		plan.tracker->Add(bytes - scratch.tracked_bytes);
	} else {
		plan.tracker->Release(scratch.tracked_bytes - bytes);
	}
	scratch.tracked_bytes = bytes;
}

//! Decode the postings of one gram of `segment` into `postings`: every
//! descriptor row of that gram, fetched in vector-sized batches through the
//! plan's projection and checked against its manifest entry, unioned across
//! refresh generations.
static void DecodeDescriptorRange(ClientContext &context, DuckTransaction &tx, ProbePlan &plan,
                                  const ProbeSegment &segment, idx_t gram_index, ProbeDecodeScratch &scratch,
                                  vector<row_t> &postings) {
	auto &descriptors = plan.descriptors;
	idx_t begin = segment.descriptor_begin;
	while (begin < segment.descriptor_end && descriptors[begin].gram_index != gram_index) {
		begin++;
	}
	idx_t end = begin;
	idx_t expected = 0;
	while (end < segment.descriptor_end && descriptors[end].gram_index == gram_index) {
		if (!CheckedAdd(expected, descriptors[end].posting_count)) {
			ThrowProbeOverflow();
		}
		end++;
	}
	if (begin == end) {
		throw InvalidInputException("ngram: admitted segment is missing a gram; the index is malformed");
	}
	ReserveExactly(postings, expected);

	if (!scratch.initialized) {
		scratch.chunk.Initialize(Allocator::Get(context), plan.decode_types);
		scratch.initialized = true;
	}
	auto &chunk = scratch.chunk;
	auto rowid_data = FlatVector::GetDataMutable<row_t>(scratch.rowids);
	for (idx_t offset = begin; offset < end; offset += STANDARD_VECTOR_SIZE) {
		ThrowIfInterrupted(context);
		auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, end - offset);
		for (idx_t i = 0; i < count; i++) {
			rowid_data[i] = descriptors[offset + i].posting_rowid;
		}
		chunk.Reset();
		// ColumnFetchState retains every pinned block it has seen. A fresh one
		// per batch releases the previous batch's BLOBs once they are decoded,
		// so fragmented generations cannot accumulate query-wide pins.
		ColumnFetchState fetch_state;
		plan.segments_entry->GetStorage().Fetch(tx, chunk, plan.decode_column_ids, scratch.rowids, count, fetch_state);
		if (chunk.size() != count) {
			throw InvalidInputException("ngram: a manifest posting row vanished; the index is malformed");
		}
		UnifiedVectorFormat key_format, segment_format, blob_format, count_format;
		chunk.data[0].ToUnifiedFormat(key_format);
		chunk.data[1].ToUnifiedFormat(segment_format);
		chunk.data[2].ToUnifiedFormat(blob_format);
		chunk.data[3].ToUnifiedFormat(count_format);
		auto key_data = UnifiedVectorFormat::GetData<uhugeint_t>(key_format);
		auto segment_data = UnifiedVectorFormat::GetData<int64_t>(segment_format);
		auto blob_data = UnifiedVectorFormat::GetData<string_t>(blob_format);
		auto count_data = UnifiedVectorFormat::GetData<int64_t>(count_format);
		for (idx_t r = 0; r < count; r++) {
			auto key_idx = key_format.sel->get_index(r);
			auto segment_idx = segment_format.sel->get_index(r);
			auto blob_idx = blob_format.sel->get_index(r);
			auto count_idx = count_format.sel->get_index(r);
			if (!key_format.validity.RowIsValid(key_idx) || !segment_format.validity.RowIsValid(segment_idx) ||
			    !blob_format.validity.RowIsValid(blob_idx) || !count_format.validity.RowIsValid(count_idx)) {
				throw InvalidInputException("ngram: segments table contains NULLs; the index is malformed");
			}
			auto &blob = blob_data[blob_idx];
			auto encoded_count = PostingsCount(blob.GetData(), blob.GetSize());
			auto &descriptor = descriptors[offset + r];
			if (key_data[key_idx] != plan.keys[gram_index] || segment_data[segment_idx] != segment.segment_no ||
			    count_data[count_idx] <= 0 || NumericCast<idx_t>(count_data[count_idx]) != descriptor.posting_count ||
			    encoded_count != descriptor.posting_count || encoded_count > expected - postings.size()) {
				throw InvalidInputException("ngram: posting row disagrees with its manifest; the index is malformed");
			}
			DecodePostings(blob.GetData(), blob.GetSize(), postings);
			plan.decoded_rowids.fetch_add(encoded_count);
			// the row's span bounded this segment's candidates before any
			// blob was read; the blob must lie exactly within it
			if (postings[postings.size() - encoded_count] != descriptor.min_rowid ||
			    postings.back() != descriptor.max_rowid) {
				throw InvalidInputException(
				    "ngram: posting rowids disagree with the row's rowid span; the index is malformed");
			}
		}
	}
	if (postings.size() != expected) {
		throw InvalidInputException("ngram: posting row count disagrees with its manifest; the index is malformed");
	}
	if (end - begin > 1) {
		// Each individual blob is strictly ascending. Only generation union
		// needs sorting and a cross-generation duplicate check.
		std::sort(postings.begin(), postings.end());
		if (std::adjacent_find(postings.begin(), postings.end()) != postings.end()) {
			throw InvalidInputException("ngram: refresh generations contain duplicate rowids; the index is malformed");
		}
	}
	auto segment_start = segment.segment_no << SEGMENT_SHIFT;
	auto segment_end = segment_start + (int64_t(1) << SEGMENT_SHIFT);
	for (auto rowid : postings) {
		if (rowid < segment_start || rowid >= segment_end || rowid > plan.hwm) {
			throw InvalidInputException("ngram: posting rowid lies outside its segment; the index is malformed");
		}
	}
}

void DecodeCandidateSegment(ClientContext &context, DuckTransaction &tx, ProbePlan &plan, idx_t segment_ordinal,
                            ProbeDecodeScratch &scratch, vector<row_t> &candidates) {
	auto &segment = plan.segments[segment_ordinal];
	D_ASSERT(!segment.gram_order.empty());
	DecodeDescriptorRange(context, tx, plan, segment, segment.gram_order[0], scratch, candidates);
	TrackDecodeBuffers(plan, scratch, candidates);
	for (idx_t position = 1; position < segment.gram_order.size() && !candidates.empty(); position++) {
		DecodeDescriptorRange(context, tx, plan, segment, segment.gram_order[position], scratch, scratch.postings);
		auto &intersection = scratch.intersection;
		ReserveExactly(intersection, MinValue(candidates.size(), scratch.postings.size()));
		TrackDecodeBuffers(plan, scratch, candidates);
		std::set_intersection(candidates.begin(), candidates.end(), scratch.postings.begin(), scratch.postings.end(),
		                      std::back_inserter(intersection));
		std::swap(candidates, intersection);
		// the previous candidates leave as soon as they are superseded
		ReserveExactly(intersection, 0);
		TrackDecodeBuffers(plan, scratch, candidates);
	}
	// the buffers this segment no longer needs leave before the next one is
	// modeled
	ReserveExactly(scratch.postings, 0);
	TrackDecodeBuffers(plan, scratch, candidates);
}

shared_ptr<vector<row_t>> TrackPublishedCandidates(ProbePlan &plan, ProbeDecodeScratch &scratch,
                                                   vector<row_t> &&candidates) {
	auto bytes = candidates.capacity() * sizeof(row_t);
	auto tracker = plan.tracker;
	// the worker's charge for this vector becomes the published charge
	scratch.tracked_bytes -= bytes;
	return shared_ptr<vector<row_t>>(new vector<row_t>(std::move(candidates)), [tracker, bytes](vector<row_t> *rowids) {
		tracker->Release(bytes);
		delete rowids;
	});
}

bool NextCandidateSegment(ClientContext &context, DuckTransaction &tx, ProbePlan &plan, ProbeDecodeScratch &scratch,
                          vector<row_t> &candidates, idx_t &segment_ordinal) {
	segment_ordinal = plan.next_segment.fetch_add(1);
	candidates.clear();
	if (segment_ordinal >= plan.segments.size()) {
		return false;
	}
	DecodeCandidateSegment(context, tx, plan, segment_ordinal, scratch, candidates);
	return true;
}

} // namespace ngram
} // namespace duckdb
