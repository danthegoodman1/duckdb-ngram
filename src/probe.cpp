#include "ngram/probe.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/string_map_set.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
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
	manager.FreeReservedMemory(size);
}

void ProbeMemoryReservation::Grow(idx_t extra) {
	manager.ReserveMemory(extra);
	size += extra;
}

static idx_t ProbeThreads(ClientContext &context) {
	return MaxValue<idx_t>(NumericCast<idx_t>(TaskScheduler::GetScheduler(context).NumberOfThreads()), 1);
}

static idx_t StatsWorkers(ClientContext &context, DuckTableEntry &stats_entry) {
	return MaxValue<idx_t>(MinValue(ProbeThreads(context), stats_entry.GetStorage().MaxThreads(context)), 1);
}

//! Counts recorded for one gram. `segment_count` is the exact number of
//! visible segment-table rows (including refresh generations), so it also
//! bounds the manifest before that vector is allowed to grow.
struct GramStats {
	idx_t row_count = 0;
	idx_t segment_count = 0;
};

struct AtomicGramStats {
	atomic<idx_t> row_count {0};
	atomic<idx_t> segment_count {0};
};

static void CheckedAtomicAdd(atomic<idx_t> &target, idx_t value) {
	auto current = target.load();
	while (true) {
		if (value > std::numeric_limits<idx_t>::max() - current) {
			throw InvalidInputException("ngram: stats counts overflow; the index is malformed");
		}
		if (target.compare_exchange_weak(current, current + value)) {
			return;
		}
	}
}

static vector<GramStats> ReadGramStats(ClientContext &context, DuckTransaction &tx, DuckTableEntry &stats_entry,
                                       const vector<string> &grams, idx_t workers, idx_t &rows_scanned,
                                       idx_t &chunks_scanned) {
	// string_t keys reference the immutable query grams for this scan. Looking
	// up a stats value therefore allocates no per-row scratch, even when a
	// malformed/unrelated stats gram is very large.
	string_map_t<idx_t> gram_index;
	for (idx_t i = 0; i < grams.size(); i++) {
		gram_index.emplace(string_t(grams[i].data(), NumericCast<uint32_t>(grams[i].size())), i);
	}
	unique_ptr<AtomicGramStats[]> totals(new AtomicGramStats[grams.size()]);

	vector<StorageIndex> column_ids;
	vector<LogicalType> types;
	AddShadowColumn(stats_entry, "gram", LogicalTypeId::VARCHAR, column_ids, types);
	AddShadowColumn(stats_entry, "row_count", LogicalTypeId::BIGINT, column_ids, types);
	AddShadowColumn(stats_entry, "segment_count", LogicalTypeId::BIGINT, column_ids, types);
	vector<Value> gram_values;
	gram_values.reserve(grams.size());
	for (auto &gram : grams) {
		gram_values.emplace_back(gram);
	}
	TableFilterSet filters;
	// Raw storage scans cannot execute an InFilter directly. OptionalFilter
	// uses it only for row-group pruning; the map below remains the exact row
	// filter and accumulator across every visible refresh generation.
	filters.PushFilter(ColumnIndex(0), make_uniq<OptionalFilter>(make_uniq<InFilter>(std::move(gram_values))));
	atomic<idx_t> scanned_rows {0};
	atomic<idx_t> scanned_chunks {0};
	ParallelScanShadowTable(
	    context, tx, stats_entry.GetStorage(), column_ids, types, &filters, workers,
	    [&](DataChunk &chunk, idx_t worker) {
		    scanned_rows.fetch_add(chunk.size());
		    scanned_chunks.fetch_add(1);
		    UnifiedVectorFormat gram_format, row_format, segment_format;
		    chunk.data[0].ToUnifiedFormat(chunk.size(), gram_format);
		    chunk.data[1].ToUnifiedFormat(chunk.size(), row_format);
		    chunk.data[2].ToUnifiedFormat(chunk.size(), segment_format);
		    auto gram_data = UnifiedVectorFormat::GetData<string_t>(gram_format);
		    auto row_data = UnifiedVectorFormat::GetData<int64_t>(row_format);
		    auto segment_data = UnifiedVectorFormat::GetData<int64_t>(segment_format);
		    for (idx_t r = 0; r < chunk.size(); r++) {
			    auto gram_idx = gram_format.sel->get_index(r);
			    auto row_idx = row_format.sel->get_index(r);
			    auto segment_idx = segment_format.sel->get_index(r);
			    if (!gram_format.validity.RowIsValid(gram_idx)) {
				    continue;
			    }
			    auto entry = gram_index.find(gram_data[gram_idx]);
			    if (entry == gram_index.end()) {
				    continue;
			    }
			    if (!row_format.validity.RowIsValid(row_idx) || !segment_format.validity.RowIsValid(segment_idx)) {
				    throw InvalidInputException("ngram: requested stats row contains NULLs; the index is malformed");
			    }
			    if (row_data[row_idx] <= 0 || segment_data[segment_idx] <= 0) {
				    throw InvalidInputException("ngram: invalid gram row in stats table; the index is malformed");
			    }
			    auto rows = NumericCast<idx_t>(row_data[row_idx]);
			    auto segments = NumericCast<idx_t>(segment_data[segment_idx]);
			    CheckedAtomicAdd(totals[entry->second].row_count, rows);
			    CheckedAtomicAdd(totals[entry->second].segment_count, segments);
		    }
	    });
	rows_scanned = scanned_rows.load();
	chunks_scanned = scanned_chunks.load();
	vector<GramStats> result(grams.size());
	for (idx_t gram = 0; gram < grams.size(); gram++) {
		result[gram].row_count = totals[gram].row_count.load();
		result[gram].segment_count = totals[gram].segment_count.load();
	}
	return result;
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

//! Memory the planner needs before it reads any stats: string copies, hash
//! nodes and buckets, stats counters, filter values and sort indexes for the
//! whole needle, without depending on STL node layouts, plus 256 KiB for each
//! stats worker's scan chunk and state. The scan's row loop allocates no
//! per-row scratch, and ordinary DuckDB allocator buffers are not charged to
//! BufferManager reservations.
static idx_t PreflightProbeBytes(const vector<string> &grams, idx_t stats_workers) {
	idx_t preflight_bytes = 4096;
	idx_t per_gram_bytes;
	idx_t stats_worker_bytes;
	if (!CheckedMultiply(grams.size(), idx_t(256), per_gram_bytes) ||
	    !CheckedMultiply(stats_workers, idx_t(256 * 1024), stats_worker_bytes) ||
	    !CheckedAdd(preflight_bytes, per_gram_bytes) || !CheckedAdd(preflight_bytes, stats_worker_bytes)) {
		ThrowProbeOverflow();
	}
	for (auto &gram : grams) {
		idx_t string_bytes;
		// query gram + stats-filter Value + selected-plan copy (only K are
		// selected, but charging all grams keeps this a simple upper bound)
		if (!CheckedMultiply(gram.size(), idx_t(3), string_bytes) || !CheckedAdd(preflight_bytes, string_bytes)) {
			ThrowProbeOverflow();
		}
	}
	return preflight_bytes;
}

//! Retain the `max_grams` rarest grams by stats row count, stable on ties, as
//! the plan's grams with their stats alongside.
static vector<GramStats> SelectRarestGrams(const vector<string> &grams, const vector<GramStats> &all_stats,
                                           idx_t max_grams, ProbePlan &plan) {
	vector<idx_t> order(grams.size());
	for (idx_t i = 0; i < order.size(); i++) {
		order[i] = i;
	}
	std::stable_sort(order.begin(), order.end(),
	                 [&](idx_t a, idx_t b) { return all_stats[a].row_count < all_stats[b].row_count; });
	order.resize(MinValue<idx_t>(order.size(), max_grams));
	vector<GramStats> selected_stats;
	selected_stats.reserve(order.size());
	plan.grams.reserve(order.size());
	for (auto index : order) {
		plan.grams.push_back(grams[index]);
		selected_stats.push_back(all_stats[index]);
	}
	return selected_stats;
}

//! The segment rows the selected stats promise, checked against the segment
//! rows visible to this transaction.
static idx_t ExpectedDescriptorCount(DuckTransaction &tx, DuckTableEntry &segments_entry,
                                     const vector<GramStats> &selected_stats) {
	idx_t descriptor_count = 0;
	for (auto &stats : selected_stats) {
		if (stats.segment_count > stats.row_count) {
			throw InvalidInputException("ngram: stats segment_count exceeds row_count; the index is malformed");
		}
		if (!CheckedAdd(descriptor_count, stats.segment_count)) {
			ThrowProbeOverflow();
		}
	}
	auto visible_segment_rows = segments_entry.GetStorage().GetTotalRows();
	if (!CheckedAdd(visible_segment_rows, LocalStorage::Get(tx).AddedRows(segments_entry.GetStorage()))) {
		ThrowProbeOverflow();
	}
	if (descriptor_count > visible_segment_rows) {
		throw InvalidInputException("ngram: stats describe more segment rows than exist; the index is malformed");
	}
	return descriptor_count;
}

//! Bytes the manifest occupies: one descriptor and one segment slot per
//! promised row, per-gram scratch, and slack.
static idx_t ManifestBytes(idx_t descriptor_count, idx_t gram_count) {
	idx_t manifest_bytes;
	idx_t gram_scratch_bytes;
	if (!CheckedMultiply(gram_count, sizeof(idx_t), gram_scratch_bytes) ||
	    !CheckedMultiply(descriptor_count, sizeof(ProbeDescriptor) + sizeof(ProbeSegment), manifest_bytes) ||
	    !CheckedAdd(manifest_bytes, gram_scratch_bytes) || !CheckedAdd(manifest_bytes, idx_t(4096))) {
		ThrowProbeOverflow();
	}
	return manifest_bytes;
}

//! Read every visible segments-table row of the selected grams into the
//! manifest, verifying each against its stats, then order the manifest by
//! segment, gram and posting rowid.
static void ReadManifest(ClientContext &context, DuckTransaction &tx, DuckTableEntry &segments_entry, int64_t hwm,
                         const vector<GramStats> &selected_stats, idx_t descriptor_count, ProbePlan &plan) {
	plan.descriptors.reserve(descriptor_count);
	plan.segments.reserve(descriptor_count);
	vector<StorageIndex> column_ids;
	vector<LogicalType> types;
	AddShadowColumn(segments_entry, "gram", LogicalTypeId::VARCHAR, column_ids, types);
	AddShadowColumn(segments_entry, "segment_no", LogicalTypeId::BIGINT, column_ids, types);
	AddShadowColumn(segments_entry, "rowid_count", LogicalTypeId::BIGINT, column_ids, types);
	column_ids.emplace_back(StorageIndex(COLUMN_IDENTIFIER_ROW_ID));
	types.emplace_back(LogicalType::ROW_TYPE);

	vector<idx_t> found_rows(plan.grams.size(), 0);
	vector<idx_t> found_descriptors(plan.grams.size(), 0);
	for (idx_t gram_index = 0; gram_index < plan.grams.size(); gram_index++) {
		TableFilterSet filters;
		filters.PushFilter(ColumnIndex(0),
		                   make_uniq<ConstantFilter>(ExpressionType::COMPARE_EQUAL, Value(plan.grams[gram_index])));
		ScanShadowTable(context, tx, segments_entry.GetStorage(), column_ids, types, &filters, [&](DataChunk &chunk) {
			UnifiedVectorFormat gram_format, segment_format, count_format, rowid_format;
			chunk.data[0].ToUnifiedFormat(chunk.size(), gram_format);
			chunk.data[1].ToUnifiedFormat(chunk.size(), segment_format);
			chunk.data[2].ToUnifiedFormat(chunk.size(), count_format);
			chunk.data[3].ToUnifiedFormat(chunk.size(), rowid_format);
			auto gram_data = UnifiedVectorFormat::GetData<string_t>(gram_format);
			auto segment_data = UnifiedVectorFormat::GetData<int64_t>(segment_format);
			auto count_data = UnifiedVectorFormat::GetData<int64_t>(count_format);
			auto rowid_data = UnifiedVectorFormat::GetData<row_t>(rowid_format);
			for (idx_t r = 0; r < chunk.size(); r++) {
				auto gram_idx = gram_format.sel->get_index(r);
				auto segment_idx = segment_format.sel->get_index(r);
				auto count_idx = count_format.sel->get_index(r);
				auto rowid_idx = rowid_format.sel->get_index(r);
				if (!gram_format.validity.RowIsValid(gram_idx) || !segment_format.validity.RowIsValid(segment_idx) ||
				    !count_format.validity.RowIsValid(count_idx) || !rowid_format.validity.RowIsValid(rowid_idx)) {
					throw InvalidInputException("ngram: segments table contains NULLs; the index is malformed");
				}
				auto &gram = gram_data[gram_idx];
				if (gram != string_t(plan.grams[gram_index]) || segment_data[segment_idx] < 0 || hwm < 0 ||
				    segment_data[segment_idx] > (hwm >> SEGMENT_SHIFT) || count_data[count_idx] <= 0) {
					throw InvalidInputException("ngram: invalid segments-table descriptor; the index is malformed");
				}
				if (plan.descriptors.size() >= descriptor_count ||
				    found_descriptors[gram_index] >= selected_stats[gram_index].segment_count) {
					throw InvalidInputException(
					    "ngram: segments and stats descriptor counts disagree; the index is malformed");
				}
				auto count = NumericCast<idx_t>(count_data[count_idx]);
				if (count > (idx_t(1) << SEGMENT_SHIFT)) {
					throw InvalidInputException(
					    "ngram: segment row_count exceeds its rowid range; the index is malformed");
				}
				if (!CheckedAdd(found_rows[gram_index], count)) {
					ThrowProbeOverflow();
				}
				found_descriptors[gram_index]++;
				plan.descriptors.push_back(
				    ProbeDescriptor {segment_data[segment_idx], gram_index, rowid_data[rowid_idx], count});
			}
		});
		if (found_descriptors[gram_index] != selected_stats[gram_index].segment_count ||
		    found_rows[gram_index] != selected_stats[gram_index].row_count) {
			throw InvalidInputException("ngram: segments and stats counts disagree; the index is malformed");
		}
	}
	if (plan.descriptors.size() != descriptor_count) {
		throw InvalidInputException("ngram: segments and stats descriptor counts disagree; the index is malformed");
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
//! all selected grams, accounting the decoded work and the peak per-worker
//! bytes until the work budget is exceeded, which declines the plan. The
//! structural checks continue past that point, so a decline in one segment
//! cannot hide corruption in a later one.
static void AdmitSegments(ProbePlan &plan, int64_t hwm, idx_t hard_work_limit, idx_t &estimated_decoded_rowids,
                          idx_t &peak_worker_bytes) {
	auto &descriptors = plan.descriptors;
	vector<idx_t> counts;
	counts.reserve(plan.grams.size());
	for (idx_t begin = 0; begin < descriptors.size();) {
		idx_t end = begin + 1;
		while (end < descriptors.size() && descriptors[end].segment_no == descriptors[begin].segment_no) {
			end++;
		}
		counts.clear();
		idx_t current_gram = DConstants::INVALID_INDEX;
		for (idx_t i = begin; i < end; i++) {
			if (descriptors[i].gram_index != current_gram) {
				current_gram = descriptors[i].gram_index;
				counts.push_back(0);
			}
			if (!CheckedAdd(counts.back(), descriptors[i].posting_count)) {
				ThrowProbeOverflow();
			}
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
		if (counts.size() != plan.grams.size()) {
			continue;
		}
		idx_t candidate_bound = segment_capacity;
		for (auto count : counts) {
			candidate_bound = MinValue(candidate_bound, count);
		}
		plan.segments.push_back(ProbeSegment {descriptors[segment_begin].segment_no, segment_begin, end});
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
		peak_worker_bytes = MaxValue(peak_worker_bytes, SegmentWorkerBytes(counts, segment_capacity));
	}
}

unique_ptr<ProbePlan> PlanIndexProbe(ClientContext &context, DuckTransaction &tx, DuckTableEntry &segments_entry,
                                     DuckTableEntry &stats_entry, const vector<string> &grams, idx_t max_grams,
                                     int64_t hwm, idx_t table_rows, double candidate_fraction, idx_t worker_cap) {
	D_ASSERT(!grams.empty());
	D_ASSERT(worker_cap > 0);
	auto plan = make_uniq<ProbePlan>();
	plan->segments_entry = &segments_entry;
	plan->hwm = hwm;

	// Both callers supply distinct grams. Account the full needle before
	// copying it or reading stats, then retain the rarest K.
	auto memory_budget = ProbeMemoryBudget(context);
	auto stats_workers = StatsWorkers(context, stats_entry);
	auto preflight_bytes = PreflightProbeBytes(grams, stats_workers);
	if (preflight_bytes > memory_budget) {
		plan->decline_reason = "query grams exceed query memory budget";
		return plan;
	}
	plan->memory_reservation =
	    make_uniq<ProbeMemoryReservation>(BufferManager::GetBufferManager(context), preflight_bytes);
	auto all_stats = ReadGramStats(context, tx, stats_entry, grams, stats_workers, plan->stats_rows_scanned,
	                               plan->stats_chunks_scanned);
	auto selected_stats = SelectRarestGrams(grams, all_stats, max_grams, *plan);

	auto descriptor_count = ExpectedDescriptorCount(tx, segments_entry, selected_stats);
	auto manifest_bytes = ManifestBytes(descriptor_count, plan->grams.size());
	if (manifest_bytes > memory_budget - preflight_bytes) {
		plan->decline_reason = "segment manifest exceeds query memory budget";
		return plan;
	}
	plan->memory_reservation->Grow(manifest_bytes);
	auto reserved_bytes = preflight_bytes + manifest_bytes;
	ReadManifest(context, tx, segments_entry, hwm, selected_stats, descriptor_count, *plan);

	idx_t estimated_decoded_rowids = 0;
	idx_t peak_worker_bytes = 0;
	AdmitSegments(*plan, hwm, MaxProbeRowids(context), estimated_decoded_rowids, peak_worker_bytes);
	if (!plan->decline_reason.empty()) {
		return plan;
	}
	if (estimated_decoded_rowids > std::numeric_limits<idx_t>::max() / sizeof(row_t)) {
		ThrowProbeOverflow();
	}
	if (candidate_fraction >= 0 &&
	    static_cast<double>(plan->candidate_upper_bound) > candidate_fraction * static_cast<double>(table_rows)) {
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
	auto possible_workers = (memory_budget - reserved_bytes) / peak_worker_bytes;
	plan->max_threads = MinValue<idx_t>(
	    plan->segments.size(), MinValue<idx_t>(worker_cap, MinValue<idx_t>(ProbeThreads(context), possible_workers)));
	D_ASSERT(plan->max_threads > 0);
	idx_t workspace_bytes;
	if (!CheckedMultiply(peak_worker_bytes, plan->max_threads, workspace_bytes)) {
		ThrowProbeOverflow();
	}
	plan->memory_reservation->Grow(workspace_bytes);
	plan->admitted = true;
	return plan;
}

static void DecodeDescriptorRange(ClientContext &context, DuckTransaction &tx, ProbePlan &plan,
                                  const ProbeSegment &segment, idx_t gram_index, idx_t &descriptor_cursor,
                                  vector<row_t> &postings) {
	auto &descriptors = plan.descriptors;
	idx_t begin = descriptor_cursor;
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
	descriptor_cursor = end;
	vector<row_t>().swap(postings);
	postings.reserve(expected);

	vector<StorageIndex> column_ids;
	vector<LogicalType> types;
	AddShadowColumn(*plan.segments_entry, "gram", LogicalTypeId::VARCHAR, column_ids, types);
	AddShadowColumn(*plan.segments_entry, "segment_no", LogicalTypeId::BIGINT, column_ids, types);
	AddShadowColumn(*plan.segments_entry, "postings", LogicalTypeId::BLOB, column_ids, types);
	AddShadowColumn(*plan.segments_entry, "rowid_count", LogicalTypeId::BIGINT, column_ids, types);
	DataChunk chunk;
	chunk.Initialize(Allocator::Get(context), types);
	ColumnFetchState fetch_state;
	Vector rowids(LogicalType::ROW_TYPE, STANDARD_VECTOR_SIZE);
	auto rowid_data = FlatVector::GetData<row_t>(rowids);
	for (idx_t offset = begin; offset < end; offset += STANDARD_VECTOR_SIZE) {
		ThrowIfInterrupted(context);
		auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, end - offset);
		for (idx_t i = 0; i < count; i++) {
			rowid_data[i] = descriptors[offset + i].posting_rowid;
		}
		chunk.Reset();
		// ColumnFetchState retains every pinned block it has seen. Drop the
		// previous batch only after its BLOBs have been decoded so fragmented
		// generations cannot accumulate query-wide pins.
		fetch_state = ColumnFetchState();
		plan.segments_entry->GetStorage().Fetch(tx, chunk, column_ids, rowids, count, fetch_state);
		if (chunk.size() != count) {
			throw InvalidInputException("ngram: a manifest posting row vanished; the index is malformed");
		}
		UnifiedVectorFormat gram_format, segment_format, blob_format, count_format;
		chunk.data[0].ToUnifiedFormat(count, gram_format);
		chunk.data[1].ToUnifiedFormat(count, segment_format);
		chunk.data[2].ToUnifiedFormat(count, blob_format);
		chunk.data[3].ToUnifiedFormat(count, count_format);
		auto gram_data = UnifiedVectorFormat::GetData<string_t>(gram_format);
		auto segment_data = UnifiedVectorFormat::GetData<int64_t>(segment_format);
		auto blob_data = UnifiedVectorFormat::GetData<string_t>(blob_format);
		auto count_data = UnifiedVectorFormat::GetData<int64_t>(count_format);
		for (idx_t r = 0; r < count; r++) {
			auto gram_idx = gram_format.sel->get_index(r);
			auto segment_idx = segment_format.sel->get_index(r);
			auto blob_idx = blob_format.sel->get_index(r);
			auto count_idx = count_format.sel->get_index(r);
			if (!gram_format.validity.RowIsValid(gram_idx) || !segment_format.validity.RowIsValid(segment_idx) ||
			    !blob_format.validity.RowIsValid(blob_idx) || !count_format.validity.RowIsValid(count_idx)) {
				throw InvalidInputException("ngram: segments table contains NULLs; the index is malformed");
			}
			auto &gram = gram_data[gram_idx];
			auto &blob = blob_data[blob_idx];
			auto encoded_count = PostingsCount(blob.GetData(), blob.GetSize());
			auto &descriptor = descriptors[offset + r];
			if (gram != string_t(plan.grams[gram_index]) || segment_data[segment_idx] != segment.segment_no ||
			    count_data[count_idx] <= 0 || NumericCast<idx_t>(count_data[count_idx]) != descriptor.posting_count ||
			    encoded_count != descriptor.posting_count || encoded_count > expected - postings.size()) {
				throw InvalidInputException("ngram: posting row disagrees with its manifest; the index is malformed");
			}
			DecodePostings(blob.GetData(), blob.GetSize(), postings);
			plan.decoded_rowids.fetch_add(encoded_count);
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

bool NextCandidateSegment(ClientContext &context, DuckTransaction &tx, ProbePlan &plan, vector<row_t> &candidates,
                          idx_t &segment_ordinal) {
	segment_ordinal = plan.next_segment.fetch_add(1);
	vector<row_t>().swap(candidates);
	if (segment_ordinal >= plan.segments.size()) {
		return false;
	}
	auto &segment = plan.segments[segment_ordinal];
	idx_t descriptor_cursor = segment.descriptor_begin;
	vector<row_t> postings;
	DecodeDescriptorRange(context, tx, plan, segment, 0, descriptor_cursor, postings);
	candidates = std::move(postings);
	for (idx_t gram = 1; gram < plan.grams.size() && !candidates.empty(); gram++) {
		DecodeDescriptorRange(context, tx, plan, segment, gram, descriptor_cursor, postings);
		vector<row_t> intersection;
		intersection.reserve(MinValue(candidates.size(), postings.size()));
		std::set_intersection(candidates.begin(), candidates.end(), postings.begin(), postings.end(),
		                      std::back_inserter(intersection));
		candidates = std::move(intersection);
	}
	return true;
}

} // namespace ngram
} // namespace duckdb
