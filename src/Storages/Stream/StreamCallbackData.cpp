#include <Storages/Stream/StorageStream.h>
#include <Storages/Stream/StreamCallbackData.h>
#include <Storages/Stream/StreamShardStore.h>
#include <base/scope_guard.h>
#include <base/sleep.h>

#include <Common/logger_useful.h>

#include <algorithm>
#include <iterator>
#include <limits>

namespace DB
{
StreamCallbackData::StreamCallbackData(
    StreamShardStore * stream_shard_store_,
    const SequenceRanges & missing_sequence_ranges_,
    size_t flush_threshold_bytes_,
    size_t flush_threshold_count_,
    Int64 flush_threshold_ms_)
    : StreamCallbackData(
        missing_sequence_ranges_,
        flush_threshold_bytes_,
        flush_threshold_count_,
        flush_threshold_ms_,
        [store = stream_shard_store_] { return store->maxCommittedSN(); },
        [store = stream_shard_store_](cluster::SchemaRecordPtrs records, SequenceRanges sequence_ranges)
        {
            try
            {
                store->commit(std::move(records), std::move(sequence_ranges));
            }
            catch (...)
            {
                LOG_ERROR(
                    store->logger,
                    "Failed to commit data for shard={}, exception={}",
                    store->shard(),
                    getCurrentExceptionMessage(true, true));
            }
        },
        stream_shard_store_->logger)
{
    stream_shard_store = stream_shard_store_;
}

StreamCallbackData::StreamCallbackData(
    const SequenceRanges & missing_sequence_ranges_,
    size_t flush_threshold_bytes_,
    size_t flush_threshold_count_,
    Int64 flush_threshold_ms_,
    std::function<int64_t()> max_committed_sn_,
    CommitSink commit_sink_,
    LoggerPtr logger_)
    : max_committed_sn_provider(std::move(max_committed_sn_))
    , commit_sink(std::move(commit_sink_))
    , logger(std::move(logger_))
    , missing_sequence_ranges(missing_sequence_ranges_)
    , flush_threshold_bytes(flush_threshold_bytes_)
    , flush_threshold_count(flush_threshold_count_)
    , flush_threshold_ms(flush_threshold_ms_)
    , last_recovery_flush_ms(MonotonicMilliseconds::now())
    /// No holes at construction means nothing to reconcile: take the unfiltered fast path
    /// immediately, exactly as a clean start would.
    , recovery_finished(missing_sequence_ranges.empty())
{
    chassert(max_committed_sn_provider && commit_sink && logger);
}

void StreamCallbackData::resetMissingSequenceRanges(SequenceRanges missing_sequence_ranges_)
{
    missing_sequence_ranges = std::move(missing_sequence_ranges_);
    recovery_records.clear();
    recovery_buffer_bytes = 0;
    recovery_buffer_rows = 0;
    records_dropped_in_recovery = 0;
    /// Re-consumption restarts from the reconciled floor, so re-derive whether recovery is needed
    /// from the refreshed hole set and re-arm the crossing check.
    highest_seen_sn = -1;
    last_recovery_flush_ms = MonotonicMilliseconds::now();
    recovery_finished = missing_sequence_ranges.empty();
}

void StreamCallbackData::wait() const
{
    while (outstanding_commits.load() != 0)
    {
        LOG_INFO(logger, "Waiting for outstanding commits={} to finish", outstanding_commits.load());
        sleepForMilliseconds(200);
    }
}

void StreamCallbackData::commit(cluster::SchemaRecordPtrs records)
{
    ++outstanding_commits;
    /// Decrement even on throw (e.g. bad_alloc while buffering under memory pressure), otherwise
    /// wait() spins forever during shutdown.
    SCOPE_EXIT({ --outstanding_commits; });

    if (recovery_finished)
        doCommit(std::move(records));
    else
        commitDuringRecovery(std::move(records));
}

void StreamCallbackData::commitDuringRecovery(cluster::SchemaRecordPtrs records)
{
    /// Recovery path.
    /// On startup the durable state can have holes (missing sequence ranges) from the last run. We
    /// re-consume from the low watermark and re-commit the missing data to avoid loss or duplicates.
    if (records.empty())
        return;

    auto max_committed_sn = max_committed_sn_provider();

    /// Records arrive in sn order, so the batch's last record carries the max sn seen so far. Track
    /// it even for dropped records: it is what proves a hole fully seen and recovery complete.
    highest_seen_sn = std::max(highest_seen_sn, records.back()->getSN());

    for (auto & record : records)
    {
        if (shouldBufferDuringRecovery(record->getSN(), missing_sequence_ranges, max_committed_sn))
        {
            recovery_buffer_bytes += record->approximateSerializedSize();
            recovery_buffer_rows += record->getBlock().rows();
            recovery_records.push_back(std::move(record));
        }
        else
        {
            ++records_dropped_in_recovery;
        }
    }

    /// Once highest_seen_sn reaches max_committed_sn every hole (no hole can end beyond it, and
    /// in-order delivery means the record at max_committed_sn is the last one any hole can need) is
    /// provably fully seen: flush the whole buffer, including any new data, and finish recovery.
    /// Requiring a record strictly beyond max_committed_sn would leave a tail hole unrepaired
    /// forever on a write-idle stream.
    const bool crossed_max = highest_seen_sn >= max_committed_sn;

    if (crossed_max || recoveryFlushThresholdReached())
        flushRecoveryBuffer(max_committed_sn);

    /// Recovery is done only once no hole remains and the buffer is drained AND we have crossed
    /// max_committed_sn. The crossing guard matters: holes can empty early (a couple of low holes,
    /// then a long tail of already-durable records up to max_committed_sn). Switching to the
    /// unfiltered fast path before the crossing would re-commit those already-durable records as
    /// duplicate overlapping parts. Until then the filtered path keeps dropping them.
    if (crossed_max && missing_sequence_ranges.empty() && recovery_records.empty())
    {
        recovery_finished = true;
        LOG_INFO(
            logger,
            "Recovery complete: highest_seen_sn={} max_committed_sn={} dropped_already_committed_records={}",
            highest_seen_sn,
            max_committed_sn,
            records_dropped_in_recovery);
    }
}

int64_t StreamCallbackData::recoveryFlushBoundarySN(const SequenceRanges & sequence_ranges, int64_t highest_seen_sn)
{
    /// A multi-part range (parts >= 1) must be committed as a whole so the sink re-splits it into
    /// the same parts and ignorePartBlock can skip the already-durable ones. So the flush must stop
    /// before the first such range that is not yet fully seen (end_sn > highest_seen_sn; in-order
    /// delivery means reaching end_sn proves the range complete). Pure gaps (parts < 0) persist
    /// every part and impose no boundary, so a large gap can flush partially.
    /// sequence_ranges is sorted ascending by start_sn.
    for (const auto & r : sequence_ranges)
        if (r.parts >= 1 && r.end_sn > highest_seen_sn)
            return r.start_sn;

    return std::numeric_limits<int64_t>::max();
}

void StreamCallbackData::flushRecoveryBuffer(int64_t max_committed_sn)
{
    if (recovery_records.empty())
    {
        last_recovery_flush_ms = MonotonicMilliseconds::now();
        return;
    }

    const auto boundary_sn = recoveryFlushBoundarySN(missing_sequence_ranges, highest_seen_sn);

    /// recovery_records are appended in arrival (sn) order, so the flushable prefix is contiguous:
    /// every record with sn < boundary_sn. Cutting on a whole-record boundary here is what preserves
    /// multi-part atomicity - a multi-part range is either wholly below the boundary (complete, all
    /// its records present) or wholly at/above it (deferred), never split.
    auto split = std::partition_point(
        recovery_records.begin(), recovery_records.end(), [boundary_sn](const auto & r) { return r->getSN() < boundary_sn; });

    if (split == recovery_records.begin())
    {
        /// Nothing safe to flush yet: the buffer is entirely one still-incomplete multi-part range.
        /// It is bounded by that range's original write size, so memory is still bounded.
        return;
    }

    cluster::SchemaRecordPtrs flushable(std::make_move_iterator(recovery_records.begin()), std::make_move_iterator(split));

    if (missing_sequence_ranges.empty())
    {
        /// Every hole has already been retired, so anything still buffered is new data beyond
        /// max_committed_sn. Commit it directly; categorization requires a non-empty range set.
        LOG_INFO(logger, "Recovery phase. Committing new records={}", flushable.size());
        doCommit(std::move(flushable));
    }
    else
    {
        /// Categorize only the flushable prefix. Passing the full range set is safe: records at/above
        /// the boundary have been excluded, so only complete (or freely-splittable pure-gap) ranges and
        /// new data can match, producing the same buckets the single-shot categorization would.
        auto range_buckets = categorizeRecordsAccordingToSequenceRanges(flushable, missing_sequence_ranges, max_committed_sn);

        for (auto & records_seqs_pair : range_buckets)
        {
            chassert(!records_seqs_pair.first.empty());

            LOG_INFO(
                logger,
                "Recovery phase. Committing records={} missing_ranges={}",
                records_seqs_pair.first.size(),
                sequenceRangesToString(records_seqs_pair.second));
            doCommit(std::move(records_seqs_pair.first), std::move(records_seqs_pair.second));
        }
    }

    recovery_records.erase(recovery_records.begin(), split);
    recovery_buffer_bytes = 0;
    recovery_buffer_rows = 0;
    for (const auto & record : recovery_records)
    {
        recovery_buffer_bytes += record->approximateSerializedSize();
        recovery_buffer_rows += record->getBlock().rows();
    }

    /// Retire every hole fully seen (end_sn <= highest_seen_sn); its records were just committed.
    /// This shrinks the hole set as we pass ranges and is what lets recovery end without buffering
    /// the whole interval. Idempotency across a mid-recovery crash is preserved because the next
    /// start re-runs populateCommittedSNFromParts / resetMissingSequenceRanges and recomputes the
    /// now-smaller hole set, so already-filled records dedup on re-consume.
    std::erase_if(missing_sequence_ranges, [this](const auto & r) { return r.end_sn <= highest_seen_sn; });

    last_recovery_flush_ms = MonotonicMilliseconds::now();
}

inline void StreamCallbackData::doCommit(cluster::SchemaRecordPtrs records, SequenceRanges sequence_ranges)
{
    commit_sink(std::move(records), std::move(sequence_ranges));
}

bool StreamCallbackData::shouldBufferDuringRecovery(int64_t sn, const SequenceRanges & sequence_ranges, int64_t max_committed_sn)
{
    if (sn > max_committed_sn)
        return true;

    for (const auto & sequence_range : sequence_ranges)
    {
        if (sn < sequence_range.start_sn)
            break;

        if (sn <= sequence_range.end_sn)
            return true;
    }

    return false;
}

std::vector<RecordsSequenceRangesPair> StreamCallbackData::categorizeRecordsAccordingToSequenceRanges(
    const cluster::SchemaRecordPtrs & records, const SequenceRanges & sequence_ranges, int64_t max_committed_sn)
{
    std::vector<RecordsSequenceRangesPair> range_buckets(sequence_ranges.size() + 1);

    for (const auto & record : records)
    {
        auto record_sn = record->getSN();
        if (record_sn > max_committed_sn)
        {
            range_buckets.back().first.push_back(record);
            continue;
        }

        if (record_sn > sequence_ranges.back().end_sn)
        {
            /// records fall in [sequence_ranges.back().end_sn, max_committed_sn] are committed
            continue;
        }

        /// Find the corresponding missing sequence range for current record
        for (size_t i = 0; i < sequence_ranges.size(); ++i)
        {
            const auto & sequence_range = sequence_ranges[i];
            if (record_sn >= sequence_range.start_sn && record_sn <= sequence_range.end_sn)
            {
                /// Found the missing range for current record
                auto & range_bucket = range_buckets[i];
                range_bucket.first.push_back(record);

                /// Collect all missing sequence range parts
                for (size_t j = i; j < sequence_ranges.size(); ++j)
                {
                    if (sequence_ranges[j].start_sn == sequence_range.start_sn)
                    {
                        auto it = std::find(range_bucket.second.begin(), range_bucket.second.end(), sequence_ranges[j]);
                        if (it == range_bucket.second.end())
                        {
                            range_bucket.second.push_back(sequence_ranges[j]);
                        }
                    }
                }
                break;
            }
        }

        /// If a record doesn't belong to any missing sequence range,
        /// it means it was committed already (we have sequence range hole)
    }

    std::vector<RecordsSequenceRangesPair> result;
    result.reserve(range_buckets.size());

    for (auto & range_bucket : range_buckets)
    {
        if (!range_bucket.first.empty())
        {
            result.push_back(std::move(range_bucket));
            chassert(range_bucket.first.empty());
            chassert(range_bucket.second.empty());
        }
    }
    return result;
}
}
