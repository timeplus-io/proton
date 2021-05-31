#include "DistributedMergeTreeCallbackData.h"
#include "StorageDistributedMergeTree.h"

#include <common/logger_useful.h>

/// Daisy : starts. Added for Daisy

namespace DB
{
void DistributedMergeTreeCallbackData::wait() const
{
    while (outstanding_commits != 0)
    {
        LOG_INFO(storage->log, "Waiting for outstanding commits={} to finish", outstanding_commits);
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
}

void DistributedMergeTreeCallbackData::commit(DWAL::RecordPtrs records)
{
    if (finishRecovery())
    {
        doCommit(std::move(records));
    }
    else
    {
        /// Recovery path
        /// During startup, there may have missing sequence ranges in last run
        /// We need re-compose these missing sequence ranges and commit again
        /// to avoid data lost and duplicate data. Duplicate data is enfored later
        /// after partitioning each missing sequence range

        for (auto & record : records)
        {
            recovery_records.push_back(std::move(record));
        }

        /// Wait until we consume a record which has sequence number larger
        /// than max committed sn
        if (recovery_records.back()->sn < storage->maxCommittedSN())
        {
            return;
        }

        auto range_buckets{categorizeRecordsAccordingToSequenceRanges(recovery_records, missing_sequence_ranges, storage->maxCommittedSN())};

        for (auto & records_seqs_pair : range_buckets)
        {
            assert(!records_seqs_pair.first.empty());

            LOG_INFO(storage->log, "Recovery phase. Committing missing_ranges={}", sequenceRangesToString(records_seqs_pair.second));
            doCommit(std::move(records_seqs_pair.first), std::move(records_seqs_pair.second));

            assert(records_seqs_pair.first.empty());
            assert(records_seqs_pair.second.empty());
        }

        /// We have done with recovery, clean up data structure to speedup fast path condition check
        recovery_records.clear();
        missing_sequence_ranges.clear();
    }
}

inline void DistributedMergeTreeCallbackData::doCommit(DWAL::RecordPtrs records, SequenceRanges sequence_ranges)
{
    ++outstanding_commits;
    try
    {
        storage->commit(std::move(records), std::move(sequence_ranges), ctx);
    }
    catch (...)
    {
        LOG_ERROR(
            storage->log, "Failed to commit data for shard={}, exception={}", storage->shard, getCurrentExceptionMessage(true, true));
    }
    --outstanding_commits;
}

std::vector<RecordsSequenceRangesPair> DistributedMergeTreeCallbackData::categorizeRecordsAccordingToSequenceRanges(
    const DWAL::RecordPtrs & records,
    const SequenceRanges & sequence_ranges,
    DWAL::RecordSequenceNumber max_committed_sn)
{
    std::vector<RecordsSequenceRangesPair> range_buckets(sequence_ranges.size() + 1);

    for (auto & record : records)
    {
        if (record->sn > max_committed_sn)
        {
            range_buckets.back().first.push_back(std::move(record));
            continue;
        }

        if (record->sn > sequence_ranges.back().end_sn)
        {
            /// records fall in [sequence_ranges.back().end_sn, max_committed_sn] are committed
            continue;
        }

        /// Find the corresponding missing sequence range for current record
        for (size_t i = 0; i < sequence_ranges.size(); ++i)
        {
            const auto & sequence_range = sequence_ranges[i];
            if (record->sn >= sequence_range.start_sn && record->sn <= sequence_range.end_sn)
            {
                /// Found the missing range for current record
                auto & range_bucket = range_buckets[i];
                range_bucket.first.push_back(std::move(record));

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
            assert(range_bucket.first.empty());
            assert(range_bucket.second.empty());
        }
    }
    return result;
}
}
