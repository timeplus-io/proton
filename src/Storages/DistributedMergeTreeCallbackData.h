#pragma once

/// Daisy : starts. Added for Daisy

#include <DistributedWriteAheadLog/IDistributedWriteAheadLog.h>
#include <Storages/MergeTree/SequenceInfo.h>

namespace DB
{

class StorageDistributedMergeTree;

using RecordsSequenceRangesPair = std::pair<IDistributedWriteAheadLog::RecordPtrs, SequenceRanges>;

struct DistributedMergeTreeCallbackData
{
    DistributedMergeTreeCallbackData(
        StorageDistributedMergeTree * storage_, const SequenceRanges & missing_sequence_ranges_, std::any & ctx_)
        : storage(storage_), missing_sequence_ranges(missing_sequence_ranges_), ctx(ctx_)
    {
    }

    void commit(IDistributedWriteAheadLog::RecordPtrs records);

    /// Wait for the outstanding commits
    void wait() const;

    /// For testing
    static std::vector<RecordsSequenceRangesPair> categorizeRecordsAccordingToSequenceRanges(
        const IDistributedWriteAheadLog::RecordPtrs & records,
        const SequenceRanges & sequence_ranges,
        IDistributedWriteAheadLog::RecordSequenceNumber max_committed_sn);

private:
    bool finishRecovery() const { return missing_sequence_ranges.empty() && recovery_records.empty(); }

    void doCommit(IDistributedWriteAheadLog::RecordPtrs records, SequenceRanges sequence_ranges = {});

private:
    StorageDistributedMergeTree * storage;

    std::atomic_uint16_t outstanding_commits = 0;
    SequenceRanges missing_sequence_ranges;
    IDistributedWriteAheadLog::RecordPtrs recovery_records;

    std::any & ctx;
};

}
