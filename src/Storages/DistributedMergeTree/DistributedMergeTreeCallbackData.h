#pragma once

/// Daisy : starts. Added for Daisy

#include <DistributedWriteAheadLog/Record.h>
#include <Storages/MergeTree/SequenceInfo.h>

#include <any>

namespace DB
{

class StorageDistributedMergeTree;

using RecordsSequenceRangesPair = std::pair<DWAL::RecordPtrs, SequenceRanges>;

struct DistributedMergeTreeCallbackData
{
    DistributedMergeTreeCallbackData(
        StorageDistributedMergeTree * storage_, const SequenceRanges & missing_sequence_ranges_, std::any & ctx_)
        : storage(storage_), missing_sequence_ranges(missing_sequence_ranges_), ctx(ctx_)
    {
    }

    void commit(DWAL::RecordPtrs records);

    /// Wait for the outstanding commits
    void wait() const;

    /// For testing
    static std::vector<RecordsSequenceRangesPair> categorizeRecordsAccordingToSequenceRanges(
        const DWAL::RecordPtrs & records,
        const SequenceRanges & sequence_ranges,
        DWAL::RecordSequenceNumber max_committed_sn);

private:
    bool finishRecovery() const { return missing_sequence_ranges.empty() && recovery_records.empty(); }

    void doCommit(DWAL::RecordPtrs records, SequenceRanges sequence_ranges = {});

private:
    StorageDistributedMergeTree * storage;

    std::atomic_uint16_t outstanding_commits = 0;
    SequenceRanges missing_sequence_ranges;
    DWAL::RecordPtrs recovery_records;

    std::any & ctx;
};

}
