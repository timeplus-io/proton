#pragma once

/// proton: starts. Added for proton

#include <DistributedWALClient/Record.h>
#include <DistributedWALClient/Results.h>
#include <Storages/MergeTree/SequenceInfo.h>

namespace DB
{
class StorageStream;

using RecordsSequenceRangesPair = std::pair<DWAL::RecordPtrs, SequenceRanges>;

struct StreamCallbackData : public DWAL::ConsumeCallbackData
{
    StreamCallbackData(StorageStream * storage_, const SequenceRanges & missing_sequence_ranges_);

    const Block & getSchema(UInt16 schema_version) const override
    {
        (void)schema_version;
        /// proton: FIXME, MVCC schema version
        return header;
    }

    void commit(DWAL::RecordPtrs records);

    /// Wait for the outstanding commits
    void wait() const;

    /// For testing
    static std::vector<RecordsSequenceRangesPair> categorizeRecordsAccordingToSequenceRanges(
        const DWAL::RecordPtrs & records, const SequenceRanges & sequence_ranges, DWAL::RecordSN max_committed_sn);

private:
    bool finishRecovery() const { return missing_sequence_ranges.empty() && recovery_records.empty(); }

    void doCommit(DWAL::RecordPtrs records, SequenceRanges sequence_ranges = {});

    friend StorageStream;

private:
    StorageStream * storage;
    Block header;

    std::atomic_uint16_t outstanding_commits = 0;
    SequenceRanges missing_sequence_ranges;
    DWAL::RecordPtrs recovery_records;
};

}
