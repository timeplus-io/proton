#pragma once

/// proton: starts. Added for proton

#include <KafkaLog/Results.h>
#include <NativeLog/Record/Record.h>
#include <Storages/MergeTree/SequenceInfo.h>

namespace DB
{
class StorageStream;

using RecordsSequenceRangesPair = std::pair<nlog::RecordPtrs, SequenceRanges>;

struct StreamCallbackData : public nlog::SchemaProvider
{
    StreamCallbackData(StorageStream * storage_, const SequenceRanges & missing_sequence_ranges_);

    const Block & getSchema(UInt16 schema_version) const override
    {
        (void)schema_version;
        /// proton: FIXME, MVCC schema version
        return header;
    }

    void commit(nlog::RecordPtrs records);

    /// Wait for the outstanding commits
    void wait() const;

    /// For testing
    static std::vector<RecordsSequenceRangesPair> categorizeRecordsAccordingToSequenceRanges(
        const nlog::RecordPtrs & records, const SequenceRanges & sequence_ranges, nlog::RecordSN max_committed_sn);

private:
    bool finishRecovery() const { return missing_sequence_ranges.empty() && recovery_records.empty(); }

    void doCommit(nlog::RecordPtrs records, SequenceRanges sequence_ranges = {});

    friend StorageStream;

private:
    StorageStream * storage;
    Block header;

    std::atomic_uint16_t outstanding_commits = 0;
    SequenceRanges missing_sequence_ranges;
    nlog::RecordPtrs recovery_records;
};

}
