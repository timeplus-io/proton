#pragma once

#include <Cluster/KafkaLog/Results.h>
#include <Cluster/SchemaRecord/SchemaRecord.h>
#include <Storages/MergeTree/SequenceInfo.h>

namespace DB
{
class StreamShardStore;

using RecordsSequenceRangesPair = std::pair<cluster::SchemaRecordPtrs, SequenceRanges>;

struct StreamCallbackData final
{
    StreamCallbackData(StreamShardStore * stream_shard_, const SequenceRanges & missing_sequence_ranges_);

    void commit(cluster::SchemaRecordPtrs records);

    /// Wait for the outstanding commits
    void wait() const;

    StreamShardStore * streamShardStore() { return stream_shard_store; }

    /// For testing
    static std::vector<RecordsSequenceRangesPair> categorizeRecordsAccordingToSequenceRanges(
        const cluster::SchemaRecordPtrs & records, const SequenceRanges & sequence_ranges, int64_t max_committed_sn);

private:
    bool finishRecovery() const { return missing_sequence_ranges.empty() && recovery_records.empty(); }

    void doCommit(cluster::SchemaRecordPtrs records, SequenceRanges sequence_ranges = {});

private:
    StreamShardStore * stream_shard_store;
    std::atomic_uint16_t outstanding_commits = 0;
    SequenceRanges missing_sequence_ranges;
    cluster::SchemaRecordPtrs recovery_records;
};

}
