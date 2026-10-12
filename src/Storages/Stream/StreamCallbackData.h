#pragma once

#include <Cluster/KafkaLog/Results.h>
#include <Cluster/SchemaRecord/SchemaRecord.h>
#include <Storages/MergeTree/SequenceInfo.h>
#include <Common/Logger.h>
#include <base/ClockUtils.h>

#include <functional>

namespace DB
{
class StreamShardStore;

using RecordsSequenceRangesPair = std::pair<cluster::SchemaRecordPtrs, SequenceRanges>;

struct StreamCallbackData final
{
    using CommitSink = std::function<void(cluster::SchemaRecordPtrs, SequenceRanges)>;

    StreamCallbackData(
        StreamShardStore * stream_shard_,
        const SequenceRanges & missing_sequence_ranges_,
        size_t flush_threshold_bytes_,
        size_t flush_threshold_count_,
        Int64 flush_threshold_ms_);

    /// Test seam: drives the recovery state machine without a real StreamShardStore.
    StreamCallbackData(
        const SequenceRanges & missing_sequence_ranges_,
        size_t flush_threshold_bytes_,
        size_t flush_threshold_count_,
        Int64 flush_threshold_ms_,
        std::function<int64_t()> max_committed_sn_,
        CommitSink commit_sink_,
        LoggerPtr logger_);

    void commit(cluster::SchemaRecordPtrs records);

    /// Re-seed the recovery state after a consume retry re-runs committed_sn reconciliation, so
    /// subsequent commits dedup already-written parts (via ignorePartBlock) instead of re-writing
    /// them into overlapping same-sn parts. Safe only once in-flight commits have been drained.
    void resetMissingSequenceRanges(SequenceRanges missing_sequence_ranges_);

    /// Wait for the outstanding commits
    void wait() const;

    StreamShardStore * streamShardStore() { return stream_shard_store; }

    /// For testing
    static std::vector<RecordsSequenceRangesPair> categorizeRecordsAccordingToSequenceRanges(
        const cluster::SchemaRecordPtrs & records, const SequenceRanges & sequence_ranges, int64_t max_committed_sn);

    /// A record is worth buffering during recovery only if a later commit needs it: it fills a
    /// missing sequence range, or it is new data beyond max_committed_sn. Requires sequence_ranges
    /// sorted ascending by start_sn (same precondition as the categorization above).
    static bool shouldBufferDuringRecovery(int64_t sn, const SequenceRanges & sequence_ranges, int64_t max_committed_sn);

    /// The largest sn (exclusive) up to which buffered recovery records can be committed without
    /// tearing a still-incomplete multi-part logical range. Records arrive in sn order, so a range
    /// whose end_sn has not yet been reached by highest_seen_sn may still be gathering its records;
    /// a multi-part range (parts >= 1) must keep every record from start_sn..end_sn in one commit so
    /// the sink re-splits it into the same parts and ignorePartBlock skips the already-durable ones.
    /// Pure gaps (parts < 0) persist every part, so they impose no boundary and can flush partially.
    /// Exposed for testing.
    static int64_t recoveryFlushBoundarySN(const SequenceRanges & sequence_ranges, int64_t highest_seen_sn);

    size_t recoveryBufferRows() const { return recovery_buffer_rows; }
    size_t recoveryBufferBytes() const { return recovery_buffer_bytes; }
    bool recoveryFinished() const { return recovery_finished; }

private:
    /// Streaming recovery: admit only records a later commit needs, accumulate them up to the flush
    /// thresholds, then commit and free that chunk. Memory stays O(flush threshold + one in-flight
    /// multi-part range) instead of O([snLoaded, max_committed_sn]).
    void commitDuringRecovery(cluster::SchemaRecordPtrs records);

    /// Commit every buffered record below the flush boundary, retire the ranges they filled, and
    /// drop them from the buffer. Once highest_seen_sn reaches max_committed_sn all holes are
    /// provably complete, so the whole buffer (including new data) is flushed.
    void flushRecoveryBuffer(int64_t max_committed_sn);

    bool recoveryFlushThresholdReached() const
    {
        return recovery_buffer_bytes >= flush_threshold_bytes || recovery_buffer_rows >= flush_threshold_count
            || (MonotonicMilliseconds::now() - last_recovery_flush_ms) >= flush_threshold_ms;
    }

    void doCommit(cluster::SchemaRecordPtrs records, SequenceRanges sequence_ranges = {});

private:
    StreamShardStore * stream_shard_store = nullptr;
    std::function<int64_t()> max_committed_sn_provider;
    CommitSink commit_sink;
    LoggerPtr logger;

    std::atomic_uint16_t outstanding_commits = 0;
    SequenceRanges missing_sequence_ranges;
    cluster::SchemaRecordPtrs recovery_records;

    /// Recovery flush thresholds, mirroring the ingest batch thresholds, so recovery parts are the
    /// same size as steady-state parts (no part explosion) while memory stays bounded.
    size_t flush_threshold_bytes;
    size_t flush_threshold_count;
    Int64 flush_threshold_ms;

    /// Bytes/rows currently held by recovery_records, tracked incrementally to avoid rescanning.
    size_t recovery_buffer_bytes = 0;
    size_t recovery_buffer_rows = 0;

    /// Highest sn observed so far (admitted or dropped). A range is fully seen once this reaches
    /// its end_sn, which is what lets a hole be retired and recovery exit.
    Int64 highest_seen_sn = -1;

    Int64 last_recovery_flush_ms = 0;

    /// Latched once recovery is provably done (all holes filled and highest_seen_sn reached
    /// max_committed_sn) so the unfiltered fast path never re-commits an already-durable record.
    bool recovery_finished = false;

    size_t records_dropped_in_recovery = 0;
};

}
