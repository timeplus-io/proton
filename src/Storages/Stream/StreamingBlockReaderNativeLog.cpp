#include <Storages/Stream/StreamingBlockReaderNativeLog.h>

#include <Bootstrap/Globals.h>
#include <Cluster/NativeLog/NativeLog.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
extern const int SEQUENCE_COMPACTED_AWAY;
}

StreamingBlockReaderNativeLog::StreamingBlockReaderNativeLog(
    Int64 sn,
    Int64 max_wait_ms_,
    UInt64 max_bytes_,
    UInt64 queued_max_bytes_,
    const cluster::StreamShard & stream_shard_,
    const IStorage * storage,
    Block sample_block,
    UInt16 query_schema_version,
    cluster::SourceColumnsDescription::PhysicalColumnPositions column_positions_,
    std::function<bool()> is_stopped_,
    bool is_local_,
    LoggerPtr logger_)
    : StreamingBlockReaderBase(
          stream_shard_,
          storage,
          std::move(sample_block),
          query_schema_version,
          std::move(column_positions_),
          std::move(is_stopped_),
          logger_)
    // nlog_client not needed
    , max_wait_ms(max_wait_ms_)
    , max_bytes(max_bytes_)
    , queued_max_bytes(queued_max_bytes_)
    , is_local(is_local_)
{
    // assert(nlog_client); // Not needed for
    assert(is_stopped);

    startFetch(sn);
}

StreamingBlockReaderNativeLog::~StreamingBlockReaderNativeLog() noexcept
{
    // No cleanup needed for direct fetch
}

cluster::SchemaRecordPtrs StreamingBlockReaderNativeLog::read()
{
    if (historical_ctx)
        return readFromHistoricalStore();

    const auto stop_sn_snapshot = getStopSN();
    if (stop_sn_snapshot && fetched_sn >= *stop_sn_snapshot)
        return {};

    // directly use NativeLog with proper parameters
    auto & native_log = Globals::getNativeLog();

    // Update fetch position
    Int64 fetch_from_sn = fetched_sn + 1;

    auto fetch_result{native_log.fetch(
        stream_shard.id_shard,
        fetch_from_sn,
        max_bytes,
        max_wait_ms,
        fetch_ctx.fetch_hint, // Use fetch hint from context for SN progression
        cluster::FetchIsolation::LogEnd)};

    if (fetch_result.hasError())
    {
        if (fetch_result.error_code == ErrorCodes::SEQUENCE_COMPACTED_AWAY)
        {
            /// The caller may need these information
            log_start_sn = fetch_result.result.log_start_sn;
            log_committed_sn = fetch_result.result.log_committed_sn;

            if (allow_fallback_to_historical_store)
            {
                LOG_INFO(
                    logger,
                    "Fetching sequence number has been compacted away, error={}, fetched_sn={}, log_start_sn={}, log_committed_sn={}. "
                    "Attempting to catch up the compacted sns from the historical store",
                    fetch_result.errorString(),
                    fetched_sn,
                    fetch_result.result.log_start_sn,
                    fetch_result.result.log_committed_sn);

                buildHistoricalQueryContext(default_fetch_range, fetch_result.result.log_start_sn, fetch_result.result.log_committed_sn);
                return {}; /// Next read() will fallback to historical store
            }
        }

        throw Exception(fetch_result.error_code, "Failed to fetch for {}, {}", stream_shard.string(), fetch_result.errorString());
    }

    // No session needed for direct fetch
    log_start_sn = fetch_result.result.log_start_sn;
    log_committed_sn = fetch_result.result.log_committed_sn;

    const auto & fetched_entries = fetch_result.result.entries;
    if (fetched_entries.empty() && fetch_ctx.fetch_hint)
    {
        LOG_TRACE(logger, "Clearing stale native-log fetch hint while fetching {}", stream_shard.string());
        fetch_ctx.fetch_hint.reset();
    }

    if (stop_sn_snapshot && *stop_sn_snapshot > log_committed_sn && fetched_entries.empty())
    {
        if (!allow_fallback_to_historical_store)
            throw Exception(
                ErrorCodes::SEQUENCE_COMPACTED_AWAY,
                "Native log is behind bounded stop sn and historical fallback is disabled, fetched_sn={}, log_start_sn={}, "
                "log_committed_sn={}, stop_sn={}",
                fetched_sn,
                log_start_sn,
                log_committed_sn,
                *stop_sn_snapshot);

        LOG_INFO(
            logger,
            "Native log is behind bounded stop sn, fetched_sn={}, log_start_sn={}, log_committed_sn={}, stop_sn={}. "
            "Attempting to catch up from the historical store",
            fetched_sn,
            log_start_sn,
            log_committed_sn,
            *stop_sn_snapshot);

        buildHistoricalQueryContext(default_fetch_range, log_start_sn, *stop_sn_snapshot);
        return {};
    }

    cluster::EntryPtrs bounded_entries;
    const auto * entries_to_deserialize = &fetched_entries;
    if (stop_sn_snapshot)
    {
        bounded_entries.reserve(fetched_entries.size());
        for (const auto & entry : fetched_entries)
        {
            if (entry->sn > *stop_sn_snapshot)
                break;
            bounded_entries.push_back(entry);
        }
        entries_to_deserialize = &bounded_entries;
    }

    size_t total_entries = entries_to_deserialize->size();

    cluster::SchemaRecordPtrs records;
    if (total_entries != 0)
    {
        records.reserve(total_entries);

        deserializeRecords(*entries_to_deserialize, records);
    }

    if (!fetched_entries.empty())
    {
        if (stop_sn_snapshot && fetched_entries.back()->sn > *stop_sn_snapshot)
            fetched_sn = *stop_sn_snapshot;
        else
            fetched_sn = fetched_entries.back()->sn;

        // Update fetch hint for next fetch - critical for SN progression.
        // When a bounded read fetched past the stop SN, the source will finish
        // and the post-boundary fetch hint must not be reused.
        if ((!stop_sn_snapshot || fetched_sn < *stop_sn_snapshot) && fetch_result.result.fetch_hint.hasFilePosition())
            fetch_ctx.fetch_hint = fetch_result.result.fetch_hint;
    }

    /// LOG_INFO(
    ///    logger,
    ///    "fetched next_log_sn_meta={}",
    ///    fetch_result.next_log_sn_metadata.string());

    return records;
}

void StreamingBlockReaderNativeLog::resetSequenceNumber(Int64 sn)
{
    // Simply update the fetch position
    startFetch(sn);
}

/// We like to drop columns from records if they are not requested
/// or we need add requested columns when they are missing in the cached records
/// We like to do this because we support partial column insert and also like
/// to prune the columns as early as possible to save memory consumption
void StreamingBlockReaderNativeLog::deserializeRecords(const cluster::EntryPtrs & entries, cluster::SchemaRecordPtrs & results)
{
    for (const auto & entry : entries)
    {
        if (entry->dataEntry() && !entry->dataEmpty())
        {
            auto record = cluster::SchemaRecord::parse(entry->data.stringView(), *schema_ctx);
            record->setSN(entry->sn);
            record->setShard(stream_shard.shard());
            record->setAppendTime(entry->append_timestamp);
            /// record->setMaxEventTime(entry->max_event_timestamp);
            if (record->opCode() == cluster::protocol::OpCode::InsertData || emit_control_block)
                results.push_back(std::move(record));
        }
        else
        {
            LOG_INFO(logger, "Skipping {} entry at sn={}", entry->typeString(), entry->sn);
        }
    }
}

void StreamingBlockReaderNativeLog::startFetch(Int64 sn)
{
    // Update fetch context for new position
    fetch_ctx.sn = sn;

    if (sn > 0)
        fetched_sn = sn - 1;
    else
        fetched_sn = -1;

    LOG_DEBUG(logger, "Updated fetch context: stream={} sn={}", stream_shard.string(), sn);
}

}
