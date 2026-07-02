#include <Storages/Stream/StreamingBlockReaderNativeLog.h>

#include <Common/logger_useful.h>

#include <Bootstrap/Globals.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Processors/Chunk.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Streaming/SequencedChunkSplitter.h>
#include <Common/quoteString.h>

#include <ranges>
#include <fmt/ranges.h>

namespace DB
{
namespace ErrorCodes
{
extern const int SEQUENCE_COMPACTED_AWAY;
}

struct HistoricalQueryContext
{
    BlockIO io;
    PullingAsyncPipelineExecutor executor;
    Block output_header;
    std::optional<size_t> sn_col_pos;

    Int64 fetch_start_sn = 0;
    Int64 fetch_end_sn = 0;
    Int64 log_start_sn = 0;
    Int64 log_committed_sn = 0;

    Int64 max_sn = 0;
    Int64 total_rows = 0;
    Int64 total_bytes = 0;

    std::unique_ptr<Streaming::SequencedChunkSplitter> sequenced_chunk_splitter;

    bool finished = false;

    HistoricalQueryContext(BlockIO && io_) : io(std::move(io_)), executor(io.pipeline) { }
};

void StreamingBlockReaderNativeLog::buildHistoricalQueryContext(Int64 fetch_range, Int64 curr_log_start_sn, Int64 curr_log_committed_sn)
{
    chassert(!historical_ctx || historical_ctx->finished);
    chassert(fetch_range > 0);

    Block header;
    if (schema_ctx->column_positions_requested.positions.empty())
    {
        /// Read all columns
        header = schema.cloneEmpty();
    }
    else
    {
        for (const auto & pos : schema_ctx->column_positions_requested.positions)
            header.insert(schema.getByPosition(pos));
    }

    auto cols_list
        = fmt::format("{}", fmt::join(header | std::views::transform([](auto & col) { return backQuoteIfNeed(col.name); }), ", "));
    auto sn_col_pos = header.tryGetPositionByName(ProtonConsts::RESERVED_EVENT_SEQUENCE_ID);
    /// Add _tp_sn to the back of cols list if not exist
    if (!sn_col_pos.has_value())
        cols_list.append(fmt::format(", {}", ProtonConsts::RESERVED_EVENT_SEQUENCE_ID));

    auto fetch_end_sn = fetched_sn + fetch_range;
    auto sql = fmt::format(
        "SELECT {} FROM table({}.{}) WHERE {} > {} AND {} <= {} ORDER BY {} ASC",
        cols_list,
        backQuoteIfNeed(stream_shard.ns),
        backQuoteIfNeed(stream_shard.name),
        ProtonConsts::RESERVED_EVENT_SEQUENCE_ID,
        fetched_sn,
        ProtonConsts::RESERVED_EVENT_SEQUENCE_ID,
        fetch_end_sn,
        ProtonConsts::RESERVED_EVENT_SEQUENCE_ID);

    auto query_context = Context::createCopy(Globals::getGlobalContext().getGlobalContext());
    query_context->makeQueryContext();
    /// Only read the current shard data, which is applicable for StorageStream and StorageKeyValueStream only.
    query_context->setShardToRead(stream_shard.shard());

    historical_ctx = std::make_shared<HistoricalQueryContext>(executeQuery(sql, std::move(query_context), /*internal=*/true));
    historical_ctx->output_header.swap(header);
    historical_ctx->sn_col_pos = sn_col_pos;
    historical_ctx->log_start_sn = curr_log_start_sn;
    historical_ctx->log_committed_sn = curr_log_committed_sn;
    historical_ctx->max_sn = fetched_sn;
    historical_ctx->fetch_start_sn = fetched_sn + 1;
    historical_ctx->fetch_end_sn = fetch_end_sn;
}

cluster::SchemaRecordPtrs StreamingBlockReaderNativeLog::readFromHistoricalStore()
{
    chassert(historical_ctx);

    auto & max_sn = historical_ctx->max_sn;

    chassert(
        historical_ctx->executor.getHeader().columns()
        == (historical_ctx->sn_col_pos.has_value() ? historical_ctx->output_header.columns()
                                                   : historical_ctx->output_header.columns() + 1));

    Stopwatch stopwatch;
    cluster::SchemaRecordPtrs records;
    size_t total_rows = 0;
    size_t total_bytes = 0;
    auto add_record = [&](Streaming::SequenceChunk && output_sequence_chunk) {
        auto & [output_sn, output_chunk] = output_sequence_chunk;
        if (!output_chunk.hasRows())
            return;

        total_rows += output_chunk.rows();
        total_bytes += output_chunk.bytes();
        auto record = std::make_shared<cluster::SchemaRecord>(
            cluster::protocol::OpCode::InsertData,
            historical_ctx->output_header.cloneWithColumns(output_chunk.detachColumns()),
            schema_ctx->schema_version_requested);
        record->setSN(output_sn);
        record->setShard(stream_shard.shard());
        /// record->setAppendTime(0);
        records.emplace_back(std::move(record));
    };

    DB::Chunk chunk;

    /// Batch pulling
    while (true)
    {
        if (total_bytes >= max_bytes || stopwatch.elapsedMilliseconds() >= static_cast<UInt64>(max_wait_ms))
            break;

        historical_ctx->finished |= !historical_ctx->executor.pull(chunk, max_wait_ms);
        if (historical_ctx->finished)
        {
            /// It's possible that the historical_ctx->sequenced_chunk_splitter is not initialized if no data
            if (historical_ctx->sequenced_chunk_splitter)
                add_record(historical_ctx->sequenced_chunk_splitter->finalize()); /// Add the final record

            break;
        }

        auto rows = chunk.rows();
        if (!rows)
        {
            if (chunk.hasSN())
                max_sn = std::max(max_sn, chunk.getSN());

            continue;
        }

        if (!historical_ctx->sequenced_chunk_splitter)
        {
            if (historical_ctx->sn_col_pos.has_value())
                historical_ctx->sequenced_chunk_splitter
                    = std::make_unique<Streaming::SequencedChunkSplitter>(*historical_ctx->sn_col_pos, /*remove_sn_col=*/false);
            else
                historical_ctx->sequenced_chunk_splitter
                    = std::make_unique<Streaming::SequencedChunkSplitter>(chunk.getNumColumns() - 1, /*remove_sn_col=*/true);
        }

        auto outputs = historical_ctx->sequenced_chunk_splitter->splitOrConcat(std::move(chunk));
        for (auto & output : outputs)
            add_record(std::move(output));

        max_sn = std::max(max_sn, historical_ctx->sequenced_chunk_splitter->currentSN());
    }

    if (max_sn > fetched_sn)
        LOG_INFO(
            logger,
            "Fetched {} rows in historical store, total_bytes={}, fetched_sns=[{}, {}]",
            total_rows,
            total_bytes,
            fetched_sn + 1,
            max_sn);

    fetched_sn = max_sn;
    historical_ctx->total_rows += total_rows;
    historical_ctx->total_bytes += total_bytes;

    SCOPE_EXIT({
        if (historical_ctx->finished)
            historical_ctx.reset();
    });

    /// If finished reading all historical data, switch to reading from the nativelog.
    /// And ensures that after catching up with historical (compacted) data,
    /// the reader can continue to fetch new data from the nativelog.
    if (historical_ctx->finished)
    {
        auto next_sn = fetched_sn + 1;
        /// Successfully catched up compacted sns
        if (next_sn >= historical_ctx->log_start_sn)
        {
            resetSequenceNumber(next_sn);
            LOG_INFO(
                logger,
                "Successfully catched up sns=[{}, {}] from historical store, total_rows={}, total_bytes={}, log_start_sn={}, "
                "log_committed_sn={}. Next fetching sn={} from nativelog",
                historical_ctx->fetch_start_sn,
                fetched_sn,
                historical_ctx->total_rows,
                historical_ctx->total_bytes,
                historical_ctx->log_start_sn,
                historical_ctx->log_committed_sn,
                next_sn);
        }
        else if (next_sn == historical_ctx->fetch_start_sn)
        {
            /// The current fetched range does not get any sn, it's possible for historical storage of mutable-stream or with TTL
            /// Try to fetch with a bigger range from historical store until reaching log_committed_sn.
            if (historical_ctx->fetch_end_sn >= historical_ctx->log_committed_sn)
                throw Exception(
                    ErrorCodes::SEQUENCE_COMPACTED_AWAY,
                    "Failed to catch up any compacted sns from historical store, fetched_sn={}, log_start_sn={}, "
                    "log_committed_sn={}",
                    fetched_sn,
                    historical_ctx->log_start_sn,
                    historical_ctx->log_committed_sn);

            /// Expand fetch range by 10000, for example:
            /// the previous fetch range is [fetched_sn + 1, fetched_sn + 10000]
            /// the expanded fetch range is [fetched_sn + 1, fetched_sn + 20000]
            auto old_fetch_end_sn = historical_ctx->fetch_end_sn;
            auto expanded_fetch_range = historical_ctx->fetch_end_sn - fetched_sn + 10'000;
            buildHistoricalQueryContext(expanded_fetch_range, historical_ctx->log_start_sn, historical_ctx->log_committed_sn);
            LOG_INFO(
                logger,
                "Not found any compacted sns from historical store ([{}, {}]), expanded fetch range to [{}, {}]",
                historical_ctx->fetch_start_sn,
                old_fetch_end_sn,
                historical_ctx->fetch_start_sn,
                historical_ctx->fetch_end_sn);
        }
        else
        {
            /// The current fetched range does not reach log_start_sn.
            /// Need to fetch the next range from historical store to continue catching up.
            auto old_fetch_start_sn = historical_ctx->fetch_start_sn;
            auto old_total_rows = historical_ctx->total_rows;
            auto old_total_bytes = historical_ctx->total_bytes;
            buildHistoricalQueryContext(default_fetch_range, historical_ctx->log_start_sn, historical_ctx->log_committed_sn);
            historical_ctx->fetch_start_sn = old_fetch_start_sn;
            historical_ctx->total_rows = old_total_rows;
            historical_ctx->total_bytes = old_total_bytes;
        }
    }

    return records;
}
}
