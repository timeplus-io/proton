#include <Storages/DistributedMergeTree/StreamingKafkaSource.h>

#include <Formats/FormatFactory.h>
#include <Interpreters/Context.h>
#include <Processors/Executors/StreamingFormatExecutor.h>
#include <Storages/Kafka/ReadBufferFromKafkaConsumer.h>
#include <base/ClockUtils.h>
#include <base/logger_useful.h>
#include <Common/ProtonCommon.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

// with default poll timeout (500ms) it will give about 5 sec delay for doing 10 retries
// when selecting from empty topic
// const auto MAX_FAILED_POLL_ATTEMPTS = 10;

StreamingKafkaSource::StreamingKafkaSource(
    StorageKafka & storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    const ContextPtr & context_,
    const Names & columns,
    Poco::Logger * log_,
    size_t max_block_size_)
    : SourceWithProgress(metadata_snapshot_->getSampleBlockForColumns(columns, storage_.getVirtuals(), storage_.getStorageID()))
    , storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
    , context(context_)
    , column_names(columns)
    , log(log_)
    , max_block_size(max_block_size_)
    , non_virtual_header(metadata_snapshot->getSampleBlockNonMaterialized())
    , virtual_header(
          metadata_snapshot->getSampleBlockForColumns(storage.getVirtualColumnNames(), storage.getVirtuals(), storage.getStorageID()))
    , handle_error_mode(storage.getHandleKafkaErrorMode())
{
    iter = result_chunks.begin();
    last_flush_ms = MonotonicMilliseconds::now();

    if (!column_names.empty())
        header = metadata_snapshot->getSampleBlockForColumns(column_names, storage.getVirtuals(), storage.getStorageID());
    else
        header = metadata_snapshot->getSampleBlockForColumns({RESERVED_EVENT_TIME}, storage.getVirtuals(), storage.getStorageID());

    header_chunk = Chunk(header.getColumns(), 0);

    buffer = storage.createStreamingReadBuffer(context->getCurrentQueryId());
    buffer->subscribe();
}

StreamingKafkaSource::~StreamingKafkaSource()
{
    if (!buffer)
        return;

    buffer->unsubscribe();
}

void StreamingKafkaSource::readAndProcess()
{
    // now it's one-time usage InputStream
    // one block of the needed size (or with desired flush timeout) is formed in one internal iteration
    // otherwise external iteration will reuse that and logic will became even more fuzzy
    MutableColumns virtual_columns = virtual_header.cloneEmptyColumns();

    auto input_format = FormatFactory::instance().getInputFormat(storage.getFormatName(), *buffer, header, context, max_block_size);

    std::optional<std::string> exception_message;

    StreamingFormatExecutor executor(header, input_format, [](const MutableColumns &, Exception &) -> size_t { return 0; });

    size_t new_rows = 0;
    exception_message.reset();
    if (buffer->poll())
        new_rows = executor.execute();

    if (new_rows)
    {
        buffer->storeLastReadMessageOffset();
    }
    else
    {
        buffer->storeLastReadMessageOffset();
        LOG_DEBUG(
            log,
            "Parsing of message (topic: {}, partition: {}, offset: {}) return no rows.",
            buffer->currentTopic(),
            buffer->currentPartition(),
            buffer->currentOffset());
        return;
    }

    auto result_block = header.cloneWithColumns(executor.getResultColumns());

    if (result_block.rows() == 0)
    {
        return;
    }

    commit();
    /// 1) Insert raw blocks to in-memory aggregation table
    /// 2) Select the final result from the aggregated table
    /// 3) Update result_blocks and iterator
    result_chunks.clear();

    result_chunks.emplace_back(result_block.getColumns(), result_block.rows());
    iter = result_chunks.begin();
}

Chunk StreamingKafkaSource::generate()
{
    if (isCancelled())
    {
        return {};
    }

    if (result_chunks.empty() || iter == result_chunks.end())
    {
        readAndProcess();

        if (isCancelled())
        {
            return {};
        }

        /// After processing blocks, check again to see if there are new results
        if (result_chunks.empty() || iter == result_chunks.end())
        {
            /// Act as a heart beat and flush
            last_flush_ms = MonotonicMilliseconds::now();
            return header_chunk.clone();
        }

        /// result_blocks is not empty, fallthrough
    }

    if (MonotonicMilliseconds::now() - last_flush_ms >= flush_interval_ms)
    {
        last_flush_ms = MonotonicMilliseconds::now();
        return header_chunk.clone();
    }

    return std::move(*iter++);
}

void StreamingKafkaSource::commit()
{
    if (!buffer)
        return;

    buffer->commit();
}

}
