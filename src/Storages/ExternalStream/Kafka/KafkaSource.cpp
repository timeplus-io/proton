#include <Checkpoint/CheckpointContext.h>
#include <Checkpoint/CheckpointCoordinator.h>
#include <Common/ProtonCommon.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/Context.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Processors/Executors/StreamingFormatExecutor.h>
#include <Storages/ExternalStream/Kafka/Kafka.h>
#include <Storages/ExternalStream/Kafka/KafkaSource.h>
#include <Storages/ExternalStream/Kafka/Topic.h>

#include <base/ClockUtils.h>

namespace DB
{
namespace ErrorCodes
{
extern const int RECOVER_CHECKPOINT_FAILED;
}

KafkaSource::KafkaSource(
    Kafka & kafka_,
    const Block & header_,
    const StorageSnapshotPtr & storage_snapshot_,
    std::shared_ptr<RdKafka::Consumer> consumer_,
    RdKafka::TopicPtr topic_,
    Int32 shard_,
    Int64 offset_,
    std::optional<Int64> high_watermark_,
    size_t max_block_size_,
    ExternalStreamCounterPtr external_stream_counter_,
    ContextPtr query_context_)
    : Streaming::ISource(header_, true, ProcessorID::KafkaSourceID)
    , kafka(kafka_)
    , storage_snapshot(storage_snapshot_)
    , max_block_size(max_block_size_)
    , header(header_)
    , non_virtual_header(storage_snapshot->metadata->getSampleBlockNonMaterialized())
    , read_buffer("", 0)
    , virtual_col_value_functions(header.columns(), nullptr)
    , virtual_col_types(header.columns(), nullptr)
    , consumer(consumer_)
    , topic(topic_)
    , shard(shard_)
    , offset(offset_)
    , high_watermark(high_watermark_.value_or(std::numeric_limits<Int64>::max()))
    /// if offset == high_watermark, it means there is no message to read, so it already reaches the end
    , reached_the_end(high_watermark_.has_value() && offset == high_watermark_)
    , external_stream_counter(external_stream_counter_)
    , query_context(std::move(query_context_))
    , logger(&Poco::Logger::get(fmt::format("{}.{}", kafka.getLoggerName(), consumer->name())))
{
    assert(external_stream_counter);

    if (offset > 0)
        setLastProcessedSN(offset - 1);

    setStreaming(!high_watermark_.has_value());

    if (auto batch_count = query_context->getSettingsRef().record_consume_batch_count; batch_count != 0)
        record_consume_batch_count = static_cast<uint32_t>(batch_count.value);

    if (auto consume_timeout = query_context->getSettingsRef().record_consume_timeout_ms; consume_timeout != 0)
        record_consume_timeout_ms = static_cast<int32_t>(consume_timeout.value);

    calculateColumnPositions();
    initFormatExecutor();

    /// If there is no data format, physical headers shall always contain 1 column
    assert((physical_header.columns() == 1 && !format_executor) || format_executor);

    header_chunk = Chunk(header.getColumns(), 0);
    iter = result_chunks_with_sns.begin();
}

KafkaSource::~KafkaSource()
{
    if (!isCancelled())
        onCancel();
}

void KafkaSource::onCancel()
{
    if (consume_started.test())
    {
        LOG_INFO(logger, "Stop consuming from topic={} shard={}", topic->name(), shard);
        try
        {
            consumer->stopConsume(*topic, shard);
        }
        catch (...)
        {
            tryLogCurrentException(logger, fmt::format("Failed to stop consuming from topic={} shard={}", topic->name(), shard));
        }
    }
}

Chunk KafkaSource::generate()
{
    if (isCancelled())
        return {};

    if (unlikely(reached_the_end))
        return {};

    if (unlikely(consumer->isStopped()))
    {
        LOG_INFO(logger, "Consumer has stopped, stop reading data, topic={} shard={}", topic->name(), shard);
        return {};
    }

    if (!consume_started.test_and_set())
    {
        LOG_INFO(logger, "Start consuming from topic={} shard={} offset={} high_watermark={}", topic->name(), shard, offset, high_watermark);
        consumer->startConsume(*topic, shard, offset, /*check_offset=*/is_streaming);
    }

    if (result_chunks_with_sns.empty() || iter == result_chunks_with_sns.end())
    {
        readAndProcess();

        if (isCancelled())
            return {};

        /// After processing blocks, check again to see if there are new results
        if (result_chunks_with_sns.empty() || iter == result_chunks_with_sns.end())
            /// Act as a heart beat
            return header_chunk.clone();

        /// result_blocks is not empty, fallthrough
    }

    setLastProcessedSN(iter->second);
    return std::move((iter++)->first);
}

void KafkaSource::readAndProcess()
{
    result_chunks_with_sns.clear();
    current_batch.clear();
    current_batch.reserve(header.columns());

    auto callback = [this](void * rkmessage, size_t total_count, void * data) {
        parseMessage(rkmessage, total_count, data);
    };

    auto error_callback = [this](rd_kafka_resp_err_t err)
    {
        LOG_ERROR(logger, "Failed to consume topic={} shard={} err={}", topic->name(), shard, rd_kafka_err2str(err));
        external_stream_counter->addToReadFailed(1);
    };

    auto current_batch_last_sn = consumer->consumeBatch(*topic, shard, record_consume_batch_count, record_consume_timeout_ms, callback, error_callback);

    if (current_batch_last_sn >= 0) /// There are new messages
    {
        /// However, current_batch can still have no data in it because the messages are invalid, i.e. the input format fails to parse them.
        /// And in such case, we still want to report the last SN.
        if (current_batch.empty())
            result_chunks_with_sns.emplace_back(header_chunk.clone(), current_batch_last_sn);
        else
        {
            auto rows = current_batch[0]->size();
            result_chunks_with_sns.emplace_back(Chunk{std::move(current_batch), rows}, current_batch_last_sn);
        }
    }

    /// All available messages up to the moment when the query was executed have been consumed, no need to read the messages beyond that point.
    /// `high_watermark` is the next available offset, i.e. the offset that will be assigned to the next message, thus need to use `high_watermark - 1`.
    if (current_batch_last_sn >= high_watermark - 1)
        reached_the_end = true;

    iter = result_chunks_with_sns.begin();
}

void KafkaSource::parseMessage(void * rkmessage, size_t  /*total_count*/, void *  /*data*/)
{
    auto * message = static_cast<rd_kafka_message_t *>(rkmessage);
    parseFormat(message);
}

void KafkaSource::parseFormat(const rd_kafka_message_t * kmessage)
{
    assert(format_executor);

    ReadBufferFromMemory buffer(static_cast<const char *>(kmessage->payload), kmessage->len);
    auto new_rows = format_executor->execute(buffer);

    external_stream_counter->addToReadBytes(kmessage->len);
    external_stream_counter->addToReadCounts(new_rows);

    if (format_error)
    {
        LOG_ERROR(logger, "Failed to parse message at {}: {}", kmessage->offset, format_error.value());
        external_stream_counter->addToReadFailed(1);
        format_error.reset();
    }

    if (!new_rows)
        return;

    auto result_block = physical_header.cloneWithColumns(format_executor->getResultColumns());
    MutableColumns new_data(result_block.mutateColumns());

    if (!request_virtual_columns)
    {
        if (!current_batch.empty())
        {
            /// Merge all data in the current batch into the same chunk to avoid too many small chunks
            for (size_t pos = 0; pos < current_batch.size(); ++pos)
                current_batch[pos]->insertRangeFrom(*new_data[pos], 0, new_rows);
        }
        else
        {
            current_batch = std::move(new_data);
        }
    }
    else
    {
        /// slower path
        if (!current_batch.empty())
        {
            assert(current_batch.size() == virtual_col_value_functions.size());

            /// slower path
            for (size_t i = 0, j = 0, n = virtual_col_value_functions.size(); i < n; ++i)
            {
                if (!virtual_col_value_functions[i])
                {
                    /// non-virtual column: physical or calculated
                    current_batch[i]->insertRangeFrom(*new_data[j], 0, new_rows);
                    ++j;
                }
                else
                {
                    current_batch[i]->insertMany(virtual_col_value_functions[i](kmessage), new_rows);
                }
            }
        }
        else
        {
            /// slower path
            for (size_t i = 0, j = 0, n = virtual_col_value_functions.size(); i < n; ++i)
            {
                if (!virtual_col_value_functions[i])
                {
                    /// non-virtual column: physical or calculated
                    current_batch.push_back(std::move(new_data[j]));
                    ++j;
                }
                else
                {
                    auto column = virtual_col_types[i]->createColumn();
                    column->insertMany(virtual_col_value_functions[i](kmessage), new_rows);
                    current_batch.push_back(std::move(column));
                }
            }
        }
    }
}

void KafkaSource::initFormatExecutor()
{
    const auto & data_format = kafka.dataFormat();

    auto input_format = FormatFactory::instance().getInputFormat(
        data_format,
        read_buffer,
        physical_header,
        query_context,
        max_block_size,
        kafka.getFormatSettings(query_context));

    format_executor = std::make_unique<StreamingFormatExecutor>(
        physical_header,
        std::move(input_format),
        [this](const MutableColumns &, Exception & ex) -> size_t
        {
            format_error = ex.what();
            return 0;
        });
}

void KafkaSource::calculateColumnPositions()
{
    for (size_t pos = 0; const auto & column : header)
    {
        /// If a virtual column is explicitely defined as a physical column in the stream definition, we should honor it,
        /// just as the virutal columns document says, and users are not recommended to do this (and they still can).
        if (std::any_of(non_virtual_header.begin(), non_virtual_header.end(), [&column](auto & non_virtual_column) { return non_virtual_column.name == column.name; }))
        {
            physical_header.insert(column);
        }
        else if (column.name == ProtonConsts::RESERVED_APPEND_TIME)
        {
            virtual_col_value_functions[pos]
                = [](const rd_kafka_message_t * kmessage) {
                    rd_kafka_timestamp_type_t ts_type;
                    auto ts = rd_kafka_message_timestamp(kmessage, &ts_type);
                    /// Only set the append time when the timestamp is actually an append time.
                    if (ts_type == RD_KAFKA_TIMESTAMP_LOG_APPEND_TIME)
                        return Decimal64(ts);
                    return Decimal64();
                };
            /// We are assuming all virtual timestamp columns have the same data type
            virtual_col_types[pos] = column.type;
        }
        else if (column.name == ProtonConsts::RESERVED_PROCESS_TIME)
        {
            virtual_col_value_functions[pos] = [](const rd_kafka_message_t *) { return Decimal64(UTCMilliseconds::now()); };
            virtual_col_types[pos] = column.type;
        }
        else if (column.name == ProtonConsts::RESERVED_EVENT_TIME)
        {
            virtual_col_value_functions[pos] = [](const rd_kafka_message_t * kmessage) {
                rd_kafka_timestamp_type_t ts_type;
                auto ts = rd_kafka_message_timestamp(kmessage, &ts_type);
                if (ts_type == RD_KAFKA_TIMESTAMP_NOT_AVAILABLE)
                    return Decimal64();
                /// Each Kafka message has only one timestamp, thus we always use it as the `_tp_time`.
                return Decimal64(ts);
            };
            virtual_col_types[pos] = column.type;
        }
        else if (column.name == ProtonConsts::RESERVED_SHARD)
        {
            virtual_col_value_functions[pos] = [](const rd_kafka_message_t * kmessage) -> Int64 { return kmessage->partition; };
            virtual_col_types[pos] = column.type;
        }
        else if (column.name == ProtonConsts::RESERVED_EVENT_SEQUENCE_ID)
        {
            virtual_col_value_functions[pos] = [](const rd_kafka_message_t * kmessage) -> Int64 { return kmessage->offset; };
            virtual_col_types[pos] = column.type;
        }
        else if (column.name == Kafka::VIRTUAL_COLUMN_MESSAGE_KEY)
        {
            virtual_col_value_functions[pos] = [](const rd_kafka_message_t * kmessage) -> String { return {static_cast<char *>(kmessage->key), kmessage->key_len}; };
            virtual_col_types[pos] = column.type;
        }
        else
        {
            physical_header.insert(column);
        }

        ++pos;
    }

    request_virtual_columns = std::any_of(virtual_col_types.begin(), virtual_col_types.end(), [](auto type) { return type != nullptr; });

    /// Clients like to read virtual columns only, add the first physical column, then we know how many rows
    if (physical_header.columns() == 0)
    {
        const auto & physical_columns = storage_snapshot->getColumns(GetColumnsOptions::Ordinary);
        const auto & physical_column = physical_columns.front();
        physical_header.insert({physical_column.type->createColumn(), physical_column.type, physical_column.name});
    }
}

/// 1) Generate a checkpoint barrier
/// 2) Checkpoint the sequence number just before the barrier
Chunk KafkaSource::doCheckpoint(CheckpointContextPtr ckpt_ctx_)
{
    /// Prepare checkpoint barrier chunk
    auto result = header_chunk.clone();
    result.setCheckpointContext(ckpt_ctx_);

    ckpt_ctx_->coordinator->checkpoint(getVersion(), getLogicID(), ckpt_ctx_, [&](WriteBuffer & wb) {
        writeStringBinary(kafka.topicName(), wb);
        writeIntBinary(shard, wb);
        writeIntBinary(lastProcessedSN(), wb);
    });

    LOG_INFO(logger, "Saved checkpoint topic={} parition={} offset={}", kafka.topicName(), shard, lastProcessedSN());

    /// FIXME, if commit failed ?
    /// Propagate checkpoint barriers
    return result;
}

void KafkaSource::doRecover(CheckpointContextPtr ckpt_ctx_)
{
    ckpt_ctx_->coordinator->recover(getLogicID(), ckpt_ctx_, [&](VersionType, ReadBuffer & rb) {
        String recovered_topic;
        Int32 recovered_partition;
        readStringBinary(recovered_topic, rb);
        readIntBinary(recovered_partition, rb);

        if (recovered_topic != kafka.topicName() || recovered_partition != shard)
            throw Exception(
                ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                "Found mismatched kafka topic-partition. recovered={}-{}, current={}-{}",
                recovered_topic,
                recovered_partition,
                kafka.topicName(),
                shard);

        Int64 recovered_last_sn;
        readIntBinary(recovered_last_sn, rb);
        setLastProcessedSN(recovered_last_sn);
    });

    LOG_INFO(logger, "Recovered checkpoint topic={} parition={} last_sn={}", kafka.topicName(), shard, lastProcessedSN());
}

void KafkaSource::doResetStartSN(Int64 sn)
{
    if (sn >= 0)
    {
        offset = sn;
        LOG_INFO(logger, "Reset offset topic={} parition={} offset={}", kafka.topicName(), shard, offset);
    }
}

}
