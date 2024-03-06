#include "KafkaSource.h"
#include "Kafka.h"

#include <Checkpoint/CheckpointContext.h>
#include <Checkpoint/CheckpointCoordinator.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <KafkaLog/KafkaWALPool.h>
#include <KafkaLog/KafkaWALSettings.h>
#include <Processors/Executors/StreamingFormatExecutor.h>
#include <base/ClockUtils.h>
#include <Common/ProtonCommon.h>
#include <Common/logger_useful.h>
#include <Common/parseIntStrict.h>

#include <librdkafka/rdkafka.h>

namespace DB
{
namespace ErrorCodes
{
extern const int INVALID_SETTING_VALUE;
extern const int LOGICAL_ERROR;
extern const int OK;
extern const int RECOVER_CHECKPOINT_FAILED;
}

KafkaSource::KafkaSource(
    Kafka * kafka,
    const Block & header_,
    const StorageSnapshotPtr & storage_snapshot_,
    ContextPtr query_context_,
    Int32 shard,
    Int64 offset,
    size_t max_block_size_,
    Poco::Logger * log_,
    ExternalStreamCounterPtr external_stream_counter_)
    : Streaming::ISource(header_, true, ProcessorID::KafkaSourceID)
    , storage_snapshot(storage_snapshot_)
    , query_context(std::move(query_context_))
    , max_block_size(max_block_size_)
    , log(log_)
    , header(header_)
    , non_virtual_header(storage_snapshot->metadata->getSampleBlockNonMaterialized())
    , consume_ctx(kafka->topic(), shard, offset)
    , read_buffer("", 0)
    , virtual_col_value_functions(header.columns(), nullptr)
    , virtual_col_types(header.columns(), nullptr)
    , ckpt_data(consume_ctx)
    , external_stream_counter(external_stream_counter_)
{
    assert(external_stream_counter);

    calculateColumnPositions();
    initConsumer(kafka);
    initFormatExecutor(kafka);

    /// If there is no data format, physical headers shall always contain 1 column
    assert((physical_header.columns() == 1 && !format_executor) || format_executor);

    header_chunk = Chunk(header.getColumns(), 0);
    iter = result_chunks.begin();
}

KafkaSource::~KafkaSource()
{
    LOG_INFO(log, "Stop streaming reading from topic={} shard={}", consume_ctx.topic, consume_ctx.partition);
    consumer->stopConsume(consume_ctx);
}

Chunk KafkaSource::generate()
{
    if (isCancelled())
        return {};

    if (result_chunks.empty() || iter == result_chunks.end())
    {
        readAndProcess();

        if (isCancelled())
            return {};

        /// After processing blocks, check again to see if there are new results
        if (result_chunks.empty() || iter == result_chunks.end())
            /// Act as a heart beat
            return header_chunk.clone();

        /// result_blocks is not empty, fallthrough
    }

    return std::move(*iter++);
}

void KafkaSource::readAndProcess()
{
    result_chunks.clear();
    current_batch.clear();
    current_batch.reserve(header.columns());

    auto res = consumer->consume(&KafkaSource::parseMessage, this, record_consume_batch_count, record_consume_timeout_ms, consume_ctx);

    if (res != ErrorCodes::OK)
    {
        LOG_ERROR(log, "Failed to consume streaming, topic={} shard={} err={}", consume_ctx.topic, consume_ctx.partition, res);
        external_stream_counter->addToReadFailed(1);
    }

    if (!current_batch.empty())
    {
        auto rows = current_batch[0]->size();
        result_chunks.emplace_back(std::move(current_batch), rows);
    }

    iter = result_chunks.begin();
}

void KafkaSource::parseMessage(void * kmessage, size_t total_count, void * data)
{
    auto * kafka = static_cast<KafkaSource *>(data);
    kafka->doParseMessage(static_cast<rd_kafka_message_t *>(kmessage), total_count);
}

void KafkaSource::doParseMessage(const rd_kafka_message_t * kmessage, size_t /*total_count*/)
{
    parseFormat(kmessage);
    ckpt_data.last_sn = kmessage->offset;
}

void KafkaSource::parseFormat(const rd_kafka_message_t * kmessage)
{
    assert(format_executor);
    assert(convert_non_virtual_to_physical_action);

    ReadBufferFromMemory buffer(static_cast<const char *>(kmessage->payload), kmessage->len);
    auto new_rows = format_executor->execute(buffer);

    external_stream_counter->addToReadBytes(kmessage->len);
    external_stream_counter->addToReadCounts(new_rows);

    if (format_error)
    {
        LOG_ERROR(log, "Failed to parse message at {}: {}", kmessage->offset, format_error.value());
        format_error.reset();
    }

    if (!new_rows)
        return;

    auto result_block = non_virtual_header.cloneWithColumns(format_executor->getResultColumns());
    convert_non_virtual_to_physical_action->execute(result_block);

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

void KafkaSource::initConsumer(const Kafka * kafka)
{
    const auto & settings_ref = query_context->getSettingsRef();

    if (settings_ref.record_consume_batch_count != 0)
        record_consume_batch_count = static_cast<uint32_t>(settings_ref.record_consume_batch_count.value);

    if (settings_ref.record_consume_timeout_ms != 0)
        record_consume_timeout_ms = static_cast<int32_t>(settings_ref.record_consume_timeout_ms.value);

    if (consume_ctx.offset == -1)
        consume_ctx.auto_offset_reset = "latest";
    else if (consume_ctx.offset == -2)
        consume_ctx.auto_offset_reset = "earliest";

    consume_ctx.enforce_offset = true;
    consumer = kafka->getConsumer(record_consume_timeout_ms);
    consumer->initTopicHandle(consume_ctx);
}

void KafkaSource::initFormatExecutor(const Kafka * kafka)
{
    const auto & data_format = kafka->dataFormat();

    auto input_format = FormatFactory::instance().getInputFormat(
        data_format,
        read_buffer,
        non_virtual_header,
        query_context,
        max_block_size,
        kafka->getFormatSettings(query_context));

    format_executor = std::make_unique<StreamingFormatExecutor>(
        non_virtual_header,
        std::move(input_format),
        [this](const MutableColumns &, Exception & ex) -> size_t
        {
            format_error = ex.what();
            return 0;
        });

    auto converting_dag = ActionsDAG::makeConvertingActions(
        non_virtual_header.cloneEmpty().getColumnsWithTypeAndName(),
        physical_header.cloneEmpty().getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Name);

    convert_non_virtual_to_physical_action = std::make_shared<ExpressionActions>(std::move(converting_dag));
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

    ckpt_ctx_->coordinator->checkpoint(State::VERSION, getLogicID(), ckpt_ctx_, [&](WriteBuffer & wb) { ckpt_data.serialize(wb); });

    /// FIXME, if commit failed ?
    /// Propagate checkpoint barriers
    return result;
}

void KafkaSource::recover(CheckpointContextPtr ckpt_ctx_)
{
    ckpt_ctx_->coordinator->recover(
        getLogicID(), ckpt_ctx_, [&](VersionType version, ReadBuffer & rb) { ckpt_data.deserialize(version, rb); });

    LOG_INFO(log, "Recovered last_sn={}", ckpt_data.last_sn);

    /// Reset consume offset started from the next of last sn
    if (ckpt_data.last_sn >= 0)
        consume_ctx.offset = ckpt_data.last_sn + 1;
}

void KafkaSource::State::serialize(WriteBuffer & wb) const
{
    writeStringBinary(topic, wb);
    writeIntBinary(partition, wb);
    writeIntBinary(last_sn, wb);
}

void KafkaSource::State::deserialize(VersionType /*version*/, ReadBuffer & rb)
{
    String recovered_topic;
    Int32 recovered_partition;
    readStringBinary(recovered_topic, rb);
    readIntBinary(recovered_partition, rb);

    if (recovered_topic != topic || recovered_partition != partition)
        throw Exception(
            ErrorCodes::RECOVER_CHECKPOINT_FAILED,
            "Found mismatched kafka topic-partition. recovered={}-{}, current={}-{}",
            recovered_topic,
            recovered_partition,
            topic,
            partition);

    readIntBinary(last_sn, rb);
}

}
