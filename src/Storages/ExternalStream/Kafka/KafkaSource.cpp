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
    Poco::Logger * log_)
    : ISource(header_, true, ProcessorID::KafkaSourceID)
    , storage_snapshot(storage_snapshot_)
    , query_context(std::move(query_context_))
    , max_block_size(max_block_size_)
    , log(log_)
    , header(header_)
    , consume_ctx(kafka->topic(), shard, offset)
    , read_buffer("", 0)
    , virtual_time_columns_calc(header.columns(), nullptr)
    , virtual_col_types(header.columns(), nullptr)
    , ckpt_data(consume_ctx)
{
    is_streaming = true;

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

    if (auto * current = ckpt_ctx.exchange(nullptr, std::memory_order_relaxed); current)
        return doCheckpoint(CheckpointContextPtr{current});

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

    auto res = consumer->consume(&KafkaSource::parseMessage, this, record_consume_batch_count, record_consume_timeout, consume_ctx);
    if (res != ErrorCodes::OK)
        LOG_ERROR(log, "Failed to consume streaming, topic={} shard={} err={}", consume_ctx.topic, consume_ctx.partition, res);

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
    if (format_executor)
        parseFormat(kmessage);
    else
        parseRaw(kmessage);

    ckpt_data.last_sn = kmessage->offset;
}

void KafkaSource::parseRaw(const rd_kafka_message_t * kmessage)
{
    if (!request_virtual_columns)
    {
        /// fast path
        assert(physical_header.columns() == 1);

        if (current_batch.empty())
            current_batch.push_back(physical_header.getByPosition(0).type->createColumn());

        current_batch.back()->insertData(static_cast<const char *>(kmessage->payload), kmessage->len);
    }
    else
    {
        /// slower path, request virtual columns
        if (!current_batch.empty())
        {
            assert(current_batch.size() == virtual_time_columns_calc.size());
            for (size_t i = 0, n = virtual_time_columns_calc.size(); i < n; ++i)
            {
                if (!virtual_time_columns_calc[i])
                    current_batch[i]->insertData(static_cast<const char *>(kmessage->payload), kmessage->len);
                else
                    current_batch[i]->insertMany(virtual_time_columns_calc[i](kmessage), 1);
            }
        }
        else
        {
            for (size_t i = 0, n = virtual_time_columns_calc.size(); i < n; ++i)
            {
                if (!virtual_time_columns_calc[i])
                {
                    current_batch.push_back(physical_header.getByPosition(0).type->createColumn());
                    current_batch.back()->insertData(static_cast<const char *>(kmessage->payload), kmessage->len);
                }
                else
                {
                    auto column = virtual_col_types[i]->createColumn();
                    column->insertMany(virtual_time_columns_calc[i](kmessage), 1);
                    current_batch.push_back(std::move(column));
                }
            }
        }
    }
}

void KafkaSource::parseFormat(const rd_kafka_message_t * kmessage)
{
    assert(format_executor);

    ReadBufferFromMemory buffer(static_cast<const char *>(kmessage->payload), kmessage->len);
    auto new_rows = format_executor->execute(buffer);
    if (!new_rows)
        return;

    if (!request_virtual_columns)
    {
        if (!current_batch.empty())
        {
            /// Merge all data in the current batch into the same chunk to avoid too many small chunks
            auto new_data(format_executor->getResultColumns());
            for (size_t pos = 0; auto & column : current_batch)
                column->insertRangeFrom(*new_data[pos], 0, new_rows);
        }
        else
        {
            current_batch = format_executor->getResultColumns();
        }
    }
    else
    {
        /// slower path
        if (!current_batch.empty())
        {
            assert(current_batch.size() == virtual_time_columns_calc.size());

            /// slower path
            auto new_data(format_executor->getResultColumns());
            for (size_t i = 0, j = 0, n = virtual_time_columns_calc.size(); i < n; ++i)
            {
                if (!virtual_time_columns_calc[i])
                {
                    /// non-virtual column: physical or calculated
                    current_batch[i]->insertRangeFrom(*new_data[j], 0, new_rows);
                    ++j;
                }
                else
                {
                    current_batch[i]->insertMany(virtual_time_columns_calc[i](kmessage), new_rows);
                }
            }
        }
        else
        {
            /// slower path
            auto new_data(format_executor->getResultColumns());
            for (size_t i = 0, j = 0, n = virtual_time_columns_calc.size(); i < n; ++i)
            {
                if (!virtual_time_columns_calc[i])
                {
                    /// non-virtual column: physical or calculated
                    current_batch.push_back(std::move(new_data[j]));
                    ++j;
                }
                else
                {
                    auto column = virtual_col_types[i]->createColumn();
                    column->insertMany(virtual_time_columns_calc[i](kmessage), new_rows);
                    current_batch.push_back(std::move(column));
                }
            }
        }
    }
}

void KafkaSource::initConsumer(const Kafka * kafka)
{
    if (query_context->getSettingsRef().record_consume_batch_count != 0)
        record_consume_batch_count = static_cast<uint32_t>(query_context->getSettingsRef().record_consume_batch_count.value);

    if (query_context->getSettingsRef().record_consume_timeout != 0)
        record_consume_timeout = static_cast<int32_t>(query_context->getSettingsRef().record_consume_timeout.value);

    if (consume_ctx.offset == -1)
        consume_ctx.auto_offset_reset = "latest";
    else if (consume_ctx.offset == -2)
        consume_ctx.auto_offset_reset = "earliest";

    consume_ctx.enforce_offset = true;
    klog::KafkaWALAuth auth
        = {.security_protocol = kafka->securityProtocol(), .username = kafka->username(), .password = kafka->password()};
    consumer = klog::KafkaWALPool::instance(nullptr).getOrCreateStreamingExternal(kafka->brokers(), auth, record_consume_timeout);
    consumer->initTopicHandle(consume_ctx);
}

void KafkaSource::initFormatExecutor(const Kafka * kafka)
{
    const auto & data_format = kafka->dataFormat();
    if (!data_format.empty())
    {
        auto input_format
            = FormatFactory::instance().getInputFormat(data_format, read_buffer, physical_header, query_context, max_block_size);

        format_executor = std::make_unique<StreamingFormatExecutor>(
            physical_header, std::move(input_format), [](const MutableColumns &, Exception &) -> size_t { return 0; });
    }
}

void KafkaSource::calculateColumnPositions()
{
    for (size_t pos = 0; const auto & column : header)
    {
        if (column.name == ProtonConsts::RESERVED_APPEND_TIME)
        {
            virtual_time_columns_calc[pos]
                = [](const rd_kafka_message_t * kmessage) { return rd_kafka_message_timestamp(kmessage, nullptr); };
            /// We are assuming all virtual timestamp columns have the same data type
            virtual_col_types[pos] = column.type;
        }
        else if (column.name == ProtonConsts::RESERVED_PROCESS_TIME)
        {
            virtual_time_columns_calc[pos] = [](const rd_kafka_message_t *) { return UTCMilliseconds::now(); };
            virtual_col_types[pos] = column.type;
        }
        else if (column.name == ProtonConsts::RESERVED_EVENT_TIME)
        {
            /// If Kafka message header contains `_tp_time`, honor it
            virtual_time_columns_calc[pos] = [](const rd_kafka_message_t * kmessage) -> Int64 {
                rd_kafka_headers_t * hdrs = nullptr;
                if (rd_kafka_message_headers(kmessage, &hdrs) == RD_KAFKA_RESP_ERR_NO_ERROR)
                {
                    /// Has headers
                    const void * value = nullptr;
                    size_t size = 0;

                    if (rd_kafka_header_get_last(hdrs, ProtonConsts::RESERVED_EVENT_TIME.c_str(), &value, &size)
                        == RD_KAFKA_RESP_ERR_NO_ERROR)
                    {
                        try
                        {
                            return parseIntStrict<Int64>(std::string_view(static_cast<const char *>(value), size + 1), 0, size);
                        }
                        catch (...)
                        {
                            return 0;
                        }
                    }
                }
                return 0;
            };
            virtual_col_types[pos] = column.type;
        }
        else if (column.name == ProtonConsts::RESERVED_SHARD)
        {
            virtual_time_columns_calc[pos] = [](const rd_kafka_message_t * kmessage) -> Int64 { return kmessage->partition; };
            virtual_col_types[pos] = column.type;
        }
        else if (column.name == ProtonConsts::RESERVED_EVENT_SEQUENCE_ID)
        {
            virtual_time_columns_calc[pos] = [](const rd_kafka_message_t * kmessage) -> Int64 { return kmessage->offset; };
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


/// It basically initiate a checkpoint
/// Since the checkpoint method is called in a different thread (CheckpointCoordinator)
/// We nee make sure it is thread safe
void KafkaSource::checkpoint(CheckpointContextPtr ckpt_ctx_)
{
    /// We assume the previous ckpt is already done
    /// Use std::atomic<std::shared_ptr<CheckpointContext>>
    assert(!ckpt_ctx.load(std::memory_order_relaxed));
    ckpt_ctx = new CheckpointContext(*ckpt_ctx_);
}

/// 1) Generate a checkpoint barrier
/// 2) Checkpoint the sequence number just before the barrier
Chunk KafkaSource::doCheckpoint(CheckpointContextPtr ckpt_ctx_)
{
    /// Prepare checkpoint barrier chunk
    auto result = header_chunk.clone();
    auto chunk_ctx = std::make_shared<ChunkContext>();
    chunk_ctx->setCheckpointContext(ckpt_ctx_);
    result.setChunkContext(std::move(chunk_ctx));

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
