#include <Storages/ExternalStream/Kafka/KafkaSource.h>

#include <Checkpoint/CheckpointContext.h>
#include <Checkpoint/CheckpointCoordinator.h>
#include <DataTypes/DataTypeNullable.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Processors/Executors/StreamingFormatExecutor.h>
#include <Storages/ExternalStream/Kafka/Kafka.h>
#include <base/ClockUtils.h>
#include <Common/ProtonCommon.h>

namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
extern const int RECOVER_CHECKPOINT_FAILED;
}

namespace ExternalStream
{

KafkaSource::StallDetector::StallDetector(
    DB::Kafka::Consumer & consumer_, Int32 partition_, Int64 initial_offset, UInt64 timeout_ms_, Poco::Logger * logger_)
    : consumer(consumer_)
    , partition(partition_)
    , timeout_ms(timeout_ms_)
    /// Initialize recorded_latest_sn with current high watermark offset for better stall detection.
    /// (so that we know if the high offset ever updated or not)
    /// And we like to poll watermark offset since the cached watermark offsets may be stale
    /// which likely returns {-1001, -1001} for this case
    , recorded_latest_sn(initial_offset == ProtonConsts::LatestSN ? consumer.queryWatermarkOffsets(partition).high : initial_offset)
    , logger(logger_)
{
}

void KafkaSource::StallDetector::checkAndHandleStall()
{
    if (timeout_ms == 0)
        return;

    if (timer.elapsedMilliseconds() < timeout_ms)
        return;

    /// Read the version first. If we read the version later after stall is detected,
    /// it's possible that it will get a new version (because some other partition already
    /// detected the stall and re-created the consumer), and then an unnecessary recreation will be triggered.
    auto version = consumer.getVersion();

    auto [last_processed_sn, latest_sn] = consumer.getProgress(partition);
    /// Before the consumer gets a valid offset in cache, `latest_sn` will be RD_KAFKA_OFFSET_INVALID
    if (last_processed_sn < 0 && (latest_sn == RD_KAFKA_OFFSET_INVALID || recorded_latest_sn == latest_sn))
        /// If we never processed any message in the partition and there were no new messages produced to the
        /// topic (by comparing the latest_sn and recorded_latest_sn), don't do stuck detection
        /// for now. For materialized views `CREATE MATERIALIZED VIEW ... AS SELECT * FROM kafka` which only
        /// tails new data, and if there are no message flows in for a long time, it is not stuck
        return;

    if (latest_sn != RD_KAFKA_OFFSET_INVALID && latest_sn != recorded_latest_sn)
        recorded_latest_sn = latest_sn;

    if (last_processed_sn != recorded_last_processed_sn)
    {
        recorded_last_processed_sn = last_processed_sn;

        /// Only restart the timers when the consumer has actually consumed more messages.
        timer.restart();
        caught_up_timer.restart();

        return;
    }

    /// latest_sn is the next available offset, i.e. it's not assigned to any message yet,
    /// so need to use recorded_latest_sn - 1.
    ///
    /// If recorded_last_processed_sn already caught up with recorded_latest_sn but both of them didn't progress
    /// for a long time, still consider it may be stuck. For scenario in which a Kafka partition is in quiesce without
    /// any data flowing in for a long time, we will recreate the consumer every 10 * timeout_ms which shall be fine
    /// since it is idle anyway and the cost is largely amortized by multiplying 10.
    if (recorded_last_processed_sn >= recorded_latest_sn - 1)
    {
        if (caught_up_timer.elapsedMilliseconds() < 10 * timeout_ms)
            return;

        /// Before considering it is stuck, query the broker to fetch the up-to-date high watermark offset
        /// to compare it with recorded_latest_sn (which was fetched from cache, check getWatermarkOffsets for details).
        /// Because we want to avoid meaningless recreation as much as possible.
        try
        {
            auto offsets = consumer.queryWatermarkOffsets(partition);
            if (recorded_latest_sn >= offsets.high)
                return;
        }
        catch (const Exception & e)
        {
            /// If queryWatermarkOffsets failed in this case, recreate the consumer.
            LOG_ERROR(logger, "Failed to query watermark offsets for stall detection, error_code={} error={}", e.code(), e.message());
        }
    }

    auto new_version = consumer.recreate(version);
    reset();

    LOG_WARNING(
        logger,
        "Consumer(version: {}) seemed stalled on partition {} at {}:{} for more than {} milliseconds, recreated a new one: {}@{}",
        version,
        partition,
        recorded_last_processed_sn,
        recorded_latest_sn,
        timeout_ms,
        consumer.name(),
        new_version);
}

void KafkaSource::StallDetector::reset()
{
    recorded_last_processed_sn = 0;
    recorded_latest_sn = 0;
    timer.restart();
    caught_up_timer.restart();
}

KafkaSource::KafkaSource(
    const Block & header_,
    const StorageSnapshotPtr & storage_snapshot_,
    const String & data_format,
    const FormatSettings & format_settings,
    const String & topic_,
    DB::Kafka::ConsumerPtr consumer_,
    Int32 shard_,
    Int64 offset_,
    std::optional<Int64> high_watermark_,
    size_t max_block_size_,
    UInt64 consumer_stall_timeout_ms,
    ExternalStreamCounterPtr external_stream_counter_,
    Poco::Logger * logger_,
    ContextPtr query_context_)
    : Streaming::ISource(header_, true, ProcessorID::KafkaSourceID)
    , ExternalStreamSource(header_, storage_snapshot_, max_block_size_, query_context_)
    , topic(topic_)
    , virtual_col_value_functions(header.columns(), nullptr)
    , virtual_col_types(header.columns(), nullptr)
    , shard(shard_)
    , offset(offset_)
    , high_watermark(high_watermark_.value_or(std::numeric_limits<Int64>::max()))
    , consumer(consumer_)
    /// if offset == high_watermark, it means there is no message to read, so it already reaches the end
    , reached_the_end(high_watermark_.has_value() && offset == high_watermark_)
    , watermark_error_log_throttler(std::make_unique<TimeBasedThrottler>(60000))
    , stall_detector(*consumer, shard, offset, consumer_stall_timeout_ms, logger_)
    , external_stream_counter(external_stream_counter_)
    , logger(logger_)
{
    assert(external_stream_counter);

    if (offset > 0)
    {
        setLastProcessedSN(offset - 1);
    }
    else if (offset == ProtonConsts::LatestSN)
    {
        /// For tail case, we like to reset the processed_sn to end_sn; otherwise if there is no new data
        /// flows in to the Kafka partition, the `lagging` (end_offset - processed_sn) will be very large
        if (auto end_offset = stall_detector.recordedLatestSN(); end_offset > 0)
            setLastProcessedSN(end_offset - 1);
    }

    setStreaming(!high_watermark_.has_value());

    if (auto batch_count = query_context->getSettingsRef().record_consume_batch_count; batch_count != 0)
        record_consume_batch_count = static_cast<uint32_t>(batch_count.value);

    if (auto consume_timeout = query_context->getSettingsRef().record_consume_timeout_ms; consume_timeout != 0)
        record_consume_timeout_ms = static_cast<int32_t>(consume_timeout.value);

    initInputFormatExecutor(data_format, format_settings);

    header_chunk = Chunk(header.getColumns(), 0);
    iter = result_chunks_with_sns.begin();

    consumer->startConsume(shard, offset);
}

KafkaSource::~KafkaSource()
{
    if (!isCancelled())
        onCancel();
}

void KafkaSource::onCancel()
{
    consumer->stopConsume(shard);
}

Chunk KafkaSource::generate()
{
    if (isCancelled())
        return {};

    if (unlikely(reached_the_end))
        return {};

    if (result_chunks_with_sns.empty() || iter == result_chunks_with_sns.end())
    {
        readAndProcess();

        if (isCancelled())
            return {};

        /// After processing blocks, check again to see if there are new results
        if (result_chunks_with_sns.empty() || iter == result_chunks_with_sns.end())
        {
            /// We have not consumed any records, do stall check
            stall_detector.checkAndHandleStall();

            /// Act as a heart beat
            return header_chunk.clone();
        }

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

    auto callback = [this](void * rkmessage, size_t /*total_count*/, void * /*data*/) {
        auto * message = static_cast<rd_kafka_message_t *>(rkmessage);
        parseFormat(message);
    };

    auto error_callback = [this](rd_kafka_resp_err_t err) {
        LOG_ERROR(logger, "Failed to consume topic={} shard={} err={}", consumer->topicName(), shard, rd_kafka_err2str(err));
        external_stream_counter->addReadFailed(1);
    };

    consumer->consumeBatch(shard, record_consume_batch_count, record_consume_timeout_ms, callback, error_callback);

    auto offsets = consumer->getLastBatchOffsets(shard);
    if (!current_batch.empty())
    {
        auto rows = current_batch[0]->size();

        assert(offsets.low >= 0 && offsets.high >= 0);
        result_chunks_with_sns.emplace_back(Chunk{std::move(current_batch), rows}, offsets.high);
    }
    else
    {
        /// Because bad messages are skipped (and logged), if it turns out that all messages in this batch were all invalid,
        /// it should still report the progress as messages were actually consumed.
        if (offsets.low >= 0)
            result_chunks_with_sns.emplace_back(header_chunk.clone(), offsets.high);
    }

    /// All available messages up to the moment when the query was executed have been consumed, no need to read the messages beyond that point.
    /// `high_watermark` is the next available offset, i.e. the offset that will be assigned to the next message, thus need to use `high_watermark - 1`.
    if (offsets.high >= high_watermark - 1)
        reached_the_end = true;

    iter = result_chunks_with_sns.begin();
}

void KafkaSource::parseFormat(const rd_kafka_message_t * kmessage)
{
    assert(format_executor);

    ReadBufferFromMemory buffer(static_cast<const char *>(kmessage->payload), kmessage->len);
    auto new_rows = format_executor->execute(buffer);

    external_stream_counter->addReadBytes(kmessage->len);
    external_stream_counter->addReadRows(new_rows);

    if (format_error)
    {
        LOG_ERROR(logger, "Failed to parse message at {}: {}", kmessage->offset, format_error.value());
        external_stream_counter->addReadFailed(1);
        format_error.reset();
    }

    if (new_rows == 0u)
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

void KafkaSource::getPhysicalHeader()
{
    auto non_virtual_header = storage_snapshot->metadata->getSampleBlockNonMaterialized();

    for (size_t pos = 0; const auto & column : header)
    {
        /// The _tp_message_key column always maps to the Kafka message key.
        if (column.name == ProtonConsts::RESERVED_MESSAGE_KEY)
        {
            bool inside_nullable = false;
            auto type_id = column.type->getTypeId();
            if (type_id == TypeIndex::Nullable)
            {
                type_id = (assert_cast<const DataTypeNullable &>(*column.type)).getNestedType()->getTypeId();
                inside_nullable = true;
            }

            switch (type_id)
            {
                case TypeIndex::Bool:
                    [[fallthrough]];
                case TypeIndex::UInt8:
                {
                    virtual_col_value_functions[pos] = [inside_nullable](const rd_kafka_message_t * kmessage) -> Field {
                        UInt8 result{0};
                        if (kmessage->key_len == 0)
                        {
                            if (inside_nullable)
                                return Null{};
                            else
                                return result;
                        }
                        assert(kmessage->key_len == sizeof(result));
                        ReadBufferFromString buf({static_cast<char *>(kmessage->key), kmessage->key_len});
                        readPODBinary(result, buf);
                        return result;
                    };
                    break;
                }
                case TypeIndex::UInt16:
                {
                    virtual_col_value_functions[pos] = [inside_nullable](const rd_kafka_message_t * kmessage) -> Field {
                        UInt16 result{0};
                        if (kmessage->key_len == 0)
                        {
                            if (inside_nullable)
                                return Null{};
                            else
                                return result;
                        }
                        assert(kmessage->key_len == sizeof(result));
                        ReadBufferFromString buf({static_cast<char *>(kmessage->key), kmessage->key_len});
                        readPODBinary(result, buf);
                        return result;
                    };
                    break;
                }
                case TypeIndex::UInt32:
                {
                    virtual_col_value_functions[pos] = [inside_nullable](const rd_kafka_message_t * kmessage) -> Field {
                        UInt32 result{0};
                        if (kmessage->key_len == 0)
                        {
                            if (inside_nullable)
                                return Null{};
                            else
                                return result;
                        }
                        assert(kmessage->key_len == sizeof(result));
                        ReadBufferFromString buf({static_cast<char *>(kmessage->key), kmessage->key_len});
                        readPODBinary(result, buf);
                        return result;
                    };
                    break;
                }
                case TypeIndex::UInt64:
                {
                    virtual_col_value_functions[pos] = [inside_nullable](const rd_kafka_message_t * kmessage) -> Field {
                        UInt64 result{0};
                        if (kmessage->key_len == 0)
                        {
                            if (inside_nullable)
                                return Null{};
                            else
                                return result;
                        }
                        assert(kmessage->key_len == sizeof(result));
                        ReadBufferFromString buf({static_cast<char *>(kmessage->key), kmessage->key_len});
                        readPODBinary(result, buf);
                        return result;
                    };
                    break;
                }
                case TypeIndex::Int8:
                {
                    virtual_col_value_functions[pos] = [inside_nullable](const rd_kafka_message_t * kmessage) -> Field {
                        Int8 result{0};
                        if (kmessage->key_len == 0)
                        {
                            if (inside_nullable)
                                return Null{};
                            else
                                return result;
                        }
                        assert(kmessage->key_len == sizeof(result));
                        ReadBufferFromString buf({static_cast<char *>(kmessage->key), kmessage->key_len});
                        readPODBinary(result, buf);
                        return result;
                    };
                    break;
                }
                case TypeIndex::Int16:
                {
                    virtual_col_value_functions[pos] = [inside_nullable](const rd_kafka_message_t * kmessage) -> Field {
                        Int16 result{0};
                        if (kmessage->key_len == 0)
                        {
                            if (inside_nullable)
                                return Null{};
                            else
                                return result;
                        }
                        assert(kmessage->key_len == sizeof(result));
                        ReadBufferFromString buf({static_cast<char *>(kmessage->key), kmessage->key_len});
                        readPODBinary(result, buf);
                        return result;
                    };
                    break;
                }
                case TypeIndex::Int32:
                {
                    virtual_col_value_functions[pos] = [inside_nullable](const rd_kafka_message_t * kmessage) -> Field {
                        Int32 result{0};
                        if (kmessage->key_len == 0)
                        {
                            if (inside_nullable)
                                return Null{};
                            else
                                return result;
                        }
                        assert(kmessage->key_len == sizeof(result));
                        ReadBufferFromString buf({static_cast<char *>(kmessage->key), kmessage->key_len});
                        readPODBinary(result, buf);
                        return result;
                    };
                    break;
                }
                case TypeIndex::Int64:
                {
                    virtual_col_value_functions[pos] = [inside_nullable](const rd_kafka_message_t * kmessage) -> Field {
                        Int64 result{0};
                        if (kmessage->key_len == 0)
                        {
                            if (inside_nullable)
                                return Null{};
                            else
                                return result;
                        }
                        assert(kmessage->key_len == sizeof(result));
                        ReadBufferFromString buf({static_cast<char *>(kmessage->key), kmessage->key_len});
                        readPODBinary(result, buf);
                        return result;
                    };
                    break;
                }
                case TypeIndex::Float32:
                {
                    virtual_col_value_functions[pos] = [inside_nullable](const rd_kafka_message_t * kmessage) -> Field {
                        Float32 result{0};
                        if (kmessage->key_len == 0)
                        {
                            if (inside_nullable)
                                return Null{};
                            else
                                return result;
                        }
                        assert(kmessage->key_len == sizeof(result));
                        ReadBufferFromString buf({static_cast<char *>(kmessage->key), kmessage->key_len});
                        readPODBinary(result, buf);
                        return result;
                    };
                    break;
                }
                case TypeIndex::Float64:
                {
                    virtual_col_value_functions[pos] = [inside_nullable](const rd_kafka_message_t * kmessage) -> Field {
                        Float64 result{0};
                        if (kmessage->key_len == 0)
                        {
                            if (inside_nullable)
                                return Null{};
                            else
                                return result;
                        }
                        assert(kmessage->key_len == sizeof(result));
                        ReadBufferFromString buf({static_cast<char *>(kmessage->key), kmessage->key_len});
                        readPODBinary(result, buf);
                        return result;
                    };
                    break;
                }
                case TypeIndex::String:
                    [[fallthrough]];
                case TypeIndex::FixedString:
                {
                    virtual_col_value_functions[pos] = [inside_nullable](const rd_kafka_message_t * kmessage) -> Field {
                        if (inside_nullable && kmessage->key_len == 0)
                            return Null{};
                        return {static_cast<char *>(kmessage->key), kmessage->key_len};
                    };
                    break;
                }
                default:
                    throw Exception(
                        ErrorCodes::ILLEGAL_COLUMN, "`_tp_message_key` column does not support type {}", column.type->getName());
            }

            virtual_col_types[pos] = column.type;
        }
        else if (column.name == ProtonConsts::RESERVED_MESSAGE_HEADERS)
        {
            virtual_col_value_functions[pos] = [](const rd_kafka_message_t * kmessage) -> Field {
                /// The returned pointer in *hdrsp is associated with the rkmessage and must not be used after destruction of the message obj
                rd_kafka_headers_t * hdrs;
                auto err = rd_kafka_message_headers(kmessage, &hdrs);

                Map result;
                if (err == RD_KAFKA_RESP_ERR__NOENT)
                    return result;

                if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
                {
                    result.reserve(1);
                    result.push_back(
                        Tuple{ProtonConsts::RESERVED_ERROR, fmt::format("Failed to parse headers, error: {}", rd_kafka_err2str(err))});
                    return result;
                }

                size_t idx = 0;
                const char * name;
                const void * val;
                size_t size;

                result.reserve(rd_kafka_header_cnt(hdrs));

                while (rd_kafka_header_get_all(hdrs, idx++, &name, &val, &size) == RD_KAFKA_RESP_ERR_NO_ERROR)
                {
                    if (val != nullptr)
                    {
                        const auto * val_str = static_cast<const char *>(val);
                        result.push_back(Tuple{name, val_str});
                    }
                    else
                        result.push_back(Tuple{name, "null"});
                }

                return result;
            };
            virtual_col_types[pos] = column.type;
        }
        /// If a virtual column is explicitly defined as a physical column in the stream definition, we should honor it,
        /// just as the virtual columns document says, and users are not recommended to do this (and they still can).
        else if (std::any_of(non_virtual_header.begin(), non_virtual_header.end(), [&column](auto & non_virtual_column) {
                     return non_virtual_column.name == column.name;
                 }))
        {
            physical_header.insert(column);
        }
        else if (column.name == ProtonConsts::RESERVED_APPEND_TIME)
        {
            virtual_col_value_functions[pos] = [](const rd_kafka_message_t * kmessage) {
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
        else if (column.name == ProtonConsts::RESERVED_MESSAGE_KEY)
        {
            virtual_col_value_functions[pos]
                = [](const rd_kafka_message_t * kmessage) -> String { return {static_cast<char *>(kmessage->key), kmessage->key_len}; };
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
        writeStringBinary(topic, wb);
        writeIntBinary(shard, wb);
        writeIntBinary(lastProcessedSN(), wb);
    });

    LOG_INFO(logger, "Saved checkpoint topic={} partition={} offset={}", topic, shard, lastProcessedSN());

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

        if (recovered_topic != topic || recovered_partition != shard)
            throw Exception(
                ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                "Found mismatched kafka topic-partition. recovered={}-{}, current={}-{}",
                recovered_topic,
                recovered_partition,
                topic,
                shard);

        Int64 recovered_last_sn;
        readIntBinary(recovered_last_sn, rb);
        setLastProcessedSN(recovered_last_sn);
    });

    LOG_INFO(logger, "Recovered checkpoint topic={} partition={} last_sn={}", topic, shard, lastCheckpointSN());
}

void KafkaSource::doResetStartSN(Int64 sn)
{
    if (sn >= 0 && sn != offset)
    {
        offset = sn;

        /// Create a new partition consumer when starting offset gets changed
        /// Reset to stop consuming first.
        consumer->stopConsume(shard);
        consumer->startConsume(shard, offset);

        reached_the_end = high_watermark == offset;

        LOG_INFO(logger, "Reset offset topic={} partition={} offset={} reached_the_end={}", topic, shard, offset, reached_the_end);
    }
}

}

}
