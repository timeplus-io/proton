#include <Storages/ExternalStream/Kafka/KafkaSource.h>

#include <Checkpoint/CheckpointContext.h>
#include <Checkpoint/CheckpointCoordinator.h>
#include <Cluster/Common/Constants.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeNullable.h>
#include <Formats/Avro/InputStreamReadBufferAdapter.h>
#include <Formats/Avro/OutputStreamWriteBufferAdapter.h>
#include <Formats/FormatFactory.h>
#include <Formats/KafkaSchemaRegistry.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Processors/Executors/StreamingFormatExecutor.h>
#include <Storages/ExternalStream/Kafka/Kafka.h>
#include <base/ClockUtils.h>
#include <base/scope_guard.h>
#include <Common/Exception.h>
#include <Common/ProtonCommon.h>

#include <Encoder.hh>
#include <Generic.hh>
#include <Specific.hh>

#include <memory>
#include <utility>


namespace CurrentMetrics
{
extern const Metric ParallelParsingThreads;
extern const Metric ParallelParsingThreadsActive;
}

namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_PARSE_DATA;
extern const int CANNOT_RECEIVE_MESSAGE;
extern const int ILLEGAL_COLUMN;
extern const int INCORRECT_DATA;
extern const int RECOVER_CHECKPOINT_FAILED;
extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
}

namespace ExternalStream
{

namespace
{
rd_kafka_message_s createKafkaMessage(const CopiedKafkaMessage & message, int32_t partition)
{
    /// Util function to create a mimic Kafka message for parsing and virtual columns generation.
    rd_kafka_message_s msg;
    msg.err = RD_KAFKA_RESP_ERR_NO_ERROR;
    msg.partition = partition;
    msg.payload = const_cast<void *>(static_cast<const void *>(message.payload.data()));
    msg.len = message.payload.size();
    msg.key = const_cast<void *>(static_cast<const void *>(message.key.data()));
    msg.key_len = message.key.size();
    msg.offset = message.offset;
    return msg;
}
}

KafkaSource::StallDetector::StallDetector(
    DB::Kafka::Consumer & consumer_, Int32 partition_, Int64 initial_offset, const KafkaSource::Timeouts & timeouts_, LoggerPtr logger_)
    : consumer(consumer_)
    , partition(partition_)
    , timeouts(timeouts_)
    /// Initialize recorded_latest_sn with current high watermark offset for better stall detection.
    /// (so that we know if the high offset ever updated or not)
    /// And we like to poll watermark offset since the cached watermark offsets may be stale
    /// which likely returns {-1001, -1001} for this case
    , recorded_latest_sn(initial_offset == cluster::Constants::LatestSN ? consumer.queryWatermarkOffsets(partition, timeouts.connection_timeout_ms).high : initial_offset)
    , logger(std::move(logger_))
{
}

void KafkaSource::StallDetector::checkAndHandleStall()
{
    if (timeouts.consumer_stall_timeout_ms == 0)
        return;

    if (timer.elapsedMilliseconds() < timeouts.consumer_stall_timeout_ms)
        return;

    auto [last_processed_sn, latest_sn] = consumer.getProgress(partition);
    /// Before the consumer gets a valid offset in cache, `latest_sn` will be RD_KAFKA_OFFSET_INVALID
    if (last_processed_sn < 0 && (latest_sn == RD_KAFKA_OFFSET_INVALID || recorded_latest_sn == latest_sn))
        /// If we never processed any message in the partition and there were no new messages produced to the
        /// topic (by comparing the latest_sn and recorded_latest_sn), don't do stuck detection
        /// for now. For materialized views `CREATE MATERIALIZED VIEW ... AS SELECT * FROM kafka` which only
        /// tails new data, and if there are no message flows in for a long time, it is not stuck
        return;

    if (latest_sn != RD_KAFKA_OFFSET_INVALID)
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
        if (caught_up_timer.elapsedMilliseconds() < 10 * timeouts.consumer_stall_timeout_ms)
            return;

        /// Before considering it is stuck, query the broker to fetch the up-to-date high watermark offset
        /// to compare it with recorded_latest_sn (which was fetched from cache, check getWatermarkOffsets for details).
        /// Because we want to avoid meaningless recreation as much as possible.
        try
        {
            auto offsets = consumer.queryWatermarkOffsets(partition, timeouts.connection_timeout_ms);
            if (recorded_latest_sn >= offsets.high)
            {
                timer.restart();
                caught_up_timer.restart();
                return;
            }
        }
        catch (const Exception & e)
        {
            /// If queryWatermarkOffsets failed in this case, recreate the consumer.
            LOG_ERROR(logger, "Failed to query watermark offsets for stall detection, error_code={} error={}", e.code(), e.message());
        }
    }

    auto new_consumer = consumer.recreate(timeouts.consumer_stall_timeout_ms);
    if (new_consumer)
    {
        LOG_WARNING(
            logger,
            "Consumer seemed stalled on partition {} at {}:{} for more than {} milliseconds, recreated a new one: {}",
            partition,
            recorded_last_processed_sn,
            recorded_latest_sn,
            timeouts.consumer_stall_timeout_ms,
            consumer.name());
    }

    timer.restart();
    caught_up_timer.restart();
}

KafkaSource::KafkaSource(
    const Block & header_,
    const StorageSnapshotPtr & storage_snapshot_,
    String data_format_,
    const FormatSettings & format_settings_,
    String topic_,
    DB::Kafka::ConsumerPtr consumer_,
    Int32 shard_,
    Int64 offset_,
    std::optional<Int64> high_watermark_,
    size_t max_block_size_,
    const Timeouts & timeouts,
    std::shared_ptr<KafkaSchemaRegistryForAvro> avro_key_schema_registry_,
    ExternalStreamCounterPtr external_stream_counter_,
    ContextPtr query_context_,
    LoggerPtr logger_)
    : Streaming::ISource(header_, true, std::move(logger_), ProcessorID::KafkaSourceID)
    , ExternalStreamSource(header_, storage_snapshot_, max_block_size_, query_context_)
    , data_format(std::move(data_format_))
    , topic(std::move(topic_))
    , partition(shard_)
    , virtual_col_value_functions(header.columns(), nullptr)
    , virtual_col_types(header.columns(), nullptr)
    , ignore_format_errors(format_settings_.ignore_parsing_errors)
    , avro_key_schema_registry(std::move(avro_key_schema_registry_))
    , offset(offset_)
    , high_watermark(high_watermark_.value_or(std::numeric_limits<Int64>::max()))
    , consumer(std::move(consumer_))
    /// if offset == high_watermark, it means there is no message to read, so it already reaches the end
    , reached_the_end(high_watermark_.has_value() && offset == high_watermark_)
    , watermark_error_log_throttler(std::make_unique<TimeBasedThrottler>(60000))
    , stall_detector(*consumer, shard_, offset, timeouts, logger)
    , external_stream_counter(std::move(external_stream_counter_))
    , connection_timeout_ms(timeouts.connection_timeout_ms)
    , format_settings(format_settings_)
{
    assert(external_stream_counter);

    setStream(shard_);

    if (offset > 0)
    {
        setLastProcessedSN(offset - 1);
    }
    else if (offset == cluster::Constants::LatestSN)
    {
        /// For tail case, we like to reset the processed_sn to end_sn; otherwise if there is no new data
        /// flows in to the Kafka partition, the `lagging` (end_offset - processed_sn) will be very large
        if (auto end_offset = stall_detector.recordedLatestSN(); end_offset > 0)
            setLastProcessedSN(end_offset - 1);
    }

    setStreaming(!high_watermark_.has_value());

    setDescription(fmt::format("topic={},partition={}", topic, partition));

    const auto & query_settings = query_context->getSettingsRef();

    if (auto batch_count = query_settings.record_consume_batch_count; batch_count != 0)
        record_consume_batch_count = static_cast<uint32_t>(batch_count.value);

    if (auto consume_timeout = query_settings.record_consume_timeout_ms; consume_timeout != 0)
        record_consume_timeout_ms = static_cast<int32_t>(consume_timeout.value);

    /// Initialize physical_header to set up request_virtual_columns
    getPhysicalHeader();

    /// Initialize parallel parsing if enabled
    if (query_settings.parallel_parsing && query_settings.parallel_parsing_threads > 0)
    {
        parallel_parsing_enabled = true;
        parallel_parsing_threads = query_settings.parallel_parsing_threads;

        if (parallel_parsing_threads > 16)
        {
            LOG_WARNING(
                logger, "'parallel_parsing_threads={}' is too large. Set the parsing threads number to 16.", parallel_parsing_threads);
            parallel_parsing_threads = 16;
        }

        /// Create thread pool for parser threads
        parser_pool.emplace(CurrentMetrics::ParallelParsingThreads, CurrentMetrics::ParallelParsingThreadsActive, parallel_parsing_threads);
        LOG_INFO(
            logger,
            "Parallel parsing enabled for Kafka source with {} threads, topic={} partition={}",
            parallel_parsing_threads,
            topic,
            partition);
    }
}

KafkaSource::~KafkaSource()
{
    if (!isCancelled())
        onCancel();
}

void KafkaSource::onCancel() noexcept
{
    if (parallel_parsing_enabled)
        finishParallelParsing();

    try
    {
        consumer->stopConsume(static_cast<int32_t>(partition));
    }
    catch (...)
    {
        tryLogCurrentException(logger, "Error occurs on cancellation.");
    }
}

Chunk KafkaSource::generate()
{
    if (isCancelled())
        return {};

    if (unlikely(reached_the_end))
        return {};

    if (!generate_inited.test_and_set())
    {
        consumer->startConsume(static_cast<int32_t>(partition), offset);

        if (parallel_parsing_enabled)
        {
            /// Initialize processing units
            const size_t read_ahead_batches = query_context->getSettingsRef().parallel_parsing_read_ahead_batches;
            initParseUnits(parallel_parsing_threads + read_ahead_batches);
            reader_thread = ThreadFromGlobalPool(&KafkaSource::readerThreadFunction, this, CurrentThread::getGroup());
        }
        else
        {
            /// Initialize processing unit. (non-parallel parsing uses processing_unit[0])
            initParseUnits(1);
        }
    }

    if (parallel_parsing_enabled)
        return generateParallel();
    else
        return generateSequential();
}

size_t KafkaSource::parseMessage(
    const std::shared_ptr<StreamingFormatExecutor> & executor, const rd_kafka_message_t * message, MutableColumns & virtual_cols)
{
    /// Parse a single Kafka message using the given format executor.
    /// Throws on parsing error.
    ReadBufferFromMemory buffer(static_cast<const char *>(message->payload), message->len);
    auto new_rows = executor->execute(buffer);
    if (new_rows > 0 && !virtual_cols.empty())
    {
        chassert(virtual_cols.size() == virtual_col_value_functions.size());
        appendVirtualColumnsRow(virtual_cols, message, new_rows);
    }
    return new_rows;
}

size_t KafkaSource::parseMessagesInBatch(
    const std::shared_ptr<StreamingFormatExecutor> & executor,
    const std::vector<StringRef> & message_refs,
    std::string & buf)
{
    /// Calculate total bytes and reserve buffer
    size_t total_bytes = 0;
    for (const auto & msg : message_refs)
        total_bytes += msg.size;

    buf.clear();
    buf.reserve(total_bytes + (data_format == "ProtobufSingle" ? message_refs.size() * 2 : 0) + 128);

    SCOPE_EXIT({
        /// Prevent buffer from keeping growing indefinitely
        if (buf.capacity() > 64 * 1024 * 1024)
        {
            LOG_INFO(
                logger,
                "Messages buffer is cleared to release memory: topic={} partition={} capacity={}",
                topic,
                partition,
                buf.capacity());

            std::string empty;
            buf.swap(empty);
        }
    });

    /// Write messages to buffer
    WriteBufferFromString wb(buf);
    for (const auto & message : message_refs)
    {
        /// Protobuf format should have varint message length as delimiter
        if (data_format == "ProtobufSingle")
            writeStringBinary(message, wb);
        else
            writeString(message, wb);
    }
    wb.finalize();

    /// Execute parsing
    ReadBufferFromMemory rb(buf);
    return executor->execute(rb);
}

Chunk KafkaSource::generateSequential()
{
    auto & unit = processing_units[0];
    unit.chunk = {};
    unit.sn_range = {-1, -1};
    unit.last_processed_record_timestamp = -1;
    unit.read_bytes = 0;

    auto callback = [this, &unit](void * rkmessage, size_t total_count, void * /*data*/) {
        const auto ** messages = static_cast<const rd_kafka_message_t **>(rkmessage);
        size_t first_valid = 0;
        while (first_valid < total_count && messages[first_valid]->offset < offset)
            ++first_valid;

        if (first_valid >= total_count)
            return;

        unit.sn_range.start = messages[first_valid]->offset;
        unit.sn_range.end = messages[total_count - 1]->offset;

        rd_kafka_timestamp_type_t ts_type;
        if (auto ts = rd_kafka_message_timestamp(messages[total_count - 1], &ts_type); ts > 0)
            unit.last_processed_record_timestamp = ts;

        for (size_t i = first_valid; i < total_count; ++i)
            unit.read_bytes += messages[i]->len;

        if (unit.format_batch_executor)
        {
            /// Try batch parsing first if available
            auto batch_parse_succeed = tryBatchParseAndGenerate(messages, first_valid, total_count, unit);
            if (batch_parse_succeed)
                return;
        }

        /// Per-message parsing path
        parseAndGenerate(messages, first_valid, total_count, unit);
    };

    auto error_callback = [this](rd_kafka_resp_err_t err, std::string_view errmsg) {
        external_stream_counter->addReadFailed(1);
        throw Exception(
            ErrorCodes::CANNOT_RECEIVE_MESSAGE,
            "Failed to consume message from kafka topic={} partition={} error_code={} error_code_msg='{}' error_msg='{}'",
            topic,
            partition,
            err,
            rd_kafka_err2str(err),
            errmsg);
    };

    consumer->consumeBatch(
        static_cast<int32_t>(partition), record_consume_batch_count, record_consume_timeout_ms, callback, error_callback);

    if (!unit.chunk)
    {
        /// If no chunk was produced, create a heartbeat chunk to report progress
        /// Do stall detection if no progress was made
        stall_detector.checkAndHandleStall();
        return header_chunk.clone();
    }

    return outputParseUnit(unit);
}

Field KafkaSource::decodeAvroKey(const rd_kafka_message_t * kmessage) const
{
    try
    {
        /// Decode the Avro-encoded Kafka message key and return it as a JSON string.
        /// The key bytes follow the Confluent wire format: 1-byte magic (0x00) + 4-byte schema ID + Avro binary payload.
        /// We deserialize the binary payload into a GenericDatum using the schema fetched from the registry,
        /// then re-encode the datum as JSON so callers receive a human-readable string representation of the key record.
        ReadBufferFromMemory key_buf(static_cast<const char *>(kmessage->key), kmessage->key_len);
        UInt32 schema_id = KafkaSchemaRegistry::readSchemaId(key_buf);
        auto schema = avro_key_schema_registry->getSchema(schema_id);
    
        auto avro_in = std::make_unique<Avro::InputStreamReadBufferAdapter>(key_buf);
        auto bin_decoder = avro::validatingDecoder(schema, avro::binaryDecoder());
        bin_decoder->init(*avro_in);
    
        avro::GenericDatum datum(schema);
        avro::decode(*bin_decoder, datum);
    
        WriteBufferFromOwnString json_buf;
        Avro::OutputStreamWriteBufferAdapter avro_out(json_buf);
        auto json_encoder = avro::jsonEncoder(schema);
        json_encoder->init(avro_out);
        avro::encode(*json_encoder, datum);
        json_encoder->flush();
    
        return Field{json_buf.str()};
    }
    catch (...)
    {
        throw DB::Exception(ErrorCodes::INCORRECT_DATA, "Failed to decode Avro message key: {}", getCurrentExceptionMessage(false));
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
                        readBinaryBigEndian(result, buf);
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
                        readBinaryBigEndian(result, buf);
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
                        readBinaryBigEndian(result, buf);
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
                        readBinaryBigEndian(result, buf);
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
                        readBinaryBigEndian(result, buf);
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
                        readBinaryBigEndian(result, buf);
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
                        readBinaryBigEndian(result, buf);
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
                        readBinaryBigEndian(result, buf);
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
                        readBinaryBigEndian(result, buf);
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
                        readBinaryBigEndian(result, buf);
                        return result;
                    };
                    break;
                }
                case TypeIndex::String:
                    [[fallthrough]];
                case TypeIndex::FixedString:
                {
                    if (avro_key_schema_registry)
                    {
                        virtual_col_value_functions[pos] = [this, inside_nullable](const rd_kafka_message_t * kmessage) -> Field
                        {
                            if (kmessage->key_len == 0)
                                return inside_nullable ? Field{Null{}} : Field{String{}};
                            return decodeAvroKey(kmessage);
                        };
                    }
                    else
                    {
                        virtual_col_value_functions[pos] = [inside_nullable](const rd_kafka_message_t * kmessage) -> Field {
                            if (inside_nullable && kmessage->key_len == 0)
                                return Null{};
                            return {static_cast<char *>(kmessage->key), kmessage->key_len};
                        };
                    }
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
        /// If a virtual column is explicitly defined as a physical column in the stream definition, we should honor it,
        /// just as the virtual columns document says, and users are not recommended to do this (and they still can).
        else if (std::ranges::any_of(
                     non_virtual_header, [&column](auto & non_virtual_column) { return non_virtual_column.name == column.name; }))
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

    request_virtual_columns = std::ranges::any_of(virtual_col_types, [](auto type_ptr) { return type_ptr != nullptr; });

    /// Clients like to read virtual columns only, add the first physical column, then we know how many rows
    if (physical_header.columns() == 0)
    {
        const auto & physical_columns = storage_snapshot->getColumns(GetColumnsOptions::Ordinary);
        const auto & physical_column = physical_columns.front();
        physical_header.insert({physical_column.type->createColumn(), physical_column.type, physical_column.name});
    }
}

void KafkaSource::doCheckpoint(CheckpointContextPtr ckpt_ctx_)
{
    ckpt_ctx_->coordinator->checkpoint(getVersion(), getLogicID(), ckpt_ctx_, [&](WriteBuffer & wb) {
        writeStringBinary(topic, wb);
        writeIntBinary<Int32>(static_cast<Int32>(partition), wb);
        writeIntBinary<Int64>(lastProcessedSN(), wb);
    });

    LOG_INFO(logger, "Saved checkpoint topic={} partition={} offset={}", topic, partition, lastProcessedSN());
}

void KafkaSource::doRecover(CheckpointContextPtr ckpt_ctx_)
{
    ckpt_ctx_->coordinator->recover(getLogicID(), ckpt_ctx_, [&](VersionType, ReadBuffer & rb) {
        String recovered_topic;
        Int32 recovered_partition;
        readStringBinary(recovered_topic, rb);
        readIntBinary<Int32>(recovered_partition, rb);

        if (recovered_topic != topic || static_cast<size_t>(recovered_partition) != partition)
            throw Exception(
                ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                "Found mismatched kafka topic-partition. recovered={}-{}, current={}-{}",
                recovered_topic,
                recovered_partition,
                topic,
                partition);

        Int64 recovered_last_sn;
        readIntBinary<Int64>(recovered_last_sn, rb);
        setLastCheckpointSN(recovered_last_sn);
    });

    LOG_INFO(logger, "Recovered checkpoint topic={} partition={} last_sn={}", topic, partition, lastCheckpointSN());
}

void KafkaSource::doResetStartSN(Int64 sn)
{
    /// This reset function is only supposed to be called during MV recover phase, thus the source should not
    /// have started consuming any data yet.
    if (generate_inited.test())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected offset reset ({}), consumer had already started", sn);

    if (sn >= 0 && sn != offset)
    {
        offset = sn;
        reached_the_end = high_watermark == offset;
        LOG_INFO(logger, "Reset offset topic={} partition={} offset={} reached_the_end={}", topic, partition, offset, reached_the_end);
    }
}

/// \return [start_offset, end_offset] of the Kafka partition. The result may be stale
std::pair<Int64, Int64> KafkaSource::sequenceRange() const
{
    try
    {
        auto marks = consumer->getWatermarkOffsets(static_cast<int32_t>(partition));
        watermark_error_log_throttler->reset();
        return {marks.low, std::max(marks.low, marks.high - 1)};
    }
    catch (...)
    {
        watermark_error_log_throttler->execute([this](auto count) {
            tryLogCurrentException(
                logger,
                fmt::format(
                    "Failed to get sequence range from Kafka topic={} shard={} consective_count={}",
                    consumer->topicName(),
                    partition,
                    count));
        });

        return {-1, -1};
    }
}

Strings KafkaSource::doFetchData(const Streaming::SequenceRange & sn_range)
{
    /// At this point, the source is already cancelled, it's safe to use `consumer`.
    auto watermark = consumer->queryWatermarkOffsets(static_cast<int32_t>(partition), connection_timeout_ms);
    if (watermark.low < 0 || watermark.high < 0)
    {
        LOG_INFO(
            logger,
            "Fetching data of sn_range ({}, {}), but topic watermark ({}, {}) didn't match",
            sn_range.start,
            sn_range.end,
            watermark.low,
            watermark.high);
        return {};
    }

    auto start = sn_range.start;
    auto end = sn_range.end;

    if (end < watermark.low)
    {
        /// All data are gone, show something to the users so that they won't be confused why messages are empty.
        return {fmt::format("Messages at offsets range ({}, {}) were gone", start, end)};
    }

    start = std::max(start, watermark.low); /// In case some data are already gone
    end = std::min(end, watermark.high); /// std::max is needed just in case high is negative
    auto count = end - start + 1;
    LOG_INFO(logger, "Fetching data sn_range=({}, {}) actual_range=({}, {}) count={}", sn_range.start, sn_range.end, start, end, count);

    if (count < 1)
        return {};

    Strings results;
    results.reserve(count);

    consumer->startConsume(static_cast<int32_t>(partition), start);
    SCOPE_EXIT_SAFE(consumer->stopConsume(static_cast<int32_t>(partition)));

    auto callback = [&count, &results](void * rkmessage, size_t total_count, void * /*data*/) {
        const auto * messages = static_cast<const rd_kafka_message_t **>(rkmessage);
        for (size_t i = 0; i < total_count && count > 0; ++i)
        {
            const auto * message = messages[i];
            results.emplace_back(static_cast<const char *>(message->payload), message->len);
            --count;
        }
    };

    auto error_callback = [this, start, end](rd_kafka_resp_err_t err, std::string_view errmsg) {
        throw Exception(
            ErrorCodes::CANNOT_RECEIVE_MESSAGE,
            "Failed to fetch messages from kafka topic={} partition={} sn_range=({}, {}) error_code={} error_code_msg='{}' error_msg='{}'",
            topic,
            partition,
            start,
            end,
            err,
            rd_kafka_err2str(err),
            errmsg);
    };

    while (count > 0)
    {
        auto pre_count = count;
        consumer->consumeBatch(
            static_cast<int32_t>(partition), static_cast<UInt32>(count), record_consume_timeout_ms, callback, error_callback);
        /// No progress was made, this is abnormal, because all messages should be in the topic
        if (count > 0 && count == pre_count)
            break;
    }

    return results;
}

void KafkaSource::finishParallelParsing() noexcept
{
    parallel_parsing_finished = true;

    /// Wake up all waiting threads by notifying all units
    for (auto & unit : processing_units)
        unit.cv.notify_all();

    if (reader_thread.joinable())
        reader_thread.join();

    if (parser_pool)
    {
        try
        {
            parser_pool->wait();
        }
        catch (...)
        {
            tryLogCurrentException(logger, "Error waiting for parser pool");
        }
    }
}

void KafkaSource::initParseUnits(size_t units_num)
{
    processing_units.resize(units_num);
    for (auto & unit : processing_units)
        std::tie(unit.format_executor, unit.format_batch_executor) = getInputFormatExecutor(data_format, format_settings);
}

Chunk KafkaSource::generateParallel()
{
    const auto unit_number = next_output_sequence % processing_units.size();
    auto & unit = processing_units[unit_number];
    Chunk result;
    {
        std::unique_lock<std::mutex> lock(unit.mutex);
        unit.cv.wait(lock, [&] { return unit.status == ParseUnitStatus::ReadyToOutput || parallel_parsing_finished; });

        if (parallel_parsing_finished)
            return {};

        result = outputParseUnit(unit);
        unit.status = ParseUnitStatus::ReadyToFill;
    }
    unit.cv.notify_all();

    ++next_output_sequence;
    return result;
}

void KafkaSource::readerThreadFunction(ThreadGroupPtr thread_group)
{
    if (thread_group)
        CurrentThread::attachToGroup(thread_group);

    SCOPE_EXIT_SAFE({
        if (thread_group)
            CurrentThread::detachFromGroupIfNotDetached();
    });

    setThreadName("KafkaReader");

    while (!parallel_parsing_finished)
    {
        std::vector<CopiedKafkaMessage> messages;

        const auto unit_index = reader_ticket_number % processing_units.size();
        ++reader_ticket_number;

        auto & unit = processing_units[unit_index];
        try
        {
            {
                std::unique_lock<std::mutex> lock(unit.mutex);

                /// Wait for unit to be ready to fill
                unit.cv.wait(lock, [&] { return unit.status == ParseUnitStatus::ReadyToFill || parallel_parsing_finished; });

                if (parallel_parsing_finished)
                    break;

                unit.sn_range = {-1, -1};
                unit.last_processed_record_timestamp = -1;
                unit.read_bytes = 0;

                auto callback = [this, &messages, &unit](void * rkmessage, size_t total_count, void * /*data*/) {
                    const auto ** msgs = static_cast<const rd_kafka_message_t **>(rkmessage);
                    size_t start_message = 0;
                    while (start_message < total_count && msgs[start_message]->offset < offset)
                        ++start_message;

                    if (start_message > 0)
                        LOG_INFO(
                            logger, "Messages skipped: start_offset={} end_offset={}", msgs[0]->offset, msgs[start_message - 1]->offset);

                    if (start_message >= total_count)
                        return;

                    /// Record message offset range and last timestamp
                    unit.sn_range = {msgs[start_message]->offset, msgs[total_count - 1]->offset};
                    rd_kafka_timestamp_type_t ts_type;
                    if (auto ts = rd_kafka_message_timestamp(msgs[total_count - 1], &ts_type); ts > 0)
                        unit.last_processed_record_timestamp = ts;

                    /// Copy messages
                    messages.reserve(total_count - start_message);
                    for (size_t i = start_message; i < total_count; ++i)
                    {
                        const auto * msg = msgs[i];
                        auto & copied = messages.emplace_back();
                        if (msg->payload && msg->len > 0)
                        {
                            copied.payload.assign(
                                static_cast<const char *>(msg->payload), static_cast<const char *>(msg->payload) + msg->len);
                            unit.read_bytes += msg->len;
                        }

                        if (msg->key && msg->key_len > 0)
                            copied.key.assign(static_cast<const char *>(msg->key), static_cast<const char *>(msg->key) + msg->key_len);

                        copied.offset = msg->offset;
                    }
                };

                auto error_callback = [this](rd_kafka_resp_err_t err, std::string_view errmsg) {
                    throw Exception(
                        ErrorCodes::CANNOT_RECEIVE_MESSAGE,
                        "Failed to consume message from kafka topic={} partition={} error_code={} error_code_msg='{}' error_msg='{}'",
                        topic,
                        partition,
                        err,
                        rd_kafka_err2str(err),
                        errmsg);
                };

                consumer->consumeBatch(
                    static_cast<int32_t>(partition), record_consume_batch_count, record_consume_timeout_ms, callback, error_callback);

                if (messages.empty())
                {
                    /// Output heartbeat chunk
                    unit.chunk = header_chunk.clone();
                    unit.status = ParseUnitStatus::ReadyToOutput;

                    /// Notify to output heartbeat Chunk and do stall detection when no new messages
                    lock.unlock();
                    unit.cv.notify_all();
                    stall_detector.checkAndHandleStall();
                }
                else
                {
                    unit.status = ParseUnitStatus::ReadyToParse;

                    lock.unlock();
                    parser_pool->scheduleOrThrowOnError(
                        [this, unit_index, group = CurrentThread::getGroup(), msgs = std::move(messages)]() {
                            parserThreadFunction(group, unit_index, msgs);
                        });
                }
            }  /// parse unit lock
        }
        catch (...)
        {
            {
                std::lock_guard lock{unit.mutex};
                unit.chunk = {};
                unit.exception = std::current_exception();
                unit.status = ParseUnitStatus::ReadyToOutput;
            }
            unit.cv.notify_all();

            /// Quit reader thread
            return;
        }
    }
}

void KafkaSource::parserThreadFunction(ThreadGroupPtr thread_group, size_t unit_index, const std::vector<CopiedKafkaMessage> & messages)
{
    setThreadName("KafkaParser");

    if (thread_group)
        CurrentThread::attachToGroupIfDetached(thread_group);

    SCOPE_EXIT_SAFE({
        if (thread_group)
            CurrentThread::detachFromGroupIfNotDetached();
    });

    auto & unit = processing_units[unit_index];

    {
        std::lock_guard<std::mutex> lock(unit.mutex);

        /// Try batch parse first. unit.status is set to READY_TO_OUTOUT when batch parse succeed.
        if (unit.format_batch_executor)
        {
            try
            {
                /// Build message refs for batch parsing
                std::vector<StringRef> message_refs;
                message_refs.reserve(messages.size());
                for (const auto & msg : messages)
                    message_refs.emplace_back(msg.payload.data(), msg.payload.size());

                auto new_rows = parseMessagesInBatch(unit.format_batch_executor, message_refs, unit.batch_buffer);

                if (request_virtual_columns)
                {
                    if (new_rows == messages.size())
                    {
                        auto virtual_columns = generateEmptyVirtualColumns();
                        for (const auto & message : messages)
                        {
                            auto tmp_msg = createKafkaMessage(message, partition);
                            appendVirtualColumnsRow(virtual_columns, &tmp_msg, 1);
                        }
                        unit.chunk = generateOutputChunk(unit.format_batch_executor->getResultColumns(), std::move(virtual_columns));
                        unit.status = ParseUnitStatus::ReadyToOutput;
                    }
                    else
                    {
                        LOG_WARNING(
                            logger,
                            "Batch parsing result row count is different than message count while virtual columns are required. Disabled the batch parsing.");
                        unit.format_batch_executor = nullptr;
                    }
                }
                else
                {
                    unit.chunk = Chunk{unit.format_batch_executor->getResultColumns(), new_rows};
                    unit.status = ParseUnitStatus::ReadyToOutput;
                }
            }
            catch (...)
            {
                /// Fall back to per-message parsing on exception
                if (logger->debug())
                {
                    tryLogCurrentException(
                        logger,
                        fmt::format(
                            "Kafka batch parse failed: topic={} partition={} offset=[{}, {}]",
                            topic,
                            partition,
                            unit.sn_range.start,
                            unit.sn_range.end));
                }
            }
        }  /// batch parse

        if (unit.status != ParseUnitStatus::ReadyToOutput)
        {
            /// Per-message parsing path

            /// Reset any accumulated result columns from batch parsing
            if (unit.format_batch_executor)
                unit.format_batch_executor->getResultColumns();

            try
            {
                auto virtual_columns = generateEmptyVirtualColumns();
                for (const auto & message : messages)
                {
                    try
                    {
                        auto tmp_kafka_msg = createKafkaMessage(message, partition);
                        parseMessage(unit.format_executor, &tmp_kafka_msg, virtual_columns);
                    }
                    catch (Exception & ex)
                    {
                        onFormatError(message.offset, ex);
                    }
                }
                unit.chunk = generateOutputChunk(unit.format_executor->getResultColumns(), std::move(virtual_columns));
                unit.status = ParseUnitStatus::ReadyToOutput;
            }
            catch (...)
            {
                unit.exception = std::current_exception();
                unit.status = ParseUnitStatus::ReadyToOutput;
            }
        }  /// Per-message parsing ends
    }  /// parse unit lock

    unit.cv.notify_all();
}

MutableColumns KafkaSource::generateEmptyVirtualColumns() const
{
    MutableColumns virtual_columns;
    if (request_virtual_columns)
    {
        /// Init virtual columns for parseMessage
        virtual_columns.resize(header.columns());
        for (size_t i = 0; i < virtual_col_types.size(); ++i)
        {
            if (virtual_col_value_functions[i])
                virtual_columns[i] = virtual_col_types[i]->createColumn();
        }
    }
    return virtual_columns;
}

void KafkaSource::appendVirtualColumnsRow(MutableColumns & virtual_columns, const rd_kafka_message_t * message, size_t rows) const
{
    for (size_t j = 0, n = virtual_col_value_functions.size(); j < n; ++j)
    {
        if (virtual_col_value_functions[j])
        {
            if (likely(rows == 1))
                virtual_columns[j]->insert(virtual_col_value_functions[j](message));
            else
                virtual_columns[j]->insertMany(virtual_col_value_functions[j](message), rows);
        }
    }
}

Chunk KafkaSource::generateOutputChunk(MutableColumns physical_columns, MutableColumns virtual_columns)
{
    const size_t read_rows = physical_columns.empty() ? 0 : physical_columns[0]->size();
    if (read_rows > 0)
    {
        if (!request_virtual_columns)
            return Chunk{std::move(physical_columns), read_rows};

        /// Combine physical and virtual columns
        for (size_t i = 0, j = 0; i < virtual_col_value_functions.size(); ++i)
        {
            if (!virtual_col_value_functions[i])
                virtual_columns[i] = std::move(physical_columns[j++]);
        }
        return Chunk{std::move(virtual_columns), read_rows};
    }

    /// Create empty chunk as heartbeat
    return header_chunk.clone();
}

Chunk KafkaSource::outputParseUnit(ParseUnit & unit)
{
    if (unit.exception)
        std::rethrow_exception(unit.exception);

    if (unit.sn_range.start >= 0)
        setLastProcessedSNRange(unit.sn_range);

    if (unit.last_processed_record_timestamp > 0)
        setLastProcessedRecordTimestamp(unit.last_processed_record_timestamp);

    /// All available messages up to the moment when the query was executed have been consumed, no need to read the messages beyond that point.
    /// `high_watermark` is the next available offset, i.e. the offset that will be assigned to the next message, thus need to use `high_watermark - 1`.
    if (unit.sn_range.end >= high_watermark - 1)
        reached_the_end = true;

    external_stream_counter->addReadBytes(unit.read_bytes);
    external_stream_counter->addReadRows(unit.chunk.rows());

    return std::move(unit.chunk);
}

void KafkaSource::onFormatError(int64_t message_offset, Exception & ex)
{
    external_stream_counter->addReadFailed(1);
    if (ignore_format_errors)
    {
        LOG_ERROR(
            logger, "Failed to parse message topic={} partition={} offset={} error={}", topic, partition, message_offset, ex.message());
    }
    else
    {
        ex.addMessage("Failed to parse message topic={} partition={} offset={}", topic, partition, message_offset);
        throw;
    }
}

bool KafkaSource::tryBatchParseAndGenerate(const rd_kafka_message_t ** messages, size_t begin, size_t end, ParseUnit & unit)
try
{
    auto message_count = end - begin;
    std::vector<StringRef> message_refs;
    message_refs.reserve(message_count);
    for (size_t i = begin; i < end; ++i)
        message_refs.emplace_back(static_cast<const char *>(messages[i]->payload), messages[i]->len);

    auto new_rows = parseMessagesInBatch(unit.format_batch_executor, message_refs, unit.batch_buffer);
    if (!request_virtual_columns)
    {
        unit.chunk = Chunk{unit.format_batch_executor->getResultColumns(), new_rows};
        return true;
    }

    if (new_rows == message_count)
    {
        auto virtual_columns = generateEmptyVirtualColumns();
        for (size_t i = begin; i < end; ++i)
            appendVirtualColumnsRow(virtual_columns, messages[i], 1);
        unit.chunk = generateOutputChunk(unit.format_batch_executor->getResultColumns(), std::move(virtual_columns));
        return true;
    }

    /// Row count mismatch - can not create virtual columns
    LOG_WARNING(
        logger,
        "Batch parsing result row count is different than message count while virtual columns are required. Disabled the batch parsing.");

    /// Reset format executor
    unit.format_batch_executor->getResultColumns();
    unit.format_batch_executor = nullptr;
    return false;
}
catch (Exception & e)
{
    LOG_WARNING(logger, "Batch parse failed: topic={} partition={} error={}", topic, partition, e.displayText());
    unit.format_batch_executor->getResultColumns();
    return false;
}

void KafkaSource::parseAndGenerate(const rd_kafka_message_t ** messages, size_t begin, size_t end, ParseUnit & unit)
{
    auto virtual_columns = generateEmptyVirtualColumns();
    for (size_t i = begin; i < end; ++i)
    {
        const auto * message = messages[i];
        try
        {
            parseMessage(unit.format_executor, message, virtual_columns);
        }
        catch (Exception & ex)
        {
            onFormatError(message->offset, ex);
        }
    }
    unit.chunk = generateOutputChunk(unit.format_executor->getResultColumns(), std::move(virtual_columns));
}

}
}
