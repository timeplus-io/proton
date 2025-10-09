#include <Storages/ExternalStream/Kafka/KafkaSink.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnDecimal.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/Kafka/mapErrorCode.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/createBlockSelector.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Formats/IRowOutputFormat.h>
#include <Storages/ExternalStream/Kafka/Kafka.h>
#include <Common/ProtonCommon.h>

#include <boost/algorithm/string/predicate.hpp>

#include <rdkafka.h>

#include <numeric>
#include <unordered_map>


namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_WRITE_TO_KAFKA;
extern const int INVALID_SETTING_VALUE;
extern const int KAFKA_PRODUCER_STOPPED;
extern const int TIMEOUT_EXCEEDED;
extern const int TYPE_MISMATCH;
}

namespace
{
ExpressionActionsPtr buildExpression(const Block & header, const ASTPtr & expr_ast, const ContextPtr & context)
{
    assert(expr_ast);

    auto syntax_result = TreeRewriter(context).analyze(const_cast<ASTPtr &>(expr_ast), header.getNamesAndTypesList());
    return ExpressionAnalyzer(expr_ast, syntax_result, context).getActions(false);
}

template <typename T>
StringRef getKeyBigEndian(const IColumn & column, size_t row, String & key_holder)
{
    const auto value = assert_cast<const T &>(column).getElement(row);
    WriteBufferFromString wb(key_holder);
    writeBinaryBigEndian(value, wb);
    wb.finalize();
    return StringRef{key_holder.data(), key_holder.size()};
}

StringRef getKey(const IColumn & column, size_t row, String & key_holder)
{
    if (column.isNullable())
    {
        const auto & col = assert_cast<const ColumnNullable &>(column);
        if (col.isNullAt(row))
            return "";
        return getKey(col.getNestedColumn(), row, key_holder);
    }

    switch (column.getDataType())
    {
        case TypeIndex::Bool:
            return getKeyBigEndian<ColumnUInt8>(column, row, key_holder);
        case TypeIndex::UInt8:
            return getKeyBigEndian<ColumnUInt8>(column, row, key_holder);
        case TypeIndex::UInt16:
            return getKeyBigEndian<ColumnUInt16>(column, row, key_holder);
        case TypeIndex::UInt32:
            return getKeyBigEndian<ColumnUInt32>(column, row, key_holder);
        case TypeIndex::UInt64:
            return getKeyBigEndian<ColumnUInt64>(column, row, key_holder);
        case TypeIndex::Int8:
            return getKeyBigEndian<ColumnInt8>(column, row, key_holder);
        case TypeIndex::Int16:
            return getKeyBigEndian<ColumnInt16>(column, row, key_holder);
        case TypeIndex::Int32:
            return getKeyBigEndian<ColumnInt32>(column, row, key_holder);
        case TypeIndex::Int64:
            return getKeyBigEndian<ColumnInt64>(column, row, key_holder);
        case TypeIndex::Float32:
            return getKeyBigEndian<ColumnFloat32>(column, row, key_holder);
        case TypeIndex::Float64:
            return getKeyBigEndian<ColumnFloat64>(column, row, key_holder);
        /// Other types are written as in memory order.
        case TypeIndex::String:
            [[fallthrough]];
        case TypeIndex::FixedString:
            [[fallthrough]];
        default:
            return column.getDataAt(row);
    }
}

std::unordered_map<String, String> getHeaders(const IColumn & column, size_t row)
{
    const auto & column_map = assert_cast<const ColumnMap &>(column);
    const auto & column_array = column_map.getNestedColumn();
    const auto & offsets = column_array.getOffsets();
    size_t offset = offsets[row - 1];
    size_t next_offset = offsets[row];
    size_t row_count = next_offset - offset;
    const auto & nested_columns = column_map.getNestedData();
    const auto & keys_column = nested_columns.getColumn(0);
    const auto & values_column = nested_columns.getColumn(1);

    std::unordered_map<String, String> headers;
    headers.reserve(row_count);
    for (size_t i = offset; i < next_offset; ++i)
        headers.emplace(keys_column.getDataAt(i), values_column.getDataAt(i));
    return headers;
}

}

namespace ExternalStream
{
using DB::Kafka::ProducerPtr;


ChunkSharder::ChunkSharder(ExpressionActionsPtr sharding_expr_, const String & column_name)
    : sharding_expr(sharding_expr_), sharding_key_column_name(column_name)
{
}

ChunkSharder::ChunkSharder()
{
    random_sharding = true;
}

BlocksWithShard ChunkSharder::shard(Block block, UInt32 shard_cnt)
{
    /// no topics have zero partitions
    assert(shard_cnt > 0);

    if (shard_cnt == 1)
        return {BlockWithShard{Block(std::move(block)), 0}};

    if (random_sharding)
        return {BlockWithShard{Block(std::move(block)), getNextShardIndex(shard_cnt)}};

    return doSharding(std::move(block), shard_cnt);
}

BlocksWithShard ChunkSharder::doSharding(Block block, UInt32 shard_cnt) const
{
    auto selector = createSelector(block, shard_cnt);

    Blocks partitioned_blocks{static_cast<size_t>(shard_cnt)};

    for (UInt32 i = 0; i < shard_cnt; ++i)
        partitioned_blocks[i] = block.cloneEmpty();

    for (size_t pos = 0; pos < block.columns(); ++pos)
    {
        MutableColumns partitioned_columns = block.getByPosition(pos).column->scatter(shard_cnt, selector);
        for (UInt32 i = 0; i < shard_cnt; ++i)
            partitioned_blocks[i].getByPosition(pos).column = std::move(partitioned_columns[i]);
    }

    BlocksWithShard blocks_with_shard;
    blocks_with_shard.reserve(partitioned_blocks.size());

    /// Filter out empty blocks
    for (size_t i = 0; i < partitioned_blocks.size(); ++i)
    {
        if (partitioned_blocks[i].rows() > 0)
            blocks_with_shard.emplace_back(std::move(partitioned_blocks[i]), i);
    }

    return blocks_with_shard;
}

IColumn::Selector ChunkSharder::createSelector(Block block, UInt32 shard_cnt) const
{
    std::vector<UInt64> slot_to_shard(shard_cnt);
    std::iota(slot_to_shard.begin(), slot_to_shard.end(), 0);

    sharding_expr->execute(block);

    const auto & key_column = block.getByName(sharding_key_column_name);

/// If key_column.type is DataTypeLowCardinality, do shard according to its dictionaryType
#define CREATE_FOR_TYPE(TYPE) \
    if (typeid_cast<const DataType##TYPE *>(key_column.type.get())) \
        return createBlockSelector<TYPE>(*key_column.column, slot_to_shard); \
    else if (auto * type_low_cardinality = typeid_cast<const DataTypeLowCardinality *>(key_column.type.get())) \
        if (typeid_cast<const DataType##TYPE *>(type_low_cardinality->getDictionaryType().get())) \
            return createBlockSelector<TYPE>(*key_column.column->convertToFullColumnIfLowCardinality(), slot_to_shard);

    CREATE_FOR_TYPE(UInt8)
    CREATE_FOR_TYPE(UInt16)
    CREATE_FOR_TYPE(UInt32)
    CREATE_FOR_TYPE(UInt64)
    CREATE_FOR_TYPE(Int8)
    CREATE_FOR_TYPE(Int16)
    CREATE_FOR_TYPE(Int32)
    CREATE_FOR_TYPE(Int64)

#undef CREATE_FOR_TYPE

    throw Exception{ErrorCodes::TYPE_MISMATCH, "Sharding key expression does not evaluate to an integer type"};
}

KafkaSink::KafkaSink(
    Kafka & kafka,
    const Block & header,
    ProducerPtr producer_,
    UInt64 connection_timeout_ms_,
    bool refresh_topic_partitions,
    ExternalStreamCounterPtr external_stream_counter_,
    LoggerPtr logger_,
    ContextPtr context)
    : SinkToStorage(header, ProcessorID::ExternalTableDataSinkID)
    , producer(std::move(producer_))
    , connection_timeout_ms(connection_timeout_ms_)
    , checkpoint_timeout_ms(context->getSettingsRef().insert_timeout_ms)
    , partition_cnt(producer->getPartitionCount(connection_timeout_ms))
    , one_message_per_row(kafka.produceOneMessagePerRow())
    , topic_refresh_interval_ms(kafka.topicRefreshIntervalMs())
    , external_stream_counter(std::move(external_stream_counter_))
    , logger(std::move(logger_))
{
    const auto & data_format = kafka.dataFormat();
    assert(!data_format.empty());

    Block output_header = header;

    /// `_tp_message_key` should not be part of the message payload.
    if (auto pos = output_header.tryGetPositionByName(ProtonConsts::RESERVED_MESSAGE_KEY); pos)
    {
        msg_key_column_pos = pos;
        output_header.erase(*pos);
    }

    /// Do not put `_tp_time` in the message payload.
    if (auto pos = output_header.tryGetPositionByName(ProtonConsts::RESERVED_EVENT_TIME); pos)
    {
        ts_column_pos = pos;
        output_header.erase(*pos);
    }

    /// Do not put `_tp_message_headers` in the message payload.
    if (auto pos = output_header.tryGetPositionByName(ProtonConsts::RESERVED_MESSAGE_HEADERS); pos)
    {
        headers_column_pos = pos;
        output_header.erase(*pos);
    }

    auto max_rows_per_message = context->getSettingsRef().max_insert_block_size.value;
    if (one_message_per_row)
        max_rows_per_message = 1;

    format_executor = std::make_unique<MessageQueueFormatExecutor>(
        output_header,
        data_format,
        kafka.getFormatSettings(context),
        max_rows_per_message,
        context->getSettingsRef().kafka_max_message_size,
        context);
    format_executor->start();

    if (kafka.hasCustomShardingExpr())
    {
        const auto & ast = kafka.shardingExprAst();
        partitioner = std::make_unique<ChunkSharder>(buildExpression(header, ast, context), ast->getColumnName());
    }
    else
        partitioner = std::make_unique<ChunkSharder>();

    if (refresh_topic_partitions)
    {
        background_jobs.emplace(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, 1);
        /// Fetch partition count regularly, so that it can send data to new partitions.
        background_jobs->scheduleOrThrowOnError([this, refresh_interval_ms = static_cast<UInt64>(topic_refresh_interval_ms)]() {
            LOG_INFO(logger, "Start topic partition count refreshing job");
            auto metadata_refresh_stopwatch = Stopwatch();
            /// Use a small sleep interval to avoid blocking operation for a long just (in case refresh_interval_ms is big).
            auto sleep_ms = std::min(UInt64(500), refresh_interval_ms);
            while (true)
            {
                if (is_finished.test())
                    break;

                std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
                /// Fetch topic metadata for partition updates
                if (metadata_refresh_stopwatch.elapsedMilliseconds() < refresh_interval_ms)
                    continue;

                metadata_refresh_stopwatch.restart();

                try
                {
                    partition_cnt = producer->getPartitionCount(connection_timeout_ms);
                }
                catch (...) /// do not break the loop until finished
                {
                    LOG_WARNING(logger, "Failed to describe topic, error code: {}", getCurrentExceptionMessage(true, true));
                }
            }
            LOG_INFO(logger, "Stopped topic partition count refreshing job");
        });
    }
}

void KafkaSink::sendMessage(const String & message, ColumnPtr ts_column, ColumnPtr headers_column, ColumnPtr key_column)
{
    Int64 ts{0};
    if (ts_column)
        ts = ts_column->getInt(current_batch_row);
    else
        ts = UTCMilliseconds::now();

    String key_holder;
    auto key = key_column ? getKey(*key_column, current_batch_row, key_holder) : StringRef{};
    auto headers = headers_column ? getHeaders(*headers_column, current_batch_row) : std::unordered_map<String, String>();

    auto * msg_headers = rd_kafka_headers_new(headers.size());
    for (const auto & [k, v] : headers)
        rd_kafka_header_add(msg_headers, k.data(), k.size(), v.data(), v.size());

    ++current_batch_row;

    constexpr size_t vu_size = 8;
    rd_kafka_vu_t vus[vu_size] = {
        {.vtype = RD_KAFKA_VTYPE_RKT, .u = {.rkt = producer->getTopicHandle()}},
        {.vtype = RD_KAFKA_VTYPE_PARTITION, .u = {.i32 = next_partition}},
        {.vtype = RD_KAFKA_VTYPE_MSGFLAGS, .u = {.i = RD_KAFKA_MSG_F_COPY | RD_KAFKA_MSG_F_BLOCK}},
        {.vtype = RD_KAFKA_VTYPE_VALUE, .u = {.mem = {.ptr = const_cast<char *>(message.data()), .size = message.size()}}},
        {.vtype = RD_KAFKA_VTYPE_KEY, .u = {.mem = {.ptr = const_cast<char *>(key.data), .size = key.size}}},
        {.vtype = RD_KAFKA_VTYPE_TIMESTAMP, .u = {.i64 = ts}},
        {.vtype = RD_KAFKA_VTYPE_HEADERS, .u = {.headers = msg_headers}},
        {.vtype = RD_KAFKA_VTYPE_OPAQUE, .u = {.ptr = this}},
    };

    auto * err = rd_kafka_produceva(producer->getHandle(), vus, vu_size);

    if (err != nullptr)
    {
        external_stream_counter->addWrittenFailed(1);

        const auto msg = fmt::format(
            "error_code={} error_name={} error={}", rd_kafka_error_code(err), rd_kafka_error_name(err), rd_kafka_error_string(err));

        rd_kafka_error_destroy(err);
        /// if rd_kafka_produceva fails, we need to free msg_headers manually
        rd_kafka_headers_destroy(msg_headers);

        throw Exception(ErrorCodes::CANNOT_WRITE_TO_KAFKA, "{}", msg);
    }

    ++state.outstandings;
    external_stream_counter->addWrittenBytes(message.size());
    external_stream_counter->addWrittenRows(1);
}

void KafkaSink::consume(Chunk chunk)
{
    if (!chunk.hasRows())
        return;

    if (unlikely(is_finished.test()))
        throw Exception(
            ErrorCodes::KAFKA_PRODUCER_STOPPED,
            "KafkaSink cannot consume data because producer has stopped, likely the underlying external stream is gone");

    auto block = getHeader().cloneWithColumns(chunk.detachColumns());

    current_batch_row = 0;

    if (msg_key_column_pos) /// using message key column
    {
        next_partition = RD_KAFKA_PARTITION_UA;

        /// When message key is used, partitioning is based on message key.
        BlocksWithShard blocks{{std::move(block), 0}};

        for (auto & block_with_shard : blocks)
        {
            /// Order is important here, since in constructor we initialize msg_key_column_pos before ts_column_pos,
            /// here it should follow the exact order to make sure the pos'es are correctly used
            ColumnPtr key_column = block_with_shard.block.getByPosition(*msg_key_column_pos).column;
            block_with_shard.block.erase(*msg_key_column_pos);

            ColumnPtr ts_column{nullptr};
            if (ts_column_pos)
            {
                ts_column.swap(block_with_shard.block.getByPosition(*ts_column_pos).column);
                block_with_shard.block.erase(*ts_column_pos);
            }

            ColumnPtr headers_column{nullptr};
            if (headers_column_pos)
            {
                headers_column.swap(block_with_shard.block.getByPosition(*headers_column_pos).column);
                block_with_shard.block.erase(*headers_column_pos);
            }

            format_executor->execute(
                block_with_shard.block,
                [this, &ts_column, &key_column, &headers_column](const String & message, size_t) { sendMessage(message, ts_column, headers_column, key_column); },
                /*flush_at_the_end=*/true);
        }
    }
    else /// message key column is not used
    {
        auto blocks = partitioner->shard(std::move(block), partition_cnt);

        for (auto & block_with_shard : blocks)
        {
            next_partition = block_with_shard.shard;

            ColumnPtr ts_column{nullptr};
            if (ts_column_pos)
            {
                ts_column.swap(block_with_shard.block.getByPosition(*ts_column_pos).column);
                block_with_shard.block.erase(*ts_column_pos);
            }

            ColumnPtr headers_column{nullptr};
            if (headers_column_pos)
            {
                headers_column.swap(block_with_shard.block.getByPosition(*headers_column_pos).column);
                block_with_shard.block.erase(*headers_column_pos);
            }

            format_executor->execute(
                block_with_shard.block,
                [this, &ts_column, &headers_column](const String & message, size_t) { sendMessage(message, ts_column, headers_column, nullptr); },
                /*flush_at_the_end=*/true);
        }
    }
}

void KafkaSink::onFinish()
{
    if (is_finished.test_and_set())
        return;

    LOG_INFO(logger, "Stopping producing messages");

    producer->stop();

    if (background_jobs)
        background_jobs->wait();

    format_executor->finish();

    /// if there are no outstandings, no need to do flushing
    if (outstandingMessages() == 0)
        return;

    /// Make sure all outstanding requests are transmitted and handled.
    /// It should not block for ever here, otherwise, it will block proton from stopping the job
    /// or block proton from terminating.
    if (auto err = rd_kafka_flush(producer->getHandle(), 15000 /* time_ms */); err)
        LOG_ERROR(logger, "Failed to flush kafka producer, error={}", rd_kafka_err2str(err));

    if (auto err = lastSeenError(); err != RD_KAFKA_RESP_ERR_NO_ERROR)
        LOG_ERROR(logger, "Failed to send messages, last_seen_error={}", rd_kafka_err2str(err));

    /// if flush does not return an error, the delivery report queue should be empty
    if (outstandingMessages() > 0)
        LOG_ERROR(logger, "Not all messsages are sent successfully, expected={} actual={}", outstandings(), acked());
}

void KafkaSink::onMessageDelivery(rd_kafka_t * /* producer */, const rd_kafka_message_t * msg, void * /*opaque*/)
{
    auto * sink = static_cast<KafkaSink *>(msg->_private);
    sink->onMessageDelivery(msg->err);
}

void KafkaSink::onMessageDelivery(rd_kafka_resp_err_t err)
{
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
    {
        state.last_error_code.store(err);
        ++state.error_count;
    }
    else
        ++state.acked;
}

KafkaSink::~KafkaSink()
{
    onFinish();
}

void KafkaSink::checkpoint(CheckpointContextPtr context)
{
    Stopwatch timer;
    do
    {
        if (auto err = lastSeenError(); err != RD_KAFKA_RESP_ERR_NO_ERROR)
            throw Exception(
                DB::Kafka::mapErrorCode(err), "Failed to send messages, error_count={} last_error={}", errorCount(), rd_kafka_err2str(err));

        auto outstanding_msgs = outstandingMessages();
        if (outstanding_msgs == 0)
            break;

        if (timer.elapsedMilliseconds() >= checkpoint_timeout_ms)
            throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Checkpoint timed out, outstandings={}", outstanding_msgs);

        LOG_INFO(logger, "Waiting for {} outstandings on checkpointing", outstanding_msgs);

        if (is_finished.test())
        {
            /// for a final check, it should not wait for too long
            if (auto err = rd_kafka_flush(producer->getHandle(), 15000 /* time_ms */); err)
                throw Exception(DB::Kafka::mapErrorCode(err), "Failed to flush kafka producer, error={}", rd_kafka_err2str(err));

            if (auto err = lastSeenError(); err != RD_KAFKA_RESP_ERR_NO_ERROR)
                throw Exception(
                    DB::Kafka::mapErrorCode(err),
                    "Failed to send messages, error_count={} last_error={}",
                    errorCount(),
                    rd_kafka_err2str(err));

            if (outstandingMessages() > 0)
                throw Exception(
                    ErrorCodes::CANNOT_WRITE_TO_KAFKA,
                    "Not all messsages are sent successfully, expected={} actual={}",
                    outstandings(),
                    acked());

            break;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    } while (true);

    state.reset();
    IProcessor::checkpoint(context);
}

void KafkaSink::State::reset()
{
    outstandings.store(0);
    acked.store(0);
    error_count.store(0);
    last_error_code.store(0);
}

}
}
