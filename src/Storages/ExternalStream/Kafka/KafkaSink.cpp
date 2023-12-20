#include <Common/logger_useful.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/createBlockSelector.h>
#include <Parsers/ASTFunction.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Formats/IRowOutputFormat.h>
#include <Storages/ExternalStream/Kafka/Kafka.h>
#include <Storages/ExternalStream/Kafka/KafkaSink.h>

#include <boost/algorithm/string/predicate.hpp>

namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_WRITE_TO_KAFKA;
extern const int MISSING_ACKNOWLEDGEMENT;
extern const int INVALID_CONFIG_PARAMETER;
extern const int TYPE_MISMATCH;
extern const int INVALID_SETTING_VALUE;
}

namespace
{
ExpressionActionsPtr buildExpression(ContextPtr context, const Block & header, const ASTPtr & expr_ast)
{
    assert(expr_ast);

    ASTPtr query = expr_ast;
    auto syntax_result = TreeRewriter(context).analyze(query, header.getNamesAndTypesList());
    return ExpressionAnalyzer(query, syntax_result, context).getActions(false);
}
}

namespace KafkaStream
{
ChunkSharder::ChunkSharder(ContextPtr context, const Block & header, const ASTPtr & sharding_expr_ast)
{
    sharding_expr = buildExpression(context, header, sharding_expr_ast);

    sharding_key_column_name = sharding_expr_ast->getColumnName();

    if (auto * shard_func = sharding_expr_ast->as<ASTFunction>())
    {
        if (shard_func->name == "rand" || shard_func->name == "RAND")
            random_sharding = true;
    }
}

ChunkSharder::ChunkSharder()
{
    random_sharding = true;
}

BlocksWithShard ChunkSharder::shard(Block block, Int32 shard_cnt) const
{
    /// no topics have zero partitions
    assert(shard_cnt > 0);

    if (shard_cnt == 1)
        return {BlockWithShard{Block(std::move(block)), 0}};

    if (random_sharding)
        return {BlockWithShard{Block(std::move(block)), getNextShardIndex(shard_cnt)}};

    return doSharding(std::move(block), shard_cnt);
}

BlocksWithShard ChunkSharder::doSharding(Block block, Int32 shard_cnt) const
{
    auto selector = createSelector(block, shard_cnt);

    Blocks partitioned_blocks{static_cast<size_t>(shard_cnt)};

    for (Int32 i = 0; i < shard_cnt; ++i)
        partitioned_blocks[i] = block.cloneEmpty();

    for (size_t pos = 0; pos < block.columns(); ++pos)
    {
        MutableColumns partitioned_columns = block.getByPosition(pos).column->scatter(shard_cnt, selector);
        for (Int32 i = 0; i < shard_cnt; ++i)
            partitioned_blocks[i].getByPosition(pos).column = std::move(partitioned_columns[i]);
    }

    BlocksWithShard blocks_with_shard;
    blocks_with_shard.reserve(partitioned_blocks.size());

    /// Filter out empty blocks
    for (size_t i = 0; i < partitioned_blocks.size(); ++i)
    {
        if (partitioned_blocks[i].rows())
            blocks_with_shard.emplace_back(std::move(partitioned_blocks[i]), i);
    }

    return blocks_with_shard;
}

IColumn::Selector ChunkSharder::createSelector(Block block, Int32 shard_cnt) const
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

    throw Exception{"Sharding key expression does not evaluate to an integer type", ErrorCodes::TYPE_MISMATCH};
}
}

KafkaSink::KafkaSink(const Kafka * kafka, const Block & header, Int32 initial_partition_cnt, const ASTPtr & message_key_ast, ContextPtr context, Poco::Logger * log_)
    : SinkToStorage(header, ProcessorID::ExternalTableDataSinkID)
    , partition_cnt(initial_partition_cnt)
    , one_message_per_row(kafka->produceOneMessagePerRow())
    , log(log_)
{
    /// default values
    std::vector<std::pair<String, String>> producer_params{
        {"enable.idempotence", "true"},
        {"message.timeout.ms", "0" /* infinite */},
    };

    static const std::unordered_set<String> allowed_properties{
        "enable.idempotence",
        "message.timeout.ms",
        "queue.buffering.max.messages",
        "queue.buffering.max.kbytes",
        "queue.buffering.max.ms",
        "message.max.bytes",
        "message.send.max.retries",
        "retries",
        "retry.backoff.ms",
        "retry.backoff.max.ms",
        "batch.num.messages",
        "batch.size",
        "compression.codec",
        "compression.type",
        "compression.level",
        "topic.metadata.refresh.interval.ms",
    };

    /// customization, overrides default values
    for (const auto & pair : kafka->properties())
    {
        if (allowed_properties.contains(pair.first))
        {
            producer_params.emplace_back(pair.first, pair.second);
            continue;
        }
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Unsupported property {}", pair.first);
    }

    /// properies from settings have higher priority
    producer_params.emplace_back("bootstrap.servers", kafka->brokers());
    kafka->auth().populateConfigs(producer_params);

    auto * conf = rd_kafka_conf_new();
    char errstr[512]{'\0'};
    for (const auto & param : producer_params)
    {
        auto ret = rd_kafka_conf_set(conf, param.first.c_str(), param.second.c_str(), errstr, sizeof(errstr));
        if (ret != RD_KAFKA_CONF_OK)
        {
            rd_kafka_conf_destroy(conf);
            throw Exception(
                ErrorCodes::INVALID_CONFIG_PARAMETER,
                "Failed to set kafka config `{}` with value `{}` error={}",
                param.first,
                param.second,
                ret);
        }
    }

    rd_kafka_conf_set_opaque(conf, this); /* needed by onMessageDelivery */
    rd_kafka_conf_set_dr_msg_cb(conf, &KafkaSink::onMessageDelivery);

    size_t value_size = 8;
    char topic_refresh_interval_ms_value[8]{'\0'}; /* max: 3600000 */
    rd_kafka_conf_get(conf, "topic.metadata.refresh.interval.ms", topic_refresh_interval_ms_value, &value_size);
    Int32 topic_refresh_interval_ms {std::stoi(topic_refresh_interval_ms_value)};

    producer.reset(rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr)));
    if (!producer)
    {
        // librdkafka will take the ownership of `conf` if `rd_kafka_new` succeeds,
        // but if it does not, we need to take care of cleaning it up by ourselves.
        rd_kafka_conf_destroy(conf);
        throw Exception("Failed to create kafka handle", klog::mapErrorCode(rd_kafka_last_error()));
    }

    topic.reset(rd_kafka_topic_new(producer.get(), kafka->topic().c_str(), nullptr));
    wb = std::make_unique<WriteBufferFromKafkaSink>([this](char * pos, size_t len) { addMessageToBatch(pos, len); });

    String data_format = kafka->dataFormat();
    if (data_format.empty())
        data_format = "JSONEachRow";

    if (message_key_ast)
    {
        message_key_expr = buildExpression(context, header, message_key_ast);
        const auto & sample_block = message_key_expr->getSampleBlock();
        /// The last column is the key column, the others are required columns (to be used to calculate the key value).
        message_key_column_pos = message_key_expr->getResultPositions().back();
        auto message_key_column_name = sample_block.getColumnsWithTypeAndName().back().name;
        /// If the key column already exists in the head, which means the message key is
        /// one of the column in the stream, in this case, it should not delete the column.
        /// Otherwise, it's a computed column, it should be deleted from the block.
        delete_message_key_column = !header.tryGetPositionByName(message_key_column_name).has_value();
    }

    if (one_message_per_row)
    {
        /// The callback allows `IRowOutputFormat` based formats produce one Kafka message per row.
        writer = FormatFactory::instance().getOutputFormat(
            data_format, *wb, header, context, [this](auto & /*column*/, auto /*row*/) { wb->next(); }, kafka->getFormatSettings(context));
        if (!static_cast<IRowOutputFormat*>(writer.get()))
            throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Data format `{}` is not a row-based foramt, it cannot be used with `one_message_per_row`", data_format);
    }
    else
    {
        writer = FormatFactory::instance().getOutputFormat(data_format, *wb, header, context, {}, kafka->getFormatSettings(context));
    }
    writer->setAutoFlush();

    if (kafka->hasCustomShardingExpr())
        partitioner = std::make_unique<KafkaStream::ChunkSharder>(context, header, kafka->shardingExprAst());
    else
        partitioner = std::make_unique<KafkaStream::ChunkSharder>();

    /// Polling message deliveries.
    polling_threads.scheduleOrThrowOnError([this]() {
        while (!is_finished.test())
            if (auto n = rd_kafka_poll(producer.get(), POLL_TIMEOUT_MS))
                LOG_TRACE(log, "polled {} events", n);
    });

    /// Monitor partition changes.
    metadata_threads.scheduleOrThrowOnError([this, topic_refresh_interval_ms]() {
        auto last_refresh_at {std::chrono::steady_clock::now()};

        while (!is_finished.test())
        {
            /// Do not sleep for topic_refresh_interval_ms, it will block cancelling the query.
            std::this_thread::sleep_for(std::chrono::milliseconds(500));

            auto now {std::chrono::steady_clock::now()};
            auto passed_ms {std::chrono::duration_cast<std::chrono::milliseconds>(now - last_refresh_at).count()};

            if (passed_ms < topic_refresh_interval_ms)
                continue;

            last_refresh_at = now;

            auto result {klog::describeTopic(topic.get(), producer.get(), log)};
            if (result.err)
            {
                LOG_WARNING(log, "Failed to describe topic, error code: {}", result.err);
                continue;
            }
            partition_cnt = result.partitions;
        }
    });
}

void KafkaSink::addMessageToBatch(char * pos, size_t len)
{
    std::string_view key;
    if (message_key_expr)
    {
        key = keys_queue.front().toView();
        keys_queue.pop_front();
    }

    /// Data at pos (which is in the WriteBuffer) will be overwritten, thus it must be copied to the message.
    /// And these copied data will be freed in the delivery callback
    /// (when it's confirm the message is either sent succesfully, or failed).
    char * payload = new char[len];
    memcpy(payload, pos, len);

    current_batch.push_back(rd_kafka_message_t{
        .partition = next_partition,
        .payload = const_cast<void *>(static_cast<const void *>(payload)),
        .len = len,
        .key = const_cast<void *>(static_cast<const void *>(key.data())),
        .key_len = key.size(),
    });

    ++state.outstandings;
}

void KafkaSink::consume(Chunk chunk)
{
    if (!chunk.hasRows())
        return;

    auto block = getHeader().cloneWithColumns(chunk.detachColumns());
    auto blocks = partitioner->shard(std::move(block), partition_cnt);

    current_batch.clear();
    keys_queue.clear();

    /// When one_message_per_row is set to true, one Kafka message will be generated for each row.
    /// Otherwise, all rows in the same block will be in the same kafka message.
    if (one_message_per_row)
        current_batch.reserve(chunk.rows());
    else
        current_batch.reserve(blocks.size());

    for (auto & blockWithShard : blocks)
    {
        next_partition = blockWithShard.shard;
        /// If message key is not used, and it's using random paritioning,
        /// simply use round robin to avoid calling the paritioner function.
        if (next_partition == RD_KAFKA_PARTITION_UA && !message_key_expr)
            next_partition = next_partition_counter++ % partition_cnt;

        if (!message_key_expr)
        {
            writer->write(blockWithShard.block);
            continue;
        }

        /// Compute and collect message keys and removed the key column from the block after executing the expression
        message_key_expr->execute(blockWithShard.block);
        const auto & message_key_column {blockWithShard.block.getByPosition(message_key_column_pos).column};

        // Collect all the message keys for creating the messages for the block.
        size_t rows {message_key_column->size()};
        for (size_t i = 0; i < rows; ++i)
        {
            /// Since the capacity is big enough, calling push_back won't invalidate iterators.
            keys_queue.push_back(message_key_column->getDataAt(i));
        }

        /// If the expression added a new column holds the key values, it should not be part of the message payload.
        /// we cannot erase the column from blockWithShard.block here, because it could lead to heap-use-after-free
        /// error when rd_kafka_produce_batch tries to read the keys.
        if (delete_message_key_column)
        {
            Block blk;
            blk.reserve(blockWithShard.block.columns());
            for (size_t i = 0; i < blockWithShard.block.columns(); ++i)
            {
                if (i != message_key_column_pos)
                    blk.insert(std::move(blockWithShard.block.getByPosition(i)));
            }
            writer->write(blk);
        }
        else
            writer->write(blockWithShard.block);
    }

    /// With `wb->setAutoFlush()`, it makes sure that all messages are generated for the chunk at this point.
    auto n = rd_kafka_produce_batch(
        topic.get(),
        RD_KAFKA_PARTITION_UA,
        RD_KAFKA_MSG_F_PARTITION | RD_KAFKA_MSG_F_BLOCK,
        current_batch.data(),
        current_batch.size());

    if (static_cast<size_t>(n) != current_batch.size())
        /// Find the first error and throw it.
        for (const auto & msg : current_batch)
            if (msg.err)
                throw Exception(klog::mapErrorCode(msg.err), rd_kafka_err2str(msg.err));
}

void KafkaSink::onFinish()
{
    if (is_finished.test_and_set())
        return;

    polling_threads.wait();

    /// if there are no outstandings, no need to do flushing
    if (hasNoOutstandings())
        return;

    /// Make sure all outstanding requests are transmitted and handled.
    /// It should not block for ever here, otherwise, it will block proton from stopping the job
    /// or block proton from terminating.
    if (auto err = rd_kafka_flush(producer.get(), 15000 /* time_ms */); err)
        LOG_ERROR(log, "Failed to flush kafka producer, error={}", rd_kafka_err2str(err));

    if (auto err = lastSeenError(); err != RD_KAFKA_RESP_ERR_NO_ERROR)
        LOG_ERROR(log, "Failed to send messages, last_seen_error={}", rd_kafka_err2str(err));

    /// if flush does not return an error, the delivery report queue should be empty
    if (!hasNoOutstandings())
        LOG_ERROR(log, "Not all messsages are sent successfully, expected={} actual={}", outstandings(), acked());
}

void KafkaSink::onMessageDelivery(rd_kafka_t * /* producer */, const rd_kafka_message_t * msg, void * opaque)
{
    static_cast<KafkaSink *>(opaque)->onMessageDelivery(msg);
}

void KafkaSink::onMessageDelivery(const rd_kafka_message_t * msg)
{
    /// Can't rely on librdkafka freeing up the payloads with RD_KAFKA_MSG_F_FREE, it will throw:
    ///   AddressSanitizer: alloc-dealloc-mismatch (operator new [] vs free)
    char * payload = static_cast<char *>(msg->payload);
    delete[] payload;

    if (msg->err)
    {
        state.last_error_code.store(msg->err);
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
    do
    {
        if (auto err = lastSeenError(); err != RD_KAFKA_RESP_ERR_NO_ERROR)
            throw Exception(
                klog::mapErrorCode(err), "Failed to send messages, error_cout={} last_error={}", error_count(), rd_kafka_err2str(err));

        if (hasNoOutstandings())
            break;

        if (is_finished.test())
        {
            /// for a final check, it should not wait for too long
            if (auto err = rd_kafka_flush(producer.get(), 15000 /* time_ms */); err)
                throw Exception(klog::mapErrorCode(err), "Failed to flush kafka producer, error={}", rd_kafka_err2str(err));

            if (auto err = lastSeenError(); err != RD_KAFKA_RESP_ERR_NO_ERROR)
                throw Exception(
                    klog::mapErrorCode(err),
                    "Failed to send messages, error_cout={} last_error={}",
                    error_count(),
                    rd_kafka_err2str(err));

            if (!hasNoOutstandings())
                throw Exception(
                    ErrorCodes::CANNOT_WRITE_TO_KAFKA,
                    "Not all messsages are sent successfully, expected={} actual={}",
                    outstandings(),
                    acked());

            break;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    } while (true);

    resetState();
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
