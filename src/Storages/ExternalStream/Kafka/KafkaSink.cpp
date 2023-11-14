#include "KafkaSink.h"

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/createBlockSelector.h>
#include <Parsers/ASTFunction.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Common/logger_useful.h>

#include <boost/algorithm/string/predicate.hpp>

namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_WRITE_TO_KAFKA;
extern const int MISSING_ACKNOWLEDGEMENT;
extern const int INVALID_CONFIG_PARAMETER;
extern const int TYPE_MISMATCH;
}

namespace KafkaStream
{
ChunkPartitioner::ChunkPartitioner(ContextPtr context, const Block & header, const ASTPtr & partitioning_expr_ast)
{
    /// `InterpreterCreateQuery::handleExternalStreamCreation` ensures this
    assert(partitioning_expr_ast);

    ASTPtr query = partitioning_expr_ast;
    auto syntax_result = TreeRewriter(context).analyze(query, header.getNamesAndTypesList());
    partitioning_expr = ExpressionAnalyzer(query, syntax_result, context).getActions(false);

    partitioning_key_column_name = partitioning_expr_ast->getColumnName();

    if (auto * shard_func = partitioning_expr_ast->as<ASTFunction>())
    {
        if (shard_func->name == "rand" || shard_func->name == "RAND")
            random_partitioning = true;
    }
}

BlocksWithShard ChunkPartitioner::partition(Block block, Int32 partition_cnt) const
{
    /// no topics have zero partitions
    assert(partition_cnt > 0);

    if (partition_cnt == 1)
        return {BlockWithShard{Block(std::move(block)), 0}};

    if (random_partitioning)
        return {BlockWithShard{Block(std::move(block)), getNextShardIndex(partition_cnt)}};

    return doParition(std::move(block), partition_cnt);
}

BlocksWithShard ChunkPartitioner::doParition(Block block, Int32 partition_cnt) const
{
    auto selector = createSelector(block, partition_cnt);

    Blocks partitioned_blocks{static_cast<size_t>(partition_cnt)};

    for (Int32 i = 0; i < partition_cnt; ++i)
        partitioned_blocks[i] = block.cloneEmpty();

    for (size_t pos = 0; pos < block.columns(); ++pos)
    {
        MutableColumns partitioned_columns = block.getByPosition(pos).column->scatter(partition_cnt, selector);
        for (Int32 i = 0; i < partition_cnt; ++i)
            partitioned_blocks[i].getByPosition(pos).column = std::move(partitioned_columns[i]);
    }

    BlocksWithShard blocks_with_shard;

    /// Filter out empty blocks
    for (size_t i = 0; i < partitioned_blocks.size(); ++i)
    {
        if (partitioned_blocks[i].rows())
            blocks_with_shard.emplace_back(std::move(partitioned_blocks[i]), i);
    }

    return blocks_with_shard;
}

IColumn::Selector ChunkPartitioner::createSelector(Block block, Int32 partition_cnt) const
{
    std::vector<UInt64> slot_to_shard(partition_cnt);
    std::iota(slot_to_shard.begin(), slot_to_shard.end(), 0);

    partitioning_expr->execute(block);

    const auto & key_column = block.getByName(partitioning_key_column_name);

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

KafkaSink::KafkaSink(const Kafka * kafka, const Block & header, ContextPtr context, Int32 initial_partition_cnt, Poco::Logger * log_)
    : SinkToStorage(header, ProcessorID::ExternalTableDataSinkID)
    , producer(nullptr, nullptr)
    , topic(nullptr, nullptr)
    , polling_threads(1)
    , partition_cnt(initial_partition_cnt)
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
    producer_params.emplace_back("security.protocol", kafka->securityProtocol());
    if (boost::iequals(kafka->securityProtocol(), "SASL_SSL"))
    {
        producer_params.emplace_back("sasl.mechanisms", "PLAIN");
        producer_params.emplace_back("sasl.username", kafka->username());
        producer_params.emplace_back("sasl.password", kafka->password());
    }

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

    auto * topic_conf = rd_kafka_conf_get_default_topic_conf(conf);
    if (!topic_conf)
    {
        topic_conf = rd_kafka_topic_conf_new();
        /// conf will take the ownership of topic_conf
        rd_kafka_conf_set_default_topic_conf(conf, topic_conf);
    }

    rd_kafka_conf_set_opaque(conf, this);
    rd_kafka_topic_conf_set_opaque(topic_conf, this);

    /// With partitioner callback, we can get the up-to-date partition count w/o additional effort.
    rd_kafka_topic_conf_set_partitioner_cb(topic_conf, &KafkaSink::onPartitioning);

    rd_kafka_conf_set_dr_msg_cb(conf, &KafkaSink::onMessageDelivery);

    producer = klog::KafkaPtr(rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr)), rd_kafka_destroy);
    if (!producer)
    {
        // librdkafka will take the ownership of `conf` if `rd_kafka_new` succeeds,
        // but if it does not, we need to take care of cleaning it up by ourselves.
        rd_kafka_conf_destroy(conf);
        throw Exception("Failed to create kafka handle", klog::mapErrorCode(rd_kafka_last_error()));
    }

    topic = klog::KTopicPtr(rd_kafka_topic_new(producer.get(), kafka->topic().c_str(), nullptr), rd_kafka_topic_destroy);
    wb = std::make_unique<WriteBufferFromKafka>(topic.get());

    String data_format = kafka->dataFormat();
    if (data_format.empty())
        data_format = "JSONEachRow";

    writer = FormatFactory::instance().getOutputFormat(data_format, *wb, header, context);
    writer->setAutoFlush();

    partitioner = std::make_unique<KafkaStream::ChunkPartitioner>(context, header, kafka->partitioning_expr_ast());

    polling_threads.scheduleOrThrowOnError([this]() {
        while (!is_finished.test())
            if (auto n = rd_kafka_poll(producer.get(), POLL_TIMEOUT_MS))
                LOG_TRACE(log, "polled {} events", n);
    });
}

void KafkaSink::consume(Chunk chunk)
{
    if (!chunk.hasRows())
        return;

    auto block = getHeader().cloneWithColumns(chunk.detachColumns());
    auto blocks = partitioner->partition(std::move(block), partition_cnt);

    for (auto & blockWithShard : blocks)
    {
        /// Since we set AutoFlush on writer, it makes sure that `writer->write` will call
        /// `wb->nextImpl` and waits for it finishes, it's safe to call `wb->write_to_partition`
        /// before `writer->write`.
        wb->setTargetPartition(blockWithShard.shard);
        writer->write(blockWithShard.block);
    }
}

void KafkaSink::onFinish()
{
    if (is_finished.test_and_set())
        return;

    polling_threads.wait();

    /// if there are no outstandings, no need to do flushing
    if (wb->hasNoOutstandings())
        return;

    /// Make sure all outstanding requests are transmitted and handled.
    /// It should not block for ever here, otherwise, it will block proton from stopping the job
    /// or block proton from terminating.
    if (auto err = rd_kafka_flush(producer.get(), 15000 /* time_ms */); err)
        LOG_ERROR(log, "Failed to flush kafka producer, error={}", rd_kafka_err2str(err));

    if (auto err = wb->lastSeenError(); err != RD_KAFKA_RESP_ERR_NO_ERROR)
        LOG_ERROR(log, "Failed to send messages, last_seen_error={}", rd_kafka_err2str(err));

    /// if flush does not return an error, the delivery report queue should be empty
    if (!wb->hasNoOutstandings())
        LOG_ERROR(log, "Not all messsages are sent successfully, expected={} actual={}", wb->outstandings(), wb->acked());
}

KafkaSink::~KafkaSink()
{
    onFinish();
}

void KafkaSink::checkpoint(CheckpointContextPtr context)
{
    do
    {
        if (auto err = wb->lastSeenError(); err != RD_KAFKA_RESP_ERR_NO_ERROR)
            throw Exception(
                klog::mapErrorCode(err), "Failed to send messages, error_cout={} last_error={}", wb->error_count(), rd_kafka_err2str(err));

        if (wb->hasNoOutstandings())
            break;

        if (is_finished.test())
        {
            /// for a final check, it should not wait for too long
            if (auto err = rd_kafka_flush(producer.get(), 15000 /* time_ms */); err)
                throw Exception(klog::mapErrorCode(err), "Failed to flush kafka producer, error={}", rd_kafka_err2str(err));

            if (auto err = wb->lastSeenError(); err != RD_KAFKA_RESP_ERR_NO_ERROR)
                throw Exception(
                    klog::mapErrorCode(err),
                    "Failed to send messages, error_cout={} last_error={}",
                    wb->error_count(),
                    rd_kafka_err2str(err));

            if (!wb->hasNoOutstandings())
                throw Exception(
                    ErrorCodes::CANNOT_WRITE_TO_KAFKA,
                    "Not all messsages are sent successfully, expected={} actual={}",
                    wb->outstandings(),
                    wb->acked());

            break;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    } while (true);

    wb->resetState();
    IProcessor::checkpoint(context);
}
}
