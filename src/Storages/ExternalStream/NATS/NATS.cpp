#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteBufferFromFile.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ExpressionListParsers.h>
#include <Processors/Sources/NullSource.h>
#include <Storages/ExternalStream/ExternalStreamTypes.h>
#include <Storages/ExternalStream/NATS/NATS.h>
#include <Storages/ExternalStream/NATS/NATSSink.h>
#include <Storages/ExternalStream/NATS/NATSSource.h>
#include <Storages/IStorage.h>
#include <Storages/SelectQueryInfo.h>
#include <Common/ProtonCommon.h>
#include <Common/logger_useful.h>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>

#include <filesystem>
#include <optional>
#include <ranges>

namespace DB
{

namespace ErrorCodes
{
extern const int INVALID_CONFIG_PARAMETER;
extern const int INVALID_SETTING_VALUE;
extern const int NO_AVAILABLE_NATS_CONSUMER;
}

namespace
{

const String MAX_CONSUMERS_CONFIG_KEY = "external_stream.nats.max_consumers_per_stream";
const size_t DEFAULT_MAX_CONSUMERS = 50;

NATS::ConfPtr createConfFromSettings(const NATSExternalStreamSettings & settings)
{
    if (settings.nats_servers.value.empty())
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Empty `nats_servers` setting for NATS external stream");

    NATS::ConfPtr conf{natsOptions_Create(), natsOptions_Destroy};
    char errstr[512]{'\0'};

    auto conf_set = [&](const String & name, const String & value) {
        auto err = natsOptions_SetURL(conf.get(), value.c_str());
        if (err != NATS_OK)
        {
            throw Exception(
                ErrorCodes::INVALID_CONFIG_PARAMETER,
                "Failed to set NATS config `{}` with value `{}` error_code={} error_msg={}",
                name,
                value,
                err,
                natsStatus_GetText(err));
        }
    };

    conf_set("servers", settings.nats_servers.value);

    return conf;
}

}

const String NATS::VIRTUAL_COLUMN_MESSAGE_KEY = "_message_key";

NATS::ConfPtr NATS::createNATSConf(NATSExternalStreamSettings settings_)
{
    return createConfFromSettings(settings_);
}

NATS::NATS(
    IStorage * storage,
    std::unique_ptr<ExternalStreamSettings> settings_,
    const ASTs & engine_args_,
    bool attach,
    ExternalStreamCounterPtr external_stream_counter_,
    ContextPtr context)
    : StorageExternalStreamImpl(storage, std::move(settings_), context)
    , engine_args(engine_args_)
    , data_format(StorageExternalStreamImpl::dataFormat())
    , external_stream_counter(external_stream_counter_)
    , conf(createNATSConf(settings->getNATSSettings()))
    , logger(&Poco::Logger::get(getLoggerName()))
{
    assert(settings->type.value == StreamTypes::NATS);
    assert(external_stream_counter);

    if (settings->nats_subject.value.empty())
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Empty `nats_subject` setting for NATS external stream");

    calculateDataFormat(storage);

    cacheVirtualColumnNamesAndTypes();

    if (!attach)
        validate();
}

bool NATS::hasCustomShardingExpr() const
{
    if (engine_args.empty())
        return false;

    if (auto * shard_func = shardingExprAst()->as<ASTFunction>())
        return !boost::iequals(shard_func->name, "rand");

    return true;
}

NamesAndTypesList NATS::getVirtuals() const
{
    return virtual_column_names_and_types;
}

void NATS::cacheVirtualColumnNamesAndTypes()
{
    virtual_column_names_and_types.push_back(
        NameAndTypePair(ProtonConsts::RESERVED_APPEND_TIME, std::make_shared<DataTypeDateTime64>(3, "UTC")));
    virtual_column_names_and_types.push_back(
        NameAndTypePair(ProtonConsts::RESERVED_EVENT_TIME, std::make_shared<DataTypeDateTime64>(3, "UTC")));
    virtual_column_names_and_types.push_back(
        NameAndTypePair(ProtonConsts::RESERVED_PROCESS_TIME, std::make_shared<DataTypeDateTime64>(3, "UTC")));
    virtual_column_names_and_types.push_back(NameAndTypePair(ProtonConsts::RESERVED_SHARD, std::make_shared<DataTypeInt32>()));
    virtual_column_names_and_types.push_back(NameAndTypePair(ProtonConsts::RESERVED_EVENT_SEQUENCE_ID, std::make_shared<DataTypeInt64>()));
    virtual_column_names_and_types.push_back(NameAndTypePair(VIRTUAL_COLUMN_MESSAGE_KEY, std::make_shared<DataTypeString>()));
}

void NATS::calculateDataFormat(const IStorage * storage)
{
    if (!data_format.empty())
        return;

    auto column_names_and_types{storage->getInMemoryMetadata().getColumns().getOrdinary()};
    if (column_names_and_types.size() == 1)
    {
        auto type = column_names_and_types.begin()->type->getTypeId();
        if (type == TypeIndex::String || type == TypeIndex::FixedString)
        {
            data_format = "RawBLOB";
            return;
        }
    }

    data_format = "JSONEachRow";
}

void NATS::validate()
{
    auto consumer = getConsumer();
    auto topic = NATSTopic(*consumer->getHandle(), topicName());
    auto partition_count = topic.getPartitionCount();
    if (partition_count < 1)
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Topic has no partitions, topic={}", topicName());
}

std::optional<UInt64> NATS::totalRows(const Settings & settings_ref)
{
    auto consumer = getConsumer();
    auto topic = NATSTopic(*consumer->getHandle(), topicName());
    auto shards_to_query = getShardsToQuery(settings_ref.shards.value, topic.getPartitionCount());
    LOG_INFO(logger, "Counting number of messages topic={} partitions=[{}]", topicName(), fmt::join(shards_to_query, ","));

    UInt64 rows = 0;
    for (auto shard : shards_to_query)
    {
        auto marks = topic.queryWatermarks(shard);
        LOG_INFO(logger, "Watermarks topic={} partition={} low={} high={}", topicName(), shard, marks.low, marks.high);
        rows += marks.high - marks.low;
    }
    return rows;
}

Pipe NATS::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t /*num_streams*/)
{
    auto consumer = getConsumer();
    auto topic_ptr = std::make_shared<NATSTopic>(*consumer->getHandle(), topicName());

    auto shards_to_query = getShardsToQuery(context->getSettingsRef().shards.value, topic_ptr->getPartitionCount());
    assert(!shards_to_query.empty());

    auto streaming = query_info.syntax_analyzer_result->streaming;

    LOG_INFO(logger, "Reading topic={} partitions=[{}] streaming={}", topicName(), fmt::join(shards_to_query, ","), streaming);

    Pipes pipes;
    pipes.reserve(shards_to_query.size());

    {
        auto header = storage_snapshot->getSampleBlockForColumns(column_names);

        auto seek_to_info = query_info.seek_to_info;
        if (!streaming && seek_to_info->getSeekTo().empty())
            seek_to_info = std::make_shared<SeekToInfo>("earliest");

        auto offsets = getOffsets(*consumer, seek_to_info, shards_to_query);
        assert(offsets.size() == shards_to_query.size());

        for (auto [shard, offset] : std::ranges::views::zip(shards_to_query, offsets))
        {
            std::optional<Int64> high_watermark = std::nullopt;
            if (!streaming)
            {
                auto marks = topic_ptr->queryWatermarks(shard);
                LOG_INFO(logger, "Watermarks topic={} partition={} low={} high={}", topicName(), shard, marks.low, marks.high);
                high_watermark = marks.high;

                if (marks.low == marks.high)
                {
                    pipes.emplace_back(std::make_shared<NullSource>(header));
                    continue;
                }
                else if (offset >= 0 && offset < marks.low)
                    offset = marks.low;
                else if (offset == nlog::LATEST_SN || offset > marks.high)
                    offset = marks.high;
            }
            pipes.emplace_back(std::make_shared<NATSSource>(
                *this,
                header,
                storage_snapshot,
                consumer,
                topic_ptr,
                shard,
                offset,
                high_watermark,
                max_block_size,
                external_stream_counter,
                context));
        }
    }

    LOG_INFO(
        logger,
        "Starting reading {} streams by seeking to {} with {} in dedicated resource group",
        pipes.size(),
        query_info.seek_to_info->getSeekTo(),
        consumer->name());

    auto pipe = Pipe::unitePipes(std::move(pipes));
    auto min_threads = context->getSettingsRef().min_threads.value;
    if (min_threads > shards_to_query.size())
        pipe.resize(min_threads);

    return pipe;
}

std::shared_ptr<NATSConsumer> NATS::getConsumer()
{
    std::lock_guard<std::mutex> lock{consumer_mutex};

    auto consumer_ref = std::find_if(consumers.begin(), consumers.end(), [](const auto & consumer) { return consumer.expired(); });
    if (consumer_ref == consumers.end() && consumers.size() >= max_consumers)
        throw Exception(
            ErrorCodes::NO_AVAILABLE_NATS_CONSUMER,
            "Reached consumers limit {}. Existing queries need to be stopped before running other queries. Or update {} to a bigger number "
            "in the config file",
            max_consumers,
            MAX_CONSUMERS_CONFIG_KEY);

    auto new_consumer = std::make_shared<NATSConsumer>(*conf, getLoggerName());
    std::weak_ptr<NATSConsumer> ref = new_consumer;

    if (consumer_ref != consumers.end())
        consumer_ref->swap(ref);
    else
        consumers.push_back(ref);

    return new_consumer;
}

std::shared_ptr<NATSProducer> NATS::getProducer()
{
    if (producer)
        return producer;

    std::lock_guard<std::mutex> lock{producer_mutex};
    if (producer)
        return producer;

    auto producer_ptr = std::make_shared<NATSProducer>(*conf, getLoggerName());
    producer.swap(producer_ptr);

    return producer;
}

std::shared_ptr<NATSTopic> NATS::getProducerTopic()
{
    if (producer_topic)
        return producer_topic;

    std::scoped_lock lock(producer_mutex);
    if (producer_topic)
        return producer_topic;

    auto topic_ptr = std::make_shared<NATSTopic>(*getProducer()->getHandle(), topicName());
    producer_topic.swap(topic_ptr);

    return producer_topic;
}

SinkToStoragePtr NATS::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr context)
{
    validate();
    return std::make_shared<NATSSink>(*this, metadata_snapshot->getSampleBlock(), external_stream_counter, context);
}

}
