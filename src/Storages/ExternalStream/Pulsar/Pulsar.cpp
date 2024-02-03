#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Parsers/ExpressionListParsers.h>
#include <Storages/ExternalStream/ExternalStreamTypes.h>
#include <Storages/ExternalStream/Pulsar/Pulsar.h>
#include <Storages/IStorage.h>
#include <Storages/SelectQueryInfo.h>
#include <Common/ProtonCommon.h>
#include <Common/logger_useful.h>
#include <Parsers/ASTFunction.h>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/split.hpp>

#include <ranges>

namespace DB
{
namespace ErrorCodes
{
extern const int OK;
extern const int INVALID_SETTING_VALUE;
extern const int RESOURCE_NOT_FOUND;
}

Pulsar::Pulsar(IStorage * storage, std::unique_ptr<ExternalStreamSettings> settings_, const ASTs & engine_args_, bool attach, ExternalStreamCounterPtr external_stream_counter_, ContextPtr context)
    : StorageExternalStreamImpl(std::move(settings_))
    , storage_id(storage->getStorageID())
    , engine_args(engine_args_)
    , kafka_properties(klog::parseProperties(settings->properties.value))
    , data_format(StorageExternalStreamImpl::dataFormat())
    , external_stream_counter(external_stream_counter_)
    , logger(&Poco::Logger::get("External-" + settings->topic.value))
{
    assert(settings->type.value == StreamTypes::KAFKA || settings->type.value == StreamTypes::REDPANDA);
    assert(external_stream_counter);

    if (settings->brokers.value.empty())
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Empty `brokers` setting for {} external stream", settings->type.value);

    if (settings->topic.value.empty())
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Empty `topic` setting for {} external stream", settings->type.value);

    if (!settings->message_key.value.empty())
    {
        validateMessageKey(settings->message_key.value, storage, context);

        /// When message_key is set, each row should be sent as one message, it doesn't make any sense otherwise.
        if (settings->isChanged("one_message_per_row") && !settings->one_message_per_row)
            throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "`one_message_per_row` cannot be set to `false` when `message_key` is set");
        settings->set("one_message_per_row", true);
    }

    calculateDataFormat(storage);

    cacheVirtualColumnNamesAndTypes();

    if (!attach)
        /// Only validate cluster / topic for external stream creation
        validate();
}

bool Pulsar::hasCustomShardingExpr() const {
    if (engine_args.empty())
        return false;

    if (auto * shard_func = shardingExprAst()->as<ASTFunction>())
        return !boost::iequals(shard_func->name, "rand");

    return true;
}

Pipe Pulsar::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t /*num_streams*/)
{
    /// User can explicitly consume specific kafka partitions by specifying `shards=` setting
    /// `SELECT * FROM kafka_stream SETTINGS shards=0,3`
    std::vector<int32_t> shards_to_query;
    if (!context->getSettingsRef().shards.value.empty())
    {
        shards_to_query = parseShards(context->getSettingsRef().shards.value);
        validate(shards_to_query);
        LOG_INFO(logger, "reading from [{}] partitions for topic={}", fmt::join(shards_to_query, ","), settings->topic.value);
    }
    else
    {
        /// We still like to validate / describe the topic if we haven't yet
        validate();

        /// Query all shards / partitions
        shards_to_query.reserve(shards);
        for (int32_t i = 0; i < shards; ++i)
            shards_to_query.push_back(i);
    }

    assert(!shards_to_query.empty());

    Pipes pipes;
    pipes.reserve(shards_to_query.size());

    // const auto & settings_ref = context->getSettingsRef();
    /*auto share_resource_group = (settings_ref.query_resource_group.value == "shared") && (settings_ref.seek_to.value == "latest");
    if (share_resource_group)
    {
        for (Int32 i = 0; i < shards; ++i)
        {
            if (!column_names.empty())
                pipes.emplace_back(source_multiplexers->createChannel(i, column_names, metadata_snapshot, context));
            else
                pipes.emplace_back(source_multiplexers->createChannel(i, {RESERVED_APPEND_TIME}, metadata_snapshot, context));
        }
    }
    else*/
    {
        /// For queries like `SELECT count(*) FROM tumble(table, now(), 5s) GROUP BY window_end` don't have required column from table.
        /// We will need add one
        Block header;
        if (!column_names.empty())
            header = storage_snapshot->getSampleBlockForColumns(column_names);
        else
            header = storage_snapshot->getSampleBlockForColumns({ProtonConsts::RESERVED_APPEND_TIME});

        auto offsets = getOffsets(query_info.seek_to_info, shards_to_query);
        assert(offsets.size() == shards_to_query.size());
        for (auto [shard, offset] : std::ranges::views::zip(shards_to_query, offsets))
            pipes.emplace_back(
                std::make_shared<KafkaSource>(this, header, storage_snapshot, context, shard, offset, max_block_size, logger, external_stream_counter));
    }

    LOG_INFO(
        logger,
        "Starting reading {} streams by seeking to {} in dedicated resource group",
        pipes.size(),
        query_info.seek_to_info->getSeekTo());

    auto pipe = Pipe::unitePipes(std::move(pipes));
    auto min_threads = context->getSettingsRef().min_threads.value;
    if (min_threads > shards_to_query.size())
        pipe.resize(min_threads);

    return pipe;
}

NamesAndTypesList Pulsar::getVirtuals() const
{
    return virtual_column_names_and_types;
}

void Pulsar::cacheVirtualColumnNamesAndTypes()
{
    virtual_column_names_and_types.push_back(NameAndTypePair(ProtonConsts::RESERVED_APPEND_TIME, std::make_shared<DataTypeDateTime64>(3, "UTC")));
    virtual_column_names_and_types.push_back(NameAndTypePair(ProtonConsts::RESERVED_EVENT_TIME, std::make_shared<DataTypeDateTime64>(3, "UTC")));
    virtual_column_names_and_types.push_back(NameAndTypePair(ProtonConsts::RESERVED_PROCESS_TIME, std::make_shared<DataTypeDateTime64>(3, "UTC")));
    virtual_column_names_and_types.push_back(NameAndTypePair(ProtonConsts::RESERVED_SHARD, std::make_shared<DataTypeInt32>()));
    virtual_column_names_and_types.push_back(NameAndTypePair(ProtonConsts::RESERVED_EVENT_SEQUENCE_ID, std::make_shared<DataTypeInt64>()));
}

//klog::KafkaWALSimpleConsumerPtr Kafka::getConsumer(int32_t fetch_wait_max_ms) const
//{
//    return klog::KafkaWALPool::instance(nullptr).getOrCreateStreamingExternal(settings->brokers.value, *auth_info, fetch_wait_max_ms);
//}

std::vector<Int64> Pulsar::getOffsets(const SeekToInfoPtr & seek_to_info, const std::vector<int32_t> & shards_to_query) const
{
    assert(seek_to_info);
    seek_to_info->replicateForShards(shards_to_query.size());
    if (!seek_to_info->isTimeBased())
    {
        return seek_to_info->getSeekPoints();
    }
    else
    {
        std::vector<klog::PartitionTimestamp> partition_timestamps;
        partition_timestamps.reserve(shards_to_query.size());
        auto seek_timestamps{seek_to_info->getSeekPoints()};
        assert(shards_to_query.size() == seek_timestamps.size());

        for (auto [shard, timestamp] : std::ranges::views::zip(shards_to_query, seek_timestamps))
            partition_timestamps.emplace_back(shard, timestamp);

        return getConsumer()->offsetsForTimestamps(settings->topic.value, partition_timestamps);
    }
}

void Pulsar::calculateDataFormat(const IStorage * storage)
{
    if (!data_format.empty())
        return;

    /// If there is only one column and its type is a string type, use RawBLOB. Use JSONEachRow otherwise.
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

/// FIXME, refactor out as util and unit test it
std::vector<int32_t> Pulsar::parseShards(const std::string & shards_setting)
{
    std::vector<String> shard_strings;
    boost::split(shard_strings, shards_setting, boost::is_any_of(","));

    std::vector<int32_t> specified_shards;
    specified_shards.reserve(shard_strings.size());
    for (const auto & shard_string : shard_strings)
    {
        try
        {
            specified_shards.push_back(std::stoi(shard_string));
        }
        catch (std::invalid_argument &)
        {
            throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Invalid shard : {}", shard_string);
        }
        catch (std::out_of_range &)
        {
            throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Shard {} is too big", shard_string);
        }

        if (specified_shards.back() < 0)
            throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Invalid shard: {}", shard_string);
    }

    return specified_shards;
}

void Pulsar::validateMessageKey(const String & message_key_, IStorage * storage, const ContextPtr & context)
{
    const auto & key = message_key_.c_str();
    Tokens tokens(key, key + message_key_.size(), 0);
    IParser::Pos pos(tokens, 0);
    Expected expected;
    ParserExpression p_id;
    if (!p_id.parse(pos, message_key_ast, expected))
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "message_key was not a valid expression, parse failed at {}, expected {}", expected.max_parsed_pos, fmt::join(expected.variants, ", "));

    if (!pos->isEnd())
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "message_key must be a single expression, got extra characters: {}", expected.max_parsed_pos);

    auto syntax_result = TreeRewriter(context).analyze(message_key_ast, storage->getInMemoryMetadata().getColumns().getAllPhysical());
    auto analyzer = ExpressionAnalyzer(message_key_ast, syntax_result, context).getActions(true);
    const auto & block = analyzer->getSampleBlock();
    if (block.columns() != 1)
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "message_key expression must return exactly one column");

    auto type_id = block.getByPosition(0).type->getTypeId();
    if (type_id != TypeIndex::String && type_id != TypeIndex::FixedString)
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "message_key must have type of string");
}

/// Validate the topic still exists, specified partitions are still valid etc
void Pulsar::validate(const std::vector<int32_t> & shards_to_query)
{
    if (shards == 0)
    {
        std::scoped_lock lock(shards_mutex);
        /// Recheck again in case in-parallel query chimes in and init the shards already
        if (shards == 0)
        {
            /// We haven't describe the topic yet
            auto result = getConsumer()->describe(settings->topic.value);
            if (result.err != ErrorCodes::OK)
                throw Exception(ErrorCodes::RESOURCE_NOT_FOUND, "{} topic doesn't exist", settings->topic.value);

            shards = result.partitions;
        }
    }

    if (!shards_to_query.empty())
    {
        /// User specified specific partitions to consume.
        /// Make sure they are valid.
        for (auto shard : shards_to_query)
        {
            if (shard >= shards)
                throw Exception(
                    ErrorCodes::INVALID_SETTING_VALUE,
                    "Invalid topic partition {} for topic {}, biggest partition is {}",
                    shard,
                    settings->topic.value,
                    shards);
        }
    }
}

SinkToStoragePtr Pulsar::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr context)
{
    /// always validate before actual use
    validate();
    return std::make_shared<KafkaSink>(
        this, metadata_snapshot->getSampleBlock(), shards, message_key_ast, context, logger, external_stream_counter);
}
}
