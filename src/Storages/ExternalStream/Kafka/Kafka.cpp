#include "Kafka.h"
#include "KafkaSink.h"
#include "KafkaSource.h"

#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <KafkaLog/KafkaWALPool.h>
#include <Storages/ExternalStream/ExternalStreamTypes.h>
#include <Storages/IStorage.h>
#include <Storages/SelectQueryInfo.h>
#include <Common/ProtonCommon.h>
#include <Common/logger_useful.h>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>

namespace DB
{
namespace ErrorCodes
{
extern const int INVALID_SETTING_VALUE;
extern const int OK;
extern const int RESOURCE_NOT_FOUND;
}

klog::KConfParams Kafka::parseProperties(String & properties)
{
    klog::KConfParams result;

    if (properties.empty())
        return result;

    /// properties example:
    /// message.max.bytes=1024;max.in.flight=1000;group.id=my-group

    std::vector<String> parts;
    boost::split(parts, properties, boost::is_any_of(";"));
    result.reserve(parts.size());

    for (const auto & part : parts)
    {
        if (unlikely(part.empty())) /* redundant / trailing ';' */
            continue;

        auto equal_pos = part.find('=');
        if (unlikely(equal_pos == std::string::npos || equal_pos == 0 || equal_pos == part.size() - 1))
            throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Invalid property `{}`, expected format: <key>=<value>.", part);

        auto key = part.substr(0, equal_pos);
        auto value = part.substr(equal_pos + 1);

        /// no spaces are supposed be around `=`, thus only need to
        /// remove the leading spaces of keys and trailing spaces of values
        boost::trim_left(key);
        boost::trim_right(value);
        result.push_back(std::make_pair(key, value));
    }

    return result;
}

Kafka::Kafka(IStorage * storage, std::unique_ptr<ExternalStreamSettings> settings_, bool attach)
    : storage_id(storage->getStorageID())
    , settings(std::move(settings_))
    , data_format(settings->data_format.value)
    , log(&Poco::Logger::get("External-" + settings->topic.value))
{
    assert(settings->type.value == StreamTypes::KAFKA || settings->type.value == StreamTypes::REDPANDA);

    if (settings->brokers.value.empty())
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Empty `brokers` setting for {} external stream", settings->type.value);

    if (settings->topic.value.empty())
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Empty `topic` setting for {} external stream", settings->type.value);

    properties_ = parseProperties(settings->properties.value);

    calculateDataFormat(storage);

    cacheVirtualColumnNamesAndTypes();

    if (!attach)
        /// Only validate cluster / topic for external stream creation
        validate();
}

Pipe Kafka::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t /*num_streams*/)
{
    validate();

    Pipes pipes;
    pipes.reserve(shards);

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

        auto offsets = getOffsets(query_info.seek_to_info);

        for (Int32 i = 0; i < shards; ++i)
            pipes.emplace_back(std::make_shared<KafkaSource>(this, header, storage_snapshot, context, i, offsets[i], max_block_size, log));
    }

    LOG_INFO(
        log,
        "Starting reading {} streams by seeking to {} in dedicated resource group",
        pipes.size(),
        query_info.seek_to_info->getSeekTo());

    auto pipe = Pipe::unitePipes(std::move(pipes));
    auto min_threads = context->getSettingsRef().min_threads.value;
    if (min_threads > static_cast<UInt64>(shards))
        pipe.resize(min_threads);
    return pipe;
}

NamesAndTypesList Kafka::getVirtuals() const
{
    return virtual_column_names_and_types;
}

void Kafka::cacheVirtualColumnNamesAndTypes()
{
    virtual_column_names_and_types.push_back(NameAndTypePair(ProtonConsts::RESERVED_APPEND_TIME, std::make_shared<DataTypeInt64>()));
    virtual_column_names_and_types.push_back(NameAndTypePair(ProtonConsts::RESERVED_EVENT_TIME, std::make_shared<DataTypeInt64>()));
    virtual_column_names_and_types.push_back(NameAndTypePair(ProtonConsts::RESERVED_PROCESS_TIME, std::make_shared<DataTypeInt64>()));
    virtual_column_names_and_types.push_back(NameAndTypePair(ProtonConsts::RESERVED_SHARD, std::make_shared<DataTypeInt32>()));
    virtual_column_names_and_types.push_back(NameAndTypePair(ProtonConsts::RESERVED_EVENT_SEQUENCE_ID, std::make_shared<DataTypeInt64>()));
}

std::vector<Int64> Kafka::getOffsets(const SeekToInfoPtr & seek_to_info) const
{
    assert(seek_to_info);
    seek_to_info->replicateForShards(shards);
    if (!seek_to_info->isTimeBased())
    {
        return seek_to_info->getSeekPoints();
    }
    else
    {
        klog::KafkaWALAuth auth = {.security_protocol = securityProtocol(), .username = username(), .password = password()};
        auto consumer = klog::KafkaWALPool::instance(nullptr).getOrCreateStreamingExternal(settings->brokers.value, auth);
        return consumer->offsetsForTimestamps(settings->topic.value, seek_to_info->getSeekPoints());
    }
}

void Kafka::calculateDataFormat(const IStorage * storage)
{
    if (!data_format.empty())
        return;

    auto column_names_and_types{storage->getInMemoryMetadata().getColumns().getOrdinary()};
    if (column_names_and_types.size() != 1)
        throw Exception(
            ErrorCodes::INVALID_SETTING_VALUE,
            "`data_format` settings is empty but the Kafka external stream definition has multiple columns. Proton doesn't know how to "
            "parse Kafka messages without a data format.");

    auto type = column_names_and_types.begin()->type;
    if (type->getTypeId() == TypeIndex::String || type->getTypeId() == TypeIndex::FixedString)
    {
        /// no-op
    }
    /// FIXME: JSON logic
    else if (type->getTypeId() == TypeIndex::Object)
        data_format = "JSONEachRow";
    else
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED, "Automatically converting Kafka message to {} type is not supported yet", type->getName());
}

void Kafka::validate()
{
    if (shards > 0)
        /// Already validated
        return;

    klog::KafkaWALAuth auth = {
        .security_protocol = settings->security_protocol.value, .username = settings->username.value, .password = settings->password.value};

    auto consumer = klog::KafkaWALPool::instance(nullptr).getOrCreateStreamingExternal(settings->brokers.value, auth);

    auto result = consumer->describe(settings->topic.value);
    if (result.err != ErrorCodes::OK)
        throw Exception(ErrorCodes::RESOURCE_NOT_FOUND, "{} topic doesn't exist", settings->topic.value);

    shards = result.partitions;
}

SinkToStoragePtr Kafka::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr context)
{
    return std::make_shared<KafkaSink>(this, metadata_snapshot->getSampleBlock(), context, log);
}
}
