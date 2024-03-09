#include "Pulsar.h"
#include "PulsarSource.h"

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

#include <pulsar/Client.h>

namespace DB
{
namespace ErrorCodes
{
extern const int OK;
extern const int INVALID_SETTING_VALUE;
extern const int RESOURCE_NOT_FOUND;
}

Pulsar::Pulsar(
    IStorage * storage,
    std::unique_ptr<ExternalStreamSettings> settings_,
    const ASTs &,
    bool,
    ExternalStreamCounterPtr external_stream_counter_,
    ContextPtr)
    : StorageExternalStreamImpl(std::move(settings_))
    , storage_id(storage->getStorageID())
    , logger(&Poco::Logger::get("External-PulsarLog"))
    , external_stream_counter(external_stream_counter_)
    , data_format(StorageExternalStreamImpl::dataFormat())
{
    assert(settings->type.value == StreamTypes::PULSAR);
    assert(external_stream_counter);

    if (settings->service_url.value.empty())
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Empty `service_url` setting for {} external stream", settings->type.value);

    if (settings->topic.value.empty())
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Empty `topic` setting for {} external stream", settings->type.value);

    calculateDataFormat(storage);

    cacheVirtualColumnNamesAndTypes();

    validate();
}

NamesAndTypesList Pulsar::getVirtuals() const
{
    NamesAndTypesList n;
    return n;
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

void Pulsar::cacheVirtualColumnNamesAndTypes()
{
    virtual_column_names_and_types.push_back(NameAndTypePair(ProtonConsts::RESERVED_APPEND_TIME, std::make_shared<DataTypeDateTime64>(3, "UTC")));
    virtual_column_names_and_types.push_back(NameAndTypePair(ProtonConsts::RESERVED_EVENT_TIME, std::make_shared<DataTypeDateTime64>(3, "UTC")));
    virtual_column_names_and_types.push_back(NameAndTypePair(ProtonConsts::RESERVED_PROCESS_TIME, std::make_shared<DataTypeDateTime64>(3, "UTC")));
    virtual_column_names_and_types.push_back(NameAndTypePair(ProtonConsts::RESERVED_SHARD, std::make_shared<DataTypeInt32>()));
    virtual_column_names_and_types.push_back(NameAndTypePair(ProtonConsts::RESERVED_EVENT_SEQUENCE_ID, std::make_shared<DataTypeInt64>()));
}

/// Validate the topic still exists, specified partitions are still valid etc
void Pulsar::validate(const std::vector<int32_t> & shards_to_query)
{
    std::scoped_lock lock(shards_mutex);
    /// We haven't describe the topic yet
    pulsar::Client client(settings->service_url.value);

    std::vector<std::string> partitions;
    pulsar::Result res = client.getPartitionsForTopic(settings->topic, partitions);
    if (res != pulsar::ResultOk) {
        throw Exception(ErrorCodes::RESOURCE_NOT_FOUND, "{} topic doesn't exist", settings->topic.value);
    }
}

Pipe Pulsar::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t /*num_streams*/)
{
    Block header;

    if (!column_names.empty())
        header = storage_snapshot->getSampleBlockForColumns(column_names);
    else
    {
        auto physical_columns{storage_snapshot->getColumns(GetColumnsOptions(GetColumnsOptions::Ordinary))};
        const auto & any_one_column = physical_columns.front();
        header.insert({any_one_column.type->createColumn(), any_one_column.type, any_one_column.name});
    }
    return Pipe(std::make_shared<PulsarSource>(
        this, std::move(header), std::move(context), max_block_size));
}
}

