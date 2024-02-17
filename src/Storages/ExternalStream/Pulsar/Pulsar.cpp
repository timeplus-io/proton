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
    ExternalStreamCounterPtr,
    ContextPtr)
    : StorageExternalStreamImpl(std::move(settings_))
    , storage_id(storage->getStorageID())
    , log(&Poco::Logger::get("External-PulsarLog"))
{
}

NamesAndTypesList Pulsar::getVirtuals() const
{
    NamesAndTypesList n;
    return n;
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
        this, std::move(header), std::move(context), max_block_size, log));
}
}
