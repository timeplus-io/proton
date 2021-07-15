#include "BlockUtils.h"

#include <Columns/ColumnsNumber.h>
#include <Core/Types.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <Databases/DatabaseFactory.h>
#include <DistributedMetadata/DDLService.h>
#include <Interpreters/Context.h>
#include <Common/typeid_cast.h>
#include <common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int OK;
}

Block buildBlock(
    const std::vector<std::pair<String, String>> & string_cols,
    const std::vector<std::pair<String, Int32>> & int32_cols,
    const std::vector<std::pair<String, UInt64>> & uint64_cols)
{
    Block block;
    const DataTypeFactory & data_type_factory = DataTypeFactory::instance();

    auto string_type = data_type_factory.get(getTypeName(TypeIndex::String));
    for (const auto & p : string_cols)
    {
        auto col = string_type->createColumn();
        col->insertData(p.second.data(), p.second.size());
        ColumnWithTypeAndName col_with_type(std::move(col), string_type, p.first);
        block.insert(col_with_type);
    }

    auto int32_type = data_type_factory.get(getTypeName(TypeIndex::Int32));
    for (const auto & p : int32_cols)
    {
        auto col = int32_type->createColumn();
        auto int32_col = typeid_cast<ColumnInt32 *>(col.get());
        int32_col->insertValue(p.second);
        ColumnWithTypeAndName col_with_type(std::move(col), int32_type, p.first);
        block.insert(col_with_type);
    }

    auto uint64_type = data_type_factory.get(getTypeName(TypeIndex::UInt64));
    for (const auto & p : uint64_cols)
    {
        auto col = uint64_type->createColumn();
        auto uint64_col = typeid_cast<ColumnUInt64 *>(col.get());
        uint64_col->insertValue(p.second);
        ColumnWithTypeAndName col_with_type(std::move(col), uint64_type, p.first);
        block.insert(col_with_type);
    }

    return block;
}

Block buildBlock(
    const std::vector<std::pair<String, std::vector<String>>> & string_cols,
    const std::vector<std::pair<String, std::vector<Int64>>> & int64_cols)
{
    Block block;
    const DataTypeFactory & data_type_factory = DataTypeFactory::instance();

    auto string_type = data_type_factory.get(getTypeName(TypeIndex::String));
    for (const auto & p : string_cols)
    {
        auto col = string_type->createColumn();
        for (auto v = p.second.begin(); v != p.second.end(); ++v)
        {
            col->insertData(v->data(), v->size());
        }

        ColumnWithTypeAndName col_with_type(std::move(col), string_type, p.first);
        block.insert(col_with_type);
    }

    auto int64_type = data_type_factory.get(getTypeName(TypeIndex::Int64));
    for (const auto & p : int64_cols)
    {
        auto col = int64_type->createColumn();
        auto int64_col = typeid_cast<ColumnInt64 *>(col.get());
        for (auto v = p.second.begin(); v != p.second.end(); ++v)
        {
            int64_col->insertValue(*v);
        }

        ColumnWithTypeAndName col_with_type(std::move(col), int64_type, p.first);
        block.insert(col_with_type);
    }

    return block;
}

void appendDDLBlock(
    Block && block,
    ContextPtr context,
    const std::vector<String> & parameter_names,
    DWAL::OpCode opCode,
    const Poco::Logger * log)
{
    DWAL::Record record{opCode, std::move(block)};
    record.headers["_version"] = "1";

    for (const auto & parameter_name : parameter_names)
    {
        const auto & query_params = context->getQueryParameters();
        auto iter = query_params.find(parameter_name);

        if (iter != query_params.end())
        {
            record.headers[parameter_name] = iter->second;
        }
    }

    /// Depending on DDLService is not ideal here, but it is convenient
    auto & ddl_service = DDLService::instance(context);

    const auto & query_id = context->getCurrentQueryId();
    const auto & result_code = ddl_service.append(record);
    if (result_code != ErrorCodes::OK)
    {
        LOG_ERROR(log, "Failed to append record to WAL, query_id={}, error={}", query_id, result_code);
        throw Exception("Failed to append record to WAL, error={}", result_code);
    }

    LOG_INFO(log, "Successfully append record to WAL, query_id={}", query_id);
}

}
