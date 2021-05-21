#include "BlockUtils.h"

#include <Core/Types.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <Databases/DatabaseFactory.h>
#include <DistributedWriteAheadLog/DistributedWriteAheadLogKafka.h>
#include <DistributedWriteAheadLog/DistributedWriteAheadLogPool.h>
#include <DistributedWriteAheadLog/IDistributedWriteAheadLog.h>
#include <Interpreters/Context.h>
#include <Common/typeid_cast.h>

#include <common/logger_useful.h>


namespace DB
{
namespace
{
    constexpr Int32 MAX_RETRIES = 3;
}

namespace ErrorCodes
{
    extern const int CONFIG_ERROR;
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
    IDistributedWriteAheadLog::OpCode opCode,
    const Poco::Logger * log)
{
    IDistributedWriteAheadLog::Record record{opCode, std::move(block)};
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

    auto wal = DistributedWriteAheadLogPool::instance(context->getGlobalContext()).getMeta();
    if (!wal)
    {
        LOG_ERROR(
            log,
            "Distributed environment is not setup. Unable to operate with DistributedMergeTree engine. query_id={} ",
            context->getCurrentQueryId());
        throw Exception(
            "Distributed environment is not setup. Unable to operate with DistributedMergeTree engine", ErrorCodes::CONFIG_ERROR);
    }

    const auto & config = context->getGlobalContext()->getConfigRef();
    auto topic = config.getString("cluster_settings.system_ddls.name");
    std::any ctx{DistributedWriteAheadLogKafkaContext{topic}};

    auto result_code = ErrorCodes::OK;
    const auto & query_id = context->getCurrentQueryId();
    for (auto i = 0; i < MAX_RETRIES; ++i)
    {
        result_code = wal->append(record, ctx).err;
        if (result_code == ErrorCodes::OK)
        {
            LOG_INFO(log, "Successfully append record to DistributedWriteAheadLog, query_id={}", query_id);
            return;
        }

        LOG_WARNING(
            log,
            "Failed to append record to DistributedWriteAheadLog, query_id={}, error={}, tried_times={}",
            query_id,
            result_code,
            i + 1);

        if (i < MAX_RETRIES - 1)
        {
            LOG_INFO(log, "Sleep for a while and will try to append record again, query_id={}", query_id);
            std::this_thread::sleep_for(std::chrono::milliseconds(1000 * (2 << i)));
        }
    }
    LOG_ERROR(log, "Failed to append record to DistributedWriteAheadLog, query_id={}, error={}", context->getCurrentQueryId(), result_code);
    throw Exception("Failed to append record to DistributedWriteAheadLog, error={}", result_code);
}

}
