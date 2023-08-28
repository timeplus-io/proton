#include "BlockUtils.h"

#include <Columns/ColumnsNumber.h>
#include <Core/Types.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <Databases/DatabaseFactory.h>
#include <Interpreters/Context.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int OK;
}

namespace Streaming
{
Block buildBlock(
    const std::vector<std::pair<String, String>> & string_cols,
    const std::vector<std::pair<String, Int32>> & int32_cols,
    const std::vector<std::pair<String, UInt64>> & uint64_cols)
{
    Block block;
    const DataTypeFactory & data_type_factory = DataTypeFactory::instance();

    auto string_type = data_type_factory.get("string", nullptr);
    for (const auto & p : string_cols)
    {
        auto col = string_type->createColumn();
        col->insertData(p.second.data(), p.second.size());
        ColumnWithTypeAndName col_with_type(std::move(col), string_type, p.first);
        block.insert(col_with_type);
    }

    auto int32_type = data_type_factory.get("int32", nullptr);
    for (const auto & p : int32_cols)
    {
        auto col = int32_type->createColumn();
        auto int32_col = typeid_cast<ColumnInt32 *>(col.get());
        int32_col->insertValue(p.second);
        ColumnWithTypeAndName col_with_type(std::move(col), int32_type, p.first);
        block.insert(col_with_type);
    }

    auto uint64_type = data_type_factory.get("uint64", nullptr);
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

    auto string_type = data_type_factory.get("string", nullptr);
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

    auto int64_type = data_type_factory.get("int64", nullptr);
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
}
}
