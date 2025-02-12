#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Common/ArenaAllocator.h>
#include <Columns/ColumnString.h>
#include <Interpreters/castColumn.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeString.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

namespace DB
{
struct Settings;

struct GroupConcatDataBase
{
    UInt64 data_size = 0;
    UInt64 allocated_size = 0;
    char * data = nullptr;

    void checkAndUpdateSize(UInt64 add, Arena * arena)
    {
        if (data_size + add >= allocated_size)
        {
            auto old_size = allocated_size;
            allocated_size = std::max(2 * allocated_size, data_size + add);
            data = arena->realloc(data, old_size, allocated_size);
        }
    }

    void insertChar(const char * str, UInt64 str_size, Arena * arena)
    {
        checkAndUpdateSize(str_size, arena);
        memcpy(data + data_size, str, str_size);
        data_size += str_size;
    }

    void insert(const IColumn * column, const SerializationPtr & serialization, size_t row_num, Arena * arena)
    {
        WriteBufferFromOwnString buff;
        serialization->serializeText(*column, row_num, buff, FormatSettings{});
        auto string = buff.stringRef();
        insertChar(string.data, string.size, arena);
    }
};

struct GroupConcatData : public GroupConcatDataBase
{
    using Offset = UInt64;
    using Allocator = MixedAlignedArenaAllocator<alignof(Offset), 4096>;
    using Offsets = PODArray<Offset, 32, Allocator>;

    Offsets offsets;
    UInt64 num_rows = 0;

    UInt64 getSize(size_t i) const
    {
        return offsets[i * 2 + 1] - offsets[i * 2];
    }

    UInt64 getString(size_t i) const
    {
        return offsets[i * 2];
    }

    void insert(const IColumn * column, const SerializationPtr & serialization, size_t row_num, Arena * arena)
    {
        WriteBufferFromOwnString buff;
        serialization->serializeText(*column, row_num, buff, {});
        auto string = buff.stringRef();

        checkAndUpdateSize(string.size, arena);
        memcpy(data + data_size, string.data, string.size);
        offsets.push_back(data_size, arena);
        data_size += string.size;
        offsets.push_back(data_size, arena);
        num_rows++;
    }
};

template <bool has_limit>
class AggregateFunctionGroupConcat : public IAggregateFunctionDataHelper<GroupConcatData, AggregateFunctionGroupConcat<has_limit>>
{
    static constexpr auto name = "groupConcat";

    SerializationPtr serialization;
    UInt64 limit;
    const String delimiter;
    const DataTypePtr type;

public:
    AggregateFunctionGroupConcat(const DataTypePtr & data_type_, const Array & parameters_, UInt64 limit_, const String & delimiter_)
        : IAggregateFunctionDataHelper<GroupConcatData, AggregateFunctionGroupConcat<has_limit>>(
            {data_type_}, parameters_)
        , limit(limit_)
        , delimiter(delimiter_)
        , type(data_type_)
    {
        serialization = isFixedString(type) ? std::make_shared<DataTypeString>()->getDefaultSerialization() : this->argument_types[0]->getDefaultSerialization();
    }

    String getName() const override { return "group_concat"; }

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeString>(); }

    void add( AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        auto & cur_data = this->data(place);

        if constexpr (has_limit)
        {
            if (cur_data.num_rows >= limit)
                return;
        }

        if (cur_data.data_size != 0)
            cur_data.insertChar(delimiter.c_str(), delimiter.size(), arena);

        if (isFixedString(type))
        {
            ColumnWithTypeAndName col = {columns[0]->getPtr(), type, "column"};
            const auto & col_str = castColumn(col, std::make_shared<DataTypeString>());
            cur_data.insert(col_str.get(), serialization, row_num, arena);
        }
        else
        {
            cur_data.insert(columns[0], serialization, row_num, arena);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & cur_data = this->data(place);
        auto & rhs_data = this->data(rhs);

        if (rhs_data.data_size == 0)
            return;

        if constexpr (has_limit)
        {
            UInt64 new_elems_count = std::min(rhs_data.num_rows, limit - cur_data.num_rows);
            for (UInt64 i = 0; i < new_elems_count; ++i)
            {
                if (cur_data.data_size != 0)
                    cur_data.insertChar(delimiter.c_str(), delimiter.size(), arena);

                cur_data.offsets.push_back(cur_data.data_size, arena);
                cur_data.insertChar(rhs_data.data + rhs_data.getString(i), rhs_data.getSize(i), arena);
                cur_data.num_rows++;
                cur_data.offsets.push_back(cur_data.data_size, arena);
            }
        }
        else
        {
            if (cur_data.data_size != 0)
                cur_data.insertChar(delimiter.c_str(), delimiter.size(), arena);

            cur_data.insertChar(rhs_data.data, rhs_data.data_size, arena);
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        auto & cur_data = this->data(place);

        writeVarUInt(cur_data.data_size, buf);

        buf.write(cur_data.data, cur_data.data_size);

        if constexpr (has_limit)
        {
            writeVarUInt(cur_data.num_rows, buf);
            for (const auto & offset : cur_data.offsets)
                writeVarUInt(offset, buf);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        auto & cur_data = this->data(place);

        UInt64 temp_size = 0;
        readVarUInt(temp_size, buf);

        cur_data.checkAndUpdateSize(temp_size, arena);

        buf.readStrict(cur_data.data + cur_data.data_size, temp_size);
        cur_data.data_size = temp_size;

        if constexpr (has_limit)
        {
            readVarUInt(cur_data.num_rows, buf);
            cur_data.offsets.resize_exact(cur_data.num_rows * 2, arena);
            for (auto & offset : cur_data.offsets)
                readVarUInt(offset, buf);
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & cur_data = this->data(place);

        if (cur_data.data_size == 0)
        {
            to.insertDefault();
            return;
        }

        auto & column_string = assert_cast<ColumnString &>(to);
        column_string.insertData(cur_data.data, cur_data.data_size);
    }

    bool allocatesMemoryInArena() const override { return true; }

};

}
