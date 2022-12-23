#pragma once

#include "CountedArgValueMap.h"

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteHelpers.h>

namespace DB
{
namespace Streaming
{
namespace
{
template <typename T>
constexpr bool isFixedType()
{
    return std::is_same_v<T, UInt8> || std::is_same_v<T, UInt16> || std::is_same_v<T, UInt32> || std::is_same_v<T, UInt64>
        || std::is_same_v<T, UInt128> || std::is_same_v<T, UInt256> || std::is_same_v<T, Int8> || std::is_same_v<T, Int16>
        || std::is_same_v<T, Int32> || std::is_same_v<T, Int64> || std::is_same_v<T, Int128> || std::is_same_v<T, Int256>
        || std::is_same_v<T, Float32> || std::is_same_v<T, Float64> || std::is_same_v<T, DataTypeDate::FieldType>
        || std::is_same_v<T, DataTypeDateTime::FieldType> || std::is_same_v<T, DateTime64> || std::is_same_v<T, Decimal32>
        || std::is_same_v<T, Decimal64> || std::is_same_v<T, Decimal128>;
}
}

/// We have a combination explosion here : 20 ResType * 20 ValType * 2  = 800 class instantiations
/// For possible values for template parameters, see AggregateFunctionMinMaxAny.h
template <typename ResType, typename ValType, bool maximum>
struct AggregateFunctionArgMinMaxData
{
private:
    CountedArgValueMap<ValType, ResType, maximum> values;

public:
    static bool allocatesMemoryInArena() { return false; }

    static const char * name()
    {
        if constexpr (maximum)
            return "arg_max";
        else
            return "arg_min";
    }

    AggregateFunctionArgMinMaxData() : values(1) { }
    explicit AggregateFunctionArgMinMaxData(int64_t size) : values(size) { }

    static constexpr bool is_nullable = false;

    bool has() const { return !values.empty(); }

    void insertResultInto(IColumn & to) const
    {
        if constexpr (isFixedType<ResType>())
        {
            if (has())
                return assert_cast<ColumnVectorOrDecimal<ResType> &>(to).getData().push_back(values.firstArg());
        }
        else if constexpr (std::is_same_v<ResType, String>)
        {
            if (has())
            {
                const auto & v = values.firstArg();
                return assert_cast<ColumnString &>(to).insertData(v.data(), v.size());
            }
        }
        else
        {
            if (has())
                return to.insert(values.firstArg());
        }

        to.insertDefault();
    }

    void write(WriteBuffer & buf, const ISerialization & serialization_res, const ISerialization & serialization_val) const
    {
        /// Write size first
        writeVarInt(values.size(), buf);

        for (const auto & [val, res_counts] : values)
        {
            /// Write value
            if constexpr (std::is_same_v<ValType, Field>)
                serialization_val.serializeBinary(val, buf, {});
            else
                writeBinary(val, buf);

            /// Write arg count
            writeVarUInt(res_counts->size(), buf);

            /// Write arg / count
            for (const auto & res_count : *res_counts)
            {
                if constexpr (std::is_same_v<ResType, Field>)
                    serialization_res.serializeBinary(res_count.arg, buf, {});
                else
                    writeBinary(res_count.arg, buf);

                writeVarUInt(res_count.count, buf);
            }
        }
    }

    void read(ReadBuffer & buf, const ISerialization & serialization_res, const ISerialization & serialization_val, Arena *)
    {
        Int64 size = 0;
        readVarInt(size, buf);

        assert(size >= 0);

        values.setCapacity(std::max(size, values.capacity()));

        for (Int64 i = 0; i < size; ++i)
        {
            ValType val;

            /// Deserialize val
            if constexpr (std::is_same_v<ValType, Field>)
                serialization_val.deserializeBinary(val, buf, {});
            else
                readBinary(val, buf);

            /// Deserialize arg
            size_t res_size = 0;
            readVarUInt(res_size, buf);

            for (size_t j = 0; j < res_size; ++j)
            {
                ResType res;
                if constexpr (std::is_same_v<ResType, Field>)
                    serialization_res.deserializeBinary(res, buf, {});
                else
                    readBinary(res, buf);

                UInt32 res_count;
                readVarUInt(res_count, buf);

                [[maybe_unused]] auto inserted = values.insert(val, std::move(res), res_count);
                assert(inserted);
            }
        }
    }

    bool add(const IColumn & column_res, const IColumn & column_val, size_t row_num, Arena *)
    {
        if constexpr (isFixedType<ValType>())
        {
            auto val = assert_cast<const ColumnVectorOrDecimal<ValType> &>(column_val).getData()[row_num];

            if constexpr (isFixedType<ResType>())
            {
                auto res = assert_cast<const ColumnVectorOrDecimal<ResType> &>(column_res).getData()[row_num];
                return values.insert(val, res);
            }
            else if constexpr (std::is_same_v<ResType, String>)
            {
                auto res = assert_cast<const ColumnString &>(column_res).getDataAt(row_num).toString();
                return values.insert(val, std::move(res));
            }
            else
            {
                return values.insert(val, column_res[row_num]);
            }
        }
        else if constexpr (std::is_same_v<ValType, String>)
        {
            auto val = assert_cast<const ColumnString &>(column_val).getDataAt(row_num).toString();

            if constexpr (isFixedType<ResType>())
            {
                auto res = assert_cast<const ColumnVectorOrDecimal<ResType> &>(column_res).getData()[row_num];
                return values.insert(std::move(val), res);
            }
            else if constexpr (std::is_same_v<ResType, String>)
            {
                auto res = assert_cast<const ColumnString &>(column_res).getDataAt(row_num).toString();
                return values.insert(std::move(val), std::move(res));
            }
            else
            {
                return values.insert(std::move(val), std::move(column_res[row_num]));
            }
        }
        else
        {
            /// Generic
            if constexpr (isFixedType<ResType>())
            {
                auto res = assert_cast<const ColumnVectorOrDecimal<ResType> &>(column_res).getData()[row_num];
                return values.insert(std::move(column_val[row_num]), res);
            }
            else if constexpr (std::is_same_v<ResType, String>)
            {
                auto res = assert_cast<const ColumnString &>(column_res).getDataAt(row_num).toString();
                return values.insert(std::move(column_val[row_num]), std::move(res));
            }
            else
            {
                return values.insert(std::move(column_val[row_num]), std::move(column_res[row_num]));
            }
        }
    }

    bool negate(const IColumn & column_res, const IColumn & column_val, size_t row_num, Arena *)
    {
        if constexpr (isFixedType<ValType>())
        {
            auto val = assert_cast<const ColumnVectorOrDecimal<ValType> &>(column_val).getData()[row_num];

            if constexpr (isFixedType<ResType>())
            {
                auto res = assert_cast<const ColumnVectorOrDecimal<ResType> &>(column_res).getData()[row_num];
                return values.erase(val, res);
            }
            else if constexpr (std::is_same_v<ResType, String>)
            {
                auto res = assert_cast<const ColumnString &>(column_res).getDataAt(row_num).toView();
                return values.erase(val, res);
            }
            else
            {
                return values.erase(val, std::move(column_res[row_num]));
            }
        }
        else if constexpr (std::is_same_v<ValType, String>)
        {
            auto val = assert_cast<const ColumnString &>(column_val).getDataAt(row_num).toView();

            if constexpr (isFixedType<ResType>())
            {
                auto res = assert_cast<const ColumnVectorOrDecimal<ResType> &>(column_res).getData()[row_num];
                return values.erase(val, res);
            }
            else if constexpr (std::is_same_v<ResType, String>)
            {
                auto res = assert_cast<const ColumnString &>(column_res).getDataAt(row_num).toView();
                return values.erase(val, res);
            }
            else
            {
                return values.erase(val, std::move(column_res[row_num]));
            }
        }
        else
        {
            /// Generic
            if constexpr (isFixedType<ResType>())
            {
                auto res = assert_cast<const ColumnVectorOrDecimal<ResType> &>(column_res).getData()[row_num];
                return values.erase(std::move(column_val[row_num]), res);
            }
            else if constexpr (std::is_same_v<ResType, String>)
            {
                auto res = assert_cast<const ColumnString &>(column_res).getDataAt(row_num).toView();
                return values.erase(std::move(column_val[row_num]), res);
            }
            else
            {
                return values.erase(std::move(column_val[row_num]), std::move(column_res[row_num]));
            }
        }
    }

    bool merge(const AggregateFunctionArgMinMaxData & rhs, Arena *)
    {
        values.merge(rhs.values);
        return true;
    }
};
}
}
