#pragma once

#include <type_traits>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnsCommon.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/AggregateFunctionSum.h>
#include <Core/DecimalFunctions.h>
#include <Common/logger_useful.h>
#include <Poco/Logger.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

struct Settings;

template <typename T> constexpr bool DecimalOrExtendedInt =
    is_decimal<T>
    || std::is_same_v<T, Int128>
    || std::is_same_v<T, Int256>
    || std::is_same_v<T, UInt128>
    || std::is_same_v<T, UInt256>;

/**
 * Helper class to encapsulate values conversion for avg and avgWeighted.
 */
template <typename Numerator, typename Denominator>
struct AvgTimeFraction
{

    struct Last
    {
        Numerator last_value;
        Denominator last_time;
    };
    std::optional<Last> last;
    std::optional<Denominator> current_time;

};


// template <typename T, typename U>
// using MaxFieldType = std::conditional_t<(sizeof(AvgTimeWeightedFieldType<T>) > sizeof(AvgTimeWeightedFieldType<U>)),
//     AvgTimeWeightedFieldType<T>, AvgTimeWeightedFieldType<U>>;

template <typename Value, typename TimeWeight>
class AggregateFunctionTimeWeighted:
    public IAggregateFunctionDataHelper<AvgTimeFraction<Value, NearestFieldType<TimeWeight>>, 
                                        AggregateFunctionTimeWeighted<Value, TimeWeight>>

{
protected:
    AggregateFunctionPtr nested_func;
    size_t prefix_size;
    size_t arguments_num;
    Poco::Logger * logger;

    AggregateDataPtr getNestedPlace(AggregateDataPtr __restrict place) const noexcept
    {
        return place + prefix_size;
    }

    ConstAggregateDataPtr getNestedPlace(ConstAggregateDataPtr __restrict place) const noexcept
    {
        return place + prefix_size;
    }
public:
    using Base = IAggregateFunctionDataHelper<AvgTimeFraction<Value, NearestFieldType<TimeWeight>>, 
                                        AggregateFunctionTimeWeighted<Value, TimeWeight>>;

    using Numerator = Value;
    using Denominator = NearestFieldType<TimeWeight>;
    using Fraction = AvgTimeFraction<Numerator, Denominator>;
    AggregateFunctionTimeWeighted(AggregateFunctionPtr nested_func_, const DataTypes & arguments, const Array & params_)
    : Base(arguments, params_)
    , nested_func(nested_func_)
    , arguments_num(arguments.size())
    , logger(&Poco::Logger::get("AggregateFunctionTimeWeighted"))
    {
        size_t nested_size = nested_func->alignOfData();
        prefix_size = (sizeof(AvgTimeFraction<Value, NearestFieldType<TimeWeight>>) + nested_size - 1) / nested_size * nested_size;
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "merge() function isn't implemented for {}", getName());
    }

    void addBatchSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        Arena * arena,
        ssize_t if_argument_pos,
        const IColumn * delta_col [[maybe_unused]]) const final
    {
        auto & data = this->data(place);

        MutableColumnPtr value_column, weight_column;
        const auto & value_data = assert_cast<const ColumnVectorOrDecimal<Value> &>(*columns[0]).getData();
        const auto & time_data = assert_cast<const ColumnVectorOrDecimal<TimeWeight> &>(*columns[1]).getData();
        auto last_row_pos = row_end - 1;

        /// last time caculation
        if (data.last.has_value())
        {
            value_column = this->argument_types[0]->createColumn();
            weight_column = ColumnUInt64::create();
            value_column->insert(data.last->last_value);
            weight_column->insert(static_cast<UInt64>(time_data[0] - data.last->last_time));
            
            ColumnRawPtrs raw_columns{value_column.get(), weight_column.get()};
            nested_func->add(getNestedPlace(place), raw_columns.data(), 0, arena);
        }

        /// caculate time
        weight_column = ColumnUInt64::create();
        if (if_argument_pos >= 0)
        {
            const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            for (size_t i = row_begin; i < last_row_pos; i++)
            {
                if (flags[i])
                {
                    if (time_data[i + 1] < time_data[i])
                        LOG_WARNING(logger, "Illegal time argument, should be in ascending order, {}, {}" ,time_data[i] ,time_data[i + 1]);
                    else
                        weight_column->insert(static_cast<UInt64>(time_data[i + 1] - time_data[i]));
                }
            }
        }
        else
        {
            for (size_t i = row_begin; i < last_row_pos; i++)
            {
                if (time_data[i + 1] < time_data[i])
                    LOG_WARNING(logger, "Illegal time argument, should be in ascending order, {}, {}" ,time_data[i] ,time_data[i + 1]);
                else
                    weight_column->insert(static_cast<UInt64>(time_data[i + 1] - time_data[i]));
            }
        }

        //weight_column->insertDefault();
        /// prepare data
        ColumnRawPtrs raw_columns{*columns[0].get(), weight_column.get()};

        nested_func-> addBatchSinglePlace(row_begin, last_row_pos, getNestedPlace(place), raw_columns.data(), arena, if_argument_pos);
        
        // if (data.last_value.has_value())
        // {
        //     data.last_value.value() = static_cast<Numerator>(value_data[row_end - 1]);
        //     data.last_time.value() = static_cast<Denominator>(time_data[row_end - 1]);
        // }
        // else
        // {
        data.last->last_value = static_cast<Numerator>(value_data[last_row_pos]);
        data.last->last_time = static_cast<Denominator>(time_data[last_row_pos]);
        /// remember current time
        if (this->argument_types.size() == 3)
            data.current_time = assert_cast<const ColumnVectorOrDecimal<TimeWeight> &>(*columns[2]).getData()[last_row_pos];

        // }
    }

    void addBatchSinglePlaceNotNull(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        const UInt8 * null_map,
        Arena * arena,
        ssize_t if_argument_pos,
        const IColumn * delta_col [[maybe_unused]])
        const final
    {
        auto & data = this->data(place);
        MutableColumnPtr value_column, weight_column;
        const auto & value_data = assert_cast<const ColumnVectorOrDecimal<Value> &>(*columns[0]).getData();
        const auto & time_data = assert_cast<const ColumnVectorOrDecimal<TimeWeight> &>(*columns[1]).getData();
        auto last_row_pos = row_end - 1;

        /// last time caculation
        if (data.last.has_value())
        {
            value_column = this->argument_types[0]->createColumn();
            weight_column = ColumnUInt64::create();
            value_column->insert(data.last->last_value);
            weight_column->insert(static_cast<UInt64>(time_data[0] - data.last->last_time));
            
            ColumnRawPtrs raw_columns{value_column.get(), weight_column.get()};
            nested_func->add(getNestedPlace(place), raw_columns.data(), 0, arena);
        }

        /// caculate time
        weight_column = ColumnUInt64::create();
        if (if_argument_pos >= 0)
        {
            const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            for (size_t i = row_begin; i < row_end - 1; i++)
            {
                if (flags[i] && !null_map[i])
                {
                    if (value_column[i + 1] < value_column[i])
                        LOG_WARNING(logger, "Illegal time argument, should be in ascending order, {}, {}" ,value_column[i] ,value_column[i + 1]);
                    else
                        weight_column->insert(static_cast<UInt64>(value_column[i + 1] - value_column[i]));
                }
            }
        }
        else
        {
            for (size_t i = row_begin; i < row_end - 1; i++)
            {
                if (!null_map[i])
                {
                    if (value_column[i + 1] < value_column[i])
                        LOG_WARNING(logger, "Illegal time argument, should be in ascending order, {}, {}" ,value_column[i] ,value_column[i + 1]);
                    else
                        weight_column->insert(static_cast<UInt64>(value_column[i + 1] - value_column[i]));
                }
            }
        }
        //weight_column->insertDefault();

        ColumnRawPtrs raw_columns{columns[0].get(), weight_column.get()};

        nested_func-> addBatchSinglePlace(row_begin, last_row_pos, getNestedPlace(place), raw_columns.data(), arena, if_argument_pos);
        
        // if (data.last_value.has_value())
        // {
        //     data.last_value.value() = static_cast<Numerator>(value_data[row_end - 1]);
        //     data.last_time.value() = static_cast<Denominator>(time_data[row_end - 1]);
        // }
        // else
        // {
        data.last->last_value = static_cast<Numerator>(value_data[last_row_pos]);
        data.last->last_time = static_cast<Denominator>(time_data[last_row_pos]);
        /// remember current time
        if (this->argument_types.size() == 3)
            data.current_time = assert_cast<const ColumnVectorOrDecimal<TimeWeight> &>(*columns[2]).getData()[last_row_pos];

        // }

    }
    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        /// FIXME, time disorder may happen, the outcome might not be accurate
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "merge() function isn't implemented for {}", getName());
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        // if (this->data(place).last_value.has_value() && this->data(place).last_time.has_value())
        // {
        //     writeBinary(this->data(place).last_value.value(), buf);
        //     if constexpr (std::is_unsigned_v<Denominator>)
        //         writeVarUInt(this->data(place).last_time.value(), buf);
        //     else
        //         writeBinary(this->data(place).last_time.value(), buf);
        // }
        // if (this->data(place).current_time.has_value())
        // {
        //     if constexpr (std::is_unsigned_v<Denominator>)
        //         writeVarUInt(this->data(place).current_time.value(), buf);
        //     else
        //         writeBinary(this->data(place).current_time.value(), buf);
        // }
        // nested_func->serialize(getNestedPlace(place), buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        // if (this->data(place).last_value.has_value() && this->data(place).last_time.has_value())
        // {
        //     readBinary(this->data(place).last_value.value(), buf);
        //     if constexpr (std::is_unsigned_v<Denominator>)
        //         readVarUInt(this->data(place).last_time.value(), buf);
        //     else /// Floating point denominator type can be used
        //         readBinary(this->data(place).last_time.value(), buf);
        // }
        // if (this->data(place).current_time.has_value())
        // {
        //     if constexpr (std::is_unsigned_v<Denominator>)
        //         readVarUInt(this->data(place).current_time.value(), buf);
        //     else
        //         readBinary(this->data(place).current_time.value(), buf);
        // }
        // nested_func->deserialize(getNestedPlace(place), buf, std::nullopt /* version */, arena);
    }

    void insertResultIntoImpl(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const
    {
        auto & data = this->data(place);
        if (data.current_time.has_value())
        {
            MutableColumnPtr value_column, weight_column;
            ColumnRawPtrs argument_raw_columns(2);

            chassert(data.last.has_value());
            value_column = this->argument_types[0]->createColumn();
            weight_column = ColumnUInt64::create();
            value_column->insert(data.last->last_value);
            weight_column->insert(data.current_time.value() - data.last->last_value);

            for (size_t i = 0; i < argument_columns.size(); ++i)
                argument_raw_columns[i] = argument_columns[i].get();

            nested_func -> add(getNestedPlace(place), argument_raw_columns.data(), 0, arena);
        }

        // assert(!data.arguments.empty());

        nested_func->insertResultInto(getNestedPlace(place), to, arena);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        insertResultIntoImpl(place, to, arena);
    }

    void insertMergeResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        insertResultIntoImpl(place, to, arena);
    }

    size_t sizeOfData() const override
    {
        return prefix_size + nested_func->sizeOfData();
    }

    void create(AggregateDataPtr __restrict place) const override
    {
        new (place) AvgTimeFraction<Value, NearestFieldType<TimeWeight>>;
        nested_func->create(getNestedPlace(place));
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override
    {
        this->data(place).~AvgTimeFraction<Value, NearestFieldType<TimeWeight>>();
        nested_func->destroy(getNestedPlace(place));
    }

    bool hasTrivialDestructor() const override
    {
        return std::is_trivially_destructible_v<AvgTimeFraction<Value, NearestFieldType<TimeWeight>>> && nested_func->hasTrivialDestructor();
    }

    void destroyUpToState(AggregateDataPtr __restrict place) const noexcept override
    {
        this->data(place).~AvgTimeFraction<Value, NearestFieldType<TimeWeight>>();
        nested_func->destroyUpToState(getNestedPlace(place));
    }

    String getName() const override
    {
        return nested_func->getName() + "_time";
    }

    DataTypePtr getReturnType() const override
    {
        return nested_func->getReturnType();
    }

    bool allocatesMemoryInArena() const override
    {
        return true;
    }

    bool isState() const override
    {
        return nested_func->isState();
    }

    bool isVersioned() const override
    {
        return nested_func->isVersioned();
    }

    size_t getVersionFromRevision(size_t revision) const override
    {
        return nested_func->getVersionFromRevision(revision);
    }

    size_t getDefaultVersion() const override
    {
        return nested_func->getDefaultVersion();
    }

    AggregateFunctionPtr getNestedFunction() const override { return nested_func; }

};
}