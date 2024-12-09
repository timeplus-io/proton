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
#include <Core/Field.h>
#include <Common/logger_useful.h>
#include <Poco/Logger.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

struct Settings;

template <typename TimeType>
struct TimeWeightedData
{

    struct Last
    {
        Field last_value;
        TimeType last_time;
    };
    std::optional<Last> last;
    std::optional<TimeType> start_time;
    std::optional<TimeType> end_time;

};

template <typename Value, typename TimeWeight>
class AggregateFunctionTimeWeighted:
    public IAggregateFunctionDataHelper<TimeWeightedData<TimeWeight>, 
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
    using Base = IAggregateFunctionDataHelper<TimeWeightedData<TimeWeight>, 
                                        AggregateFunctionTimeWeighted<Value, TimeWeight>>;

    AggregateFunctionTimeWeighted(AggregateFunctionPtr nested_func_, const DataTypes & arguments, const Array & params_)
    : Base(arguments, params_)
    , nested_func(nested_func_)
    , arguments_num(arguments.size())
    , logger(&Poco::Logger::get("AggregateFunctionTimeWeighted"))
    {
        size_t nested_size = nested_func->alignOfData();
        prefix_size = (sizeof(TimeWeightedData<TimeWeight>) + nested_size - 1) / nested_size * nested_size;
    }

    void last_time_calculation(size_t row_begin, size_t row_end, AggregateDataPtr __restrict place, const IColumn ** columns, Arena * arena) const
    {
        auto & data = this->data(place);
        const auto & time_data = assert_cast<const ColumnVectorOrDecimal<TimeWeight> &>(*columns[1]).getData();
        /// last time caculation
        if (data.last.has_value())
        {
            MutableColumnPtr value_column, weight_column;
            value_column = this->argument_types[0]->createColumn();
            weight_column = ColumnUInt64::create();
            if (time_data[row_begin] >= data.last->last_time) [[likely]]
            {
                value_column->insert(data.last->last_value);
                weight_column->insert(static_cast<UInt64>(time_data[row_begin] - data.last->last_time));
            }
            else
            {
                LOG_WARNING(logger, "Illegal time argument, should be in ascending order, {}, {}" ,data.last->last_time ,time_data[row_begin]);
            }
            ColumnRawPtrs raw_columns{value_column.get(), weight_column.get()};
            nested_func->add(getNestedPlace(place), raw_columns.data(), 0, arena);
        }
        
        const auto & value_data = assert_cast<const ColumnVectorOrDecimal<Value> &>(*columns[0]).getData();
        auto last_row_pos = row_end - 1;
        data.last = {
            static_cast<Value>(value_data[last_row_pos]),
            static_cast<TimeWeight>(time_data[last_row_pos])
        };
        /// remember start time
        data.start_time = time_data[row_begin];
        /// remember current time
        if (this->argument_types.size() == 3)
            data.end_time = assert_cast<const ColumnVectorOrDecimal<TimeWeight> &>(*columns[2]).getData()[last_row_pos];
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
      
        last_time_calculation(row_num, row_num + 1, place, columns, arena);
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

        if (if_argument_pos >= 0 || delta_col != nullptr)
            return nested_func->addBatchSinglePlace(row_begin, row_end, place, columns, arena, if_argument_pos, delta_col);
        else if (row_end - row_begin == 1)
            return add(place, columns, 0, arena);

        const auto & time_data = assert_cast<const ColumnVectorOrDecimal<TimeWeight> &>(*columns[1]).getData();
        last_time_calculation(row_begin, row_end, place, columns, arena);

        auto last_row_pos = row_end - 1;
        /// caculate time
        MutableColumnPtr weight_column = ColumnUInt64::create();
        for (size_t i = row_begin; i < last_row_pos; i++)
        {
            if (time_data[i + 1] >= time_data[i]) [[likely]]
                weight_column->insert(static_cast<UInt64>(time_data[i + 1] - time_data[i]));
            else
                LOG_WARNING(logger, "Illegal time argument, should be in ascending order, {}, {}" ,time_data[i] ,time_data[i + 1]);
        }
        /// prepare data
        ColumnRawPtrs raw_columns{columns[0], weight_column.get()};

        nested_func->addBatchSinglePlace(row_begin, last_row_pos, getNestedPlace(place), raw_columns.data(), arena, if_argument_pos, delta_col);
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
        if (if_argument_pos >= 0 || delta_col != nullptr)
            return nested_func->addBatchSinglePlaceNotNull(row_begin, row_end, place, columns, null_map, arena, if_argument_pos, delta_col);
        else if (row_end - row_begin == 1)
            return add(place, columns, 0, arena);

        // const auto & value_data = assert_cast<const ColumnVectorOrDecimal<Value> &>(*columns[0]).getData();
        const auto & time_data = assert_cast<const ColumnVectorOrDecimal<TimeWeight> &>(*columns[1]).getData();

        last_time_calculation(row_begin, row_end, place, columns, arena);        

        auto last_row_pos = row_end - 1;
        /// caculate time
        MutableColumnPtr weight_column = ColumnUInt64::create();
        for (size_t i = row_begin; i < row_end - 1; i++)
        {
            if (!null_map[i])
            {
                if (time_data[i + 1] < time_data[i])
                    LOG_WARNING(logger, "Illegal time argument, should be in ascending order, {}, {}" ,time_data[i] ,time_data[i + 1]);
                else
                    weight_column->insert(static_cast<UInt64>(time_data[i + 1] - time_data[i]));
            }
        }
        ColumnRawPtrs raw_columns{columns[0], weight_column.get()};

        nested_func-> addBatchSinglePlaceNotNull(row_begin, last_row_pos, getNestedPlace(place), raw_columns.data(), null_map, arena, if_argument_pos, delta_col);
    }
    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        /// FIXME, time disorder may happen, the outcome might not be accurate
        auto & data = this->data(place);
        auto & rhs_data = this->data(rhs);
        if (data.last.has_value())
        {
            if (rhs_data.start_time.has_value())
            {
                MutableColumnPtr value_column, weight_column;
                value_column = this->argument_types[0]->createColumn();
                weight_column = ColumnUInt64::create();
                if (rhs_data.start_time.value() >= data.last->last_time)
                {
                    value_column->insert(data.last->last_value);
                    weight_column->insert(static_cast<UInt64>(rhs_data.start_time.value() - data.last->last_time));
                    if (rhs_data.last.has_value())
                        data.last = rhs_data.last;
                    if (rhs_data.end_time.has_value())
                        data.end_time = rhs_data.end_time;
                }
                else
                {
                    if (data.start_time.has_value())
                    {
                        if (data.start_time.value() >= rhs_data.last->last_time)
                        {
                            value_column->insert(rhs_data.last->last_value);
                            weight_column->insert(static_cast<UInt64>(data.last->last_time - rhs_data.start_time.value()));
                            data.start_time.value() = rhs_data.start_time.value();
                        }
                        else
                        {
                            LOG_WARNING(logger, "Illegal time argument, should be in ascending order, {}, {}" ,data.last->last_time ,rhs_data.start_time.value());
                        }
                    }
                }
                ColumnRawPtrs raw_columns{value_column.get(), weight_column.get()};
                nested_func->add(getNestedPlace(place), raw_columns.data(), 0, arena);
            }
        }
        else
        {
            if (rhs_data.last.has_value())
                data.last = rhs_data.last;
            if (rhs_data.start_time.has_value())
                data.start_time = rhs_data.start_time;
            if (rhs_data.end_time.has_value())
                data.end_time = rhs_data.end_time;
        }

        nested_func->merge(getNestedPlace(place), rhs, arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        writeBinary(this->data(place).last.has_value(), buf);
        if (this->data(place).last.has_value())
        {
            writeBinary(true, buf);
            writeFieldBinary(this->data(place).last->last_value, buf);
            if constexpr (std::is_unsigned_v<TimeWeight>)
                writeVarUInt(this->data(place).last->last_time, buf);
            else
                writeBinary(this->data(place).last->last_time, buf);
        }

        writeBinary(this->data(place).start_time.has_value(), buf);
        if (this->data(place).start_time.has_value())
        {
            writeBinary(true, buf);
            if constexpr (std::is_unsigned_v<TimeWeight>)
                writeVarUInt(this->data(place).start_time.value(), buf);
            else
                writeBinary(this->data(place).start_time.value(), buf);
        }

        writeBinary(this->data(place).end_time.has_value(), buf);
        if (this->data(place).end_time.has_value())
        {
            writeBinary(true, buf);
            if constexpr (std::is_unsigned_v<TimeWeight>)
                writeVarUInt(this->data(place).end_time.value(), buf);
            else
                writeBinary(this->data(place).end_time.value(), buf);
        }
        nested_func->serialize(getNestedPlace(place), buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        bool last_has_value, start_has_value, end_has_value;
        readBinary(last_has_value, buf);
        if(last_has_value)
        {
            this->data(place).last->last_value = readFieldBinary(buf);
            if constexpr (std::is_unsigned_v<TimeWeight>)
                readVarUInt(this->data(place).last->last_time, buf);
            else /// Floating point TimeWeight type can be used
                readBinary(this->data(place).last->last_time, buf);
        }

        readBinary(start_has_value, buf);
        if(start_has_value)
        {
            if constexpr (std::is_unsigned_v<TimeWeight>)
                readVarUInt(this->data(place).start_time.value(), buf);
            else
                readBinary(this->data(place).start_time.value(), buf);
        }

        readBinary(end_has_value, buf);
        if(end_has_value)
        {
            if constexpr (std::is_unsigned_v<TimeWeight>)
                readVarUInt(this->data(place).end_time.value(), buf);
            else
                readBinary(this->data(place).end_time.value(), buf);
        }

        nested_func->deserialize(getNestedPlace(place), buf, std::nullopt /* version */, arena);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        auto & data = this->data(place);
        if (data.end_time.has_value())
        {
            MutableColumnPtr value_column, weight_column;

            chassert(data.last.has_value());
            value_column = this->argument_types[0]->createColumn();
            weight_column = ColumnUInt64::create();
            if (data.end_time.value() >= data.last->last_time) [[likely]]
            {
                value_column->insert(data.last->last_value);
                weight_column->insert(static_cast<UInt64>(data.end_time.value() - data.last->last_time));
            }
            else
            {
                LOG_WARNING(logger, "Illegal time argument, should be in ascending order, {}, {}" ,data.last->last_time ,data.end_time.value());
            }
            

            ColumnRawPtrs raw_columns{value_column.get(), weight_column.get()};

            nested_func -> add(getNestedPlace(place), raw_columns.data(), 0, arena);
        }

        // assert(!data.arguments.empty());

        nested_func->insertResultInto(getNestedPlace(place), to, arena);
    }

    // void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    // {
    //     insertResultIntoImpl(place, to, arena);
    // }

    // void insertMergeResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    // {
    //     insertResultIntoImpl(place, to, arena);
    // }

    size_t sizeOfData() const override
    {
        return prefix_size + nested_func->sizeOfData();
    }

    void create(AggregateDataPtr __restrict place) const override
    {
        new (place) TimeWeightedData<TimeWeight>;
        nested_func->create(getNestedPlace(place));
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override
    {
        this->data(place).~TimeWeightedData<TimeWeight>();
        nested_func->destroy(getNestedPlace(place));
    }

    bool hasTrivialDestructor() const override
    {
        return std::is_trivially_destructible_v<TimeWeightedData<TimeWeight>> && nested_func->hasTrivialDestructor();
    }

    void destroyUpToState(AggregateDataPtr __restrict place) const noexcept override
    {
        this->data(place).~TimeWeightedData<TimeWeight>();
        nested_func->destroyUpToState(getNestedPlace(place));
    }

    String getName() const override
    {
        return nested_func->getName() + "_time_weighted";
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
