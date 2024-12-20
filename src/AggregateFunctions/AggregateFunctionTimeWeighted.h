#pragma once

#include <type_traits>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnsCommon.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Core/DecimalFunctions.h>
#include <Core/Field.h>
#include <Common/logger_useful.h>
#include <Poco/Logger.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int INVALID_DATA;
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

template <typename Value, typename TimeType>
class AggregateFunctionTimeWeighted:
    public IAggregateFunctionDataHelper<TimeWeightedData<TimeType>, 
                                        AggregateFunctionTimeWeighted<Value, TimeType>>

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
    using Base = IAggregateFunctionDataHelper<TimeWeightedData<TimeType>, 
                                        AggregateFunctionTimeWeighted<Value, TimeType>>;

    AggregateFunctionTimeWeighted(AggregateFunctionPtr nested_func_, const DataTypes & arguments, const Array & params_)
    : Base(arguments, params_)
    , nested_func(nested_func_)
    , arguments_num(arguments.size())
    , logger(&Poco::Logger::get("AggregateFunctionTimeWeighted"))
    {
        size_t nested_size = nested_func->alignOfData();
        prefix_size = (sizeof(TimeWeightedData<TimeType>) + nested_size - 1) / nested_size * nested_size;
    }

    void calculateLastTime(const TimeType & end_time, AggregateDataPtr __restrict place, Arena * arena) const
    {
        auto & data = this->data(place);
        /// last time caculation
        if (data.last.has_value())
        {
            MutableColumnPtr value_column, weight_column;
            value_column = this->argument_types[0]->createColumn();
            weight_column = ColumnUInt64::create();
            if (end_time >= data.last->last_time) [[likely]]
            {
                value_column->insert(data.last->last_value);
                weight_column->insert(static_cast<UInt64>(end_time - data.last->last_time));
            }
            else
            {
                LOG_WARNING(logger, "Illegal time argument, should be in ascending order, {}, {}" , data.last->last_time, end_time);
            }

            ColumnRawPtrs raw_columns{value_column.get(), weight_column.get()};
            nested_func->add(getNestedPlace(place), raw_columns.data(), 0, arena);
        }

    }

    void storeLastData(size_t last_row_pos, AggregateDataPtr __restrict place, const IColumn ** columns) const
    {
        auto & data = this->data(place);
        const auto & value_data = assert_cast<const ColumnVectorOrDecimal<Value> &>(*columns[0]).getData();
        const auto & time_data = assert_cast<const ColumnVectorOrDecimal<TimeType> &>(*columns[1]).getData();
        data.last = {
            static_cast<Value>(value_data[last_row_pos]),
            static_cast<TimeType>(time_data[last_row_pos])
        };

        /// remember current time
        if (this->argument_types.size() == 3)
            data.end_time = assert_cast<const ColumnVectorOrDecimal<TimeType> &>(*columns[2]).getData()[last_row_pos];
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        const auto & time_data = assert_cast<const ColumnVectorOrDecimal<TimeType> &>(*columns[1]).getData();

        /// remember start time, only works in the first time
        if (!this->data(place).start_time.has_value())
            this->data(place).start_time = time_data[row_num];

        calculateLastTime(time_data[row_num], place, arena);
        storeLastData(row_num, place, columns);
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
            return add(place, columns, row_begin, arena);

        const auto & time_data = assert_cast<const ColumnVectorOrDecimal<TimeType> &>(*columns[1]).getData();
        calculateLastTime(time_data[row_begin], place, arena);

        /// remember start time, only works in the first time
        if (!this->data(place).start_time.has_value())
            this->data(place).start_time = time_data[row_begin];

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

        storeLastData(last_row_pos, place, columns);
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
            return add(place, columns, row_begin, arena);

        // const auto & value_data = assert_cast<const ColumnVectorOrDecimal<Value> &>(*columns[0]).getData();
        const auto & time_data = assert_cast<const ColumnVectorOrDecimal<TimeType> &>(*columns[1]).getData();
        calculateLastTime(time_data[row_begin], place, arena); 

        /// remember start time, only works in the first time
        if (!this->data(place).start_time.has_value())
            this->data(place).start_time = time_data[row_begin];       

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
    
        storeLastData(last_row_pos, place, columns);
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
                if (rhs_data.start_time.value() < data.last->last_time)
                    throw Exception(ErrorCodes::INVALID_DATA, "Illegal time argument, should be in ascending order, {}, {}" ,data.last->last_time ,rhs_data.start_time.value());
                
                MutableColumnPtr value_column, weight_column;
                value_column = this->argument_types[0]->createColumn();
                weight_column = ColumnUInt64::create();

                value_column->insert(data.last->last_value);
                weight_column->insert(static_cast<UInt64>(rhs_data.start_time.value() - data.last->last_time));
                if (rhs_data.last.has_value())
                    data.last = rhs_data.last;
                if (rhs_data.end_time.has_value())
                    data.end_time = rhs_data.end_time;

                ColumnRawPtrs raw_columns{value_column.get(), weight_column.get()};
                nested_func->add(getNestedPlace(place), raw_columns.data(), 0, arena);
            }
        }
        else
        {
            /// if current data is empty, we will directly use the rhs data.
            data.last = rhs_data.last;
            data.start_time = rhs_data.start_time;
            data.end_time = rhs_data.end_time;
        }

        nested_func->merge(getNestedPlace(place), getNestedPlace(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> version) const override
    {
        auto & data = this->data(place);
        auto has_last = data.last.has_value();
        writeBinary(has_last, buf);
        if (has_last)
        {
            writeBinary(true, buf);
            writeFieldBinary(data.last->last_value, buf);
            writeBinary(data.last->last_time, buf);
        }

        auto has_start_time = data.start_time.has_value();
        writeBinary(has_start_time, buf);
        if (has_start_time)
        {
            writeBinary(true, buf);
            writeBinary(data.start_time.value(), buf);
        }

        auto has_end_time = data.end_time.has_value();
        writeBinary(has_end_time, buf);
        if (has_end_time)
        {
            writeBinary(true, buf);
            writeBinary(data.end_time.value(), buf);
        }

        nested_func->serialize(getNestedPlace(place), buf, version);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> version , Arena * arena) const override
    {
        auto & data = this->data(place);
        bool last_has_value;
        readBinary(last_has_value, buf);
        if (last_has_value)
        {
            data.last->last_value = readFieldBinary(buf);
            readBinary(data.last->last_time, buf);
        }

        bool start_has_value;
        readBinary(start_has_value, buf);
        if (start_has_value)
            readBinary(data.start_time.emplace(), buf);

        bool end_has_value;
        readBinary(end_has_value, buf);
        if (end_has_value)
            readBinary(data.end_time.emplace(), buf);

        nested_func->deserialize(getNestedPlace(place), buf, version, arena);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        auto & data = this->data(place);
        if (data.end_time.has_value())
            calculateLastTime(data.end_time.value(), place, arena);

        nested_func->insertResultInto(getNestedPlace(place), to, arena);
    }

    size_t sizeOfData() const override
    {
        return prefix_size + nested_func->sizeOfData();
    }

    void create(AggregateDataPtr __restrict place) const override
    {
        new (place) TimeWeightedData<TimeType>;
        nested_func->create(getNestedPlace(place));
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override
    {
        this->data(place).~TimeWeightedData<TimeType>();
        nested_func->destroy(getNestedPlace(place));
    }

    bool hasTrivialDestructor() const override
    {
        return std::is_trivially_destructible_v<TimeWeightedData<TimeType>> && nested_func->hasTrivialDestructor();
    }

    void destroyUpToState(AggregateDataPtr __restrict place) const noexcept override
    {
        this->data(place).~TimeWeightedData<TimeType>();
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
        return nested_func->allocatesMemoryInArena();
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
