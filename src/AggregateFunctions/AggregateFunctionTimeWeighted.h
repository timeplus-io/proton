#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Field.h>
#include <DataTypes/DataTypesDecimal.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/Exception.h>
#include <Common/MemoryHelpers.h>
#include <Common/assert_cast.h>

#include <optional>
#include <type_traits>

namespace DB
{

namespace ErrorCodes
{
extern const int INVALID_DATA;
}

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
class AggregateFunctionTimeWeighted final
    : public IAggregateFunctionDataHelper<TimeWeightedData<TimeType>, AggregateFunctionTimeWeighted<Value, TimeType>>
{
private:
    using Data = TimeWeightedData<TimeType>;
    using Base = IAggregateFunctionDataHelper<Data, AggregateFunctionTimeWeighted<Value, TimeType>>;

    AggregateFunctionPtr nested_func;
    size_t prefix_size = 0;

    AggregateDataPtr getNestedPlace(AggregateDataPtr __restrict place) const noexcept { return place + prefix_size; }

    ConstAggregateDataPtr getNestedPlace(ConstAggregateDataPtr __restrict place) const noexcept { return place + prefix_size; }

    static UInt64 checkedDuration(TimeType start_time, TimeType end_time)
    {
        if (end_time < start_time)
            throw Exception(
                ErrorCodes::INVALID_DATA, "Illegal time argument, should be in ascending order, {} then {}", start_time, end_time);

        return static_cast<UInt64>(end_time - start_time);
    }

    static bool shouldUseRhsLast(const Data & data, const Data & rhs_data)
    {
        if (!rhs_data.last.has_value())
            return false;
        if (!data.last.has_value())
            return true;

        if (rhs_data.last->last_time > data.last->last_time)
            return true;
        if (rhs_data.last->last_time < data.last->last_time)
            return false;

        if (rhs_data.start_time.has_value() != data.start_time.has_value())
            return rhs_data.start_time.has_value();
        if (rhs_data.start_time.has_value() && data.start_time.has_value())
            return rhs_data.start_time.value() > data.start_time.value();

        return false;
    }

    String getCanonicalTimeWeightedName() const
    {
        const String nested_name = nested_func->getName();
        if (nested_name == "avg_weighted")
            return "avg_time_weighted";
        if (nested_name == "median_exact_weighted" || nested_name == "quantile_exact_weighted")
            return "median_time_weighted";
        return nested_name + "_time_weighted";
    }

    void addWeightedValueToNested(AggregateDataPtr __restrict nested_place, const Field & value, UInt64 weight, Arena * arena) const
    {
        MutableColumnPtr value_column = this->argument_types[0]->createColumn();
        value_column->insert(value);

        auto weight_column = ColumnUInt64::create();
        weight_column->insert(weight);

        ColumnRawPtrs raw_columns{value_column.get(), weight_column.get()};
        nested_func->add(nested_place, raw_columns.data(), 0, arena);
    }

    void addLastIntervalToPlace(AggregateDataPtr __restrict place, TimeType end_time, Arena * arena) const
    {
        auto & data = this->data(place);
        if (!data.last.has_value())
            return;

        const UInt64 weight = checkedDuration(data.last->last_time, end_time);
        addWeightedValueToNested(getNestedPlace(place), data.last->last_value, weight, arena);
    }

    void storeLastData(size_t row_num, AggregateDataPtr __restrict place, const IColumn ** columns) const
    {
        auto & data = this->data(place);
        const auto & value_data = assert_cast<const ColumnVectorOrDecimal<Value> &>(*columns[0]).getData();
        const auto & time_data = assert_cast<const ColumnVectorOrDecimal<TimeType> &>(*columns[1]).getData();
        data.last = typename Data::Last{Field(value_data[row_num]), static_cast<TimeType>(time_data[row_num])};

        if (this->argument_types.size() == 3)
            data.end_time = assert_cast<const ColumnVectorOrDecimal<TimeType> &>(*columns[2]).getData()[row_num];
    }

    template <bool merge_result>
    void insertResultIntoImpl(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const
    {
        const auto & data = this->data(place);
        if (!data.end_time.has_value() || !data.last.has_value())
        {
            if constexpr (merge_result)
                nested_func->insertMergeResultInto(getNestedPlace(place), to, arena);
            else
                nested_func->insertResultInto(getNestedPlace(place), to, arena);

            return;
        }

        auto tmp_state = alignedAllocate(nested_func->sizeOfData(), nested_func->alignOfData());
        auto * tmp_place = tmp_state.get();
        nested_func->create(tmp_place);
        try
        {
            nested_func->merge(tmp_place, getNestedPlace(place), arena);
            const UInt64 weight = checkedDuration(data.last->last_time, data.end_time.value());
            addWeightedValueToNested(tmp_place, data.last->last_value, weight, arena);

            if constexpr (merge_result)
                nested_func->insertMergeResultInto(tmp_place, to, arena);
            else
                nested_func->insertResultInto(tmp_place, to, arena);
        }
        catch (...)
        {
            nested_func->destroy(tmp_place);
            throw;
        }

        nested_func->destroy(tmp_place);
    }

public:
    AggregateFunctionTimeWeighted(AggregateFunctionPtr nested_func_, const DataTypes & arguments, const Array & params_)
        : Base(arguments, params_, nested_func_->getResultType()), nested_func(std::move(nested_func_))
    {
        const size_t nested_align = nested_func->alignOfData();
        prefix_size = (sizeof(Data) + nested_align - 1) / nested_align * nested_align;
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        const auto & time_data = assert_cast<const ColumnVectorOrDecimal<TimeType> &>(*columns[1]).getData();
        auto & data = this->data(place);

        if (!data.start_time.has_value())
            data.start_time = time_data[row_num];

        addLastIntervalToPlace(place, time_data[row_num], arena);
        storeLastData(row_num, place, columns);
    }

    void addBatchSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        Arena * arena,
        ssize_t if_argument_pos,
        const IColumn * delta_col) const override
    {
        if (row_begin >= row_end)
            return;

        if (if_argument_pos >= 0 || delta_col != nullptr || row_end - row_begin == 1)
            return Base::addBatchSinglePlace(row_begin, row_end, place, columns, arena, if_argument_pos, delta_col);

        const auto & time_data = assert_cast<const ColumnVectorOrDecimal<TimeType> &>(*columns[1]).getData();
        for (size_t i = row_begin + 1; i < row_end; ++i)
            checkedDuration(time_data[i - 1], time_data[i]);

        auto & data = this->data(place);
        if (!data.start_time.has_value())
            data.start_time = time_data[row_begin];

        addLastIntervalToPlace(place, time_data[row_begin], arena);

        auto weight_column = ColumnUInt64::create();
        /// Keep row indices aligned with the original input columns for nested addBatchSinglePlace().
        /// For row_begin > 0, prefix zeros so weight_column[i] is valid for i in [row_begin, row_end - 2].
        weight_column->reserve(row_end - 1);
        for (size_t i = 0; i < row_begin; ++i)
            weight_column->insert(0);
        for (size_t i = row_begin; i + 1 < row_end; ++i)
            weight_column->insert(static_cast<UInt64>(time_data[i + 1] - time_data[i]));

        ColumnRawPtrs raw_columns{columns[0], weight_column.get()};
        nested_func->addBatchSinglePlace(row_begin, row_end - 1, getNestedPlace(place), raw_columns.data(), arena, -1, nullptr);
        storeLastData(row_end - 1, place, columns);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & data = this->data(place);
        const auto & rhs_data = this->data(rhs);

        const bool place_before_rhs
            = data.last.has_value() && rhs_data.start_time.has_value() && data.last->last_time < rhs_data.start_time.value();
        const bool rhs_before_place
            = rhs_data.last.has_value() && data.start_time.has_value() && rhs_data.last->last_time < data.start_time.value();

        if (place_before_rhs)
        {
            addLastIntervalToPlace(place, rhs_data.start_time.value(), arena);
        }
        else if (rhs_before_place)
        {
            const UInt64 weight = checkedDuration(rhs_data.last->last_time, data.start_time.value());
            addWeightedValueToNested(getNestedPlace(place), rhs_data.last->last_value, weight, arena);
        }

        nested_func->merge(getNestedPlace(place), getNestedPlace(rhs), arena);

        if (!data.start_time.has_value() && rhs_data.start_time.has_value())
            data.start_time = rhs_data.start_time;
        else if (data.start_time.has_value() && rhs_data.start_time.has_value() && rhs_data.start_time.value() < data.start_time.value())
            data.start_time = rhs_data.start_time;

        if (shouldUseRhsLast(data, rhs_data))
        {
            data.last = rhs_data.last;
            data.end_time = rhs_data.end_time;
        }
        else if (!data.end_time.has_value() && rhs_data.end_time.has_value())
        {
            data.end_time = rhs_data.end_time;
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> version) const override
    {
        const auto & data = this->data(place);

        writeBinary(data.last.has_value(), buf);
        if (data.last.has_value())
        {
            writeFieldBinary(data.last->last_value, buf);
            writeBinary(data.last->last_time, buf);
        }

        writeBinary(data.start_time.has_value(), buf);
        if (data.start_time.has_value())
            writeBinary(data.start_time.value(), buf);

        writeBinary(data.end_time.has_value(), buf);
        if (data.end_time.has_value())
            writeBinary(data.end_time.value(), buf);

        nested_func->serialize(getNestedPlace(place), buf, version);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> version, Arena * arena) const override
    {
        auto & data = this->data(place);

        bool has_last = false;
        readBinary(has_last, buf);
        if (has_last)
        {
            data.last.emplace();
            data.last->last_value = readFieldBinary(buf);
            readBinary(data.last->last_time, buf);
        }

        bool has_start_time = false;
        readBinary(has_start_time, buf);
        if (has_start_time)
            readBinary(data.start_time.emplace(), buf);

        bool has_end_time = false;
        readBinary(has_end_time, buf);
        if (has_end_time)
            readBinary(data.end_time.emplace(), buf);

        nested_func->deserialize(getNestedPlace(place), buf, version, arena);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        insertResultIntoImpl<false>(place, to, arena);
    }

    void insertMergeResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        insertResultIntoImpl<true>(place, to, arena);
    }

    size_t sizeOfData() const override { return prefix_size + nested_func->sizeOfData(); }

    size_t alignOfData() const override { return nested_func->alignOfData(); }

    void create(AggregateDataPtr __restrict place) const override
    {
        new (place) Data;
        nested_func->create(getNestedPlace(place));
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override
    {
        this->data(place).~Data();
        nested_func->destroy(getNestedPlace(place));
    }

    void destroyUpToState(AggregateDataPtr __restrict place) const noexcept override
    {
        this->data(place).~Data();
        nested_func->destroyUpToState(getNestedPlace(place));
    }

    bool hasTrivialDestructor() const override { return std::is_trivially_destructible_v<Data> && nested_func->hasTrivialDestructor(); }

    String getName() const override { return getCanonicalTimeWeightedName(); }

    bool allocatesMemoryInArena() const override { return true; }

    bool isState() const override { return nested_func->isState(); }

    bool isVersioned() const override { return nested_func->isVersioned(); }

    size_t getVersionFromRevision(size_t revision) const override { return nested_func->getVersionFromRevision(revision); }

    size_t getDefaultVersion() const override { return nested_func->getDefaultVersion(); }

    AggregateFunctionPtr getNestedFunction() const override { return nested_func; }
};
}
