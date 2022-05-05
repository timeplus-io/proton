#pragma once

#include <type_traits>
#include <utility>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>

#include <AggregateFunctions/IAggregateFunction.h>


namespace DB
{
struct Settings;

struct AggregationFunctionXirrData
{
    using ValueDatePair = std::pair<Float64, Int32>;
    using ValueDatePairs = std::vector<ValueDatePair>;

    Float64 rate;
    ValueDatePairs value_date;
    bool start = false;
    bool changed = true;
    Float64 result;
};

template <typename V, typename D>
class AggregationFunctionXirr final : public IAggregateFunctionDataHelper<AggregationFunctionXirrData, AggregationFunctionXirr<V, D>>
{
private:
    static constexpr int MAX_STEPS = {100};
    static constexpr Float64 EPSILON = {0.0000001};
    static constexpr Float64 DAY_IN_YEAR = {365.0};

    bool hasGuess;

public:
    AggregationFunctionXirr(const DataTypes & arguments, const Array & params)
        : IAggregateFunctionDataHelper<AggregationFunctionXirrData, AggregationFunctionXirr<V, D>>{arguments, params}
    {
        hasGuess = (arguments.size() == 3);
    }

    AggregationFunctionXirr() : IAggregateFunctionDataHelper<AggregationFunctionXirrData, AggregationFunctionXirr<V, D>>{} { }

    String getName() const override { return "xirr"; }

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeNumber<Float64>>(); }

    bool allocatesMemoryInArena() const override { return false; }

    void NO_SANITIZE_UNDEFINED ALWAYS_INLINE
    add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        auto value = assert_cast<const ColumnVector<V> &>(*columns[0]).getData()[row_num];
        auto date = assert_cast<const ColumnVector<D> &>(*columns[1]).getData()[row_num];

        if (!hasGuess)
        {
            this->data(place).start = true;
            this->data(place).rate = 0.1 * 100;
        }
        else if (!this->data(place).start)
        {
            auto r = (*columns[2]).getFloat64(row_num);

            this->data(place).start = true;
            this->data(place).rate = r * 100;
        }

        this->data(place).value_date.emplace_back(static_cast<Float64>(value), static_cast<Int32>(date));
        this->data(place).changed = true;
    }

    void NO_SANITIZE_UNDEFINED ALWAYS_INLINE merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        auto & place_data = this->data(place);
        auto & rhs_data = this->data(rhs);

        auto & pv = place_data.value_date;
        auto & rv = rhs_data.value_date;
        pv.insert(pv.end(), rv.begin(), rv.end());
        place_data.changed = true;
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        writeFloatBinary(this->data(place).rate, buf);
        writeVectorBinary(this->data(place).value_date, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        readFloatBinary(this->data(place).rate, buf);
        readVectorBinary(this->data(place).value_date, buf);
        this->data(place).start = true;
        this->data(place).changed = true;
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & place_data = this->data(place);

        if (!place_data.changed)
            return assert_cast<ColumnVector<Float64> &>(to).getData().push_back(place_data.result);

        calculateRate(place);
        assert_cast<ColumnVector<Float64> &>(to).getData().push_back(place_data.result);
    }

private:
    Float64 ALWAYS_INLINE disc(Float64 v, Int32 d, Float64 rate, Int32 d0) const
    {
        Float64 exp, coef;
        exp = (d - d0) / DAY_IN_YEAR;
        coef = pow(1 + rate / 100, exp);

        return v / coef;
    }

    Float64 ALWAYS_INLINE calculateValue(const AggregationFunctionXirrData::ValueDatePairs & value_date, Float64 rate) const
    {
        Float64 f = {0};
        for (const auto & i : value_date)
            f = f + disc(i.first, i.second, rate, value_date[0].second);

        return f;
    }

    void calculateRate(AggregateDataPtr __restrict place) const
    {
        auto & place_data = this->data(place);
        auto & pv = place_data.value_date;

        std::sort(pv.begin(), pv.end(), [](const auto & l, const auto & r) -> bool { return l.second < r.second; });

        Float64 r1 = place_data.rate;
        Float64 r2 = place_data.rate + 1;
        Float64 rn = {0};

        Float64 f1 = calculateValue(pv, r1);
        Float64 f2 = calculateValue(pv, r2);
        Float64 fn = {0};
        Float64 dF;

        bool quit = false;
        bool result = true;

        Float64 scale = {1};
        int n = {0};

        while (!quit)
        {
            if ((fabs(f2 - f1) <= EPSILON) || (fabs(r2 - r1) <= EPSILON))
            {
                quit = true;
                result = false;
            }
            else
            {
                dF = (f2 - f1) / (r2 - r1);
                rn = r1 + (0 - f1) / dF / scale;
                n = n + 1;

                if (rn > -100)
                {
                    fn = calculateValue(pv, rn);
                }

                if (fabs(rn - r1) / ((fabs(r1) + fabs(r2)) / 2) <= 0.0000005)
                {
                    quit = true;
                }
                else if (n >= MAX_STEPS)
                {
                    quit = true;
                    result = false;
                }
                else if ((rn > -100) == false)
                {
                    scale = scale * 2;
                }
                else
                {
                    scale = 1;
                    r2 = r1;
                    f2 = f1;
                    r1 = rn;
                    f1 = fn;
                }
            }
        }

        place_data.changed = false;

        if (result)
            place_data.result = rn / 100;
        else
            place_data.result = 0;
    }
};

}
