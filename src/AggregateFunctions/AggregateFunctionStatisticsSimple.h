#pragma once

#include <cmath>

#include <base/arithmeticOverflow.h>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/Moments.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnDecimal.h>


/** This is simple, not numerically stable
  *  implementations of variance/covariance/correlation functions.
  *
  * It is about two times faster than stable variants.
  * Numerical errors may occur during summation.
  *
  * This implementation is selected as default,
  *  because "you don't pay for what you don't need" principle.
  *
  * For more sophisticated implementation, look at AggregateFunctionStatistics.h
  */

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

struct Settings;

enum class StatisticsFunctionKind: uint8_t
{
    var_pop, var_samp,
    stddev_pop, stddev_samp,
    skew_pop, skew_samp,
    kurt_pop, kurt_samp,
    covar_pop, covar_samp,
    corr
};


template <typename T, size_t _level>
struct StatFuncOneArg
{
    using Type1 = T;
    using Type2 = T;
    using ResultType = std::conditional_t<std::is_same_v<T, Float32>, Float32, Float64>;
    using Data = std::conditional_t<is_decimal<T>, VarMomentsDecimal<Decimal128, _level>, VarMoments<ResultType, _level>>;

    static constexpr UInt32 num_args = 1;
};

template <typename T1, typename T2, template <typename> typename Moments>
struct StatFuncTwoArg
{
    using Type2 = T2;
    using Type1 = T1;
    using ResultType = std::conditional_t<std::is_same_v<T1, T2> && std::is_same_v<T1, Float32>, Float32, Float64>;
    using Data = Moments<ResultType>;

    static constexpr UInt32 num_args = 2;
};


template <typename StatFunc>
class AggregateFunctionVarianceSimple final
    : public IAggregateFunctionDataHelper<typename StatFunc::Data, AggregateFunctionVarianceSimple<StatFunc>>
{
public:
    using T1 = typename StatFunc::Type1;
    using T2 = typename StatFunc::Type2;
    using ColVecT1 = ColumnVectorOrDecimal<T1>;
    using ColVecT2 = ColumnVectorOrDecimal<T2>;
    using ResultType = typename StatFunc::ResultType;
    using ColVecResult = ColumnVector<ResultType>;

    explicit AggregateFunctionVarianceSimple(const DataTypes & argument_types_, StatisticsFunctionKind kind_)
        : IAggregateFunctionDataHelper<typename StatFunc::Data, AggregateFunctionVarianceSimple<StatFunc>>(argument_types_, {}, std::make_shared<DataTypeNumber<ResultType>>())
        , src_scale(0), kind(kind_)
    {}

    AggregateFunctionVarianceSimple(const IDataType & data_type, const DataTypes & argument_types_, StatisticsFunctionKind kind_)
        : IAggregateFunctionDataHelper<typename StatFunc::Data, AggregateFunctionVarianceSimple<StatFunc>>(argument_types_, {}, std::make_shared<DataTypeNumber<ResultType>>())
        , src_scale(getDecimalScale(data_type)), kind(kind_)
    {}

    String getName() const override
    {
        return String(magic_enum::enum_name(kind));
    }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        if constexpr (StatFunc::num_args == 2)
            this->data(place).add(
                static_cast<ResultType>(static_cast<const ColVecT1 &>(*columns[0]).getData()[row_num]),
                static_cast<ResultType>(static_cast<const ColVecT2 &>(*columns[1]).getData()[row_num]));
        else
        {
            if constexpr (is_decimal<T1>)
            {
                this->data(place).add(static_cast<ResultType>(
                    static_cast<const ColVecT1 &>(*columns[0]).getData()[row_num].value));
            }
            else
                this->data(place).add(
                    static_cast<ResultType>(static_cast<const ColVecT1 &>(*columns[0]).getData()[row_num]));
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        this->data(place).read(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        const auto & data = this->data(place);
        auto & dst = static_cast<ColVecResult &>(to).getData();

                if constexpr (is_decimal<T1>)
        {
            switch (kind)
            {
                case StatisticsFunctionKind::var_pop:
                {
                    dst.push_back(data.getPopulation(src_scale * 2));
                    break;
                }
                case StatisticsFunctionKind::var_samp:
                {
                    dst.push_back(data.getSample(src_scale * 2));
                    break;
                }
                case StatisticsFunctionKind::stddev_pop:
                {
                    dst.push_back(sqrt(data.getPopulation(src_scale * 2)));
                    break;
                }
                case StatisticsFunctionKind::stddev_samp:
                {
                    dst.push_back(sqrt(data.getSample(src_scale * 2)));
                    break;
                }
                case StatisticsFunctionKind::skew_pop:
                {
                    Float64 var_value = data.getPopulation(src_scale * 2);

                    if (var_value > 0)
                        dst.push_back(data.getMoment3(src_scale * 3) / pow(var_value, 1.5));
                    else
                        dst.push_back(std::numeric_limits<Float64>::quiet_NaN());

                    break;
                }
                case StatisticsFunctionKind::skew_samp:
                {
                    Float64 var_value = data.getSample(src_scale * 2);

                    if (var_value > 0)
                        dst.push_back(data.getMoment3(src_scale * 3) / pow(var_value, 1.5));
                    else
                        dst.push_back(std::numeric_limits<Float64>::quiet_NaN());

                    break;
                }
                case StatisticsFunctionKind::kurt_pop:
                {
                    Float64 var_value = data.getPopulation(src_scale * 2);

                    if (var_value > 0)
                        dst.push_back(data.getMoment4(src_scale * 4) / pow(var_value, 2));
                    else
                        dst.push_back(std::numeric_limits<Float64>::quiet_NaN());

                    break;
                }
                case StatisticsFunctionKind::kurt_samp:
                {
                    Float64 var_value = data.getSample(src_scale * 2);

                    if (var_value > 0)
                        dst.push_back(data.getMoment4(src_scale * 4) / pow(var_value, 2));
                    else
                        dst.push_back(std::numeric_limits<Float64>::quiet_NaN());

                    break;
                }
                default:
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected statistical function kind");
            }
        }
        else
        {
            switch (kind)
            {
                case StatisticsFunctionKind::var_pop:
                {
                    dst.push_back(data.getPopulation());
                    break;
                }
                case StatisticsFunctionKind::var_samp:
                {
                    dst.push_back(data.getSample());
                    break;
                }
                case StatisticsFunctionKind::stddev_pop:
                {
                    dst.push_back(sqrt(data.getPopulation()));
                    break;
                }
                case StatisticsFunctionKind::stddev_samp:
                {
                    dst.push_back(sqrt(data.getSample()));
                    break;
                }
                case StatisticsFunctionKind::skew_pop:
                {
                    ResultType var_value = data.getPopulation();

                    if (var_value > 0)
                        dst.push_back(static_cast<ResultType>(data.getMoment3() / pow(var_value, 1.5)));
                    else
                        dst.push_back(std::numeric_limits<ResultType>::quiet_NaN());

                    break;
                }
                case StatisticsFunctionKind::skew_samp:
                {
                    ResultType var_value = data.getSample();

                    if (var_value > 0)
                        dst.push_back(static_cast<ResultType>(data.getMoment3() / pow(var_value, 1.5)));
                    else
                        dst.push_back(std::numeric_limits<ResultType>::quiet_NaN());

                    break;
                }
                case StatisticsFunctionKind::kurt_pop:
                {
                    ResultType var_value = data.getPopulation();

                    if (var_value > 0)
                        dst.push_back(static_cast<ResultType>(data.getMoment4() / pow(var_value, 2)));
                    else
                        dst.push_back(std::numeric_limits<ResultType>::quiet_NaN());

                    break;
                }
                case StatisticsFunctionKind::kurt_samp:
                {
                    ResultType var_value = data.getSample();

                    if (var_value > 0)
                        dst.push_back(static_cast<ResultType>(data.getMoment4() / pow(var_value, 2)));
                    else
                        dst.push_back(std::numeric_limits<ResultType>::quiet_NaN());

                    break;
                }
                case StatisticsFunctionKind::covar_pop:
                {
                    dst.push_back(data.getPopulation());
                    break;
                }
                case StatisticsFunctionKind::covar_samp:
                {
                    dst.push_back(data.getSample());
                    break;
                }
                case StatisticsFunctionKind::corr:
                {
                    dst.push_back(data.get());
                    break;
                }
            }
        }
    }

private:
    UInt32 src_scale;
    StatisticsFunctionKind kind;
};


struct Settings;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

template <template <typename> typename FunctionTemplate, StatisticsFunctionKind kind>
AggregateFunctionPtr createAggregateFunctionStatisticsUnary(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    AggregateFunctionPtr res;
    const DataTypePtr & data_type = argument_types[0];
    if (isDecimal(data_type))
        res.reset(createWithDecimalType<FunctionTemplate>(*data_type, *data_type, argument_types, kind));
    else
        res.reset(createWithNumericType<FunctionTemplate>(*data_type, argument_types, kind));

    if (!res)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument for aggregate function {}",
                        argument_types[0]->getName(), name);
    return res;
}

template <template <typename, typename> typename FunctionTemplate, StatisticsFunctionKind kind>
AggregateFunctionPtr createAggregateFunctionStatisticsBinary(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertBinary(name, argument_types);

    AggregateFunctionPtr res(createWithTwoBasicNumericTypes<FunctionTemplate>(*argument_types[0], *argument_types[1], argument_types, kind));
    if (!res)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal types {} and {} of arguments for aggregate function {}",
            argument_types[0]->getName(), argument_types[1]->getName(), name);

    return res;
}

}

}
