#include "AggregateFunctionSum.h"

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>


namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace Streaming
{
namespace
{

template <typename T>
struct SumSimple
{
    /// @note It uses slow Decimal128 (cause we need such a variant). sumWithOverflow is faster for Decimal32/64
    using ResultType
        = std::conditional_t<is_decimal<T>, std::conditional_t<std::is_same_v<T, Decimal256>, Decimal256, Decimal128>, NearestFieldType<T>>;
    using AggregateDataType = AggregateFunctionSumData<ResultType>;
    using Function = AggregateFunctionSum<T, ResultType, AggregateDataType, AggregateFunctionTypeSum>;
};

template <typename T>
struct SumSameType
{
    using ResultType = T;
    using AggregateDataType = AggregateFunctionSumData<ResultType>;
    using Function = AggregateFunctionSum<T, ResultType, AggregateDataType, AggregateFunctionTypeSumWithOverflow>;
};

template <typename T>
struct SumKahan
{
    using ResultType = T;
    using AggregateDataType = AggregateFunctionSumKahanData<ResultType>;
    using Function = AggregateFunctionSum<T, ResultType, AggregateDataType, AggregateFunctionTypeSumKahan>;
};

template <typename T>
using AggregateFunctionSumSimple = typename SumSimple<T>::Function;
template <typename T>
using AggregateFunctionSumWithOverflow = typename SumSameType<T>::Function;
template <typename T>
using AggregateFunctionSumKahan = std::conditional_t<is_decimal<T>, typename SumSimple<T>::Function, typename SumKahan<T>::Function>;


template <template <typename> class Function>
AggregateFunctionPtr
createAggregateFunctionSum(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    AggregateFunctionPtr res;
    DataTypePtr data_type = argument_types[0];
    if (isDecimal(data_type))
        res.reset(createWithDecimalType<Function>(*data_type, *data_type, argument_types));
    else
        res.reset(createWithNumericType<Function>(*data_type, argument_types));

    if (!res)
        throw Exception(
            "Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name,
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    return res;
}

AggregateFunctionPtr
createAggregateFunctionSumKahan(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    DataTypePtr data_type = argument_types[0];
    WhichDataType which(data_type);

    if (which.isDecimal())
        return AggregateFunctionPtr(createWithDecimalType<AggregateFunctionSumKahan>(*data_type, *data_type, argument_types));
    else if (which.isFloat32())
        return AggregateFunctionPtr(new SumKahan<Float32>::Function(*data_type, argument_types));
    else if (which.isFloat64())
        return AggregateFunctionPtr(new SumKahan<Float64>::Function(*data_type, argument_types));

    throw Exception(
        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
        "Illegal type {} of argument for aggregate function. Only float32 / double make sense for sum_kahan",
        argument_types[0]->getName());
}
}

void registerAggregateFunctionSumRetract(AggregateFunctionFactory & factory)
{
    factory.registerFunction(
        "__sum_retract", createAggregateFunctionSum<AggregateFunctionSumSimple>, AggregateFunctionFactory::CaseSensitive);
    factory.registerFunction("__sum_with_overflow_retract", createAggregateFunctionSum<AggregateFunctionSumWithOverflow>);
    factory.registerFunction("__sum_kahan_retract", createAggregateFunctionSumKahan);
}
}
}
