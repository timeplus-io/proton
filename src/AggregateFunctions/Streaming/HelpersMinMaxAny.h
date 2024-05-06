#pragma once

#include "AggregateFunctionMinMaxAny.h"

#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>

#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>

namespace DB
{
struct Settings;

namespace Streaming
{

/// min, max
template <template <typename> class AggregateFunctionTemplate, template <typename> class Data, bool maximum>
static IAggregateFunction * createAggregateFunctionCountedValue(
    const String & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
{
    assertNoParameters(name, parameters);
    assertBinary(name, argument_types);

    const DataTypePtr & argument_type = argument_types[0];

    WhichDataType which(argument_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return new AggregateFunctionTemplate<Data<CountedValuesDataFixed<TYPE, maximum>>>(argument_type, settings);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

    if (which.isBool())
        return new AggregateFunctionTemplate<Data<CountedValuesDataFixed<UInt8, maximum>>>(argument_type, settings);

    if (which.idx == TypeIndex::Date)
        return new AggregateFunctionTemplate<Data<CountedValuesDataFixed<DataTypeDate::FieldType, maximum>>>(argument_type, settings);
    if (which.idx == TypeIndex::DateTime)
        return new AggregateFunctionTemplate<Data<CountedValuesDataFixed<DataTypeDateTime::FieldType, maximum>>>(argument_type, settings);
    if (which.idx == TypeIndex::DateTime64)
        return new AggregateFunctionTemplate<Data<CountedValuesDataFixed<DateTime64, maximum>>>(argument_type, settings);
    if (which.idx == TypeIndex::Decimal32)
        return new AggregateFunctionTemplate<Data<CountedValuesDataFixed<Decimal32, maximum>>>(argument_type, settings);
    if (which.idx == TypeIndex::Decimal64)
        return new AggregateFunctionTemplate<Data<CountedValuesDataFixed<Decimal64, maximum>>>(argument_type, settings);
    if (which.idx == TypeIndex::Decimal128)
        return new AggregateFunctionTemplate<Data<CountedValuesDataFixed<Decimal128, maximum>>>(argument_type, settings);
    if (which.idx == TypeIndex::String)
        return new AggregateFunctionTemplate<Data<CountedValuesDataString<maximum>>>(argument_type, settings);

    return new AggregateFunctionTemplate<Data<CountedValuesDataGeneric<maximum>>>(argument_type, settings);
}

template <template <typename> class AggregateFunctionTemplate, template <typename, bool...> class Data>
static IAggregateFunction *
createAggregateFunctionSingleValue(const String & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertBinary(name, argument_types);

    const DataTypePtr & argument_type = argument_types[0];
    WhichDataType which(argument_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) return new AggregateFunctionTemplate<Data<SingleValueDataFixed<TYPE>>>(argument_type); /// NOLINT
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

    if (which.idx == TypeIndex::Date)
        return new AggregateFunctionTemplate<Data<SingleValueDataFixed<DataTypeDate::FieldType>>>(argument_type);
    if (which.idx == TypeIndex::DateTime)
        return new AggregateFunctionTemplate<Data<SingleValueDataFixed<DataTypeDateTime::FieldType>>>(argument_type);
    if (which.idx == TypeIndex::DateTime64)
        return new AggregateFunctionTemplate<Data<SingleValueDataFixed<DateTime64>>>(argument_type);
    if (which.idx == TypeIndex::Decimal32)
        return new AggregateFunctionTemplate<Data<SingleValueDataFixed<Decimal32>>>(argument_type);
    if (which.idx == TypeIndex::Decimal64)
        return new AggregateFunctionTemplate<Data<SingleValueDataFixed<Decimal64>>>(argument_type);
    if (which.idx == TypeIndex::Decimal128)
        return new AggregateFunctionTemplate<Data<SingleValueDataFixed<Decimal128>>>(argument_type);
    if (which.idx == TypeIndex::Decimal256)
        return new AggregateFunctionTemplate<Data<SingleValueDataFixed<Decimal256>>>(argument_type);
    if (which.idx == TypeIndex::String)
        return new AggregateFunctionTemplate<Data<SingleValueDataString>>(argument_type);

    return new AggregateFunctionTemplate<Data<SingleValueDataGeneric>>(argument_type);
}
}
}
