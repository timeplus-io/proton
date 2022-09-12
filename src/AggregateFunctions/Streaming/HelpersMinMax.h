#pragma once

#include "AggregateFunctionMinMax.h"

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
    assertUnary(name, argument_types);

    const DataTypePtr & argument_type = argument_types[0];

    WhichDataType which(argument_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return new AggregateFunctionTemplate<Data<CountedValuesDataFixed<TYPE, maximum>>>(argument_type, settings);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

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
}
}
