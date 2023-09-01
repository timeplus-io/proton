#pragma once

#include "config.h"

#if USE_ARG_MIN_MAX_FUNCS

#include "AggregateFunctionArgMinMax.h"
#include "AggregateFunctionArgMinMaxData.h"

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

template <typename ResType, bool maximum>
static IAggregateFunction *
createAggregateFunctionCountedArgMinMaxSecond(const DataTypePtr & res_type, const DataTypePtr & val_type, const Settings * settings)
{
    WhichDataType which(val_type);

#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxData<ResType, TYPE, maximum>>(res_type, val_type, settings);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

    if (which.isBool())
        return new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxData<ResType, UInt8, maximum>>(res_type, val_type, settings);

    if (which.idx == TypeIndex::Date)
        return new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxData<ResType, DataTypeDate::FieldType, maximum>>(
            res_type, val_type, settings);
    if (which.idx == TypeIndex::DateTime)
        return new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxData<ResType, DataTypeDateTime::FieldType, maximum>>(
            res_type, val_type, settings);
    if (which.idx == TypeIndex::DateTime64)
        return new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxData<ResType, DateTime64, maximum>>(res_type, val_type, settings);
    if (which.idx == TypeIndex::Decimal32)
        return new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxData<ResType, Decimal32, maximum>>(res_type, val_type, settings);
    if (which.idx == TypeIndex::Decimal64)
        return new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxData<ResType, Decimal64, maximum>>(res_type, val_type, settings);
    if (which.idx == TypeIndex::Decimal128)
        return new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxData<ResType, Decimal128, maximum>>(res_type, val_type, settings);
    if (which.idx == TypeIndex::String)
        return new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxData<ResType, String, maximum>>(res_type, val_type, settings);

    return new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxData<ResType, Field, maximum>>(res_type, val_type, settings);
}

/// arg_min, arg_max
template <bool maximum>
static IAggregateFunction * createAggregateFunctionCountedArgMinMax(
    const String & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
{
    assertNoParameters(name, parameters);
    /// arg_min_if/arg_max_if requries three arguments
    assertArityAtLeast<2>(name, argument_types);

    const DataTypePtr & res_type = argument_types[0];
    const DataTypePtr & val_type = argument_types[1];

    WhichDataType which(res_type);

#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return createAggregateFunctionCountedArgMinMaxSecond<TYPE, maximum>(res_type, val_type, settings);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

    if (which.isBool())
        return createAggregateFunctionCountedArgMinMaxSecond<UInt8, maximum>(res_type, val_type, settings);

    if (which.idx == TypeIndex::Date)
        return createAggregateFunctionCountedArgMinMaxSecond<DataTypeDate::FieldType, maximum>(res_type, val_type, settings);
    if (which.idx == TypeIndex::DateTime)
        return createAggregateFunctionCountedArgMinMaxSecond<DataTypeDateTime::FieldType, maximum>(res_type, val_type, settings);
    if (which.idx == TypeIndex::DateTime64)
        return createAggregateFunctionCountedArgMinMaxSecond<DateTime64, maximum>(res_type, val_type, settings);
    if (which.idx == TypeIndex::Decimal32)
        return createAggregateFunctionCountedArgMinMaxSecond<Decimal32, maximum>(res_type, val_type, settings);
    if (which.idx == TypeIndex::Decimal64)
        return createAggregateFunctionCountedArgMinMaxSecond<Decimal64, maximum>(res_type, val_type, settings);
    if (which.idx == TypeIndex::Decimal128)
        return createAggregateFunctionCountedArgMinMaxSecond<Decimal128, maximum>(res_type, val_type, settings);
    if (which.idx == TypeIndex::String)
        return createAggregateFunctionCountedArgMinMaxSecond<String, maximum>(res_type, val_type, settings);

    return createAggregateFunctionCountedArgMinMaxSecond<Field, maximum>(res_type, val_type, settings);
}
}
}

#endif
