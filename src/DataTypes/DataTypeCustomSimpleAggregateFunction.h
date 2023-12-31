#pragma once

#include <DataTypes/DataTypeCustom.h>
#include <AggregateFunctions/IAggregateFunction.h>

#include <IO/ReadHelpers.h>

namespace DB
{

/** The type SimpleAggregateFunction(fct, type) is meant to be used in an AggregatingMergeTree. It behaves like a standard
 * data type but when rows are merged, an aggregation function is applied.
 *
 * The aggregation function is limited to simple functions whose merge state is the final result:
 * any, any_last, min, max, sum
 *
 * Examples:
 *
 * SimpleAggregateFunction(sum, Nullable(Float64))
 * SimpleAggregateFunction(any_last, LowCardinality(Nullable(String)))
 * SimpleAggregateFunction(any_last, IPv4)
 *
 * Technically, a standard IDataType is instantiated and customized with IDataTypeCustomName and DataTypeCustomDesc.
  */

class DataTypeCustomSimpleAggregateFunction : public IDataTypeCustomName
{
private:
    const AggregateFunctionPtr function;
    const DataTypes argument_types;
    const Array parameters;

public:
    DataTypeCustomSimpleAggregateFunction(const AggregateFunctionPtr & function_, const DataTypes & argument_types_, const Array & parameters_)
            : function(function_), argument_types(argument_types_), parameters(parameters_) {}

    AggregateFunctionPtr getFunction() const { return function; }
    String getName() const override;
    static void checkSupportedFunctions(const AggregateFunctionPtr & function);
};

}
