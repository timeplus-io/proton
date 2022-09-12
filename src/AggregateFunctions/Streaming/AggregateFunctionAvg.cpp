#include "AggregateFunctionAvg.h"
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>

#include <memory>

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
bool allowType(const DataTypePtr & type) noexcept
{
    const WhichDataType t(type);
    return t.isInt() || t.isUInt() || t.isFloat() || t.isDecimal();
}

AggregateFunctionPtr
createAggregateFunctionAvg(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    const DataTypePtr & data_type = argument_types[0];

    if (!allowType(data_type))
        throw Exception(
            "Illegal type " + data_type->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    AggregateFunctionPtr res;

    if (isDecimal(data_type))
        res.reset(createWithDecimalType<AggregateFunctionAvg>(*data_type, argument_types, getDecimalScale(*data_type)));
    else
        res.reset(createWithNumericType<AggregateFunctionAvg>(*data_type, argument_types));

    return res;
}
}

void registerAggregateFunctionAvgRetract(AggregateFunctionFactory & factory)
{
    factory.registerFunction("__avg_retract", createAggregateFunctionAvg, AggregateFunctionFactory::CaseSensitive);
}
}
}
