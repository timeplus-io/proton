#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionBitwise.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

template <template <typename> class Data>
AggregateFunctionPtr createAggregateFunctionBitwise(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    if (!argument_types[0]->canBeUsedInBitOperations())
        throw Exception("The type " + argument_types[0]->getName() + " of argument for aggregate function " + name
            + " is illegal, because it cannot be used in bitwise operations",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    AggregateFunctionPtr res(createWithUnsignedIntegerType<AggregateFunctionBitwise, Data>(*argument_types[0], argument_types[0]));

    if (!res)
        throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return res;
}

}

void registerAggregateFunctionsBitwise(AggregateFunctionFactory & factory)
{
    factory.registerFunction("group_bit_or", createAggregateFunctionBitwise<AggregateFunctionGroupBitOrData>);
    factory.registerFunction("group_bit_and", createAggregateFunctionBitwise<AggregateFunctionGroupBitAndData>);
    factory.registerFunction("group_bit_xor", createAggregateFunctionBitwise<AggregateFunctionGroupBitXorData>);

    /// Aliases for compatibility with MySQL.
    factory.registerAlias("bit_or", "group_bit_or", AggregateFunctionFactory::CaseSensitive);
    factory.registerAlias("bit_and", "group_bit_and", AggregateFunctionFactory::CaseSensitive);
    factory.registerAlias("bit_xor", "group_bit_xor", AggregateFunctionFactory::CaseSensitive);
}

}
