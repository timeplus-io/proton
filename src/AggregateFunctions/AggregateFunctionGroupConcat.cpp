#include <AggregateFunctions/AggregateFunctionGroupConcat.h>
#include <Columns/ColumnString.h>
#include <Interpreters/castColumn.h>

namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
}

AggregateFunctionPtr createAggregateFunctionGroupConcat(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertUnary(name, argument_types);

    bool has_limit = false;
    UInt64 limit = 0;
    String delimiter;

    if (!parameters.empty())
    {
        auto type = parameters[0].getType();
        if (type != Field::Types::String)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First parameter for aggregate function {} should be string", name);

        delimiter = parameters[0].safeGet<String>();
    }
    if (parameters.size() == 2)
    {
        auto type = parameters[1].getType();

        if (type != Field::Types::Int64 && type != Field::Types::UInt64)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Second parameter for aggregate function {} should be a positive number", name);

        if ((type == Field::Types::Int64 && parameters[1].safeGet<Int64>() <= 0) ||
            (type == Field::Types::UInt64 && parameters[1].safeGet<UInt64>() == 0))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Second parameter for aggregate function {} should be a positive number, got: {}", name, parameters[1].safeGet<Int64>());

        has_limit = true;
        limit = parameters[1].safeGet<UInt64>();
    }

    if (has_limit)
        return std::make_shared<AggregateFunctionGroupConcat</* has_limit= */ true>>(argument_types[0], parameters, static_cast<UInt64>(limit), delimiter);

    return std::make_shared<AggregateFunctionGroupConcat</* has_limit= */ false>>(argument_types[0], parameters, static_cast<UInt64>(limit), delimiter);
}

void registerAggregateFunctionGroupConcat(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = false, .is_order_dependent = true };

    factory.registerFunction("group_concat", { createAggregateFunctionGroupConcat, properties });
}

}
