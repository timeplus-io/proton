#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionMaxIntersections.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{
    AggregateFunctionPtr createAggregateFunctionMaxIntersections(
        AggregateFunctionIntersectionsKind kind,
        const std::string & name, const DataTypes & argument_types, const Array & parameters)
    {
        assertBinary(name, argument_types);
        assertNoParameters(name, parameters);

        AggregateFunctionPtr res(createWithNumericType<AggregateFunctionIntersectionsMax>(*argument_types[0], kind, argument_types));
        if (!res)
            throw Exception("Illegal types " + argument_types[0]->getName() + " and " + argument_types[1]->getName()
                + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return res;
    }
}

void registerAggregateFunctionsMaxIntersections(AggregateFunctionFactory & factory)
{
    factory.registerFunction("max_intersections",
        [](const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            return createAggregateFunctionMaxIntersections(AggregateFunctionIntersectionsKind::Count, name, argument_types, parameters);
        });

    factory.registerFunction("max_intersections_position",
        [](const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            return createAggregateFunctionMaxIntersections(AggregateFunctionIntersectionsKind::Position, name, argument_types, parameters);
        });
}

}
