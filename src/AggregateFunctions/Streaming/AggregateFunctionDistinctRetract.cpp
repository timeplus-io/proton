#include <AggregateFunctions/Streaming/AggregateFunctionDistinctRetract.h>

#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>
#include <AggregateFunctions/Helpers.h>

namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace Streaming
{
namespace
{

    class AggregateFunctionCombinatorDistinctRetract final : public IAggregateFunctionCombinator
    {
    public:
        String getName() const override { return "_distinct_retract"; }

        DataTypes transformArguments(const DataTypes & arguments) const override
        {
            if (arguments.empty())
                throw Exception(
                    "Incorrect number of arguments for aggregate function with " + getName() + " suffix",
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

            return arguments;
        }

        AggregateFunctionPtr transformAggregateFunction(
            const AggregateFunctionPtr & nested_function,
            const AggregateFunctionProperties &,
            const DataTypes & arguments,
            const Array & params) const override
        {
            if (arguments.size() == 2)
            {
                /// means just one distinct key column
                /// why 2? one is delta column, the other is the distinct key column
                AggregateFunctionPtr res;
                res.reset(createWithNumericType<AggregateFunctionDistinctRetract, AggregateFunctionDistinctRetractSingleNumericData>(
                    *arguments[0], nested_function, arguments, params));
                if (res)
                    return res;

                if (arguments[0]->isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
                    return std::make_shared<AggregateFunctionDistinctRetract<AggregateFunctionDistinctRetractSingleGenericData<true>>>(
                        nested_function, arguments, params);
                else
                    return std::make_shared<AggregateFunctionDistinctRetract<AggregateFunctionDistinctRetractSingleGenericData<false>>>(
                        nested_function, arguments, params);
            }
            /// atually, now it doesn't support multiple distinct key columns(it will throw an exception)
            /// TODO: support multiple distinct key columns
            return std::make_shared<AggregateFunctionDistinctRetract<AggregateFunctionDistinctRetractMultipleGenericData>>(
                nested_function, arguments, params);
        }
    };

}

void registerAggregateFunctionCombinatorDistinctRetract(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorDistinctRetract>());
}

}
}
