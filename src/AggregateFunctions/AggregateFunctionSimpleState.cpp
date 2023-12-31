#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>
#include <AggregateFunctions/AggregateFunctionSimpleState.h>

namespace DB
{
struct Settings;
namespace
{
    class AggregateFunctionCombinatorSimpleState final : public IAggregateFunctionCombinator
    {
    public:
        String getName() const override { return "_simple_state"; }

        DataTypes transformArguments(const DataTypes & arguments) const override { return arguments; }

        AggregateFunctionPtr transformAggregateFunction(
            const AggregateFunctionPtr & nested_function,
            const AggregateFunctionProperties &,
            const DataTypes & arguments,
            const Array & params) const override
        {
            return std::make_shared<AggregateFunctionSimpleState>(nested_function, arguments, params);
        }
    };

}

void registerAggregateFunctionCombinatorSimpleState(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorSimpleState>());
}

}
