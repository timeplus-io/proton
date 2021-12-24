#include "AggregateFunctionDistinctStreaming.h"

#include "AggregateFunctionCombinatorFactory.h"
#include "Helpers.h"

#include <Common/typeid_cast.h>

/// proton: starts.
namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

    class AggregateFunctionCombinatorDistinctStreaming final : public IAggregateFunctionCombinator
    {
    public:
        String getName() const override { return "DistinctStreaming"; }

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
            AggregateFunctionPtr res;
            if (arguments.size() == 1)
            {
                res.reset(createWithNumericType<AggregateFunctionDistinctStreaming, AggregateFunctionDistinctSingleNumericData>(
                    *arguments[0], nested_function, arguments, params));

                if (res)
                    return res;

                if (arguments[0]->isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
                    return std::make_shared<AggregateFunctionDistinctStreaming<AggregateFunctionDistinctSingleGenericData<true>>>(
                        nested_function, arguments, params);
                else
                    return std::make_shared<AggregateFunctionDistinctStreaming<AggregateFunctionDistinctSingleGenericData<false>>>(
                        nested_function, arguments, params);
            }

            return std::make_shared<AggregateFunctionDistinctStreaming<AggregateFunctionDistinctMultipleGenericData>>(
                nested_function, arguments, params);
        }
    };

}

void registerAggregateFunctionCombinatorDistinctStreaming(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorDistinctStreaming>());
}

}
/// proton: ends.
