#include "AggregateFunctionMinMaxK.h"

#include "AggregateFunctionFactory.h"
#include "FactoryHelpers.h"
#include "Helpers.h"

#include <Common/FieldVisitorConvertToNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


namespace
{

    /// Substitute return type for Date and DateTime
    template <bool is_min>
    class AggregateFunctionMinMaxKDate : public AggregateFunctionMinMaxK<DataTypeDate::FieldType, is_min>
    {
        using AggregateFunctionMinMaxK<DataTypeDate::FieldType, is_min>::AggregateFunctionMinMaxK;
        DataTypePtr getReturnType() const override { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeDate>()); }
    };

    template <bool is_min>
    class AggregateFunctionMinMaxKDateTime : public AggregateFunctionMinMaxK<DataTypeDateTime::FieldType, is_min>
    {
        using AggregateFunctionMinMaxK<DataTypeDateTime::FieldType, is_min>::AggregateFunctionMinMaxK;
        DataTypePtr getReturnType() const override { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateTime>()); }
    };


    template <bool is_min>
    static IAggregateFunction * createWithExtraTypes(const DataTypePtr & argument_type, UInt64 k, const Array & params)
    {
        WhichDataType which(argument_type);
        if (which.idx == TypeIndex::Date)
            return new AggregateFunctionMinMaxKDate<is_min>(k, argument_type, params);
        if (which.idx == TypeIndex::DateTime)
            return new AggregateFunctionMinMaxKDateTime<is_min>(k, argument_type, params);

        /// Check that we can use plain version of AggregateFunctionMinMaxKGeneric
        if (argument_type->isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
            return new AggregateFunctionMinMaxKGeneric<true, is_min>(k, argument_type, params);
        else
            return new AggregateFunctionMinMaxKGeneric<false, is_min>(k, argument_type, params);
    }


    template <bool is_min>
    AggregateFunctionPtr
    createAggregateFunctionMinMaxK(const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *)
    {
        assertUnary(name, argument_types);
        UInt64 k = 10; /// default values

        if (!params.empty())
        {
            if (params.size() > 1)
                throw Exception("Aggregate function " + name + " requires one parameters.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

            k = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), params[0]);
            if (k > TOP_K_MAX_SIZE)
                throw Exception(
                    "Too large parameter(s) for aggregate function " + name + ". Maximum: " + toString(TOP_K_MAX_SIZE),
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND);

            if (k == 0)
                throw Exception("Parameter 0 is illegal for aggregate function " + name, ErrorCodes::ARGUMENT_OUT_OF_BOUND);
        }

        AggregateFunctionPtr res(createWithNumericType<AggregateFunctionMinMaxK, is_min>(*argument_types[0], k, argument_types[0], params));

        if (!res)
            res = AggregateFunctionPtr(createWithExtraTypes<is_min>(argument_types[0], k, params));

        if (!res)
            throw Exception(
                "Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name,
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return res;
    }

}

void registerAggregateFunctionMinMaxK(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = {.returns_default_when_only_null = false, .is_order_dependent = true};

    factory.registerFunction("maxK", {createAggregateFunctionMinMaxK<false>, properties});
    factory.registerFunction("minK", {createAggregateFunctionMinMaxK<true>, properties});
}

}
