#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionTopKExact.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>


namespace DB
{

struct Settings;

namespace
{

/// Substitute return type for Date and DateTime
template <bool is_weighted>
class AggregateFunctionTopKExactDate : public AggregateFunctionTopKExact<DataTypeDate::FieldType, is_weighted>
{
    using AggregateFunctionTopKExact<DataTypeDate::FieldType, is_weighted>::AggregateFunctionTopKExact;
    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeDate>()); }
};

template <bool is_weighted>
class AggregateFunctionTopKExactDateTime : public AggregateFunctionTopKExact<DataTypeDateTime::FieldType, is_weighted>
{
    using AggregateFunctionTopKExact<DataTypeDateTime::FieldType, is_weighted>::AggregateFunctionTopKExact;
    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateTime>()); }
};

/// proton: starts. Extended with count

template <bool is_weighted>
class AggregateFunctionTopKExactDateWithCount : public AggregateFunctionTopKExactWithCount<DataTypeDate::FieldType, is_weighted>
{
    using AggregateFunctionTopKExactWithCount<DataTypeDate::FieldType, is_weighted>::AggregateFunctionTopKExactWithCount;
    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeArray>(
            std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeDate>(), std::make_shared<DataTypeUInt64>()}));
    }
};

template <bool is_weighted>
class AggregateFunctionTopKExactDateTimeWithCount : public AggregateFunctionTopKExactWithCount<DataTypeDateTime::FieldType, is_weighted>
{
    using AggregateFunctionTopKExactWithCount<DataTypeDateTime::FieldType, is_weighted>::AggregateFunctionTopKExactWithCount;
    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeArray>(
            std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeDateTime>(), std::make_shared<DataTypeUInt64>()}));
    }
};

template <bool is_weighted, bool with_count>
IAggregateFunction *
createWithExtraTypes(const DataTypes & argument_types, UInt64 threshold, UInt64 memory_size, const Array & params)
{
    if (argument_types.empty())
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Got empty arguments list");

    WhichDataType which(argument_types[0]);

    if constexpr (with_count)
    {
        if (which.idx == TypeIndex::Date)
            return new AggregateFunctionTopKExactDateWithCount<is_weighted>(threshold, memory_size, argument_types, params);
        if (which.idx == TypeIndex::DateTime)
            return new AggregateFunctionTopKExactDateTimeWithCount<is_weighted>(threshold, memory_size, argument_types, params);

        /// Check that we can use plain version of AggregateFunctionTopKExactGeneric
        if (argument_types[0]->isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
            return new AggregateFunctionTopKExactGenericWithCount<true, is_weighted>(threshold, memory_size, argument_types, params);
        else
            return new AggregateFunctionTopKExactGenericWithCount<false, is_weighted>(threshold, memory_size, argument_types, params);
    }
    else
    {
        if (which.idx == TypeIndex::Date)
            return new AggregateFunctionTopKExactDate<is_weighted>(threshold, memory_size, argument_types, params);
        if (which.idx == TypeIndex::DateTime)
            return new AggregateFunctionTopKExactDateTime<is_weighted>(threshold, memory_size, argument_types, params);

        /// Check that we can use plain version of AggregateFunctionTopKExactGeneric
        if (argument_types[0]->isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
            return new AggregateFunctionTopKExactGeneric<true, is_weighted>(threshold, memory_size, argument_types, params);
        else
            return new AggregateFunctionTopKExactGeneric<false, is_weighted>(threshold, memory_size, argument_types, params);
    }
}

template <bool is_weighted>
AggregateFunctionPtr
createAggregateFunctionTopKExact(const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *)
{
    if (!is_weighted)
    {
        assertUnary(name, argument_types);
    }
    else
    {
        assertBinary(name, argument_types);
        if (!isInteger(argument_types[1]))
            throw Exception(
                "The second argument for aggregate function 'top_k_weighted' must have integer type", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    /// Synatx: top_k_exact(key, k [, with_count = true][, limit_memory_size=100*1024*1024 byte]
    /// Synatx: top_k_exact_weighted(key, weighted, k[, with_count = true][, limit_memory_size=100*1024*1024 byte]
    UInt64 threshold = 10; /// default values
    bool with_count = true;
    UInt64 memory_size = 100*1024*1024; //default reserved memory for top_k_exact is 100M

    if (!params.empty())
    {
        if (params.size() > 3)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function '{}' requires three parameters or less", name);

        if (params.size() >= 2)
            with_count = applyVisitor(FieldVisitorConvertToNumber<bool>(), params[1]);

        if (params.size() == 3)
            memory_size = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), params[2]);

        threshold = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), params[0]);

        if (threshold == 0)
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Parameter 0 is illegal for aggregate function '{}'", name);
    }

    AggregateFunctionPtr res;
    if (with_count)
    {
        res.reset(createWithNumericType<AggregateFunctionTopKExactWithCount, is_weighted>(
            *argument_types[0], threshold, memory_size, argument_types, params));

        if (!res)
            res = AggregateFunctionPtr(createWithExtraTypes<is_weighted, true>(argument_types, threshold, memory_size, params));
    }
    else
    {
        res.reset(createWithNumericType<AggregateFunctionTopKExact, is_weighted>(
            *argument_types[0], threshold, memory_size, argument_types, params));

        if (!res)
            res = AggregateFunctionPtr(createWithExtraTypes<is_weighted, false>(argument_types, threshold, memory_size, params));
    }

    if (!res)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of argument for aggregate function '{}'",
            argument_types[0]->getName(),
            name);
    return res;
}
}

void registerAggregateFunctionTopKExact(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = {.returns_default_when_only_null = false, .is_order_dependent = true};
    
    factory.registerFunction("top_k_exact", {createAggregateFunctionTopKExact<false>, properties});
    factory.registerFunction("top_k_exact_weighted", {createAggregateFunctionTopKExact<true>, properties});
}
/// proton: ends.
}
