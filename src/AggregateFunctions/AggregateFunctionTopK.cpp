#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionTopK.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>


static inline constexpr UInt64 TOP_K_MAX_SIZE = 0xFFFFFF;


namespace DB
{

struct Settings;

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


namespace
{

/// Substitute return type for Date and DateTime
template <bool is_weighted>
class AggregateFunctionTopKDate : public AggregateFunctionTopK<DataTypeDate::FieldType, is_weighted>
{
    using AggregateFunctionTopK<DataTypeDate::FieldType, is_weighted>::AggregateFunctionTopK;
    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeDate>()); }
};

template <bool is_weighted>
class AggregateFunctionTopKExactDate : public AggregateFunctionTopKExact<DataTypeDate::FieldType, is_weighted>
{
    using AggregateFunctionTopKExact<DataTypeDate::FieldType, is_weighted>::AggregateFunctionTopKExact;
    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeDate>()); }
};

template <bool is_weighted>
class AggregateFunctionTopKDateTime : public AggregateFunctionTopK<DataTypeDateTime::FieldType, is_weighted>
{
    using AggregateFunctionTopK<DataTypeDateTime::FieldType, is_weighted>::AggregateFunctionTopK;
    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateTime>()); }
};

template <bool is_weighted>
class AggregateFunctionTopKExactDateTime : public AggregateFunctionTopKExact<DataTypeDateTime::FieldType, is_weighted>
{
    using AggregateFunctionTopKExact<DataTypeDateTime::FieldType, is_weighted>::AggregateFunctionTopKExact;
    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateTime>()); }
};

/// proton: starts. Extended with count
template <bool is_weighted>
class AggregateFunctionTopKDateWithCount : public AggregateFunctionTopKWithCount<DataTypeDate::FieldType, is_weighted>
{
    using AggregateFunctionTopKWithCount<DataTypeDate::FieldType, is_weighted>::AggregateFunctionTopKWithCount;
    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeArray>(
            std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeDate>(), std::make_shared<DataTypeUInt64>()}));
    }
};

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
class AggregateFunctionTopKDateTimeWithCount : public AggregateFunctionTopKWithCount<DataTypeDateTime::FieldType, is_weighted>
{
    using AggregateFunctionTopKWithCount<DataTypeDateTime::FieldType, is_weighted>::AggregateFunctionTopKWithCount;
    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeArray>(
            std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeDateTime>(), std::make_shared<DataTypeUInt64>()}));
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

template <bool is_weighted, bool with_count, bool is_exact>
IAggregateFunction * createWithExtraTypes(const DataTypes & argument_types, UInt64 threshold, UInt64 load_factor, const Array & params)
{
    if (argument_types.empty())
        throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Got empty arguments list");

    WhichDataType which(argument_types[0]);

    if constexpr (with_count)
    {
        if (which.idx == TypeIndex::Date)
        {
            if (is_exact)
                return new AggregateFunctionTopKExactDateWithCount<is_weighted>(threshold, argument_types, params);
            else
                return new AggregateFunctionTopKDateWithCount<is_weighted>(threshold, load_factor, argument_types, params);
        }
        if (which.idx == TypeIndex::DateTime)
        {
            if (is_exact)
                return new AggregateFunctionTopKExactDateTimeWithCount<is_weighted>(threshold, argument_types, params);
            else
                return new AggregateFunctionTopKDateTimeWithCount<is_weighted>(threshold, load_factor, argument_types, params);
        }

        /// Check that we can use plain version of AggregateFunctionTopKGeneric
        if (argument_types[0]->isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
        {
            if (is_exact)
                return new AggregateFunctionTopKExactGenericWithCount<true, is_weighted>(threshold, argument_types, params);
            else
                return new AggregateFunctionTopKGenericWithCount<true, is_weighted>(threshold, load_factor, argument_types, params);
        }
        else
        {
            if (is_exact)
                return new AggregateFunctionTopKExactGenericWithCount<false, is_weighted>(threshold, argument_types, params);
            else
                return new AggregateFunctionTopKGenericWithCount<false, is_weighted>(threshold, load_factor, argument_types, params);
        }
    }
    else
    {
        if (which.idx == TypeIndex::Date)
        {
            if (is_exact)
                return new AggregateFunctionTopKExactDate<is_weighted>(threshold, argument_types, params);
            else
                return new AggregateFunctionTopKDate<is_weighted>(threshold, load_factor, argument_types, params);
        }
        if (which.idx == TypeIndex::DateTime)
        {
            if (is_exact)
                return new AggregateFunctionTopKExactDateTime<is_weighted>(threshold, argument_types, params);
            else
                return new AggregateFunctionTopKDateTime<is_weighted>(threshold, load_factor, argument_types, params);
        }

        /// Check that we can use plain version of AggregateFunctionTopKGeneric
        if (argument_types[0]->isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
        {
            if (is_exact)
                return new AggregateFunctionTopKExactGeneric<true, is_weighted>(threshold, argument_types, params);
            else
                return new AggregateFunctionTopKGeneric<true, is_weighted>(threshold, load_factor, argument_types, params);
        }
        else
        {
            if (is_exact)
                return new AggregateFunctionTopKExactGeneric<false, is_weighted>(threshold, argument_types, params);
            else
                return new AggregateFunctionTopKGeneric<false, is_weighted>(threshold, load_factor, argument_types, params);
        }
    }
}


template <bool is_weighted>
AggregateFunctionPtr
createAggregateFunctionTopK(const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *)
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

    /// Synatx: top_k(key, k [, with_count = true] [, load_factor = 1000)
    /// Synatx: top_k_weighted(key, weighted, k,  [, with_count = true] [, load_factor = 1000)
    UInt64 threshold = 10; /// default values
    UInt64 load_factor = 1000;
    bool with_count = true;

    if (!params.empty())
    {
        if (params.size() > 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function '{}' requires three parameters or less", name);

        if (params.size() >= 2)
            with_count = applyVisitor(FieldVisitorConvertToNumber<bool>(), params[1]);

        if (params.size() == 3)
            load_factor = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), params[2]);

        threshold = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), params[0]);

        if (threshold > TOP_K_MAX_SIZE || load_factor > TOP_K_MAX_SIZE || threshold * load_factor > TOP_K_MAX_SIZE)
            throw Exception(
                ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                "Too large parameter(s) for aggregate function '{}' (maximum is {})",
                name,
                toString(TOP_K_MAX_SIZE));

        if (threshold == 0)
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Parameter 0 is illegal for aggregate function '{}'", name);
    }

    AggregateFunctionPtr res;
    if (with_count)
    {
        res.reset(createWithNumericType<AggregateFunctionTopKWithCount, is_weighted>(
            *argument_types[0], threshold, load_factor, argument_types, params));

        if (!res)
            res = AggregateFunctionPtr(createWithExtraTypes<is_weighted, true, false>(argument_types, threshold, load_factor, params));
    }
    else
    {
        res.reset(
            createWithNumericType<AggregateFunctionTopK, is_weighted>(*argument_types[0], threshold, load_factor, argument_types, params));

        if (!res)
            res = AggregateFunctionPtr(createWithExtraTypes<is_weighted, false, false>(argument_types, threshold, load_factor, params));
    }

    if (!res)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of argument for aggregate function '{}'",
            argument_types[0]->getName(),
            name);
    return res;
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

    /// Synatx: top_k_exact(key, k [, with_count = true] [, load_factor = 1000)
    /// Synatx: top_k_exact_weighted(key, weighted, k,  [, with_count = true] [, load_factor = 1000)
    UInt64 threshold = 10; /// default values
    bool with_count = true;

    if (!params.empty())
    {
        if (params.size() > 2)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function '{}' requires three parameters or less", name);

        if (params.size() == 2)
            with_count = applyVisitor(FieldVisitorConvertToNumber<bool>(), params[1]);

        threshold = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), params[0]);

        if (threshold == 0)
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Parameter 0 is illegal for aggregate function '{}'", name);
    }

    AggregateFunctionPtr res;
    if (with_count)
    {
        res.reset(createWithNumericType<AggregateFunctionTopKExactWithCount, is_weighted>(
            *argument_types[0], threshold, argument_types, params));

        if (!res)
            res = AggregateFunctionPtr(createWithExtraTypes<is_weighted, true, true>(argument_types, threshold, 0, params));
    }
    else
    {
        res.reset(createWithNumericType<AggregateFunctionTopKExact, is_weighted>(
            *argument_types[0], threshold, argument_types, params));

        if (!res)
            res = AggregateFunctionPtr(createWithExtraTypes<is_weighted, false, true>(argument_types, threshold, 0, params));
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

void registerAggregateFunctionTopK(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = {.returns_default_when_only_null = false, .is_order_dependent = true};

    factory.registerFunction("top_k", {createAggregateFunctionTopK<false>, properties});
    factory.registerFunction("top_k_weighted", {createAggregateFunctionTopK<true>, properties});
    factory.registerFunction("top_k_exact", {createAggregateFunctionTopKExact<false>, properties});
    factory.registerFunction("top_k_exact_weighted", {createAggregateFunctionTopKExact<true>, properties});
}
/// proton: ends.
}
