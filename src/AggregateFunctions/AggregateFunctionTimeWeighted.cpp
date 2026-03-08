#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>
#include <AggregateFunctions/AggregateFunctionTimeWeighted.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

bool isSupportedValueType(const DataTypePtr & data_type) noexcept
{
    const WhichDataType dt(data_type);
    return dt.isInt() || dt.isUInt() || dt.isFloat() || dt.isDecimal();
}

DataTypes transformTimeWeightedArguments(const String & name, const DataTypes & arguments)
{
    if (arguments.size() != 2 && arguments.size() != 3)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Incorrect number of arguments for aggregate function {}", name);

    if (!isSupportedValueType(arguments[0]))
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal value type {} for aggregate function {}", arguments[0]->getName(), name);

    const WhichDataType time_type(arguments[1]);
    if (!time_type.isDateOrDate32() && !time_type.isDateTime() && !time_type.isDateTime64())
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal time type {} for aggregate function {}", arguments[1]->getName(), name);

    if (arguments.size() == 3 && !arguments[2]->equals(*arguments[1]))
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "The second and the third argument should be the same for aggregate function {}, but got {} and {}",
            name,
            arguments[1]->getName(),
            arguments[2]->getName());

    return {arguments[0], std::make_shared<DataTypeUInt64>()};
}

#define AT_SWITCH(LINE) \
    switch (which.idx) \
    { \
        LINE(Int8); \
        LINE(Int16); \
        LINE(Int32); \
        LINE(Int64); \
        LINE(Int128); \
        LINE(Int256); \
        LINE(UInt8); \
        LINE(UInt16); \
        LINE(UInt32); \
        LINE(UInt64); \
        LINE(UInt128); \
        LINE(UInt256); \
        LINE(Decimal32); \
        LINE(Decimal64); \
        LINE(Decimal128); \
        LINE(Decimal256); \
        LINE(Float32); \
        LINE(Float64); \
        default: \
            return nullptr; \
    }

template <class First, class... TArgs>
IAggregateFunction * createForTimeType(const IDataType & time_type, TArgs &&... args)
{
    const WhichDataType which(time_type);
    switch (which.idx)
    {
        case TypeIndex::Date:
            return new AggregateFunctionTimeWeighted<First, DataTypeDate::FieldType>(std::forward<TArgs>(args)...);
        case TypeIndex::Date32:
            return new AggregateFunctionTimeWeighted<First, DataTypeDate32::FieldType>(std::forward<TArgs>(args)...);
        case TypeIndex::DateTime:
            return new AggregateFunctionTimeWeighted<First, DataTypeDateTime::FieldType>(std::forward<TArgs>(args)...);
        case TypeIndex::DateTime64:
            return new AggregateFunctionTimeWeighted<First, DataTypeDateTime64::FieldType>(std::forward<TArgs>(args)...);
        default:
            return nullptr;
    }
}

template <class... TArgs>
IAggregateFunction * createTimeWeighted(const IDataType & value_type, const IDataType & time_type, TArgs &&... args)
{
    const WhichDataType which(value_type);
#define LINE(Type) \
    case TypeIndex::Type: \
        return createForTimeType<Type, TArgs...>(time_type, std::forward<TArgs>(args)...);
    AT_SWITCH(LINE)
#undef LINE
}

AggregateFunctionPtr
createTimeWeightedAggregate(const AggregateFunctionPtr & nested_function, const DataTypes & arguments, const Array & params)
{
    AggregateFunctionPtr ptr;
    ptr.reset(createTimeWeighted(*arguments[0], *arguments[1], nested_function, arguments, params));
    if (!ptr)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal argument types in aggregate function {}", nested_function->getName());

    return ptr;
}

class AggregateFunctionCombinatorTimeWeighted final : public IAggregateFunctionCombinator
{
public:
    String getName() const override { return "_time_weighted"; }

    DataTypes transformArguments(const DataTypes & arguments) const override
    {
        return transformTimeWeightedArguments(String{"aggregate function with "} + getName() + " suffix", arguments);
    }

    AggregateFunctionPtr transformAggregateFunction(
        const AggregateFunctionPtr & nested_function,
        const AggregateFunctionProperties &,
        const DataTypes & arguments,
        const Array & params) const override
    {
        return createTimeWeightedAggregate(nested_function, arguments, params);
    }
};
}

void registerAggregateFunctionCombinatorTimeWeighted(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorTimeWeighted>());
}
}
