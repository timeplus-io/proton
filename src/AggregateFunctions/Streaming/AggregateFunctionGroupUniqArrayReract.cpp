#include <AggregateFunctions/Streaming/AggregateFunctionGroupUniqArrayRetract.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int BAD_ARGUMENTS;
}

namespace Streaming
{
namespace
{

/// Substitute return type for Date and DateTime
template <typename HasLimit>
class AggregateFunctionGroupUniqArrayDateRetract : public AggregateFunctionGroupUniqArrayRetract<DataTypeDate::FieldType, HasLimit>
{
public:
    explicit AggregateFunctionGroupUniqArrayDateRetract(
        const DataTypePtr & argument_type, const Array & parameters_, UInt64 max_elems_ = std::numeric_limits<UInt64>::max())
        : AggregateFunctionGroupUniqArrayRetract<DataTypeDate::FieldType, HasLimit>(argument_type, parameters_, max_elems_)
    {
    }
    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeDate>()); }
};

template <typename HasLimit>
class AggregateFunctionGroupUniqArrayDateTimeRetract : public AggregateFunctionGroupUniqArrayRetract<DataTypeDateTime::FieldType, HasLimit>
{
public:
    explicit AggregateFunctionGroupUniqArrayDateTimeRetract(
        const DataTypePtr & argument_type, const Array & parameters_, UInt64 max_elems_ = std::numeric_limits<UInt64>::max())
        : AggregateFunctionGroupUniqArrayRetract<DataTypeDateTime::FieldType, HasLimit>(argument_type, parameters_, max_elems_)
    {
    }
    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateTime>()); }
};

template <typename HasLimit, typename... TArgs>
IAggregateFunction * createWithExtraTypes(const DataTypePtr & argument_type, TArgs &&... args)
{
    WhichDataType which(argument_type);
    if (which.idx == TypeIndex::Date)
        return new AggregateFunctionGroupUniqArrayDateRetract<HasLimit>(argument_type, std::forward<TArgs>(args)...);
    else if (which.idx == TypeIndex::DateTime)
        return new AggregateFunctionGroupUniqArrayDateRetract<HasLimit>(argument_type, std::forward<TArgs>(args)...);
    else
    {
        /// Check that we can use plain version of AggregateFunctionGroupUniqArrayRetractGeneric
        if (argument_type->isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
            return new AggregateFunctionGroupUniqArrayRetractGeneric<true, HasLimit>(argument_type, std::forward<TArgs>(args)...);
        else
            return new AggregateFunctionGroupUniqArrayRetractGeneric<false, HasLimit>(argument_type, std::forward<TArgs>(args)...);
    }
}

template <typename HasLimit, typename... TArgs>
inline AggregateFunctionPtr
createAggregateFunctionGroupUniqArrayRetractImpl(const std::string & name, const DataTypePtr & argument_type, TArgs... args)
{
    AggregateFunctionPtr res(createWithNumericType<AggregateFunctionGroupUniqArrayRetract, HasLimit, const DataTypePtr &, TArgs...>(
        *argument_type, argument_type, std::forward<TArgs>(args)...));

    if (!res)
        res = AggregateFunctionPtr(createWithExtraTypes<HasLimit>(argument_type, std::forward<TArgs>(args)...));

    if (!res)
        throw Exception(
            "Illegal type " + argument_type->getName() + " of argument for aggregate function " + name,
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return res;
}

AggregateFunctionPtr createAggregateFunctionGroupUniqArrayRetract(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertUnary(name, argument_types);

    bool limit_size = false;
    Int64 max_elems = std::numeric_limits<Int64>::max();

    if (parameters.empty())
    {
        // no limit
    }
    else if (parameters.size() == 1)
    {
        auto type = parameters[0].getType();
        if (type != Field::Types::Int64 && type != Field::Types::UInt64)
            throw Exception("Parameter for aggregate function " + name + " should be positive number", ErrorCodes::BAD_ARGUMENTS);

        if ((type == Field::Types::Int64 && parameters[0].get<Int64>() < 0)
            || (type == Field::Types::UInt64 && parameters[0].get<UInt64>() == 0))
            throw Exception("Parameter for aggregate function " + name + " should be positive number", ErrorCodes::BAD_ARGUMENTS);

        limit_size = true;
        max_elems = parameters[0].get<Int64>();
    }
    else
        throw Exception(
            "Incorrect number of parameters for aggregate function " + name + ", should be 0 or 1",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    if (!limit_size)
        return createAggregateFunctionGroupUniqArrayRetractImpl<std::false_type>(name, argument_types[0], parameters);
    else
        return createAggregateFunctionGroupUniqArrayRetractImpl<std::true_type>(name, argument_types[0], parameters, max_elems);
}

}

void registerAggregateFunctionGroupUniqArrayRetract(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = {.returns_default_when_only_null = false, .is_order_dependent = true};

    factory.registerFunction("group_uniq_array_retract", {createAggregateFunctionGroupUniqArrayRetract, properties});
}

}
}
