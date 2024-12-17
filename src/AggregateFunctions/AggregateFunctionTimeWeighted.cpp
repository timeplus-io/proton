#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>
#include <AggregateFunctions/AggregateFunctionTimeWeighted.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <AggregateFunctions/IAggregateFunction.h>

#include <memory>

namespace DB
{

namespace
{

class AggregateFunctionCombinatorTimeWeighted final : public IAggregateFunctionCombinator
{
public:
    String getName() const override { return "_time_weighted"; }

    DataTypes transformArguments(const DataTypes & arguments) const override
    {
        if (arguments.size() != 2 && arguments.size() != 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Incorrect number of arguments for aggregate function with {} suffix", this->getName());

        const auto & data_type_time_weight = arguments[1];
        const WhichDataType t_dt(data_type_time_weight);

        if (!t_dt.isDateOrDate32() && !t_dt.isDateTime() && !t_dt.isDateTime64())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Types {} are non-conforming as time weighted arguments for aggregate function {}", data_type_time_weight->getName(), this->getName());

        if (arguments.size() == 3)
        {
            const auto & data_type_third_arg = arguments[2];
            
            if(!data_type_third_arg->equals(*data_type_time_weight))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The second and the third argument should be the same for aggregate function {}, but now it's {} and {}", this->getName(), data_type_third_arg->getName(), data_type_time_weight->getName());
        }

        return {arguments[0], std::make_shared<DataTypeUInt64>()};
    }

    /// Decimal128 and Decimal256 aren't supported
    #define AT_SWITCH(LINE) \
        switch (which.idx) \
        { \
            LINE(Int8); LINE(Int16); LINE(Int32); LINE(Int64); LINE(Int128); LINE(Int256); \
            LINE(UInt8); LINE(UInt16); LINE(UInt32); LINE(UInt64); LINE(UInt128); LINE(UInt256); \
            LINE(Decimal32); LINE(Decimal64); \
            LINE(Float32); LINE(Float64); \
            default: return nullptr; \
        }

    // Not using helper functions because there are no templates for binary decimal/numeric function.
    template <class... TArgs>
    IAggregateFunction * create(const IDataType & first_type, const IDataType & second_type, TArgs && ... args) const
    {
        const WhichDataType which(first_type);

    #define LINE(Type) \
        case TypeIndex::Type:       return create<Type, TArgs...>(second_type, std::forward<TArgs>(args)...)
        AT_SWITCH(LINE)
    #undef LINE
    }
    template <class First, class ... TArgs>
    IAggregateFunction * create(const IDataType & second_type, TArgs && ... args) const
    {
        const WhichDataType which(second_type);

        switch (which.idx)
        {
            case TypeIndex::Date:    return new AggregateFunctionTimeWeighted<First, DataTypeDate::FieldType>(std::forward<TArgs>(args)...);
            case TypeIndex::Date32:   return new AggregateFunctionTimeWeighted<First, DataTypeDate32::FieldType>(std::forward<TArgs>(args)...);
            case TypeIndex::DateTime:   return new AggregateFunctionTimeWeighted<First, DataTypeDateTime::FieldType>(std::forward<TArgs>(args)...);
            case TypeIndex::DateTime64: return new AggregateFunctionTimeWeighted<First, DataTypeDateTime64::FieldType>(std::forward<TArgs>(args)...);
            default: return nullptr;
        }
    }

    AggregateFunctionPtr transformAggregateFunction(
        const AggregateFunctionPtr & nested_function,
        const AggregateFunctionProperties &,
        const DataTypes & arguments,
        const Array & params) const override
    {
        AggregateFunctionPtr ptr;
        const auto & data_type = arguments[0];
        const auto & data_type_time_weight = arguments[1];
        ptr.reset(create(*data_type, *data_type_time_weight, nested_function, arguments, params));
        if(!ptr)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal argument types existed in {} function", this->getName());

        return ptr;
    }
};
}

void registerAggregateFunctionCombinatorTimeWeighted(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorTimeWeighted>());
}
}
