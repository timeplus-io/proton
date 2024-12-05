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

namespace ErrorCodes
{

}

namespace
{

class AggregateFunctionCombinatorTimeWeighted final : public IAggregateFunctionCombinator
{
public:
    String getName() const override { return "_time_weighted"; }

    DataTypes transformArguments(const DataTypes & arguments) const override
    {
        if (arguments.size() != 2 && arguments.size() != 3)
            throw Exception("Incorrect number of arguments for aggregate function with " + getName() + " suffix",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        DataTypes nested_arguments;
        nested_arguments.push_back(arguments[0]);
        //delete
        if (isDate(*arguments.begin()))
            return nested_arguments;
        if (isDate(arguments.back()))
            nested_arguments.push_back(std::make_shared<DataTypeUInt16>());
        else if(isDate32(arguments.back()))
            nested_arguments.push_back(std::make_shared<DataTypeInt32>());
        else if(isDateTime(arguments.back()))
            nested_arguments.push_back(std::make_shared<DataTypeUInt32>());
        else if(isDateTime64(arguments.back()))
            nested_arguments.push_back(std::make_shared<DataTypeFloat64>());

        return nested_arguments;
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
        if (arguments.size() != 3 && arguments.size() != 2)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Function {} should have two or three arguments",this->getName());

        const auto data_type = static_cast<const DataTypePtr>(arguments[0]);
        const auto data_type_time_weight = static_cast<const DataTypePtr>(arguments[1]);
        const WhichDataType dt(data_type), t_dt(data_type_time_weight);

        if ((dt.isInt() || dt.isUInt() || dt.isFloat() || dt.isDecimal()) && (t_dt.isDateOrDate32() || t_dt.isDateTime()|| t_dt.isDateTime64()))
        else    
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Types {} and {} are non-conforming as arguments for aggregate function {}", data_type->getName(), data_type_time_weight->getName(), this->getName());

        if (arguments.size() == 3)
        {
            const auto data_type_third_arg = static_cast<const DataTypePtr>(arguments[2]);
            
            if(data_type_third_arg != data_type_time_weight)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The second and the third argument should be the same for aggregate function {}", this->getName());   
            // AggregateFunctionPtr ptr;

            // // const bool left_decimal = isDecimal(data_type);
            // // data_type_time_weight = UInt64;
            // // auto data_type_uint64 = std::make_shared<DataTypeUInt64>();
            // // if (left_decimal)
            // //     ptr.reset(create(*data_type, *data_type_time_weight, nested_function, arguments, params, 
            // //         getDecimalScale(*data_type)));
            // // else
            // ptr.reset(create(*data_type, *data_type_time_weight, nested_function, arguments, params));      
            // return ptr;
        }
        AggregateFunctionPtr ptr;
        ptr.reset(create(*data_type, *data_type_time_weight, nested_function, arguments, params));

        return ptr;
    }
};
}

void registerAggregateFunctionCombinatorTimeWeighted(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorTimeWeighted>());
}
}
