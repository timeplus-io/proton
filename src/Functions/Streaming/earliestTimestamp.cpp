#include <DataTypes/DataTypeDateTime.h>

#include <Core/DecimalFunctions.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>
#include <base/map.h>

namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

/// Get the constant earliest time '1970-1-1 00:00:00'.
class FunctionEarliestTimestamp : public IFunction
{
public:
    static constexpr auto name = "earliest_timestamp";

    FunctionEarliestTimestamp(DataTypePtr return_type_) : return_type(return_type_) { }

    String getName() const override { return name; }

    bool isDeterministic() const override { return false; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    /// We do not use default implementation for LowCardinality because this is not a pure function.
    /// If used, optimization for LC may execute function only for dictionary, which gives wrong result.
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override { return return_type; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return result_type->createColumnConst(input_rows_count, UInt64(0));
    }

private:
    DataTypePtr return_type;
};

class EarliestTimestampOverloadResolver : public IFunctionOverloadResolver
{
public:
    static constexpr auto name = "earliest_timestamp";

    String getName() const override { return name; }

    bool isDeterministic() const override { return false; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    static FunctionOverloadResolverPtr create(ContextPtr) { return std::make_unique<EarliestTimestampOverloadResolver>(); }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() > 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Arguments size of function {} should be 0 or 1", getName());

        if (arguments.size() == 1 && !isStringOrFixedString(arguments[0].type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Arguments of function {} should be string or fixed_string", getName());

        if (arguments.size() == 1)
            return std::make_shared<DataTypeDateTime>(extractTimeZoneNameFromFunctionArguments(arguments, 0, 0));
        else
            return std::make_shared<DataTypeDateTime>();
    }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type) const override
    {
        return std::make_unique<FunctionToFunctionBaseAdaptor>(
            std::make_shared<FunctionEarliestTimestamp>(result_type),
            collections::map<DataTypes>(arguments, [](const auto & elem) { return elem.type; }),
            result_type);
    }
};

}

REGISTER_FUNCTION(EarliestTimestamp)
{
    factory.registerFunction<EarliestTimestampOverloadResolver>({}, FunctionFactory::CaseInsensitive);
    factory.registerAlias("earliest_ts", "earliest_timestamp", FunctionFactory::CaseInsensitive);
}

}
