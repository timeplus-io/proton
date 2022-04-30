#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/ObjectUtils.h>

namespace DB
{
namespace
{

/** to_concrete_type(x) - get the concrete type name
  * Returns name of IDataType instance (name of data type).
  * It is mainly used to obtain the concrete type of dynamic Object
  */
class FunctionToConcreteType : public IFunction
{
public:

    static constexpr auto name = "to_concrete_type";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionToConcreteType>();
    }

    String getName() const override
    {
        return name;
    }

    bool useDefaultImplementationForNulls() const override { return false; }

    bool isShortCircuit(ShortCircuitSettings & settings, size_t /*number_of_arguments*/) const override
    {
        settings.enable_lazy_execution_for_first_argument = false;
        settings.enable_lazy_execution_for_common_descendants_of_arguments = true;
        settings.force_enable_lazy_execution = true;
        return true;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    bool isSuitableForConstantFolding() const override { return false; }

    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        if (isObject(arguments[0].type))
            return DataTypeString().createColumnConst(
                input_rows_count, getConcreteTypeFromObject(assert_cast<const ColumnObject &>(*arguments[0].column))->getName());
        else
            return DataTypeString().createColumnConst(input_rows_count, arguments[0].type->getName());
    }

    ColumnPtr getConstantResultForNonConstArguments(const ColumnsWithTypeAndName & arguments, const DataTypePtr &) const override
    {
        if (isObject(arguments[0].type))
            return DataTypeString().createColumnConst(
                1, getConcreteTypeFromObject(assert_cast<const ColumnObject &>(*arguments[0].column))->getName());
        else
            return DataTypeString().createColumnConst(1, arguments[0].type->getName());
    }

    ColumnNumbers getArgumentsThatDontImplyNullableReturnType(size_t /*number_of_arguments*/) const override { return {0}; }
};

}

void registerFunctionToConcreteType(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToConcreteType>();
}

}
