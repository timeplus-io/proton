#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/CastOverloadResolver.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionHelpers.h>


namespace DB
{
namespace
{
    class FunctionToBool : public IFunction
    {
    private:
        ContextPtr context;

        static String getReturnTypeName(const DataTypePtr & argument)
        {
            return argument->isNullable() ? "nullable(bool)" : "bool";
        }

    public:
        static constexpr auto name = "to_bool";

        static FunctionPtr create(ContextPtr)
        {
            return std::make_shared<FunctionToBool>();
        }

        std::string getName() const override
        {
            return name;
        }

        size_t getNumberOfArguments() const override { return 1; }
        bool useDefaultImplementationForConstants() const override { return true; }
        bool useDefaultImplementationForNulls() const override { return false; }
        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

        DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
        {
            return DataTypeFactory::instance().get(getReturnTypeName(arguments[0]));
        }

        ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t) const override
        {
            ColumnsWithTypeAndName cast_args
            {
                arguments[0],
                {
                    /// FIXME(yokofly): Direct conversion from String to Field is not possible. Added toField for explicit casting.
                    DataTypeString().createColumnConst(arguments[0].column->size(), toField(getReturnTypeName(arguments[0].type))),
                    std::make_shared<DataTypeString>(),
                    ""
                }
            };

            FunctionOverloadResolverPtr func_builder_cast = createInternalCastOverloadResolver(CastType::nonAccurate, {});
            auto func_cast = func_builder_cast->build(cast_args);
            return func_cast->execute(cast_args, result_type, arguments[0].column->size());
        }
    };
}

REGISTER_FUNCTION(ToBool)
{
    factory.registerFunction<FunctionToBool>();
}

}
