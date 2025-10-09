#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>

#if USE_PYTHON_UDF
#include <Python.h>
#endif

namespace DB
{
namespace
{
class FunctionPythonStatus : public IFunction
{
public:
    static constexpr auto name = "python_status";

    explicit FunctionPythonStatus() = default;

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override { return std::make_shared<DataTypeString>(); }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
#if USE_PYTHON_UDF
        String python_status = fmt::format(R"({{"initialized": {}}})", static_cast<bool>(Py_IsInitialized()));
#else
        String python_status = fmt::format(R"({{"initialized": {}}})", false);
#endif
        auto col = ColumnString::create();

        for (size_t i = 0; i < input_rows_count; ++i)
            col->insert(python_status);

        return col;
    }

    static FunctionPtr create(ContextPtr /*context*/) { return std::make_shared<FunctionPythonStatus>(); }
};
}

REGISTER_FUNCTION(PythonStatus)
{
    factory.registerFunction<FunctionPythonStatus>();
}


}
