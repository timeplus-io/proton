#ifdef ENABLE_PYTHON_UDF
#pragma once

#include <Functions/UserDefined/UserDefinedFunctionBase.h>
#include <Functions/UserDefined/UserDefinedFunctionConfiguration.h>

#include <memory>
#include <Python.h>

namespace DB
{
class PythonExecutionContext
{
public:

private:

};

class PythonUserDefinedFunction final : public UserDefinedFunctionBase
{
public:
    explicit PythonUserDefinedFunction(
        ExternalUserDefinedFunctionsLoader::UserDefinedExecutableFunctionPtr executable_function_, ContextPtr context_);

    ~ PythonUserDefinedFunction() override;
    ColumnPtr userDefinedExecuteImpl(
        const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /*input_rows_count*/) const override;

private:

    PyObject * py_function = nullptr;

    /// number of the UDF arguments
    size_t arg_num = 0;

    bool using_numpy = false;
};
}
#endif