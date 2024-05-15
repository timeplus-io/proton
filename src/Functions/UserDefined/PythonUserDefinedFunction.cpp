#ifdef ENABLE_PYTHON_UDF
#include <Functions/UserDefined/PythonUserDefinedFunction.h>
#include <Columns/IColumn.h>

#include <CPython/validatePython.h>
#include <CPython/ConvertDatatypes.h>
#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION
#include <numpy/arrayobject.h>
namespace DB
{

namespace ErrorCodes
{
extern const int AGGREGATE_FUNCTION_THROW;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int UDF_RUNNING_ERROR;
}

PythonUserDefinedFunction::PythonUserDefinedFunction(
    ExternalUserDefinedFunctionsLoader::UserDefinedExecutableFunctionPtr executable_function_, ContextPtr context_)
    : UserDefinedFunctionBase(executable_function_, context_, "PythonUserDefinedFunction")
{
    const auto * config = executable_function->getConfiguration()->as<PythonUserDefinedFunctionConfiguration>();
    using_numpy = config->using_numpy;
    arg_num = config->arguments.size();

    PyGILState_STATE gstate = PyGILState_Ensure();
    /// get the function object by UDF name
    py_function = PyObject_GetAttrString(PyImport_AddModule("__main__"), config->name.c_str());
    PyGILState_Release(gstate);

}


ColumnPtr PythonUserDefinedFunction::userDefinedExecuteImpl(
    const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /*input_rows_count*/) const
{
    PyGILState_STATE gstate = PyGILState_Ensure();

    if (using_numpy)
        import_array()

    /// step 1: convert the column to python data type
    PyObject * py_args = PyTuple_New(arg_num);
    for (size_t i = 0; i < arg_num; i++)
    {
        PyObject * py_arg = nullptr;
        if (using_numpy)
            py_arg = cpython::convertColumnToNumpyArray(arguments[i]);
        else
            py_arg = cpython::convertColumnToPythonList(arguments[i]);
        PyTuple_SetItem(py_args, i, py_arg);
    }

    /// step 2: call the python function with the arguments
    PyObject * py_result = PyObject_CallObject(py_function, py_args);
    Py_XDECREF(py_args);
    if (py_result == nullptr)
    {
        std::string error_message = cpython::catchException();
        PyGILState_Release(gstate);
        throw Exception(ErrorCodes::UDF_RUNNING_ERROR, "UDF running error, detail message {}", error_message);
    }

    /// step 3: convert the python data to column
    ColumnPtr column = nullptr;
    if (using_numpy)
        column = cpython::covertNumpyArrayToColumn(py_result, result_type);
    else
        column = cpython::convertPythonListToColumn(py_result, result_type);

    PyGILState_Release(gstate);
    return column;
}

PythonUserDefinedFunction::~PythonUserDefinedFunction()
{
    /// release the py_function
    Py_XDECREF(py_function);
}
}
#endif