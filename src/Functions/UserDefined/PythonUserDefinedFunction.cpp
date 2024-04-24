#include <Functions/UserDefined/PythonUserDefinedFunction.h>
#include <Columns/IColumn.h>

#include <CPython/validatePython.h>
#include <CPython/ConvertDatatypes.h>
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
    const char * source = config->source.c_str();
    arg_num = config->arguments.size();

    PyGILState_STATE gstate = PyGILState_Ensure();
    mainstate = PyThreadState_Get();
    substate = Py_NewInterpreter();

    PyObject * py_byte_code = Py_CompileString(source, "", Py_file_input);
    
    /// py_byte_code it can't be nullptr, because in the register process, we have already checked the source code
    assert(py_byte_code != nullptr);

    PyObject * exe_result = PyEval_EvalCode(py_byte_code, PyModule_GetDict(PyImport_AddModule("__main__")), nullptr);

    /// same as above, exe_result can't be nullptr
    assert(exe_result != nullptr);

    /// get the function object by UDF name
    py_function = PyObject_GetAttrString(PyImport_AddModule("__main__"), config->name.c_str());
    PyThreadState_Swap(mainstate);
    PyGILState_Release(gstate);

}


ColumnPtr PythonUserDefinedFunction::userDefinedExecuteImpl(
    const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /*input_rows_count*/) const
{
    PyGILState_STATE gstate = PyGILState_Ensure();

    /// switch to the sub-interpreter
    PyThreadState *oldstate = PyThreadState_Swap(substate);

    /// step 1: convert the column to python data type
    PyObject * py_args = PyTuple_New(arg_num);
    for (size_t i = 0; i < arg_num; i++)
    {
        PyObject * py_list = cpython::convertColumnToPythonList(arguments[i]);
        PyTuple_SetItem(py_args, i, py_list);
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

    /// step 3: convert the python list to column
    auto column = cpython::convertPythonListToColumn(py_result, result_type);

    /// return to the main interpreter
    PyThreadState_Swap(oldstate);
    PyGILState_Release(gstate);
    return column;
}

PythonUserDefinedFunction::~PythonUserDefinedFunction()
{
    /// switch to the substate
    PyThreadState_Swap(substate);

    /// release the py_function
    Py_XDECREF(py_function);
    Py_EndInterpreter(substate);
    PyThreadState_Swap(nullptr);
}
}