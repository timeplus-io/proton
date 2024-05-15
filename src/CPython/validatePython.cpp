#ifdef ENABLE_PYTHON_UDF
#include <CPython/validatePython.h>
#include <Common/Exception.h>
#include <functional>
namespace DB
{

namespace ErrorCodes
{
extern const int AGGREGATE_FUNCTION_THROW;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int NOT_IMPLEMENTED;
extern const int UDF_COMPILE_ERROR;
extern const int UDF_MEMORY_THRESHOLD_EXCEEDED;
extern const int UDF_INTERNAL_ERROR;
}
namespace cpython
{



std::string catchException()
{
    PyObject *ptype, *pvalue, *ptraceback;
    PyErr_Fetch(&ptype, &pvalue, &ptraceback);

    PyObject* pvalueStr = PyObject_Str(pvalue);
    const char* pStrErrorMessage = PyUnicode_AsUTF8(pvalueStr);

    Py_XDECREF(ptype);
    Py_XDECREF(pvalue);
    Py_XDECREF(ptraceback);
    Py_XDECREF(pvalueStr);

    return pStrErrorMessage;
}

void tryCompileAndRegister(const std::string & source, std::function<void(PyObject *)> validate_function)
{
    /// Validate the source code of the aggregation function
    PyGILState_STATE gstate;
    gstate = PyGILState_Ensure();
    const char * py_source_code = source.c_str();

    /// compile the python code , throw exception if the code is invalid
    PyObject *py_byte_code = Py_CompileString(py_source_code, "", Py_file_input);
    if (py_byte_code == NULL)
    {
        std::string error_message = catchException();
        throw Exception(ErrorCodes::UDF_COMPILE_ERROR, "the Python UDF is invalid. Detail error{}", error_message);
        // release the GIL
        PyGILState_Release(gstate);
    }
    /// validate the function, and register function or class to python namespace
    validate_function(py_byte_code);
    Py_XDECREF(py_byte_code);
    PyGILState_Release(gstate);
}

void validateAggregationFunctionSource(const std::string & source, const std::string & function_name, const std::vector<std::string> & required_member_funcs)
{
    auto validate_member_func = [&function_name, &required_member_funcs](PyObject *py_byte_code)
    {
        PyObject * exe_result = PyEval_EvalCode(py_byte_code, PyModule_GetDict(PyImport_AddModule("__main__")), nullptr);
        if (exe_result == nullptr)
        {
            std::string error_message = cpython::catchException();
            throw Exception(ErrorCodes::UDF_INTERNAL_ERROR, "UDF running error, detail message {}", error_message);
        }
        PyObject * pyClass = PyObject_GetAttrString(PyImport_AddModule("__main__"), function_name.c_str());
        if (pyClass == nullptr)
            throw Exception(ErrorCodes::UDF_INTERNAL_ERROR, "The python class name is not the same as the user defined fucntion name: {}", function_name);
        
        PyObject * py_instance = PyObject_CallObject(pyClass, nullptr);
        for (size_t i = 0; i < required_member_funcs.size(); i++)
        {
            PyObject * py_member_func = PyObject_GetAttrString(py_instance, required_member_funcs[i].c_str());
            if (py_member_func == nullptr)
                throw Exception(ErrorCodes::UDF_INTERNAL_ERROR, "The python class does not have the member function: {}", required_member_funcs[i]);
            Py_XDECREF(py_member_func);
        }
        /// release the memory
        Py_XDECREF(py_instance);

    };
    tryCompileAndRegister(source, std::move(validate_member_func));
}

void validateStatelessFunctionSource(const std::string & source, const std::string & function_name)
{
    auto validate_function_name = [&function_name](PyObject *py_byte_code)
    {
        PyObject * exe_result = PyEval_EvalCode(py_byte_code, PyModule_GetDict(PyImport_AddModule("__main__")), nullptr);
        if (exe_result == nullptr)
        {
            std::string error_message = cpython::catchException();
            throw Exception(ErrorCodes::UDF_INTERNAL_ERROR, "UDF running error, detail message {}", error_message);
        }
        PyObject * py_function = PyObject_GetAttrString(PyImport_AddModule("__main__"), function_name.c_str());
        if (py_function == nullptr)
            throw Exception(ErrorCodes::UDF_INTERNAL_ERROR, "The python function name is not the same as the user defined fucntion name: {}", function_name);
        Py_XDECREF(py_function);
    };

    tryCompileAndRegister(source, std::move(validate_function_name));
}

}
}
#endif