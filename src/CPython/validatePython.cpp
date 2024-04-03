#include <CPython/validatePython.h>
#include <Common/Exception.h>
namespace DB
{

namespace ErrorCodes
{
extern const int AGGREGATE_FUNCTION_THROW;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int NOT_IMPLEMENTED;
extern const int UDF_COMPILE_ERROR;
extern const int UDF_MEMORY_THRESHOLD_EXCEEDED;
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

void tryCompile(const std::string & source)
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
    PyGILState_Release(gstate);
}

void validateAggregationFunctionSource(const std::vector<std::string> & required_member_funcs, const std::string & source)
{
    tryCompile(source);
}

void validateStatelessFunctionSource(const std::string & source)
{
    // Validate the source code of the stateless function
    tryCompile(source);
}

}
}
