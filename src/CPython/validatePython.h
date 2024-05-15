#ifdef ENABLE_PYTHON_UDF
#include <Python.h>
#include <Poco/Util/JSONConfiguration.h>
#include <vector>
#include <string>

namespace DB
{
namespace cpython
{
/// Catch the exception
std::string catchException();

void validateAggregationFunctionSource(const std::string & source, const std::string & function_name, const std::vector<std::string> & required_member_funcs);
void validateStatelessFunctionSource(const std::string & source, const std::string & function_name);
void tryCompileAndRegister(const std::string & source, std::function<void(PyObject *)> validate_function);


void dryRunPythonUDF(const std::string & source, const std::string & function_name, const std::vector<std::string> & arguments, const std::string & return_type);
}
}
#endif