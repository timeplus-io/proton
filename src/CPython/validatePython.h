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

void validateAggregationFunctionSource(const std::vector<std::string> & required_member_funcs, const std::string & source);
void validateStatelessFunctionSource(const std::string & source);
void tyrCompile(const std::string & source);


void dryRunPythonUDF(const std::string & source, const std::string & function_name, const std::vector<std::string> & arguments, const std::string & return_type);
}
}
