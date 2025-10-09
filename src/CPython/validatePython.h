#pragma once
#include <Poco/Util/JSONConfiguration.h>


#include <string>
#include <vector>

namespace DB::cpython
{
/**
 * @brief below 2 functions is for validating the python source code, it will do 3 things:
 * 1. Check if the source code can be compiled, then register the function to the python interpreter
 * 2. check the UDF/A name is consistent with the ddl
 * 3. using default data type value to dry run the function, and check if the return type is correct
 */
void validateAggregationFunctionSource(Poco::JSON::Object::Ptr config, const std::vector<std::string> & required_member_funcs);
void validateStatelessFunctionSource(Poco::JSON::Object::Ptr config);
}
