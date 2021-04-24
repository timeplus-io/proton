#pragma once

#include <Poco/JSON/Parser.h>
#include <common/types.h>

#include <map>


namespace DB
{
/// Return true if validation is good; otherwise returns false and `error_msg` will bet setup
bool validateSchema(const std::map<String, std::map<String, String>> & schema, const Poco::JSON::Object::Ptr & payload, String & error_msg);
}
