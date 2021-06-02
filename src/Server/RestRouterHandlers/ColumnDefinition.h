#pragma once

#include <Poco/JSON/Parser.h>
#include <common/types.h>

class ASTPtr;
class ContextPtr;

namespace DB
{
String getCreateColumnDefination(const Poco::JSON::Object::Ptr & payload);
String getUpdateColumnDefination(const Poco::JSON::Object::Ptr & payload, String & column_name);
}
