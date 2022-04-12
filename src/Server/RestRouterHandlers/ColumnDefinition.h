#pragma once

#include <Poco/JSON/Parser.h>
#include <base/types.h>

class ASTPtr;
class ContextPtr;

namespace DB
{
String getCreateColumnDefinition(const Poco::JSON::Object::Ptr & payload);
String getUpdateColumnDefination(const Poco::JSON::Object::Ptr & payload, const String & database, const String & table, String & column);
}
