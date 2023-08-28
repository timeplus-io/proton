#pragma once

#include <base/types.h>
#include <Poco/JSON/Parser.h>

class ASTPtr;
class ContextPtr;

namespace DB
{
String getCreateColumnDefinition(const Poco::JSON::Object::Ptr & payload);
String getUpdateColumnDefinition(
    ContextPtr ctx, const Poco::JSON::Object::Ptr & payload, const String & database, const String & table, String & column);
}
