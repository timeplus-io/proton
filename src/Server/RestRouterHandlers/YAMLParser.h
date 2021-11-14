#pragma once

#include <Poco/JSON/Object.h>
#include <base/types.h>

namespace DB
{
Poco::JSON::Object parseToJson(const String & path);
};
