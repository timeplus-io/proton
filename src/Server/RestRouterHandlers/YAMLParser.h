#pragma once

#include <common/types.h>
#include <Poco/JSON/Object.h>

namespace DB
{
Poco::JSON::Object parseToJson(const String & path);
};
