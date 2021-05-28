#pragma once

#include <common/types.h>

#include <Poco/URI.h>

#include  <utility>

namespace Poco
{
class Logger;
}

namespace DB
{
/// Return response and http status code. Note it returns `-1` as
/// a speicial HTTP code which indicates if it is not a retriable failure
std::pair<String, Int32> sendRequest(
    const Poco::URI & uri,
    const String & method,
    const String & query_id,
    const String & user,
    const String & password,
    const String & payload,
    Poco::Logger * log);
}
