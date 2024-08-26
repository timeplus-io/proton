#pragma once

#include <base/types.h>

#include <IO/ConnectionTimeouts.h>

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
    const std::vector<std::pair<String, String>> & headers,
    Poco::Logger * log,
    ConnectionTimeouts timeouts = ConnectionTimeouts({2, 0}, {5, 0}, {10, 0}));
}
