#pragma once

#include <string>
#include <unordered_set>

namespace DB
{
/// Valid log levels that can be used for dynamic log level changes
/// These correspond to Poco log levels and our internal LogsLevel enum
inline const std::unordered_set<std::string> & getValidLogLevels()
{
    static const std::unordered_set<std::string> VALID_LOG_LEVELS
        = {"none", "fatal", "critical", "error", "warning", "notice", "information", "debug", "trace", "test"};
    return VALID_LOG_LEVELS;
}
}
