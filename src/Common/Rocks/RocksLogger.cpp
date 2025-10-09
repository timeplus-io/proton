#include <Common/Rocks/RocksLogger.h>
#include <Common/logger_useful.h>

#include <cstdio>

namespace DB
{

void RocksLogger::Logv(const char * format, va_list ap)
{
    /// Always forward to the level-aware method with INFO_LEVEL
    Logv(rocksdb::InfoLogLevel::INFO_LEVEL, format, ap);
}

void RocksLogger::Logv(const rocksdb::InfoLogLevel logLevel, const char * format, va_list ap)
{
    /// Log max 16 KB
    char buffer[16 * 1024] = {'\0'};

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wformat-nonliteral"
#endif /// __clang__

    auto n = vsnprintf(buffer, 16 * 1024 - 1, format, ap);
    if (n < 0)
        return;

#ifdef __clang__
#pragma clang diagnostic pop
#endif /// __clang__

    /// Map RocksDB log levels to Poco log levels, protecting against FATAL
    switch (logLevel)
    {
        case rocksdb::InfoLogLevel::DEBUG_LEVEL:
            LOG_DEBUG(logger, "{}", buffer);
            break;
        case rocksdb::InfoLogLevel::INFO_LEVEL:
            LOG_INFO(logger, "{}", buffer);
            break;
        case rocksdb::InfoLogLevel::WARN_LEVEL:
            LOG_WARNING(logger, "{}", buffer);
            break;
        case rocksdb::InfoLogLevel::ERROR_LEVEL:
            LOG_ERROR(logger, "{}", buffer);
            break;
        case rocksdb::InfoLogLevel::FATAL_LEVEL:
            /// Map FATAL to ERROR to prevent server crashes
            LOG_ERROR(logger, "{}", buffer);
            break;
        default:
            LOG_INFO(logger, "{}", buffer);
            break;
    }
}

}
