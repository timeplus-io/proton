#pragma once

#include <rocksdb/env.h>

namespace Poco
{
class Logger;
}

using LoggerPtr = std::shared_ptr<Poco::Logger>;

namespace DB
{

class RocksLogger final : public rocksdb::Logger
{
public:
    explicit RocksLogger(rocksdb::InfoLogLevel level_, LoggerPtr logger_) : rocksdb::Logger(level_), logger(logger_) { }
    void Logv(const rocksdb::InfoLogLevel log_level, const char* format, va_list ap) override;
    void Logv(const char * format, va_list ap) override;

private:
    LoggerPtr logger;
};

}
