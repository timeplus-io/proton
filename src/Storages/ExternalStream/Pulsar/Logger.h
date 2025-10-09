
#pragma once

#include "config.h"

#if USE_PULSAR

#    include <pulsar/Logger.h>
#    include <Poco/Logger.h>

using LoggerPtr = std::shared_ptr<Poco::Logger>;

namespace DB
{

namespace ExternalStream
{

class PulsarLogger final : public pulsar::Logger
{
public:
    explicit PulsarLogger(const std::string & file_name_, LoggerPtr);
    ~PulsarLogger() override = default;

    bool isEnabled(Level level) override;
    void log(Level level, int line, const std::string & message) override;

private:
    std::string file_name;
    LoggerPtr logger;
};

class PulsarLoggerFactory final : public pulsar::LoggerFactory
{
public:
    explicit PulsarLoggerFactory(LoggerPtr);
    ~PulsarLoggerFactory() override = default;

    pulsar::Logger * getLogger(const std::string & file_name) override;

private:
    LoggerPtr logger;
};

}

}

#endif
