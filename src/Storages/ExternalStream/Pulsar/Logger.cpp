#include <Storages/ExternalStream/Pulsar/Logger.h>

#if USE_PULSAR

#    include <Common/logger_useful.h>

#    include <Poco/Message.h>

namespace DB
{

namespace ExternalStream
{

PulsarLogger::PulsarLogger(const std::string & file_name_, Poco::Logger * logger_) : file_name(file_name_), logger(logger_)
{
}

bool PulsarLogger::isEnabled(Level level)
{
    switch (level)
    {
        case LEVEL_DEBUG:
            return logger->is(Poco::Message::PRIO_DEBUG);
        case LEVEL_INFO:
            return logger->is(Poco::Message::PRIO_INFORMATION);
        case LEVEL_WARN:
            return logger->is(Poco::Message::PRIO_WARNING);
        case LEVEL_ERROR:
            return logger->is(Poco::Message::PRIO_ERROR);
    }
}

void PulsarLogger::log(Level level, int line, const std::string & message)
{
    switch (level)
    {
        case LEVEL_DEBUG:
            LOG_DEBUG(logger, "{}:{} - {}", file_name, line, message);
            break;
        case LEVEL_INFO:
            LOG_INFO(logger, "{}:{} - {}", file_name, line, message);
            break;
        case LEVEL_WARN:
            LOG_WARNING(logger, "{}:{} - {}", file_name, line, message);
            break;
        case LEVEL_ERROR:
            LOG_ERROR(logger, "{}:{} - {}", file_name, line, message);
    }
}

PulsarLoggerFactory::PulsarLoggerFactory(Poco::Logger * logger_) : logger(logger_)
{
}

pulsar::Logger * PulsarLoggerFactory::getLogger(const std::string & file_name)
{
    /// The pulsar library requires the pointer must be allocated with the new keyword in C++.
    return new PulsarLogger(file_name, logger);
}

}

}


#endif
