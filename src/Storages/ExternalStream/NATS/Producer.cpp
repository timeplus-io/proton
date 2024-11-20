#include <nats/nats.h>
#include <Common/ThreadPool.h>
#include <Storages/ExternalStream/NATS/Producer.h>

namespace DB
{

namespace NATS
{

Producer::Producer(const natsOptions & nats_conf, UInt64 poll_timeout_ms, const String & logger_name_prefix)
: poll_timeout_ms(poll_timeout_ms)
{
    natsStatus s = natsConnection_Connect(&nc, &nats_conf);
    if (s != NATS_OK)
    {
        throw Exception("Failed to create NATS connection: " + String(natsStatus_GetText(s)));
    }

    logger = &Poco::Logger::get(fmt::format("{}.{}", logger_name_prefix, name()));
    LOG_INFO(logger, "Created producer");
}

Producer::~Producer()
{
    setStopped();
    poller.wait();
    natsConnection_Destroy(nc);
}

void Producer::backgroundPoll() const
{
    LOG_INFO(logger, "Start producer poll");

    while (!stopped.test())
        natsConnection_Flush(nc, poll_timeout_ms);

    LOG_INFO(logger, "Producer poll stopped");
}

void Producer::publish(const std::string & subject, const std::string & message)
{
    natsStatus s = natsConnection_PublishString(nc, subject.c_str(), message.c_str());
    if (s != NATS_OK)
    {
        throw Exception("Failed to publish message: " + String(natsStatus_GetText(s)));
    }
}

}

}
