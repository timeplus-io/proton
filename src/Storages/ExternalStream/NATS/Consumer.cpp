#include <Poco/Logger.h>
#include <Storages/ExternalStream/NATS/Consumer.h>

namespace DB
{

namespace NATS
{

Consumer::Consumer(const natsOptions & nats_conf, const String & logger_name_prefix)
{
    natsStatus s = natsConnection_Connect(&conn, &nats_conf);
    if (s != NATS_OK)
    {
        throw Exception("Failed to create NATS connection: " + std::string(natsStatus_GetText(s)));
    }

    logger = &Poco::Logger::get(fmt::format("{}.{}", logger_name_prefix, name()));
    LOG_INFO(logger, "Created consumer");
}

Consumer::~Consumer()
{
    setStopped();
    natsConnection_Destroy(conn);
}

void Consumer::backgroundPoll() const
{
    LOG_INFO(logger, "Start consumer poll");

    while (!stopped.test())
    {
        // Polling logic for NATS
    }

    LOG_INFO(logger, "Consumer poll stopped");
}

void Consumer::startConsume(const std::string & subject, const std::string & queue_group)
{
    if (!started.test_and_set())
        poller.scheduleOrThrowOnError([this] { backgroundPoll(); });

    natsStatus s = natsConnection_QueueSubscribe(&sub, conn, subject.c_str(), queue_group.c_str(), onMessage, this);
    if (s != NATS_OK)
    {
        throw Exception("Failed to start consuming subject=" + subject + " queue_group=" + queue_group + " error=" + std::string(natsStatus_GetText(s)));
    }
}

void Consumer::stopConsume()
{
    natsSubscription_Unsubscribe(sub);
    natsSubscription_Destroy(sub);
}

void Consumer::onMessage(natsConnection *nc, natsSubscription *sub, natsMsg *msg, void *closure)
{
    Consumer *consumer = static_cast<Consumer*>(closure);
    // Message handling logic
    natsMsg_Destroy(msg);
}

}

}
