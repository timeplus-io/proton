#pragma once

#include <nats/nats.h>
#include <Common/logger_useful.h>
#include <Common/ThreadPool.h>

namespace DB
{

namespace NATS
{

class Consumer
{
public:
    Consumer(const natsOptions & nats_conf, const String & logger_name_prefix);
    ~Consumer();

    natsConnection * getHandle() const { return conn; }

    void startConsume(const std::string & subject, const std::string & queue_group);
    void stopConsume();

    void setStopped() {
        stopped.test_and_set();
        LOG_INFO(logger, "Stopped");
    }

    bool isStopped() const { return stopped.test(); }

    std::string name() const { return natsConnection_GetConnectedUrl(conn); }

private:
    static void onMessage(natsConnection *nc, natsSubscription *sub, natsMsg *msg, void *closure);
    void backgroundPoll() const;

    natsConnection * conn = nullptr;
    natsSubscription * sub = nullptr;
    ThreadPool poller;
    Poco::Logger * logger;

    std::atomic_flag started;
    std::atomic_flag stopped;
};

}

}
