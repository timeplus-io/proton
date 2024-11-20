#pragma once

#include <nats/nats.h>
#include <Common/ThreadPool.h>

namespace DB
{

namespace NATS
{

class Producer
{
public:
    Producer(const natsOptions & nats_conf, UInt64 poll_timeout_ms, const String & logger_name_prefix);
    ~Producer();

    void publish(const std::string & subject, const std::string & message);

    std::string name() const { return natsConnection_Name(nc); }

    void setStopped() { stopped.test_and_set(); }

    bool isStopped() const { return stopped.test(); }

private:
    void backgroundPoll() const;

    natsConnection * nc {nullptr};
    UInt64 poll_timeout_ms {0};
    ThreadPool poller;
    Poco::Logger * logger;

    std::atomic_flag stopped;
};

}

}
