#pragma once

#include <Common/ThreadPool.h>
#include <KafkaLog/KafkaWALCommon.h>

#include <boost/core/noncopyable.hpp>

namespace DB
{

namespace RdKafka
{

class Producer : boost::noncopyable
{
public:
    Producer(const rd_kafka_conf_t & rk_conf, UInt64 poll_timeout_ms, const String & logger_name_prefix);
    ~Producer() { shutdown(); }

    rd_kafka_t * getHandle() const { return rk.get(); }

    std::string name() const { return rd_kafka_name(rk.get()); }

    void shutdown() { stopped = true; }

    bool isStopped() const { return stopped; }

private:
    void backgroundPoll(UInt64 poll_timeout_ms) const;

    klog::KafkaPtr rk {nullptr, rd_kafka_destroy};
    ThreadPool poller;
    bool stopped;
    Poco::Logger * logger;
};

}

}
