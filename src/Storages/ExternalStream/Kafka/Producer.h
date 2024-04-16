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
    ~Producer();

    rd_kafka_t * getHandle() const { return rk.get(); }

    std::string name() const { return rd_kafka_name(rk.get()); }

    void setStopped() { stopped.test_and_set(); }

    bool isStopped() const { return stopped.test(); }

private:
    void backgroundPoll(UInt64 poll_timeout_ms) const;

    klog::KafkaPtr rk {nullptr, rd_kafka_destroy};
    ThreadPool poller;
    Poco::Logger * logger;

    std::atomic_flag stopped = ATOMIC_FLAG_INIT;
};

}

}
