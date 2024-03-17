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
    Producer(rd_kafka_conf_t * rk_conf, Poco::Logger * logger_);
    ~Producer()
    {
        stopped.test_and_set();
    }

    rd_kafka_t * getHandle() const { return rk.get(); }

private:
    std::string name() const { return rd_kafka_name(rk.get()); }
    void backgroundPoll() const;

    klog::KafkaPtr rk {nullptr, rd_kafka_destroy};
    ThreadPool poller;
    std::atomic_flag stopped;
    Poco::Logger * logger;
};

}

}
