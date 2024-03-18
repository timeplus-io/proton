#pragma once

#include <boost/core/noncopyable.hpp>
#include <librdkafka/rdkafka.h>

namespace DB
{

namespace RdKafka
{

using Properties = std::vector<std::pair<std::string, std::string>>;

class Topic : boost::noncopyable
{
public:
    Topic(rd_kafka_t & rk, const std::string & name, void * opaque = nullptr);
    ~Topic() = default;

    rd_kafka_topic_t * getHandle() const { return rkt.get(); }
    std::string name() const { return rd_kafka_topic_name(rkt.get()); }
    int describe() const;

private:
    rd_kafka_t & rdk;
    std::unique_ptr<rd_kafka_topic_t, decltype(rd_kafka_topic_destroy) *> rkt {nullptr, rd_kafka_topic_destroy};
};

using TopicPtr = std::shared_ptr<Topic>;

}

}
