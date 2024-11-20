#pragma once

#include <nats/nats.h>
#include <boost/core/noncopyable.hpp>

namespace DB
{

namespace NATS
{

struct WatermarkOffsets
{
    int64_t low;
    int64_t high;
};

class Topic : boost::noncopyable
{
public:
    Topic(natsConnection * conn, const std::string & subject);
    ~Topic() = default;

    natsSubscription * getHandle() const { return sub; }
    std::string name() const { return subject; }
    int getPartitionCount() const;
    WatermarkOffsets queryWatermarks() const;

private:
    natsConnection * conn;
    natsSubscription * sub;
    std::string subject;
};

using TopicPtr = std::shared_ptr<Topic>;

}

}
