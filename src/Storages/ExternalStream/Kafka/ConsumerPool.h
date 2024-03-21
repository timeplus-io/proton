#pragma once

#include <Common/PoolBase.h>
#include <Interpreters/StorageID.h>
#include <Storages/ExternalStream/Kafka/Consumer.h>

namespace DB
{

namespace RdKafka
{

/** Interface for consumer pools.
  *
  * Usage
  * ConsumerPool pool(...);
  *
  *    void thread()
  *    {
  *        auto consumer = pool.get();
  *        consumer->consumeStart(...);
  *    }
  */

class IConsumerPool : private boost::noncopyable
{
public:
    using Entry = PoolBase<Consumer>::Entry;

    virtual ~IConsumerPool() = default;

    /// Selects the consumer to work. If `max_wait_ms` equals -1, it's a blocking call.
    virtual Entry get(Int64 max_wait_ms) = 0;
};

using ConsumerPoolPtr = std::unique_ptr<IConsumerPool>;

/** A common consumer pool.
  */
class ConsumerPool : public IConsumerPool, private PoolBase<Consumer>
{
public:
    using Entry = IConsumerPool::Entry;
    using Base = PoolBase<Consumer>;

    ConsumerPool(unsigned size, const StorageID & storage_id, rd_kafka_conf_t & conf, UInt64 poll_timeout_ms_, Poco::Logger * consumer_logger_)
       : Base(size,
        &Poco::Logger::get("RdKafkaConsumerPool (" + storage_id.getFullNameNotQuoted() + ")"))
        , rd_conf(conf)
        , poll_timeout_ms(poll_timeout_ms_)
        , consumer_logger(consumer_logger_)
    {
    }

    Entry get(Int64 max_wait_ms) override
    {
        Entry entry;

        if (max_wait_ms < 0)
            entry = Base::get(-1);
        else
            entry = Base::get(Poco::Timespan(max_wait_ms).totalMilliseconds());

        return entry;
    }

protected:
    /** Creates a new object to put in the pool. */
    std::shared_ptr<Consumer> allocObject() override
    {
        return std::make_shared<Consumer>(rd_conf, poll_timeout_ms, consumer_logger);
    }

private:
    rd_kafka_conf_t & rd_conf;
    UInt64 poll_timeout_ms {0};
    Poco::Logger * consumer_logger;
};

}

}
