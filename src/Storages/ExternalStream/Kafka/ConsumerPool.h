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

    virtual void shutdown() {}
};

using ConsumerPoolPtr = std::unique_ptr<IConsumerPool>;

/** A common consumer pool.
  */
class ConsumerPool : public IConsumerPool, private PoolBase<Consumer>
{
public:
    using Entry = IConsumerPool::Entry;
    using Base = PoolBase<Consumer>;

    ConsumerPool(unsigned size, const StorageID & storage_id, rd_kafka_conf_t & conf, UInt64 poll_timeout_ms_, const String & consumer_logger_name_prefix_)
       : Base(size,
        &Poco::Logger::get("RdKafkaConsumerPool (" + storage_id.getFullNameNotQuoted() + ")"))
        , rd_conf(conf)
        , poll_timeout_ms(poll_timeout_ms_)
        , consumer_logger_name_prefix(consumer_logger_name_prefix_)
    {
    }

    void shutdown() override
    {
        if (stopped.test_and_set())
            return;

        for (const auto & pooled_consumer : items)
            pooled_consumer->object->shutdown();

        LOG_INFO(log, "Shutting down consumer pool, waiting for all consumers to be freed");
        waitForNoMoreInUse();
        LOG_INFO(log, "All consumers are freed");
    }

    Entry get(Int64 max_wait_ms) override
    {
        if (stopped.test())
            return Entry();

        if (max_wait_ms < 0)
            return Base::get(-1);
        else
            return Base::get(Poco::Timespan(max_wait_ms).totalMilliseconds());
    }

protected:
    /** Creates a new object to put in the pool. */
    std::shared_ptr<Consumer> allocObject() override
    {
        return std::make_shared<Consumer>(rd_conf, poll_timeout_ms, consumer_logger_name_prefix);
    }

private:
    rd_kafka_conf_t & rd_conf;
    UInt64 poll_timeout_ms {0};
    String consumer_logger_name_prefix;

    std::atomic_flag stopped {false};
};

}

}
