#pragma once

#include "KafkaWALConsumer.h"
#include "KafkaWALSettings.h"
#include "Results.h"

#include <Common/ThreadPool.h>
#include <common/ClockUtils.h>

#include <boost/noncopyable.hpp>

namespace DWAL
{

/// KafkaWALConsumerMultiplexer has a dedicated thread consuming a list of topic partitions
/// in a dedicated consumer group by using high level KafkaWALConsumer. It then routes
/// the records to different targets by calling the corresponding callbacks.
/// It is multithread safe.
/// KafkaWALConsumerMultiplexer is usually pooled and long lasting as the whole program
class KafkaWALConsumerMultiplexer final : private boost::noncopyable
{
public:
    struct CallbackContext
    {
    private:
        ConsumeCallback callback;
        void * data;

        /// Only support same callback per topic
        std::vector<int32_t> partitions;

        int64_t last_call_ts = DB::MonotonicMilliseconds::now();

    public:
        CallbackContext(ConsumeCallback callback_, void * data_, int32_t partition_)
            : callback(callback_), data(data_), partitions({partition_})
        {
        }

        friend class KafkaWALConsumerMultiplexer;
    };

    using CallbackContextPtr = std::shared_ptr<CallbackContext>;

    struct Result
    {
        int32_t err;
        std::weak_ptr<CallbackContext> ctx;

        Result(int32_t err_, std::weak_ptr<CallbackContext> ctx_) : err(err_), ctx(ctx_) { }
    };

public:
    /// Please pay attention to the `settings.group_id`. If end user likes to consume
    /// a list of partitions with multiple consumer multiplexers, they will need use
    /// the same `settings.group_id`
    explicit KafkaWALConsumerMultiplexer(std::unique_ptr<KafkaWALSettings> settings);
    ~KafkaWALConsumerMultiplexer();

    void startup();
    void shutdown();

    /// Register a callback for a partition of a topic at specific offset.
    /// Once registered, the callback will be invoked asynchronously in a background thread when there
    /// is new data available, so make sure the callback handles thread safety correctly.
    /// Please note if this function is called against same topic but with different partitions serveral
    /// times, the late call will override the `callback `/ `data` of previous call.
    /// The returned Result contains a weak_ptr of CallbackContext which is used to check
    /// if the registered `callback` will be used any longer. The caller usually can do
    /// the following steps to make sure a graceful shutdown:
    /// 1. Caller calls addSubscription. If success, keep the weak_ptr around
    /// 2. During shutdown, caller calls removeSubscription. The CallbackContextPtr is
    /// removed from the internal data structure of mulitplexer and its reference count
    /// will be decremented by 1. In normal case, the reference count will drop to 0 and the
    /// CallbackContext get dtored in removeSubscription. However there is another case
    /// in which the CallbackContextPtr is copied out of the internal data structure
    /// when it needs get called (please note that, when callback gets called, we are not
    /// holding the internal lock, which is designed in this way on purpose to avoid
    /// arbitrary long time lock over a callback). In the late case, multiplexer is still
    /// referencing the callback / data, so it is probably not safe for the caller to
    /// dtor itself, the callback and the data. When the callback is finished in multipexer
    /// the reference count to the CallbackContext will further get decremented and will drop
    /// to zero. So caller needs poll the weak_ptr to make sure nobody
    /// (in this case the multipler) is referencing the `callback`, and then do a shutdown and
    /// dtor.
    /// Essentially the lifetimes of `callback and data` parameters are maintained
    /// by the caller but referenced by multiplexer (in a different background thread);
    /// The lifetime of CallbackContext in the returned Result is maintained by
    /// multiplexer but polled by caller to decided that the `callback and data` is not
    /// referenced by multiplexer any more
    Result addSubscription(const TopicPartitionOffset & tpo, ConsumeCallback callback, void * data);

    /// Return true if the subscription is good, otherwise false
    /// Remove the registered callback for a partition of a topic.
    /// `offset` in `tpo` is not inspected
    int32_t removeSubscription(const TopicPartitionOffset & tpo);

    int32_t commit(const TopicPartitionOffset & tpo);

private:
    void backgroundPoll();
    void handleResult(ConsumeResult result) const;

    /// `flush` calls callbacks perioridically with empty records
    /// callbacks can use this as a signal to flush outstanding offset
    /// to checkpoint
    void flush() const;

private:
    std::atomic_flag inited = ATOMIC_FLAG_INIT;
    std::atomic_flag stopped = ATOMIC_FLAG_INIT;

    int32_t shared_subscription_flush_threshold_count;
    int32_t shared_subscription_flush_threshold_bytes;
    int32_t shared_subscription_flush_threshold_ms;

    std::unique_ptr<KafkaWALConsumer> consumer;

    ThreadPool poller;

    mutable std::mutex callbacks_mutex;

    /// `callbacks` are indexed by `topic` name. For now, we assume in one single node
    /// there is only one unique table / topic
    std::unordered_map<std::string, CallbackContextPtr> callbacks;

    Poco::Logger * log;
};

using KafkaWALConsumerMultiplexerPtr = std::shared_ptr<KafkaWALConsumerMultiplexer>;
using KafkaWALConsumerMultiplexerPtrs = std::vector<KafkaWALConsumerMultiplexerPtr>;
}
