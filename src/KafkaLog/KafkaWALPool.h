#pragma once

#include "KafkaWAL.h"
#include "KafkaWALCluster.h"
#include "KafkaWALConsumerMultiplexer.h"
#include "KafkaWALSettings.h"
#include "KafkaWALSimpleConsumer.h"

#include <Interpreters/Context_fwd.h>

#include <mutex>

namespace klog
{
/// Pooling WAL. Singleton
/// The pool will be initied during system startup and will be read only after that.
/// So it doesn't hold any mutex in the multi-thread access env.
class KafkaWALPool : private boost::noncopyable
{
public:
    static KafkaWALPool & instance(const DB::ContextPtr & global_context);

    explicit KafkaWALPool(DB::ContextPtr global_context);
    ~KafkaWALPool();

    KafkaWALPtr get(const std::string & cluster_id);

    KafkaWALPtr getMeta() const;

    KafkaWALConsumerMultiplexerPtr getOrCreateConsumerMultiplexer(const std::string & cluster_id);

    KafkaWALSimpleConsumerPtr getOrCreateStreaming(const String & cluster_id);

    KafkaWALSimpleConsumerPtr getOrCreateStreamingExternal(const String & brokers, const KafkaWALAuth & auth);

    std::vector<KafkaWALClusterPtr> clusters(const KafkaWALContext & ctx) const;

    bool enabled() const { return meta_wal != nullptr; }

    void startup();
    void shutdown();

private:
    void init();
    void init(const std::string & key);

private:
    DB::ContextPtr global_context;

    std::atomic_flag inited = ATOMIC_FLAG_INIT;
    std::atomic_flag stopped = ATOMIC_FLAG_INIT;

    std::string default_cluster;
    KafkaWALPtr meta_wal;

    std::unordered_map<String, std::pair<std::atomic_uint64_t, KafkaWALPtrs>> wals;

    std::unordered_map<String, std::shared_ptr<KafkaWALSettings>> cluster_kafka_settings;

    std::mutex multiplexer_mutex;
    std::unordered_map<String, std::pair<size_t, KafkaWALConsumerMultiplexerPtrs>> multiplexers;

    std::mutex streaming_lock;
    std::unordered_map<String, std::pair<size_t, KafkaWALSimpleConsumerPtrs>> streaming_consumers;

    std::mutex external_streaming_lock;
    std::unordered_map<String, std::pair<size_t, KafkaWALSimpleConsumerPtrs>> external_streaming_consumers;

    Poco::Logger * log;
};
}
