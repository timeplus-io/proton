#pragma once

#include <Cluster/KafkaLog/KafkaWAL.h>
#include <Cluster/KafkaLog/KafkaWALCluster.h>
#include <Cluster/KafkaLog/KafkaWALConsumerMultiplexer.h>
#include <Cluster/KafkaLog/KafkaWALSettings.h>
#include <Cluster/KafkaLog/KafkaWALSimpleConsumer.h>

#include <mutex>

namespace cluster::klog
{
struct KafkaLogConfig
{
    KafkaWALSettings settings;
    bool enabled = false;
    bool system_default = false;
    int32_t dedicated_subscription_pool_size = 2;
    int32_t shared_subscription_pool_max_size = 10;
    int32_t pool_size = 100;
};

/// Pooling WAL. Singleton
/// The pool will be inited during system startup and will be read only after that.
/// So it doesn't hold any mutex in the multi-thread access env.
class KafkaWALPool final : private boost::noncopyable
{
public:
    static KafkaWALPool & instance(std::vector<KafkaLogConfig> kconfigs);

    explicit KafkaWALPool(std::vector<KafkaLogConfig> kconfigs);
    ~KafkaWALPool();

    KafkaWALPtr get(const std::string & cluster_id);

    KafkaWALPtr getMeta() const;

    KafkaWALConsumerMultiplexerPtr getOrCreateConsumerMultiplexer(const std::string & cluster_id);

    KafkaWALSimpleConsumerPtr getOrCreateStreaming(const String & cluster_id);

    std::vector<KafkaWALClusterPtr> clusters(const KafkaWALContext & ctx) const;

    bool valid() const { return meta_wal != nullptr; }

    void startup();
    void shutdown();

private:
    void createWALs(const KafkaLogConfig & kconfig);

private:
    std::atomic_flag inited;
    std::atomic_flag stopped;

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

    LoggerPtr log;
};
}
