#pragma once

#include "KafkaWAL.h"
#include "KafkaWALCluster.h"
#include "KafkaWALConsumerMultiplexer.h"
#include "KafkaWALSettings.h"

#include <Interpreters/Context_fwd.h>

#include <mutex>

namespace DWAL
{
/// Pooling WAL. Singleton
/// The pool will be initied during system startup and will be read only after that.
/// So it doesn't hold any mutext in the multithread access env.
class KafkaWALPool : private boost::noncopyable
{
public:
    static KafkaWALPool & instance(DB::ContextPtr global_context);

    explicit KafkaWALPool(DB::ContextPtr global_context);
    ~KafkaWALPool();

    KafkaWALPtr get(const std::string & cluster_id) const;

    KafkaWALPtr getMeta() const;

    KafkaWALConsumerMultiplexerPtr getOrCreateConsumerMultiplexer(const std::string & cluster_id);

    std::vector<KafkaWALClusterPtr> clusters(const KafkaWALContext & ctx) const;

    void startup();
    void shutdown();

private:
    void init(const std::string & key);

private:
    DB::ContextPtr global_context;

    std::atomic_flag inited = ATOMIC_FLAG_INIT;
    std::atomic_flag stopped = ATOMIC_FLAG_INIT;

    std::string default_cluster;
    KafkaWALPtr meta_wal;

    std::unordered_map<String, KafkaWALPtrs> wals;
    mutable std::unordered_map<String, std::atomic_uint64_t> indexes;

    std::unordered_map<String, std::shared_ptr<KafkaWALSettings>> cluster_kafka_settings;

    std::mutex multiplexer_mutex;
    std::unordered_map<String, std::pair<size_t, KafkaWALConsumerMultiplexerPtrs>> multiplexers;

    Poco::Logger * log;
};
}
