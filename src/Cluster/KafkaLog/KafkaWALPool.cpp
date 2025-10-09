#include <Cluster/KafkaLog/KafkaWALPool.h>

#include <Common/logger_useful.h>


namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int TOO_MANY_SIMULTANEOUS_QUERIES;
}
}

namespace cluster::klog
{

KafkaWALPool & KafkaWALPool::instance(std::vector<klog::KafkaLogConfig> kconfigs)
{
    static KafkaWALPool pool{std::move(kconfigs)};
    return pool;
}

KafkaWALPool::KafkaWALPool(std::vector<klog::KafkaLogConfig> kconfigs) : log(getLogger("KafkaWALPool"))
{
    for (auto & kconfig : kconfigs)
        createWALs(kconfig);
}

KafkaWALPool::~KafkaWALPool()
{
    shutdown();
    LOG_INFO(log, "dtored");
}

void KafkaWALPool::startup()
{
    if (inited.test_and_set())
    {
        LOG_ERROR(log, "Already started");
        return;
    }

    LOG_INFO(log, "Starting");

    if (!wals.empty() && default_cluster.empty())
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Default Kafka WAL cluster is not assigned");

    LOG_INFO(log, "Started");
}

void KafkaWALPool::shutdown()
{
    if (stopped.test_and_set())
        return;

    LOG_INFO(log, "Stopping");

    for (auto & cluster_wals : wals)
        for (auto & wal : cluster_wals.second.second)
            wal->shutdown();

    if (meta_wal)
        meta_wal->shutdown();

    {
        std::lock_guard lock{streaming_lock};
        for (auto & cluster_consumers : streaming_consumers)
            for (auto & consumer : cluster_consumers.second.second)
                consumer->shutdown();

        streaming_consumers.clear();
    }

    {
        std::lock_guard lock{multiplexer_mutex};
        for (auto & cluster_multiplexers : multiplexers)
            for (auto & multiplexer : cluster_multiplexers.second.second)
                multiplexer->shutdown();

        multiplexers.clear();
    }

    LOG_INFO(log, "Stopped");
}

void KafkaWALPool::createWALs(const KafkaLogConfig & kconfig)
{
    if (wals.contains(kconfig.settings.cluster_id))
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Duplicated Kafka cluster id {}", kconfig.settings.cluster_id);

    /// Create WALs
    LOG_INFO(log, "Creating KafkaLog with settings: {}", kconfig.settings.string());

    for (int32_t i = 0; i < kconfig.dedicated_subscription_pool_size; ++i)
    {
        auto ksettings = kconfig.settings.clone();

        ksettings->group_id += "-dedicated";
        auto kwal = std::make_shared<KafkaWAL>(std::move(ksettings));

        kwal->startup();

        wals[kconfig.settings.cluster_id].first = 0;
        wals[kconfig.settings.cluster_id].second.push_back(kwal);
    }

    streaming_consumers.insert({kconfig.settings.cluster_id, {static_cast<size_t>(kconfig.pool_size), {}}});

    multiplexers.insert({kconfig.settings.cluster_id, {static_cast<size_t>(kconfig.shared_subscription_pool_max_size), {}}});

    cluster_kafka_settings.emplace(kconfig.settings.cluster_id, kconfig.settings.clone());

    if (kconfig.system_default)
    {
        LOG_INFO(log, "Setting {} cluster as default Kafka WAL cluster", kconfig.settings.cluster_id);
        default_cluster = kconfig.settings.cluster_id;

        /// Meta WAL with a different consumer group
        auto ksettings = kconfig.settings.clone();
        ksettings->group_id += "-meta";
        meta_wal = std::make_shared<KafkaWAL>(std::move(ksettings));
        meta_wal->startup();
    }
}

KafkaWALPtr KafkaWALPool::get(const std::string & cluster_id)
{
    if (cluster_id.empty() && !default_cluster.empty())
        return get(default_cluster);

    auto iter = wals.find(cluster_id);
    if (iter == wals.end())
    {
        LOG_ERROR(log, "Unknown streaming storage cluster_id={}", cluster_id);
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unknown kafka cluster_id={}", cluster_id);
    }

    /// Round robin
    return iter->second.second[iter->second.first.fetch_add(1) % iter->second.second.size()];
}

KafkaWALPtr KafkaWALPool::getMeta() const
{
    return meta_wal;
}

KafkaWALSimpleConsumerPtr KafkaWALPool::getOrCreateStreaming(const String & cluster_id)
{
    if (cluster_id.empty() && !default_cluster.empty())
        return getOrCreateStreaming(default_cluster);

    std::lock_guard lock{streaming_lock};

    auto iter = streaming_consumers.find(cluster_id);
    if (iter == streaming_consumers.end())
    {
        LOG_ERROR(log, "Unknown streaming storage cluster_id={}", cluster_id);
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unknown streaming storage cluster id");
    }

    for (const auto & consumer : iter->second.second)
        if (consumer.use_count() == 1)
            return consumer;

    /// consumer is used up and if we didn't reach maximum
    if (iter->second.second.size() < iter->second.first)
    {
        /// Create one
        auto siter = cluster_kafka_settings.find(cluster_id);
        assert(siter != cluster_kafka_settings.end());

        auto ksettings = siter->second->clone();

        /// Streaming WALs have a different group ID
        ksettings->group_id += "-streaming-query-" + std::to_string(iter->second.second.size() + 1);

        /// We don't care offset checkpointing for WALs used for streaming processing,
        /// No auto commit
        ksettings->enable_auto_commit = false;

        auto consumer = std::make_shared<KafkaWALSimpleConsumer>(std::move(ksettings));
        consumer->startup();
        iter->second.second.push_back(consumer);
        return consumer;
    }
    else
    {
        LOG_ERROR(log, "Streaming processing pool in cluster={} is used up, size={}", cluster_id, iter->second.first);
        throw DB::Exception(DB::ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES, "Max streaming processing pool size has been reached");
    }
}

KafkaWALConsumerMultiplexerPtr KafkaWALPool::getOrCreateConsumerMultiplexer(const std::string & cluster_id)
{
    if (cluster_id.empty() && !default_cluster.empty())
        return getOrCreateConsumerMultiplexer(default_cluster);

    std::lock_guard lock{multiplexer_mutex};

    auto iter = multiplexers.find(cluster_id);
    if (iter == multiplexers.end())
        throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unknown kafka cluster_id={}", cluster_id);

    auto & cluster_multiplexers = iter->second.second;

    auto create_multiplexer = [&, this]() -> KafkaWALConsumerMultiplexerPtr {
        auto kafka_settings = cluster_kafka_settings[cluster_id]->clone();
        kafka_settings->group_id += "-shared";

        auto multiplexer = std::make_shared<KafkaWALConsumerMultiplexer>(std::move(kafka_settings));
        multiplexer->startup();
        cluster_multiplexers.push_back(multiplexer);
        return multiplexer;
    };

    if (cluster_multiplexers.empty())
        return create_multiplexer();

    KafkaWALConsumerMultiplexerPtr * best_multiplexer = nullptr;

    /// Find best multiplexer with minimum ref count
    /// FIXME : better algo with more metrics like records consumed ?
    for (auto & multiplexer : iter->second.second)
    {
        if (!best_multiplexer)
        {
            best_multiplexer = &multiplexer;
            continue;
        }

        if (multiplexer.use_count() < best_multiplexer->use_count())
            best_multiplexer = &multiplexer;
    }

    if (best_multiplexer->use_count() >= 20)
    {
        /// If the best multiplexer's use count reaches 20 (FIXME: configurable), and if we didn't reach
        /// the maximum multiplexers in this Kafka cluster, create a new one to balance the load
        if (cluster_multiplexers.size() < iter->second.first)
            return create_multiplexer();
    }

    return *best_multiplexer;
}

std::vector<KafkaWALClusterPtr> KafkaWALPool::clusters(const KafkaWALContext & ctx) const
{
    std::vector<KafkaWALClusterPtr> results;
    results.reserve(wals.size());

    for (const auto & cluster_wal : wals)
    {
        auto result{cluster_wal.second.second.back()->cluster(ctx)};
        if (result)
            results.push_back(std::move(result));
    }

    return results;
}
}
