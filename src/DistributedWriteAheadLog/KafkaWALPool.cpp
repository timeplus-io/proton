#include "KafkaWALPool.h"

#include <Interpreters/Context.h>
#include <common/logger_useful.h>

#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}
}

namespace DWAL
{
namespace
{
    /// Globals
    const std::string SYSTEM_WALS_KEY = "cluster_settings.streaming_storage";
    const std::string SYSTEM_WALS_KEY_PREFIX = "cluster_settings.streaming_storage.";
}

KafkaWALPool & KafkaWALPool::instance(DB::ContextPtr global_context)
{
    static KafkaWALPool pool{global_context};
    return pool;
}

KafkaWALPool::KafkaWALPool(DB::ContextPtr global_context_)
    : global_context(global_context_), log(&Poco::Logger::get("KafkaWALPool"))
{
}

KafkaWALPool::~KafkaWALPool()
{
    shutdown();
}

void KafkaWALPool::startup()
{
    if (!global_context->isDistributed())
    {
        return;
    }

    if (inited.test_and_set())
    {
        LOG_ERROR(log, "Already started");
        return;
    }

    LOG_INFO(log, "Starting");

    const auto & config = global_context->getConfigRef();

    Poco::Util::AbstractConfiguration::Keys sys_WAL_keys;
    config.keys(SYSTEM_WALS_KEY, sys_WAL_keys);

    for (const auto & key : sys_WAL_keys)
    {
        init(key);
    }

    if (!wals.empty() && default_cluster.empty())
    {
        throw DB::Exception("Default Kafka WAL cluster is not assigned", DB::ErrorCodes::BAD_ARGUMENTS);
    }

    LOG_INFO(log, "Started");
}

void KafkaWALPool::shutdown()
{
    if (stopped.test_and_set())
    {
        return;
    }

    LOG_INFO(log, "Stopping");

    for (auto & cluster_wals : wals)
    {
        for (auto & wal : cluster_wals.second)
        {
            wal->shutdown();
        }
    }

    if (meta_wal)
    {
        meta_wal->shutdown();
    }

    LOG_INFO(log, "Stopped");
}

void KafkaWALPool::init(const std::string & key)
{
    /// FIXME; for now, we only support kafka, so assume it is kafka
    /// assert(key.startswith("kafka"));

    const auto & config = global_context->getConfigRef();

    KafkaWALSettings kafka_settings;
    int32_t dedicated_subscription_wal_pool_size = 2;
    int32_t shared_subscription_wal_pool_max_size = 10;
    bool system_default = false;

    std::vector<std::tuple<String, String, void *>> settings = {
        {".default", "Bool", &system_default},
        {".cluster_id", "String", &kafka_settings.cluster_id},
        {".security_protocol", "String", &kafka_settings.security_protocol},
        {".brokers", "String", &kafka_settings.brokers},
        {".topic_metadata_refresh_interval_ms", "Int32", &kafka_settings.topic_metadata_refresh_interval_ms},
        {".message_max_bytes", "Int32", &kafka_settings.message_max_bytes},
        {".statistic_internal_ms", "Int32", &kafka_settings.statistic_internal_ms},
        {".debug", "String", &kafka_settings.debug},
        {".enable_idempotence", "Bool", &kafka_settings.enable_idempotence},
        {".queue_buffering_max_messages", "Int32", &kafka_settings.queue_buffering_max_messages},
        {".queue_buffering_max_kbytes", "Int32", &kafka_settings.queue_buffering_max_kbytes},
        {".queue_buffering_max_ms", "Int32", &kafka_settings.queue_buffering_max_ms},
        {".message_send_max_retries", "Int32", &kafka_settings.message_send_max_retries},
        {".retry_backoff_ms", "Int32", &kafka_settings.retry_backoff_ms},
        {".compression_codec", "String", &kafka_settings.compression_codec},
        {".message_timeout_ms", "Int32", &kafka_settings.message_timeout_ms},
        {".message_delivery_async_poll_ms", "Int32", &kafka_settings.message_delivery_async_poll_ms},
        {".message_delivery_sync_poll_ms", "Int32", &kafka_settings.message_delivery_sync_poll_ms},
        {".group_id", "String", &kafka_settings.group_id},
        {".enable_auto_commit", "Bool", &kafka_settings.enable_auto_commit},
        {".check_crcs", "Bool", &kafka_settings.check_crcs},
        {".auto_commit_interval_ms", "Int32", &kafka_settings.auto_commit_interval_ms},
        {".fetch_message_max_bytes", "Int32", &kafka_settings.fetch_message_max_bytes},
        {".fetch_wait_max_ms", "Int32", &kafka_settings.fetch_wait_max_ms},
        {".queued_min_messages", "Int32", &kafka_settings.queued_min_messages},
        {".queued_max_messages_kbytes", "Int32", &kafka_settings.queued_max_messages_kbytes},
        {".session_timeout_ms", "Int32", &kafka_settings.session_timeout_ms},
        {".max_poll_interval_ms", "Int32", &kafka_settings.max_poll_interval_ms},
        {".dedicated_subscription_pool_size", "Int32", &dedicated_subscription_wal_pool_size},
        {".shared_subscription_pool_max_size", "Int32", &shared_subscription_wal_pool_max_size},
        {".shared_subscription_flush_threshold_count", "Int32", &kafka_settings.shared_subscription_flush_threshold_count},
        {".shared_subscription_flush_threshold_bytes", "Int32", &kafka_settings.shared_subscription_flush_threshold_bytes},
        {".shared_subscription_flush_threshold_ms", "Int32", &kafka_settings.shared_subscription_flush_threshold_ms},
    };

    for (const auto & t : settings)
    {
        auto k = SYSTEM_WALS_KEY_PREFIX + key + std::get<0>(t);
        if (config.has(k))
        {
            const auto & type = std::get<1>(t);
            if (type == "String")
            {
                *static_cast<std::string *>(std::get<2>(t)) = config.getString(k);
            }
            else if (type == "Int32")
            {
                auto i = config.getInt(k);
                if (i <= 0)
                {
                    throw DB::Exception("Invalid setting " + std::get<0>(t), DB::ErrorCodes::BAD_ARGUMENTS);
                }
                *static_cast<int32_t *>(std::get<2>(t)) = i;
            }
            else if (type == "Bool")
            {
                *static_cast<bool *>(std::get<2>(t)) = config.getBool(k);
            }
        }
    }

    if (kafka_settings.brokers.empty())
    {
        LOG_ERROR(log, "Invalid system kafka settings, empty brokers, will skip settings in this segment");
        return;
    }

    if (kafka_settings.group_id.empty())
    {
        /// FIXME
        kafka_settings.group_id = global_context->getNodeIdentity();
    }

    if (wals.contains(kafka_settings.cluster_id))
    {
        throw DB::Exception("Duplicated Kafka cluster id " + kafka_settings.cluster_id, DB::ErrorCodes::BAD_ARGUMENTS);
    }

    /// Create WALs
    LOG_INFO(log, "Creating Kafka WAL with settings: {}", kafka_settings.string());

    for (int32_t i = 0; i < dedicated_subscription_wal_pool_size; ++i)
    {
        auto ksettings = kafka_settings.clone();

        ksettings->group_id += "-dedicated";
        auto kwal = std::make_shared<KafkaWAL>(std::move(ksettings));

        kwal->startup();
        wals[kafka_settings.cluster_id].push_back(kwal);
    }
    indexes[kafka_settings.cluster_id] = 0;

    multiplexers.emplace(
        kafka_settings.cluster_id, std::make_pair<size_t, KafkaWALConsumerMultiplexerPtrs>(shared_subscription_wal_pool_max_size, {}));
    cluster_kafka_settings.emplace(kafka_settings.cluster_id, kafka_settings.clone());

    if (system_default)
    {
        LOG_INFO(log, "Setting {} cluster as default Kafka WAL cluster", kafka_settings.cluster_id);
        default_cluster = kafka_settings.cluster_id;

        /// Meta WAL with a different consumer group
        auto ksettings = kafka_settings.clone();
        ksettings->group_id += "-meta";
        meta_wal = std::make_shared<KafkaWAL>(std::move(ksettings));
        meta_wal->startup();
    }
}

KafkaWALPtr KafkaWALPool::get(const std::string & cluster_id) const
{
    if (cluster_id.empty() && !default_cluster.empty())
    {
        return get(default_cluster);
    }

    auto iter = wals.find(cluster_id);
    if (iter == wals.end())
    {
        throw DB::Exception("Unknown kafka cluster_id=" + cluster_id, DB::ErrorCodes::BAD_ARGUMENTS);
    }

    return iter->second[indexes[cluster_id]++ % iter->second.size()];
}

KafkaWALPtr KafkaWALPool::getMeta() const { return meta_wal; }

KafkaWALConsumerMultiplexerPtr KafkaWALPool::getOrCreateConsumerMultiplexer(const std::string & cluster_id)
{
    if (cluster_id.empty() && !default_cluster.empty())
    {
        return getOrCreateConsumerMultiplexer(default_cluster);
    }

    std::lock_guard lock{multiplexer_mutex};

    auto iter = multiplexers.find(cluster_id);
    if (iter == multiplexers.end())
    {
        throw DB::Exception("Unknown kafka cluster_id=" + cluster_id, DB::ErrorCodes::BAD_ARGUMENTS);
    }

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
    {
        return create_multiplexer();
    }

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
        {
            best_multiplexer = &multiplexer;
        }
    }

    if (best_multiplexer->use_count() >= 20)
    {
        /// If the best multiplexer's use count reaches 20 (FIXME: configurable), and if we didn't reach
        /// the maximum multiplexers in this Kafka cluster, create a new one to balance the load
        if (cluster_multiplexers.size() < iter->second.first)
        {
            return create_multiplexer();
        }
    }

    return *best_multiplexer;
}

std::vector<KafkaWALClusterPtr> KafkaWALPool::clusters(const KafkaWALContext & ctx) const
{
    std::vector<KafkaWALClusterPtr> results;
    results.reserve(wals.size());

    for (const auto & cluster_wal : wals)
    {
        auto result{cluster_wal.second.back()->cluster(ctx)};
        if (result)
        {
            results.push_back(std::move(result));
        }
    }

    return results;
}
}
