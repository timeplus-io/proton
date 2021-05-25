#include "DistributedWriteAheadLogPool.h"

#include <DistributedWriteAheadLog/DistributedWriteAheadLogKafka.h>
#include <Interpreters/Context.h>
#include <common/logger_useful.h>

#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{
    /// Globals
    const String SYSTEM_DWALS_KEY = "cluster_settings.streaming_storage";
    const String SYSTEM_DWALS_KEY_PREFIX = "cluster_settings.streaming_storage.";
}

DistributedWriteAheadLogPool & DistributedWriteAheadLogPool::instance(ContextPtr global_context)
{
    static DistributedWriteAheadLogPool pool{global_context};
    return pool;
}

DistributedWriteAheadLogPool::DistributedWriteAheadLogPool(ContextPtr global_context_)
    : global_context(global_context_), log(&Poco::Logger::get("DistributedWriteAheadLogPool"))
{
}

DistributedWriteAheadLogPool::~DistributedWriteAheadLogPool()
{
    shutdown();
}

void DistributedWriteAheadLogPool::startup()
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

    Poco::Util::AbstractConfiguration::Keys sys_dwal_keys;
    config.keys(SYSTEM_DWALS_KEY, sys_dwal_keys);

    for (const auto & key : sys_dwal_keys)
    {
        init(key);
    }

    if (!wals.empty() && default_cluster.empty())
    {
        throw Exception("Default Kafka DWAL cluster is not assigned", ErrorCodes::BAD_ARGUMENTS);
    }

    LOG_INFO(log, "Started");
}

void DistributedWriteAheadLogPool::shutdown()
{
    if (!global_context->isDistributed())
    {
        return;
    }

    if (stopped.test_and_set())
    {
        return;
    }

    LOG_INFO(log, "Stopping");

    for (auto & dwals : wals)
    {
        for (auto & dwal : dwals.second)
        {
            dwal->shutdown();
        }
    }

    if (meta_wal)
    {
        meta_wal->shutdown();
    }

    LOG_INFO(log, "Stopped");
}

void DistributedWriteAheadLogPool::init(const String & key)
{
    /// FIXME; for now, we only support kafka, so assume it is kafka
    /// assert(key.startswith("kafka"));

    const auto & config = global_context->getConfigRef();

    DistributedWriteAheadLogKafkaSettings kafka_settings;
    Int32 dwal_pool_size = 0;
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
        {".message_max_bytes", "Int32", &kafka_settings.message_max_bytes},
        {".enable_auto_commit", "Bool", &kafka_settings.enable_auto_commit},
        {".check_crcs", "Bool", &kafka_settings.check_crcs},
        {".auto_commit_interval_ms", "Int32", &kafka_settings.auto_commit_interval_ms},
        {".fetch_message_max_bytes", "Int32", &kafka_settings.fetch_message_max_bytes},
        {".queued_min_messages", "Int32", &kafka_settings.queued_min_messages},
        {".queued_max_messages_kbytes", "Int32", &kafka_settings.queued_max_messages_kbytes},
        {".internal_pool_size", "Int32", &dwal_pool_size},
    };

    for (const auto & t : settings)
    {
        auto k = SYSTEM_DWALS_KEY_PREFIX + key + std::get<0>(t);
        if (config.has(k))
        {
            const auto & type = std::get<1>(t);
            if (type == "String")
            {
                *static_cast<String *>(std::get<2>(t)) = config.getString(k);
            }
            else if (type == "Int32")
            {
                auto i = config.getInt(k);
                if (i <= 0)
                {
                    throw Exception("Invalid setting " + std::get<0>(t), ErrorCodes::BAD_ARGUMENTS);
                }
                *static_cast<Int32 *>(std::get<2>(t)) = i;
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
        throw Exception("Duplicated Kafka cluster id " + kafka_settings.cluster_id, ErrorCodes::BAD_ARGUMENTS);
    }

    /// Create DWALs
    LOG_INFO(log, "Creating Kafka DWAL with settings: {}", kafka_settings.string());

    for (Int32 i = 0; i < dwal_pool_size; ++i)
    {
        auto kwal
            = std::make_shared<DistributedWriteAheadLogKafka>(std::make_unique<DistributedWriteAheadLogKafkaSettings>(kafka_settings));

        kwal->startup();
        wals[kafka_settings.cluster_id].push_back(kwal);

    }
    indexes[kafka_settings.cluster_id] = 0;

    if (system_default)
    {
        LOG_INFO(log, "Setting {} cluster as default Kafka DWAL cluster", kafka_settings.cluster_id);
        default_cluster = kafka_settings.cluster_id;

        /// Meta DWal with a different consumer group
        kafka_settings.group_id += "-meta";
        meta_wal = std::make_shared<DistributedWriteAheadLogKafka>(std::make_unique<DistributedWriteAheadLogKafkaSettings>(kafka_settings));
        meta_wal->startup();
    }
}

DistributedWriteAheadLogPtr DistributedWriteAheadLogPool::get(const String & id) const
{
    if (id.empty() && !default_cluster.empty())
    {
        return get(default_cluster);
    }

    auto iter = wals.find(id);
    if (iter == wals.end())
    {
        return nullptr;
    }

    return iter->second[indexes[id]++ % iter->second.size()];
}

DistributedWriteAheadLogPtr DistributedWriteAheadLogPool::getMeta() const
{
    return meta_wal;
}
}
