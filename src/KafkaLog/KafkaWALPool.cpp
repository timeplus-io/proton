#include "KafkaWALPool.h"
#include <vector>

#include <Interpreters/Context.h>
#include <Common/logger_useful.h>

#include <boost/algorithm/string/predicate.hpp>
#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int TOO_MANY_SIMULTANEOUS_QUERIES;
}
}

namespace klog
{
namespace
{
/// Globals
const std::string SYSTEM_WALS_KEY = "cluster_settings.logstore";
const std::string SYSTEM_WALS_KEY_PREFIX = SYSTEM_WALS_KEY + ".";


int32_t bucketFetchWaitMax(int32_t fetch_wait_max_ms)
{
    if (fetch_wait_max_ms <= 10)
        return 10;
    else if (fetch_wait_max_ms <= 20)
        return 20;
    else if (fetch_wait_max_ms <= 30)
        return 30;
    else if (fetch_wait_max_ms <= 40)
        return 40;
    else if (fetch_wait_max_ms <= 50)
        return 50;
    else if (fetch_wait_max_ms <= 100)
        return 100;
    else if (fetch_wait_max_ms <= 200)
        return 200;
    else if (fetch_wait_max_ms <= 300)
        return 300;
    else if (fetch_wait_max_ms <= 400)
        return 400;
    else if (fetch_wait_max_ms <= 500)
        return 500;

    return 500;
}
}

KafkaWALPool & KafkaWALPool::instance(const DB::ContextPtr & global_context)
{
    static KafkaWALPool pool{global_context};
    return pool;
}

KafkaWALPool::KafkaWALPool(DB::ContextPtr global_context) : log(&Poco::Logger::get("KafkaWALPool"))
{
    init(global_context);
}

KafkaWALPool::~KafkaWALPool()
{
    shutdown();
    LOG_INFO(log, "dtored");
}

void KafkaWALPool::startup()
{
    if (!enabled())
        return;

    if (inited.test_and_set())
    {
        LOG_ERROR(log, "Already started");
        return;
    }

    LOG_INFO(log, "Starting");

    if (!wals.empty() && default_cluster.empty())
        throw DB::Exception("Default Kafka WAL cluster is not assigned", DB::ErrorCodes::BAD_ARGUMENTS);

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

void KafkaWALPool::init(DB::ContextPtr global_context)
{
    const auto & config = global_context->getConfigRef();

    Poco::Util::AbstractConfiguration::Keys sys_wal_keys;
    config.keys(SYSTEM_WALS_KEY, sys_wal_keys);

    for (const auto & key : sys_wal_keys)
        init(key, global_context);
}

void KafkaWALPool::init(const std::string & key, DB::ContextPtr global_context)
{
    if (!key.starts_with("kafka"))
        return;

    const auto & config = global_context->getConfigRef();

    KafkaWALSettings kafka_settings;
    int32_t dedicated_subscription_wal_pool_size = 2;
    int32_t shared_subscription_wal_pool_max_size = 10;
    int32_t streaming_wal_pool_size = 100;
    bool system_default = false;
    bool enabled = false;

    std::vector<std::tuple<std::string, std::string, void *>> settings = {
        {".enabled", "bool", &enabled},
        {".default", "bool", &system_default},
        {".cluster_id", "string", &kafka_settings.cluster_id},
        {".brokers", "string", &kafka_settings.brokers},
        {".topic_metadata_refresh_interval_ms", "int32", &kafka_settings.topic_metadata_refresh_interval_ms},
        {".message_max_bytes", "int32", &kafka_settings.message_max_bytes},
        {".statistic_internal_ms", "int32", &kafka_settings.statistic_internal_ms},
        {".debug", "string", &kafka_settings.debug},
        {".enable_idempotence", "bool", &kafka_settings.enable_idempotence},
        {".queue_buffering_max_messages", "int32", &kafka_settings.queue_buffering_max_messages},
        {".queue_buffering_max_kbytes", "int32", &kafka_settings.queue_buffering_max_kbytes},
        {".queue_buffering_max_ms", "int32", &kafka_settings.queue_buffering_max_ms},
        {".message_send_max_retries", "int32", &kafka_settings.message_send_max_retries},
        {".retry_backoff_ms", "int32", &kafka_settings.retry_backoff_ms},
        {".compression_codec", "string", &kafka_settings.compression_codec},
        {".message_timeout_ms", "int32", &kafka_settings.message_timeout_ms},
        {".message_delivery_async_poll_ms", "int32", &kafka_settings.message_delivery_async_poll_ms},
        {".message_delivery_sync_poll_ms", "int32", &kafka_settings.message_delivery_sync_poll_ms},
        {".group_id", "string", &kafka_settings.group_id},
        {".check_crcs", "bool", &kafka_settings.check_crcs},
        {".auto_commit_interval_ms", "int32", &kafka_settings.auto_commit_interval_ms},
        {".fetch_message_max_bytes", "int32", &kafka_settings.fetch_message_max_bytes},
        {".fetch_wait_max_ms", "int32", &kafka_settings.fetch_wait_max_ms},
        {".queued_min_messages", "int32", &kafka_settings.queued_min_messages},
        {".queued_max_messages_kbytes", "int32", &kafka_settings.queued_max_messages_kbytes},
        {".session_timeout_ms", "int32", &kafka_settings.session_timeout_ms},
        {".max_poll_interval_ms", "int32", &kafka_settings.max_poll_interval_ms},
        {".dedicated_subscription_pool_size", "int32", &dedicated_subscription_wal_pool_size},
        {".shared_subscription_pool_max_size", "int32", &shared_subscription_wal_pool_max_size},
        {".shared_subscription_flush_threshold_count", "int32", &kafka_settings.shared_subscription_flush_threshold_count},
        {".shared_subscription_flush_threshold_bytes", "int32", &kafka_settings.shared_subscription_flush_threshold_bytes},
        {".shared_subscription_flush_threshold_ms", "int32", &kafka_settings.shared_subscription_flush_threshold_ms},
        {".streaming_processing_pool_size", "int32", &streaming_wal_pool_size},
        {".security_protocol", "string", &kafka_settings.auth.security_protocol},
        {".username", "string", &kafka_settings.auth.username},
        {".password", "string", &kafka_settings.auth.password},
        {".ssl_ca_cert_file", "string", &kafka_settings.auth.ssl_ca_cert_file},
    };

    for (const auto & t : settings)
    {
        auto k = fmt::format("{}{}{}", SYSTEM_WALS_KEY_PREFIX, key, std::get<0>(t));
        if (config.has(k))
        {
            const auto & type = std::get<1>(t);
            if (type == "string")
            {
                *static_cast<std::string *>(std::get<2>(t)) = config.getString(k);
            }
            else if (type == "int32")
            {
                auto i = config.getInt(k);
                if (i < 0)
                {
                    throw DB::Exception("Invalid setting " + std::get<0>(t), DB::ErrorCodes::BAD_ARGUMENTS);
                }
                *static_cast<int32_t *>(std::get<2>(t)) = i;
            }
            else if (type == "bool")
            {
                *static_cast<bool *>(std::get<2>(t)) = config.getBool(k);
            }
        }
    }

    if (!enabled)
    {
        LOG_INFO(log, "Kafka config section={}{} is disabled, ignore it", SYSTEM_WALS_KEY_PREFIX, key);
        return;
    }

    if (kafka_settings.brokers.empty())
    {
        LOG_ERROR(log, "Invalid system kafka settings, empty brokers, will skip settings in this segment");
        return;
    }

    if (kafka_settings.group_id.empty())
        /// FIXME
        kafka_settings.group_id = global_context->getNodeIdentity();

    if (wals.contains(kafka_settings.cluster_id))
        throw DB::Exception("Duplicated Kafka cluster id " + kafka_settings.cluster_id, DB::ErrorCodes::BAD_ARGUMENTS);

    if (!boost::iequals(kafka_settings.auth.security_protocol, "plaintext")
        && !boost::iequals(kafka_settings.auth.security_protocol, "sasl_plaintext")
        && !boost::iequals(kafka_settings.auth.security_protocol, "sasl_ssl"))
        throw DB::Exception(
            DB::ErrorCodes::NOT_IMPLEMENTED,
            "Invalid logstore kafka settings security_protocol: {}. Only plaintext, sasl_plaintext or sasl_ssl are supported",
            kafka_settings.auth.security_protocol);

    /// Create WALs
    LOG_INFO(log, "Creating KafkaLog with settings: {}", kafka_settings.string());

    for (int32_t i = 0; i < dedicated_subscription_wal_pool_size; ++i)
    {
        auto ksettings = kafka_settings.clone();

        ksettings->group_id += "-dedicated";
        auto kwal = std::make_shared<KafkaWAL>(std::move(ksettings));

        kwal->startup();

        wals[kafka_settings.cluster_id].first = 0;
        wals[kafka_settings.cluster_id].second.push_back(kwal);
    }

    streaming_consumers.insert({kafka_settings.cluster_id, {static_cast<size_t>(streaming_wal_pool_size), {}}});

    multiplexers.insert({kafka_settings.cluster_id, {static_cast<size_t>(shared_subscription_wal_pool_max_size), {}}});

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

KafkaWALPtr KafkaWALPool::get(const std::string & cluster_id)
{
    if (cluster_id.empty() && !default_cluster.empty())
        return get(default_cluster);

    auto iter = wals.find(cluster_id);
    if (iter == wals.end())
    {
        LOG_ERROR(log, "Unknown streaming storage cluster_id={}", cluster_id);
        throw DB::Exception("Unknown kafka cluster_id=" + cluster_id, DB::ErrorCodes::BAD_ARGUMENTS);
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
        throw DB::Exception("Unknown streaming storage cluster id", DB::ErrorCodes::BAD_ARGUMENTS);
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
        throw DB::Exception("Max streaming processing pool size has been reached", DB::ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES);
    }
}

KafkaWALConsumerMultiplexerPtr KafkaWALPool::getOrCreateConsumerMultiplexer(const std::string & cluster_id)
{
    if (cluster_id.empty() && !default_cluster.empty())
        return getOrCreateConsumerMultiplexer(default_cluster);

    std::lock_guard lock{multiplexer_mutex};

    auto iter = multiplexers.find(cluster_id);
    if (iter == multiplexers.end())
        throw DB::Exception("Unknown kafka cluster_id=" + cluster_id, DB::ErrorCodes::BAD_ARGUMENTS);

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

KafkaWALSimpleConsumerPtr KafkaWALPool::getOrCreateStreamingExternal(const String & brokers, const KafkaWALAuth & auth, int32_t fetch_wait_max_ms)
{
    assert(!brokers.empty());

    fetch_wait_max_ms = bucketFetchWaitMax(fetch_wait_max_ms);

    std::lock_guard lock{external_streaming_lock};

    if (!external_streaming_consumers.contains(brokers))
        /// FIXME, configurable max cached consumers
        external_streaming_consumers.emplace(brokers, std::pair<size_t, KafkaWALSimpleConsumerPtrs>(100, KafkaWALSimpleConsumerPtrs{}));

    /// FIXME, remove cached cluster
    if (external_streaming_consumers.size() > 1000)
        throw DB::Exception("Too many external Kafka cluster registered", DB::ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES);

    auto & consumers = external_streaming_consumers[brokers];

    for (const auto & consumer : consumers.second)
    {
        const auto & consumer_settings = consumer->getSettings();
        if (consumer.use_count() == 1 && consumer_settings.fetch_wait_max_ms == fetch_wait_max_ms && consumer_settings.auth == auth)
        {
            LOG_INFO(log, "Reusing external Kafka consume with settings={}", consumer_settings.string());
            return consumer;
        }
    }

    /// consumer is used up and if we didn't reach maximum
    if (consumers.second.size() < consumers.first)
    {
        if (!boost::iequals(auth.security_protocol, "plaintext")
            && !boost::iequals(auth.security_protocol, "sasl_plaintext")
            && !boost::iequals(auth.security_protocol, "sasl_ssl"))
            throw DB::Exception(
                DB::ErrorCodes::NOT_IMPLEMENTED,
                "Invalid logstore kafka settings security_protocol: {}. Only plaintext, sasl_plaintext or sasl_ssl are supported",
                auth.security_protocol);

        /// Create one
        auto ksettings = std::make_unique<KafkaWALSettings>();

    ksettings->fetch_wait_max_ms = fetch_wait_max_ms;

    ksettings->brokers = brokers;

    /// Streaming WALs have a different group ID
    ksettings->group_id += "-tp-external-streaming-query-" + std::to_string(consumers.second.size() + 1);
    ksettings->auth = auth;

        /// We don't care offset checkpointing for WALs used for streaming processing,
        /// No auto commit
        ksettings->enable_auto_commit = false;

        LOG_INFO(log, "Create new external Kafka consume with settings={{{}}}", ksettings->string());

        auto consumer = std::make_shared<KafkaWALSimpleConsumer>(std::move(ksettings));
        consumer->startup();
        consumers.second.push_back(consumer);
        return consumer;
    }
    else
    {
        LOG_ERROR(log, "External streaming processing pool in cluster={} is used up, size={}", brokers, consumers.first);
        throw DB::Exception("Max external streaming processing pool size has been reached", DB::ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES);
    }
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
