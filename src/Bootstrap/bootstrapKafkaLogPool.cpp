#include <Bootstrap/bootstrapKafkaLogPool.h>

#include <Interpreters/Context.h>
#include <Cluster/KafkaLog/KafkaWALPool.h>

#include <boost/algorithm/string/predicate.hpp>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int NOT_IMPLEMENTED;
}

namespace
{
/// Globals
const std::string LOGSTORE_KEY_PREFIX = "logstore";

void getKafkaLogConfig(const std::string & key, const ContextPtr & global_context, std::vector<cluster::klog::KafkaLogConfig> & kconfigs)
{
    const auto & config = global_context->getConfigRef();

    cluster::klog::KafkaLogConfig kconfig;

    std::vector<std::tuple<std::string, std::string, void *>> settings = {
        {".enabled", "bool", &kconfig.enabled},
        {".default", "bool", &kconfig.system_default},
        {".cluster_id", "string", &kconfig.settings.cluster_id},
        {".brokers", "string", &kconfig.settings.brokers},
        {".topic_metadata_refresh_interval_ms", "int32", &kconfig.settings.topic_metadata_refresh_interval_ms},
        {".message_max_bytes", "int32", &kconfig.settings.message_max_bytes},
        {".statistic_internal_ms", "int32", &kconfig.settings.statistic_internal_ms},
        {".debug", "string", &kconfig.settings.debug},
        {".enable_idempotence", "bool", &kconfig.settings.enable_idempotence},
        {".queue_buffering_max_messages", "int32", &kconfig.settings.queue_buffering_max_messages},
        {".queue_buffering_max_kbytes", "int32", &kconfig.settings.queue_buffering_max_kbytes},
        {".queue_buffering_max_ms", "int32", &kconfig.settings.queue_buffering_max_ms},
        {".message_send_max_retries", "int32", &kconfig.settings.message_send_max_retries},
        {".retry_backoff_ms", "int32", &kconfig.settings.retry_backoff_ms},
        {".compression_codec", "string", &kconfig.settings.compression_codec},
        {".message_timeout_ms", "int32", &kconfig.settings.message_timeout_ms},
        {".message_delivery_async_poll_ms", "int32", &kconfig.settings.message_delivery_async_poll_ms},
        {".message_delivery_sync_poll_ms", "int32", &kconfig.settings.message_delivery_sync_poll_ms},
        {".group_id", "string", &kconfig.settings.group_id},
        {".check_crcs", "bool", &kconfig.settings.check_crcs},
        {".auto_commit_interval_ms", "int32", &kconfig.settings.auto_commit_interval_ms},
        {".fetch_message_max_bytes", "int32", &kconfig.settings.fetch_message_max_bytes},
        {".fetch_wait_max_ms", "int32", &kconfig.settings.fetch_wait_max_ms},
        {".queued_min_messages", "int32", &kconfig.settings.queued_min_messages},
        {".queued_max_messages_kbytes", "int32", &kconfig.settings.queued_max_messages_kbytes},
        {".session_timeout_ms", "int32", &kconfig.settings.session_timeout_ms},
        {".max_poll_interval_ms", "int32", &kconfig.settings.max_poll_interval_ms},
        {".dedicated_subscription_pool_size", "int32", &kconfig.dedicated_subscription_pool_size},
        {".shared_subscription_pool_max_size", "int32", &kconfig.shared_subscription_pool_max_size},
        {".shared_subscription_flush_threshold_count", "int32", &kconfig.settings.shared_subscription_flush_threshold_count},
        {".shared_subscription_flush_threshold_bytes", "int32", &kconfig.settings.shared_subscription_flush_threshold_bytes},
        {".shared_subscription_flush_threshold_ms", "int32", &kconfig.settings.shared_subscription_flush_threshold_ms},
        {".streaming_processing_pool_size", "int32", &kconfig.pool_size},
        {".security_protocol", "string", &kconfig.settings.auth.security_protocol},
        {".username", "string", &kconfig.settings.auth.username},
        {".password", "string", &kconfig.settings.auth.password},
    };

    for (const auto & t : settings)
    {
        auto k = fmt::format("{}.{}{}", LOGSTORE_KEY_PREFIX, key, std::get<0>(t));
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
                    throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Invalid setting {}", std::get<0>(t));
                }
                *static_cast<int32_t *>(std::get<2>(t)) = i;
            }
            else if (type == "bool")
            {
                *static_cast<bool *>(std::get<2>(t)) = config.getBool(k);
            }
        }
    }

    if (!kconfig.enabled)
        return;

    if (kconfig.settings.brokers.empty())
        return;

    if (kconfig.settings.group_id.empty())
        /// FIXME
        kconfig.settings.group_id = toString(global_context->getNodeUUID());

    if (!boost::iequals(kconfig.settings.auth.security_protocol, "plaintext")
        && !boost::iequals(kconfig.settings.auth.security_protocol, "sasl_ssl"))
        throw DB::Exception(
            DB::ErrorCodes::NOT_IMPLEMENTED,
            "Invalid logstore kafka settings security_protocol: {}. Only plaintext or sasl_ssl are supported",
            kconfig.settings.auth.security_protocol);

    kconfigs.push_back(std::move(kconfig));
}
}

cluster::klog::KafkaWALPool * bootstrapKafkaLogPool(const ContextPtr & global_context)
{
    const auto & config = global_context->getConfigRef();

    Poco::Util::AbstractConfiguration::Keys logstore_keys;
    config.keys(LOGSTORE_KEY_PREFIX, logstore_keys);

    std::vector<cluster::klog::KafkaLogConfig> kconfigs;

    for (const auto & key : logstore_keys)
    {
        if (!key.starts_with("kafkalog"))
            continue;

        getKafkaLogConfig(key, global_context, kconfigs);
    }

    if (!kconfigs.empty())
    {
        auto & wal_pool = cluster::klog::KafkaWALPool::instance(std::move(kconfigs));
        if (wal_pool.valid())
            return &wal_pool;
    }
    return nullptr;
}

}
