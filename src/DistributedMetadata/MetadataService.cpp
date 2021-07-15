#include "MetadataService.h"

#include <DistributedWriteAheadLog/KafkaWALPool.h>
#include <Interpreters/Context.h>
#include <Common/setThreadName.h>
#include <common/getFQDNOrHostName.h>
#include <common/logger_useful.h>

#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int OK;
    extern const int RESOURCE_ALREADY_EXISTS;
    extern const int RESOURCE_NOT_FOUND;
    extern const int MSG_SIZE_TOO_LARGE;
}

namespace
{
    /// Globals
    const String SYSTEM_ROLES_KEY = "cluster_settings.node_roles";
    const String NAME_KEY = "name";
    const String REPLICATION_FACTOR_KEY = "replication_factor";
    const String DATA_RETENTION_KEY = "data_retention";
    const String LOG_ROLL_SIZE_KEY = "log_roll_size";
    const String LOG_ROLL_PERIOD_KEY = "log_roll_period";
    const String COMPRESSION_KEY = "client_side_compression";

    const String THIS_HOST = getFQDNOrHostName();
}

MetadataService::MetadataService(const ContextPtr & global_context_, const String & service_name)
    : global_context(global_context_)
    , dwal_append_ctx("")
    , dwal_consume_ctx("")
    , dwal(DWAL::KafkaWALPool::instance(global_context_).getMeta())
    , log(&Poco::Logger::get(service_name))
{
}

MetadataService::~MetadataService()
{
    shutdown();
}

void MetadataService::shutdown()
{
    if (stopped.test_and_set())
    {
        /// Already shutdown
        return;
    }

    LOG_INFO(log, "Stopping");

    preShutdown();

    if (pool)
    {
        pool->wait();
    }
    LOG_INFO(log, "Stopped");
}


std::vector<DWAL::KafkaWALClusterPtr> MetadataService::clusters()
{
    return DWAL::KafkaWALPool::instance(global_context).clusters(dwal_append_ctx);
}

void MetadataService::setupRecordHeaders(DWAL::Record & record, const String & version) const
{
    record.headers["_https_port"] = std::to_string(https_port);
    record.headers["_http_port"] = std::to_string(http_port);
    record.headers["_tcp_port"] = std::to_string(tcp_port);
    record.headers["_tcp_port_secure"] = std::to_string(tcp_port_secure);

    record.headers["_node_roles"] = node_roles;
    record.headers["_host"] = THIS_HOST;
    record.headers["_channel"] = global_context->getChannel();

    record.setIdempotentKey(global_context->getNodeIdentity());
    record.headers["_version"] = version;
}

void MetadataService::initPorts()
{
    https_port = global_context->getConfigRef().getInt("https_port", -1);
    http_port = global_context->getConfigRef().getInt("http_port", -1);
    tcp_port_secure = global_context->getConfigRef().getInt("tcp_port_secure", -1);
    tcp_port = global_context->getConfigRef().getInt("tcp_port", -1);
}

void MetadataService::doDeleteDWal(const DWAL::KafkaWALContext & ctx) const
{
    int retries = 3;
    while (retries--)
    {
        auto err = dwal->remove(ctx.topic, ctx);
        if (err == ErrorCodes::OK)
        {
            /// FIXME, if the error is fatal. throws
            LOG_INFO(log, "Successfully deleted topic={}", ctx.topic);
            break;
        }
        else if (err == ErrorCodes::RESOURCE_NOT_FOUND)
        {
            LOG_INFO(log, "Topic={} not exists", ctx.topic);
            break;
        }
        else
        {
            LOG_INFO(log, "Failed to delete topic={}, will retry ...", ctx.topic);
            std::this_thread::sleep_for(std::chrono::milliseconds(2000));
        }
    }
}

void MetadataService::waitUntilDWalReady(const DWAL::KafkaWALContext & ctx) const
{
    while (1)
    {
        if (dwal->describe(ctx.topic, ctx).err == ErrorCodes::OK)
        {
            return;
        }
        else
        {
            LOG_INFO(log, "Wait for topic={} to be ready...", ctx.topic);
            std::this_thread::sleep_for(std::chrono::milliseconds(2000));
        }
    }
}

/// Try indefinitely to create dwal
void MetadataService::doCreateDWal(const DWAL::KafkaWALContext & ctx) const
{
    if (dwal->describe(ctx.topic, ctx).err == ErrorCodes::OK)
    {
        LOG_INFO(log, "Found topic={} already exists", ctx.topic);
        return;
    }

    LOG_INFO(log, "Didn't find topic={}, create one with settings={}", ctx.topic, ctx.string());

    while (!stopped.test())
    {
        auto err = dwal->create(ctx.topic, ctx);
        if (err == ErrorCodes::OK)
        {
            /// FIXME, if the error is fatal. throws
            LOG_INFO(log, "Successfully created topic={}", ctx.topic);
            break;
        }
        else if (err == ErrorCodes::RESOURCE_ALREADY_EXISTS)
        {
            LOG_INFO(log, "Topic={} already exists", ctx.topic);
            break;
        }
        else
        {
            LOG_INFO(log, "Failed to create topic={}, will retry indefinitely ...", ctx.topic);
            std::this_thread::sleep_for(std::chrono::milliseconds(2000));
        }
    }

    waitUntilDWalReady(ctx);
}

void MetadataService::tailingRecords()
{
    try
    {
        doTailingRecords();
    }
    catch (...)
    {
        LOG_ERROR(log, "Failed to consume data, exception={}", getCurrentExceptionMessage(true, true));
    }
}

void MetadataService::doTailingRecords()
{
    auto thr_name = log->name();
    if (thr_name.size() > 15)
    {
        thr_name = String{thr_name.begin(), thr_name.begin() + 14};
    }

    setThreadName(thr_name.c_str());

    auto [ batch, timeout ] = batchSizeAndTimeout();

    LOG_INFO(log, "Starting tailing records");

    while (!stopped.test())
    {
        auto result{dwal->consume(batch, timeout, dwal_consume_ctx)};
        if (result.err != ErrorCodes::OK)
        {
            LOG_ERROR(log, "Failed to consume data, error={}", result.err);
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            continue;
        }

        if (result.records.empty())
        {
            continue;
        }

        try
        {
            processRecords(result.records);
        }
        catch (...)
        {
            LOG_ERROR(log, "Failed to process records, exception={}", getCurrentExceptionMessage(true, true));
        }
    }

    dwal->stopConsume(dwal_consume_ctx);
}

void MetadataService::startup()
{
    if (!global_context->isDistributed())
    {
        return;
    }

    assert(dwal);

    const auto & config = global_context->getConfigRef();

    /// If this node has `target` role, start background thread tailing records
    Poco::Util::AbstractConfiguration::Keys role_keys;
    config.keys(SYSTEM_ROLES_KEY, role_keys);

    /// Every service on every node has data producing capability
    /// For example, every node can call produce node metrics via PlacementService
    const auto & conf = configSettings();

    String topic = config.getString(conf.key_prefix + NAME_KEY , conf.default_name);
    auto replication_factor = config.getInt(conf.key_prefix + REPLICATION_FACTOR_KEY, 1);

    DWAL::KafkaWALContext kctx{topic, 1, replication_factor, cleanupPolicy()};
    /// Topic settings
    kctx.retention_ms = config.getInt(conf.key_prefix + DATA_RETENTION_KEY, conf.default_data_retention);
    if (kctx.retention_ms > 0)
    {
        /// Hour based in config
        kctx.retention_ms *= 3600 * 1000;
    }

    /// Compact topics
    if (kctx.cleanup_policy == "compact")
    {
        /// In MBs
        kctx.segment_bytes = config.getInt(conf.key_prefix + LOG_ROLL_SIZE_KEY, 100) * 1024 * 1024;
        /// In Seconds
        kctx.segment_ms = config.getInt(conf.key_prefix + LOG_ROLL_PERIOD_KEY, 7200) * 1000;
    }

    /// Producer settings
    kctx.request_required_acks = conf.request_required_acks;
    kctx.request_timeout_ms = conf.request_timeout_ms;
    /// Compression settings
    kctx.client_side_compression = config.getBool(conf.key_prefix + COMPRESSION_KEY , false);

    const String & this_role = role();
    for (const auto & key : role_keys)
    {
        /// Node only with corresponding service role has corresponding data consuming capability
        auto node_role = config.getString(SYSTEM_ROLES_KEY + "." + key, "");
        boost::trim(node_role);
        boost::to_lower(node_role);
        node_roles += node_role + ",";

        if (node_role == this_role && !pool)
        {
            LOG_INFO(log, "Detects the current log has `{}` role", this_role);

            /// First try to create corresponding dwal
            doCreateDWal(kctx);

            /// Consumer settings
            kctx.auto_offset_reset = conf.auto_offset_reset;
            kctx.offset = conf.initial_default_offset;
            dwal_consume_ctx = kctx;
            dwal->initConsumerTopicHandle(dwal_consume_ctx);

            pool.emplace(1);
            pool->scheduleOrThrowOnError([this] { tailingRecords(); });
        }
    }

    if (!node_roles.empty())
    {
        node_roles.pop_back();
    }

    initPorts();

    waitUntilDWalReady(kctx);

    /// Append handle is cached for multiple threads access
    dwal_append_ctx = std::move(kctx);
    dwal->initProducerTopicHandle(dwal_append_ctx);

    postStartup();
}
}
