#include "MetadataService.h"

#include <DistributedWriteAheadLog/DistributedWriteAheadLogKafka.h>
#include <DistributedWriteAheadLog/DistributedWriteAheadLogPool.h>
#include <Interpreters/Context.h>
#include <Common/setThreadName.h>
#include <common/logger_useful.h>

#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int OK;
    extern const int RESOURCE_ALREADY_EXISTS;
    extern const int RESOURCE_NOT_FOUND;
}

namespace
{
    /// Globals
    const String SYSTEM_ROLES_KEY = "cluster_settings.node_roles";
}

MetadataService::MetadataService(const ContextPtr & global_context_, const String & service_name)
    : global_context(global_context_)
    , dwal(DistributedWriteAheadLogPool::instance(global_context_).getDefault())
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
        /// already shutdown
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

void MetadataService::doDeleteDWal(std::any & ctx)
{
    auto kctx = std::any_cast<DistributedWriteAheadLogKafkaContext &>(ctx);

    int retries = 3;
    while (retries--)
    {
        auto err = dwal->remove(kctx.topic, ctx);
        if (err == ErrorCodes::OK)
        {
            /// FIXME, if the error is fatal. throws
            LOG_INFO(log, "Successfully deleted topic={}", kctx.topic);
            break;
        }
        else if (err == ErrorCodes::RESOURCE_NOT_FOUND)
        {
            LOG_INFO(log, "Topic={} not exists", kctx.topic);
            break;
        }
        else
        {
            LOG_INFO(log, "Failed to delete topic={}, will retry ...", kctx.topic);
            std::this_thread::sleep_for(std::chrono::milliseconds(2000));
        }
    }
}

void MetadataService::waitUntilDWalReady(std::any & ctx)
{
    auto kctx = std::any_cast<DistributedWriteAheadLogKafkaContext &>(ctx);
    while (1)
    {
        if (dwal->describe(kctx.topic, ctx) == ErrorCodes::OK)
        {
            return;
        }
        else
        {
            LOG_INFO(log, "Wait for topic={} to be ready...", kctx.topic);
            std::this_thread::sleep_for(std::chrono::milliseconds(2000));
        }
    }
}

/// Try indefinitely to create dwal
void MetadataService::doCreateDWal(std::any & ctx)
{
    auto kctx = std::any_cast<DistributedWriteAheadLogKafkaContext &>(ctx);
    if (dwal->describe(kctx.topic, ctx) == ErrorCodes::OK)
    {
        return;
    }

    LOG_INFO(log, "Didn't find topic={}, create one", kctx.topic);

    while (!stopped.test())
    {
        auto err = dwal->create(kctx.topic, ctx);
        if (err == ErrorCodes::OK)
        {
            /// FIXME, if the error is fatal. throws
            LOG_INFO(log, "Successfully created topic={}", kctx.topic);
            break;
        }
        else if (err == ErrorCodes::RESOURCE_ALREADY_EXISTS)
        {
            LOG_INFO(log, "Topic={} already exists", kctx.topic);
            break;
        }
        else
        {
            LOG_INFO(log, "Failed to create topic={}, will retry indefinitely ...", kctx.topic);
            std::this_thread::sleep_for(std::chrono::milliseconds(2000));
        }
    }

    waitUntilDWalReady(ctx);
}

void MetadataService::createDWal()
{
    doCreateDWal(dwal_append_ctx);
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

        processRecords(result.records);
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

    String topic = config.getString(conf.name_key, conf.default_name);
    auto replication_factor = config.getInt(conf.replication_factor_key, 1);
    DistributedWriteAheadLogKafkaContext kctx{topic, 1, replication_factor, cleanupPolicy()};
    /// Topic settings
    kctx.retention_ms = config.getInt(conf.data_retention_key, conf.default_data_retention);
    if (kctx.retention_ms > 0)
    {
        kctx.retention_ms *= 3600 * 1000;
    }

    /// Producer settings
    kctx.request_required_acks = conf.request_required_acks;
    kctx.request_timeout_ms = conf.request_timeout_ms;

    std::any append_ctx = kctx;

    const String & this_role = role();
    for (const auto & key : role_keys)
    {
        /// Node only with corresponding service role has corresponding data consuming capability
        if (config.getString(SYSTEM_ROLES_KEY + "." + key, "") == this_role)
        {
            LOG_INFO(log, "Detects the current log has `{}` role", this_role);

            /// First try to create corresponding dwal
            doCreateDWal(append_ctx);

            /// Consumer settings
            kctx.auto_offset_reset = conf.auto_offset_reset;
            kctx.offset = conf.initial_default_offset;
            dwal_consume_ctx = kctx;

            pool.emplace(1);
            pool->scheduleOrThrowOnError([this] { tailingRecords(); });

            break;
        }
    }

    /// Append ctx is cached for multiple threads access
    waitUntilDWalReady(append_ctx);
    kctx.topic_handle = static_cast<DistributedWriteAheadLogKafka *>(dwal.get())->initProducerTopic(kctx);
    /// kctx will be moved over to append ctx
    dwal_append_ctx = std::move(kctx);

    postStartup();
}
}
