#include "CheckpointCoordinator.h"
#include "CheckpointContext.h"
#include "CheckpointStorageFactory.h"

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Processors/Executors/PipelineExecutor.h>

#include <Poco/Logger.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{
namespace ErrorCodes
{
extern const int QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING;
extern const int RESOURCE_NOT_FOUND;
}

namespace
{
const String QUERY_CKPT_FILE = "query";

const UInt64 DEFAULT_CKPT_LAST_ACCESS_TTL = 7 * 81400; /// in seconds
const UInt64 DEFAULT_CKPT_LAST_ACCESS_CHECK_INTERVAL = 7200; /// in seconds
const UInt64 DEFAULT_CKPT_DELETE_GRACE_INTERVAL = 60; /// in seconds
const UInt64 DEFAULT_CKPT_TEARDOWN_FLUSH_TIMEOUT = 60; /// in seconds

String formatNodeDescriptions(const std::vector<ExecutingGraph::NodeDescription> & node_descs)
{
    std::vector<String> desc;
    desc.reserve(node_descs.size());

    for (const auto & node_desc : node_descs)
        desc.emplace_back(node_desc.string());

    return fmt::format("{}", fmt::join(desc, ", "));
}
}

struct CheckpointableQuery
{
    /// Query DAG, maybe using std::weak_ptr makes more sense
    std::weak_ptr<PipelineExecutor> executor;

    absl::flat_hash_set<UInt32> ack_node_ids_readonly;
    absl::flat_hash_set<UInt32> ack_node_ids;

    Int64 current_epoch = 0;
    Int64 last_epoch = 0;

    /// Return true if all ack nodes acked
    bool ack(UInt32 node_id)
    {
        ack_node_ids.erase(node_id);
        return ack_node_ids.empty();
    }

    void prepareForNextEpoch()
    {
        last_epoch = current_epoch;
        current_epoch = 0;
        ack_node_ids = ack_node_ids_readonly;
    }

    String ackNodeDescriptions() const
    {
        auto exec = executor.lock();
        assert(exec);
        auto node_descs = exec->getExecGraph().nodeDescriptions(
            ack_node_ids_readonly.begin(), ack_node_ids_readonly.end(), ack_node_ids_readonly.size());

        return formatNodeDescriptions(node_descs);
    }

    String outstandingAckNodeDescriptions() const
    {
        auto exec = executor.lock();
        assert(exec);
        auto node_descs = exec->getExecGraph().nodeDescriptions(ack_node_ids.begin(), ack_node_ids.end(), ack_node_ids.size());

        return formatNodeDescriptions(node_descs);
    }

    String sourceNodeDescriptions() const
    {
        auto exec = executor.lock();
        assert(exec);
        auto source_nodes = exec->getCheckpointSourceNodeIDs();
        auto node_descs = exec->getExecGraph().nodeDescriptions(source_nodes.begin(), source_nodes.end(), source_nodes.size());

        return formatNodeDescriptions(node_descs);
    }

    CheckpointableQuery(std::weak_ptr<PipelineExecutor> executor_, std::optional<Int64> recovered_epoch) : executor(std::move(executor_))
    {
        auto exec = executor.lock();
        assert(exec);
        auto node_ids{exec->getCheckpointAckNodeIDs()};

        ack_node_ids_readonly.reserve(node_ids.size());
        ack_node_ids_readonly.insert(node_ids.begin(), node_ids.end());

        assert(!ack_node_ids_readonly.empty());

        if (recovered_epoch)
            current_epoch = *recovered_epoch;

        prepareForNextEpoch();
    }
};

CheckpointCoordinator::CheckpointCoordinator(DB::ContextPtr global_context) : logger(&Poco::Logger::get("CheckpointCoordinator"))
{
    const auto & config = global_context->getConfigRef();
    ckpt = CheckpointStorageFactory::create(config);

    last_access_ttl = config.getUInt64("checkpoint.last_access_ttl", DEFAULT_CKPT_LAST_ACCESS_TTL);
    last_access_check_interval = config.getUInt64("checkpoint.last_access_check_interval", DEFAULT_CKPT_LAST_ACCESS_CHECK_INTERVAL);
    grace_interval = config.getUInt64("checkpoint.delete_grace_interval", DEFAULT_CKPT_DELETE_GRACE_INTERVAL);
    teardown_flush_timeout = config.getUInt64("checkpoint.teardown_flush_timeout", DEFAULT_CKPT_TEARDOWN_FLUSH_TIMEOUT);
}

CheckpointCoordinator::~CheckpointCoordinator()
{
    shutdown();
}

void CheckpointCoordinator::startup()
{
    if (started.test_and_set())
        return;

    pool.emplace(2);

    timer_service.startup();
    pool->scheduleOrThrow([this] { removeExpiredCheckpoints(true); });
}

void CheckpointCoordinator::shutdown()
{
    if (stopped.test_and_set())
        return;

    timer_service.shutdown();

    if (pool)
        pool->wait();
}

void CheckpointCoordinator::registerQuery(
    const String & qid,
    const String & query,
    UInt64 ckpt_interval,
    std::weak_ptr<PipelineExecutor> executor,
    std::optional<Int64> recovered_epoch)
{
    auto ckpt_query = std::make_unique<CheckpointableQuery>(executor, std::move(recovered_epoch));
    auto ack_nodes_desc = ckpt_query->ackNodeDescriptions();
    auto source_nodes_desc = ckpt_query->sourceNodeDescriptions();

    bool query_exists = false;
    {
        std::scoped_lock lock(mutex);

        auto [_, inserted] = queries.emplace(qid, std::move(ckpt_query));
        query_exists = !inserted;
    }

    if (query_exists)
    {
        LOG_INFO(logger, "Failed to register query={} since the query id already exists", qid);
        throw Exception(
            ErrorCodes::QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING, "Failed to register query={} since the query id already exists", qid);
    }

    auto ckpt_ctx = std::make_shared<CheckpointContext>(0, qid, this);

    /// First persist the graph
    {
        auto exec = executor.lock();
        assert(exec);
        exec->serialize(ckpt_ctx);
    }

    /// Then persist the query in a different file.
    /// We choose to persist query here for easier to recover the query from the ckpt
    checkpoint(1, QUERY_CKPT_FILE, ckpt_ctx, [&](WriteBuffer & wb) {
        /// Query
        DB::writeStringBinary(query, wb);
    });

    auto interval = ckpt_interval ? ckpt_interval : ckpt->defaultInterval();

    LOG_INFO(
        logger,
        "Register query={} with {} seconds checkpoint interval, source_node_descriptions={}, ack_node_descriptions={}",
        qid,
        interval,
        source_nodes_desc,
        ack_nodes_desc);

    timer_service.runAfter(
        interval, [query_id = qid, checkpoint_interval = interval, this]() { triggerCheckpoint(query_id, checkpoint_interval); });
}

void CheckpointCoordinator::deregisterQuery(const String & qid)
{
    size_t n = 0;
    {
        std::scoped_lock lock(mutex);

        n = queries.erase(qid);
    }

    if (n)
        LOG_INFO(logger, "Deregistered query={}", qid);
}

void CheckpointCoordinator::removeCheckpoint(const String & qid)
{
    LOG_INFO(logger, "Going to clean checkpoints for query={}", qid);

    std::weak_ptr<PipelineExecutor> executor;
    {
        /// If query with qid is running, cancel it first
        std::scoped_lock lock(mutex);
        auto iter = queries.find(qid);
        if (iter != queries.end())
            executor = iter->second->executor;
    }

    if (auto exec = executor.lock())
        exec->cancel();

    /// Mark the checkpoint to be removed and and schedule the checkpoint
    /// deletion later
    auto ckpt_ctx = std::make_shared<CheckpointContext>(0, qid, this);
    if (ckpt->markRemove(ckpt_ctx))
        timer_service.runAfter(grace_interval, [ckpt_ctx, this]() {
            pool->scheduleOrThrow([ckpt_ctx, this]() {
                ckpt->remove(ckpt_ctx);
                LOG_INFO(logger, "Cleaned checkpoints for query={}", ckpt_ctx->qid);
            });
        });
}

void CheckpointCoordinator::triggerCheckpoint(const String & qid, UInt64 checkpoint_interval)
{
    std::weak_ptr<PipelineExecutor> executor;
    CheckpointContextPtr ckpt_ctx;
    {
        std::scoped_lock lock(mutex);

        auto iter = queries.find(qid);
        if (iter == queries.end())
            /// Already canceled the query
            return;

        if (iter->second->current_epoch != 0)
        {
            ckpt_ctx = std::make_shared<CheckpointContext>(iter->second->current_epoch, qid, this);
        }
        else
        {
            /// Notify the source processor to start checkpoint in a new epoch
            executor = iter->second->executor;
            ckpt_ctx = std::make_shared<CheckpointContext>(iter->second->last_epoch + 1, qid, this);
            iter->second->current_epoch = ckpt_ctx->epoch; /// new epoch in process
        }
    }

    if (doTriggerCheckpoint(executor, std::move(ckpt_ctx)))
        timer_service.runAfter(
            checkpoint_interval, [query_id = qid, interval = checkpoint_interval, this]() { triggerCheckpoint(query_id, interval); });
    else
        /// Retry after interval - min(15s, checkpoint_interval)
        timer_service.runAfter(std::min<double>(15, checkpoint_interval), [query_id = qid, interval = checkpoint_interval, this]() { triggerCheckpoint(query_id, interval); });
}

void CheckpointCoordinator::preCheckpoint(DB::CheckpointContextPtr ckpt_ctx)
{
    ckpt->preCheckpoint(std::move(ckpt_ctx));
}

void CheckpointCoordinator::checkpoint(
    VersionType version, const String & key, CheckpointContextPtr ckpt_ctx, std::function<void(WriteBuffer &)> do_ckpt)
{
    ckpt->checkpoint(version, key, std::move(ckpt_ctx), std::move(do_ckpt));
}

void CheckpointCoordinator::checkpoint(
    VersionType version, UInt32 node_id, CheckpointContextPtr ckpt_ctx, std::function<void(WriteBuffer &)> do_ckpt)
{
    ckpt->checkpoint(version, fmt::format("{}", node_id), ckpt_ctx, std::move(do_ckpt));

    checkpointed(version, node_id, std::move(ckpt_ctx));
}

void CheckpointCoordinator::checkpointed(VersionType /*version*/, UInt32 node_id, CheckpointContextPtr ckpt_ctx)
{
    bool ckpt_epoch_done = false;
    {
        std::scoped_lock lock(mutex);
        auto iter = queries.find(ckpt_ctx->qid);
        if (iter == queries.end())
            /// Unregistered
            return;

        assert(ckpt_ctx->epoch == iter->second->current_epoch);

        if (iter->second->ack(node_id))
        {
            /// All ack nodes in the DAG have acked
            /// 1) Commit `committed` flag file to query ckpt directory for that epoch
            ckpt->commit(ckpt_ctx);

            /// 2) Prepare for next epoch
            iter->second->prepareForNextEpoch();

            ckpt_epoch_done = true;
        }
    }

    /// 3) Delete ckpts for prev epochs
    if (ckpt_epoch_done)
        ckpt->remove(std::move(ckpt_ctx));
}

String CheckpointCoordinator::getQuery(const String & qid)
{
    auto ckpt_ctx = std::make_shared<CheckpointContext>(0, qid, this);
    if (!ckpt->exists(QUERY_CKPT_FILE, ckpt_ctx))
        throw Exception(ErrorCodes::RESOURCE_NOT_FOUND, "Can't find query={}", qid);

    String query;
    recover(QUERY_CKPT_FILE, std::move(ckpt_ctx), [&query](VersionType /*version*/, ReadBuffer & rb) { DB::readStringBinary(query, rb); });

    return query;
}

void CheckpointCoordinator::recover(const String & qid, std::function<void(CheckpointContextPtr)> do_recover)
{
    auto last_committed_epoch = ckpt->recover(qid);
    do_recover(std::make_shared<CheckpointContext>(last_committed_epoch, qid, this));
}

void CheckpointCoordinator::recover(
    const String & key, CheckpointContextPtr ckpt_ctx, std::function<void(VersionType version, ReadBuffer &)> do_recover)
{
    ckpt->recover(key, std::move(ckpt_ctx), std::move(do_recover));
}

void CheckpointCoordinator::recover(
    UInt32 node_id, CheckpointContextPtr ckpt_ctx, std::function<void(VersionType version, ReadBuffer &)> do_recover)
{
    ckpt->recover(fmt::format("{}", node_id), std::move(ckpt_ctx), std::move(do_recover));
}

void CheckpointCoordinator::removeExpiredCheckpoints(bool delete_marked)
{
    try
    {
        ckpt->removeExpired(last_access_ttl, delete_marked, [this](const String & qid) {
            std::scoped_lock lock(mutex);
            return !queries.contains(qid);
        });
    }
    catch (const Exception & e)
    {
        LOG_ERROR(logger, "Failed to remove expired checkpoints error={}", e.message());
    }
    catch (const std::exception & ex)
    {
        LOG_ERROR(logger, "Failed to remove expired checkpoints error={}", ex.what());
    }
    catch (...)
    {
        tryLogCurrentException(logger, "Failed to remove expired checkpoints");
    }

    /// Every last_access_check_interval seconds, check if there are any expired checkpoints to delete
    timer_service.runAfter(last_access_check_interval, [this]() { removeExpiredCheckpoints(false); });
}

bool CheckpointCoordinator::doTriggerCheckpoint(const std::weak_ptr<PipelineExecutor> & executor, CheckpointContextPtr ckpt_ctx)
{
    bool triggered = false;
    SCOPE_EXIT({
        if (!triggered)
            resetCurrentCheckpointEpoch(ckpt_ctx->qid);
    });

    /// Create directory before hand. Then all other processors don't need
    /// check and create target epoch ckpt directory.
    try
    {
        auto exec = executor.lock();
        if (!exec)
        {
            LOG_ERROR(
                logger,
                "Failed to trigger checkpointing state for query={} epoch={}, since prev checkpoint is still in-progress or it was "
                "already cancelled",
                ckpt_ctx->qid,
                ckpt_ctx->epoch);
            return false;
        }

        if (!exec->hasProcessedNewDataSinceLastCheckpoint())
        {
            LOG_INFO(
                logger,
                "Skipped checkpointing state for query={} epoch={}, since there is no new data processed",
                ckpt_ctx->qid,
                ckpt_ctx->epoch);
            return false;
        }

        preCheckpoint(ckpt_ctx);

        exec->triggerCheckpoint(ckpt_ctx);

        triggered = true;
    
        LOG_INFO(logger, "Triggered checkpointing state for query={} epoch={}", ckpt_ctx->qid, ckpt_ctx->epoch);
        return true;
    }
    catch (const Exception & e)
    {
        LOG_ERROR(logger, "Failed to trigger checkpointing state for query={} epoch={} error={}", ckpt_ctx->qid, ckpt_ctx->epoch, e.message());
    }
    catch (const std::exception & ex)
    {
        LOG_ERROR(logger, "Failed to trigger checkpointing state for query={} epoch={} error={}", ckpt_ctx->qid, ckpt_ctx->epoch, ex.what());
    }
    catch (...)
    {
        tryLogCurrentException(logger, fmt::format("Failed to trigger checkpointing state for query={} epoch={}", ckpt_ctx->qid, ckpt_ctx->epoch));
    }
    return false;
}

void CheckpointCoordinator::triggerLastCheckpointAndFlush()
{
    LOG_INFO(logger, "Trigger last checkpoint and flush begin");
    Stopwatch stopwatch;

    std::vector<std::weak_ptr<PipelineExecutor>> executors;
    std::vector<CheckpointContextPtr> ckpt_ctxes;

    {
        std::scoped_lock lock(mutex);

        executors.reserve(queries.size());
        ckpt_ctxes.reserve(queries.size());
        for (auto & [qid, query] : queries)
        {
            if (query->current_epoch != 0)
            {
                executors.emplace_back();
                ckpt_ctxes.emplace_back(std::make_shared<CheckpointContext>(query->current_epoch, qid, this));
            }
            else
            {
                /// Notify the source processor to start checkpoint in a new epoch
                executors.emplace_back(query->executor);
                ckpt_ctxes.emplace_back(std::make_shared<CheckpointContext>(query->last_epoch + 1, qid, this));
                query->current_epoch = ckpt_ctxes.back()->epoch; /// new epoch in process
            }
        }
    }

    assert(executors.size() == ckpt_ctxes.size());

    /// <query_id, triggered_epoch>
    std::vector<std::pair<std::string_view, Int64>> triggered_queries;
    triggered_queries.reserve(executors.size());
    for (size_t i = 0; i < executors.size(); ++i)
    {
        /// FIXME: So far we've only enforced a simple flush strategy by triggering new checkpoint once (regardless of success)
        if (doTriggerCheckpoint(executors[i], ckpt_ctxes[i]))
            triggered_queries.emplace_back(ckpt_ctxes[i]->qid, ckpt_ctxes[i]->epoch);
    }

    // Wait for last checkpoint flush completed
    while (stopwatch.elapsedSeconds() < teardown_flush_timeout)
    {
        for (auto qid_iter = triggered_queries.begin(); qid_iter != triggered_queries.end();)
        {
            std::scoped_lock lock(mutex);
            auto iter = queries.find(qid_iter->first);
            if (iter == queries.end())
                /// Unregistered
                return;

            /// Remove when triggerd epoch is commited
            /// NOTE: `commited epoch > triggerd epoch` is possible, if there is new checkpoint triggered after triggered last checkpoint flush
            if (iter->second->last_epoch >= qid_iter->second)
                qid_iter = triggered_queries.erase(qid_iter);
            else
                ++qid_iter;
        }

        if (triggered_queries.empty())
            break;

        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    stopwatch.stop();
    LOG_INFO(logger, "Trigger last checkpoint and flush end (elapsed {} milliseconds)", stopwatch.elapsedMilliseconds());
}

void CheckpointCoordinator::resetCurrentCheckpointEpoch(const String & qid)
{
    std::scoped_lock lock(mutex);
    auto iter = queries.find(qid);
    if (iter == queries.end())
        /// Already canceled the query
        return;

    iter->second->current_epoch = 0;
}

}
