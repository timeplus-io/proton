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
    std::shared_ptr<PipelineExecutor> executor;

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
        auto node_descs = executor->getExecGraph().nodeDescriptions(
            ack_node_ids_readonly.begin(), ack_node_ids_readonly.end(), ack_node_ids_readonly.size());

        return formatNodeDescriptions(node_descs);
    }

    String outstandingAckNodeDescriptions() const
    {
        auto node_descs = executor->getExecGraph().nodeDescriptions(ack_node_ids.begin(), ack_node_ids.end(), ack_node_ids.size());

        return formatNodeDescriptions(node_descs);
    }

    String sourceNodeDescriptions() const
    {
        auto source_nodes = executor->getCheckpointSourceNodeIDs();
        auto node_descs = executor->getExecGraph().nodeDescriptions(source_nodes.begin(), source_nodes.end(), source_nodes.size());

        return formatNodeDescriptions(node_descs);
    }

    CheckpointableQuery(std::shared_ptr<PipelineExecutor> executor_, std::optional<Int64> recovered_epoch) : executor(std::move(executor_))
    {
        assert(executor);
        auto node_ids{executor->getCheckpointAckNodeIDs()};

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
    std::shared_ptr<PipelineExecutor> executor,
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
    executor->serialize(ckpt_ctx);

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

    PipelineExecutorPtr executor;
    {
        /// If query with qid is running, cancel it first
        std::scoped_lock lock(mutex);
        auto iter = queries.find(qid);
        if (iter != queries.end())
            executor = iter->second->executor;
    }

    if (executor)
        executor->cancel();

    /// Mark the checkpoint to be removed and and schedule the checkpoint
    /// deletion later
    auto ckpt_ctx = std::make_shared<CheckpointContext>(0, qid, this);
    ckpt->markRemove(ckpt_ctx);

    timer_service.runAfter(grace_interval, [ckpt_ctx, this]() {
        pool->scheduleOrThrow([ckpt_ctx, this]() {
            ckpt->remove(ckpt_ctx);
            LOG_INFO(logger, "Cleaned checkpoints for query={}", ckpt_ctx->qid);
        });
    });
}

void CheckpointCoordinator::triggerCheckpoint(const String & qid, UInt64 checkpoint_interval)
{
    Int64 next_epoch = 0;

    String node_desc;
    std::shared_ptr<PipelineExecutor> executor;
    std::vector<std::pair<Int32, String>> outstanding_ckpt_nodes;
    {
        std::scoped_lock lock(mutex);

        auto iter = queries.find(qid);
        if (iter == queries.end())
            /// Already canceled the query
            return;

        if (iter->second->current_epoch != 0)
        {
            next_epoch = iter->second->current_epoch;
            node_desc = iter->second->outstandingAckNodeDescriptions();
        }
        else
        {
            /// Notify the source processor to start checkpoint in a new epoch
            next_epoch = iter->second->last_epoch + 1;
            executor = iter->second->executor;
        }
    }

    if (executor)
    {
        /// Create directory before hand. Then all other processors don't need
        /// check and create target epoch ckpt directory.
        auto ckpt_ctx = std::make_shared<CheckpointContext>(next_epoch, qid, this);
        try
        {
            preCheckpoint(ckpt_ctx);
            executor->triggerCheckpoint(std::move(ckpt_ctx));
            LOG_INFO(logger, "Triggered checkpointing state for query={} epoch={}", qid, next_epoch);
        }
        catch (const Exception & e)
        {
            LOG_ERROR(logger, "Failed to trigger checkpointing state for query={} epoch={} error={}", qid, next_epoch, e.message());
        }
        catch (const std::exception & ex)
        {
            LOG_ERROR(logger, "Failed to trigger checkpointing state for query={} epoch={} error={}", qid, next_epoch, ex.what());
        }
        catch (...)
        {
            tryLogCurrentException(logger, fmt::format("Failed to trigger checkpointing state for query={} epoch={}", qid, next_epoch));
        }

        timer_service.runAfter(
            checkpoint_interval, [query_id = qid, interval = checkpoint_interval, this]() { triggerCheckpoint(query_id, interval); });
    }
    else
    {
        /// prev ckpt is still in progress, reschedule to check later
        LOG_INFO(
            logger,
            "Prev checkpoint for query={} epoch={} is still in-progress, outstanding_ack_node_descriptions={}",
            qid,
            next_epoch,
            node_desc);

        timer_service.runAfter(15, [query_id = qid, interval = checkpoint_interval, this]() { triggerCheckpoint(query_id, interval); });
    }
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

        assert(ckpt_ctx->epoch == iter->second->current_epoch || iter->second->current_epoch == 0);

        iter->second->current_epoch = ckpt_ctx->epoch;

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
}
