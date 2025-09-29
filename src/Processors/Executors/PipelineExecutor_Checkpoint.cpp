#include <Processors/Executors/PipelineExecutor.h>

#include <Bootstrap/Globals.h>
#include <Checkpoint/CheckpointContext.h>
#include <Checkpoint/CheckpointCoordinator.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Common/VersionRevision.h>

namespace DB
{
void PipelineExecutor::registerCheckpoint(ExecuteMode exec_mode_, CheckpointContextPtr ckpt_ctx)
{
    if (exec_mode_ == ExecuteMode::Normal)
        return;

    chassert(process_list_element);

    graph->initCheckpointNodes();

    const auto & settings = process_list_element->getContext()->getSettingsRef();

    /// In case when run a `subscribe query`, there is no checkpoint context provided externally
    if (!ckpt_ctx)
    {
        auto & ckpt_coordinator = Globals::getCheckpointCoordinator();
        ckpt_ctx = std::make_shared<CheckpointContext>(
            process_list_element->getClientInfo().current_query_id,
            ckpt_coordinator.getCheckpointStorage(CheckpointReplicationType::LocalFileSystem),
            &ckpt_coordinator);
    }

    std::optional<CheckpointEpoch> recovered_epoch;
    CheckpointSettingsPtr ckpt_settings;
    if (exec_mode_ == ExecuteMode::Recover)
        std::tie(recovered_epoch, ckpt_settings) = recover(ckpt_ctx);
    else
        ckpt_settings = CheckpointSettings::parse(settings.checkpoint_settings);

    /// Overwrite the checkpoint interval if it's set in the query settings
    if (settings.checkpoint_interval.value > 0)
        ckpt_settings->interval = settings.checkpoint_interval.value;

    /// Here requires a lock to cover following scenarios in multi-threads:
    /// 1) registerQuery() -> dtor() OR cancel() -> deregisterQuery()
    /// 2) dtor() OR cancel() -> deregisterQuery() -> registerQuery()
    /// 3) dtor() OR cancel() -> registerQuery() -> deregisterQuery()
    std::lock_guard lock(register_checkpoint_mutex);
    if (cancelled)
        return;

    exec_mode = exec_mode_;
    auto & ckpt_coordinator = Globals::getCheckpointCoordinator();
    ckpt_coordinator.registerQuery(ckpt_ctx, process_list_element->getQuery(), std::move(ckpt_settings), weak_from_this(), recovered_epoch);
}

void PipelineExecutor::deregisterCheckpoint()
{
    std::lock_guard lock(register_checkpoint_mutex);
    if (exec_mode == ExecuteMode::Subscribe || exec_mode == ExecuteMode::Recover)
    {
        auto & ckpt_coordinator = Globals::getCheckpointCoordinator();
        ckpt_coordinator.deregisterQuery(process_list_element->getClientInfo().current_query_id);

        /// Reset exec_mode to NORMAL to avoid re-execute deregisterQuery again although it doesn't hurt
        exec_mode = ExecuteMode::Normal;
    }
}

bool PipelineExecutor::checkpointRegistered() const
{
    std::lock_guard lock(register_checkpoint_mutex);
    return exec_mode == ExecuteMode::Subscribe || exec_mode == ExecuteMode::Recover;
}

void PipelineExecutor::checkpointGraph(WriteBuffer & wb) const
{
    /// For recover mode, we don't want to re-checkpoint dag.ckpt
    /// Create the query ckpt folder.
    UInt64 interval = process_list_element->getContext()->getSettingsRef().checkpoint_interval.value;

    /// Interval
    DB::writeIntBinary(interval, wb);
    /// Threads
    DB::writeIntBinary(execute_threads, wb);
    /// Graph
    graph->serialize(wb);
}

bool PipelineExecutor::hasProcessedNewDataSinceLastCheckpoint() const noexcept
{
    return graph->hasProcessedNewDataSinceLastCheckpoint();
}

void PipelineExecutor::triggerCheckpoint(CheckpointContextPtr ckpt_ctx)
{
    graph->triggerCheckpoint(std::move(ckpt_ctx));
}

std::pair<CheckpointEpoch, CheckpointSettingsPtr> PipelineExecutor::recover(CheckpointContextPtr ckpt_ctx)
{
    Stopwatch stopwatch;
    auto & ckpt_coordinator = Globals::getCheckpointCoordinator();

    /// Validate the execute graph doesn't change
    ckpt_coordinator.recoverGraph(ckpt_ctx, [&](VersionType version_, ReadBuffer & rb) {
        version = version_;

        /// FIXME: Interval Deprecated, it shall be removed in next big release
        UInt64 recovered_interval = 0;
        DB::readIntBinary(recovered_interval, rb);

        /// Threads
        DB::readIntBinary(execute_threads, rb);

        /// Graph
        try
        {
            graph->deserialize(rb);
        }
        catch (...)
        {
            LOG_ERROR(
                log,
                "Failed to recover query states from checkpoint : {}. Maybe the new query plan is not compatible with checkpointed. "
                "checkpointed_query={}",
                getCurrentExceptionMessage(false),
                process_list_element->getQuery());
            throw;
        }
    });

    /// Replaces with recovered checkpoint properties
    auto ckpt_settings = ckpt_coordinator.tryGetCheckpointSettings(ckpt_ctx);
    if (!ckpt_settings)
        ckpt_settings = CheckpointSettings::parse(process_list_element->getContext()->getSettingsRef().checkpoint_settings);

    /// Recover query states from checkpoint
    const auto & ckpt_storage = ckpt_coordinator.getCheckpointStorage(ckpt_settings->replication_type);
    auto recovered_epoch = ckpt_storage.getLastCommittedEpoch(ckpt_ctx);
    if (!recovered_epoch.empty())
        graph->recover(ckpt_ctx->cloneWithEpoch(recovered_epoch));

    LOG_INFO(
        log,
        "Recovered query states from checkpoint epoch={}, query_id={}, settings={{{}}}, took={}ms",
        recovered_epoch,
        ckpt_ctx->qid,
        ckpt_settings->raw_settings,
        stopwatch.elapsedMilliseconds());

    return {recovered_epoch, ckpt_settings};
}

VersionType PipelineExecutor::getVersionFromRevision(UInt64 revision) const
{
    if (version)
        return *version;

    return static_cast<VersionType>(revision);
}

VersionType PipelineExecutor::getVersion() const
{
    auto ver = getVersionFromRevision(ProtonRevision::getVersionRevision());

    if (!version)
        version = ver;

    return ver;
}
}
