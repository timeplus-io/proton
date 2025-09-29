#include <Storages/MatView/StorageMaterializedView.h>

#include <Bootstrap/Globals.h>
#include <Checkpoint/CheckpointContext.h>
#include <Checkpoint/CheckpointCoordinator.h>
#include <Interpreters/Context.h>

#include <Common/logger_useful.h>

#include <magic_enum.hpp>


namespace DB
{
void StorageMaterializedView::prepareCheckpoint()
{
    const auto & settings = pipeline_state.query_context->getSettingsRef();

    /// To disable checkpoints and stop local checkpoint materializing, set setting "checkpoint_interval" to -1.
    /// FIXME: need a better way to disable checkpoint
    if (settings.checkpoint_interval.value < 0)
    {
        pipeline_state.disable_checkpoint.store(true);
        pipeline_state.exec_mode = ExecuteMode::Normal;
        return;
    }

    pipeline_state.disable_checkpoint.store(false);

    auto & ckpt_coordinator = Globals::getCheckpointCoordinator();
    auto ckpt_settings = ckpt_coordinator.tryGetCheckpointSettings(curr_ckpt_ctx);
    if (!ckpt_settings)
        ckpt_settings = CheckpointSettings::parse(settings.checkpoint_settings);

    if (!curr_ckpt_ctx)
    {
        const auto & ckpt_storage = ckpt_coordinator.getCheckpointStorage(CheckpointReplicationType::LocalFileSystem);
        auto ckpt_ctx = std::make_shared<CheckpointContext>(getInnerQueryId(), ckpt_storage, &ckpt_coordinator);
        curr_ckpt_ctx = std::move(ckpt_ctx);
    }

    auto last_committed_epoch = curr_ckpt_ctx->storage.getLastCommittedEpoch(curr_ckpt_ctx);
    pipeline_state.exec_mode = last_committed_epoch > 0 ? ExecuteMode::Recover : ExecuteMode::Subscribe;
}
}
