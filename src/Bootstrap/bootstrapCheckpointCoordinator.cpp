#include <Bootstrap/ConfigSetting.h>
#include <Bootstrap/bootstrapCheckpointCoordinator.h>

#include <Checkpoint/CheckpointCoordinator.h>
#include <Interpreters/Context.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{
namespace
{

}

CheckpointCoordinator & bootstrapStateCheckpointCoordinator(const ContextPtr & global_context)
{
    CheckpointConfig ckpt_config;

    const auto & config = global_context->getConfigRef();

    ConfigSettings config_settings = {
        {".last_access_ttl", ckpt_config.last_access_ttl_sec},
        {".last_access_check_interval", ckpt_config.last_access_check_interval_sec},
        {".delete_grace_interval", ckpt_config.delete_grace_interval_sec},
        {".teardown_flush_timeout", ckpt_config.teardown_flush_timeout_sec},
        {".log_segment_size", ckpt_config.log_segment_size},
        {".log_max_entry_size", ckpt_config.log_max_entry_size},
        {".log_max_cached_entries", ckpt_config.log_max_cached_entries},
        {".log_flush_interval_entries", ckpt_config.log_flush_interval_entries},
        {".log_flush_bytes", ckpt_config.log_flush_bytes},
        {".log_retention_size", ckpt_config.log_retention_size},
        {".log_retention_ms", ckpt_config.log_retention_ms},
        {".log_min_size_to_keep", ckpt_config.log_min_size_to_keep},
        {".min_interval", ckpt_config.min_interval_sec},
        {".light_state_interval", ckpt_config.light_state_interval_sec},
        {".heavy_state_size_threshold", ckpt_config.heavy_state_size_threshold},
        {".heavy_state_interval", ckpt_config.heavy_state_interval},
        {".path", ckpt_config.path},
        {".interval", ckpt_config.interval_sec},
    };

    parseConfigSettings(config_settings, config, "query_state_checkpoint");

    return DB::CheckpointCoordinator::instance(std::move(ckpt_config));
}
}
