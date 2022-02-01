#include "LogCompactor.h"

namespace nlog
{
LogCompactor::LogCompactor(
    const LogCompactorConfig & config_, std::vector<fs::path> log_dirs_, ConcurrentHashMap<std::string, ConcurrentHashMapPtr<TopicShard, LogPtr>> & logs_)
    : config(config_), log_dirs(std::move(log_dirs_)), logs(logs_)
{
    (void)logs;
}

void LogCompactor::startup()
{
}

void LogCompactor::shutdown()
{
}

void LogCompactor::abortAndPause(const TopicShard & topic_shard)
{
    (void)topic_shard;
}

void LogCompactor::abort(const TopicShard & topic_shard)
{
    (void)topic_shard;
}

/// Resume the compacting of paused shards
void LogCompactor::resume(const std::vector<TopicShard> & topic_shards)
{
    (void)topic_shards;
}

/// Truncate compactor offset checkpoint for the given shard if its checkpointed offset
/// is larger than the given offset
void LogCompactor::maybeTruncateCheckpoint(const fs::path & log_dir, const TopicShard & topic_shard, int64_t offset)
{
    (void)log_dir;
    (void)topic_shard;
    (void)offset;
}

void LogCompactor::updateCheckpoints(const fs::path & log_dir, std::optional<TopicShard> shard_to_remove)
{
    (void)log_dir;
    (void)shard_to_remove;
}
}
