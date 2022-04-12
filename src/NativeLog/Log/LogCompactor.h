#pragma once

#include "Log.h"
#include "LogCompactorConfig.h"

#include <NativeLog/Base/Concurrent/ConcurrentHashMap.h>
#include <NativeLog/Common/StreamShard.h>

#include <boost/noncopyable.hpp>

#include <memory>

namespace nlog
{
class LogCompactor final : public boost::noncopyable
{
public:
    LogCompactor(const LogCompactorConfig & config_, std::vector<fs::path> log_dirs_, ConcurrentHashMap<std::string, ConcurrentHashMapPtr<StreamShard, LogPtr>> & logs_);

    void startup();
    void shutdown();

    /// Abort the compacting of a particular shard if it's in progress, and pause any future
    /// compacting of this shard. This call blocks until the compacting of the shard is aborted
    /// and paused
    void abortAndPause(const StreamShard & stream_shard);

    /// Abort the compacting of a particular shard, if it's in progress
    /// This call blocks until the compacting of the shard is aborted.
    void abort(const StreamShard & stream_shard);

    /// Resume the compacting of paused shards
    void resume(const std::vector<StreamShard> & stream_shards);

    /// Truncate compactor sn checkpoint for the given shard if its checkpointed sn
    /// is larger than the given sn
    void maybeTruncateCheckpoint(const fs::path & log_dir, const StreamShard & stream_shard, int64_t sn);

    /// Update checkpoint file to remove shard if necessary
    void updateCheckpoints(const fs::path & log_dir, std::optional<StreamShard> shard_to_remove = {});

private:
    LogCompactorConfig config;
    std::vector<fs::path> log_dirs;

    ConcurrentHashMap<std::string, ConcurrentHashMapPtr<StreamShard, LogPtr>> & logs;
};

using LogCompactorPtr = std::shared_ptr<LogCompactor>;
}
