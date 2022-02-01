#pragma once

#include <NativeLog/Base/Concurrent/BlockingQueue.h>
#include <NativeLog/Base/Concurrent/ConcurrentHashMap.h>
#include <NativeLog/Base/Stds.h>

#include <boost/noncopyable.hpp>

namespace nlog
{
class LogDirFailureChannel final : private boost::noncopyable
{
public:
    explicit LogDirFailureChannel(size_t log_dir_num) : offline_log_dir_queue(log_dir_num) { }

    void maybeAddOfflineDir(const fs::path & log_dir)
    {
        if (auto res = offline_log_dirs.emplace(log_dir, true); res.second)
            offline_log_dir_queue.add(log_dir);
    }

    /// Get the next offline dir.
    /// The method will wait if necessary until a new offline log directory becomes available
    fs::path takeNextOfflineLogDir() { return offline_log_dir_queue.take(); }

    bool hasOfflineLogDir(const fs::path & log_dir) const { return offline_log_dirs.contains(log_dir); }


private:
    ConcurrentHashMap<fs::path, bool> offline_log_dirs;
    BlockingQueue<fs::path> offline_log_dir_queue;
};

using LogDirFailureChannelPtr = std::shared_ptr<LogDirFailureChannel>;
}
