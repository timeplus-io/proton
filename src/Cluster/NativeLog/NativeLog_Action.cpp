#include <Cluster/NativeLog/NativeLog.h>

#include <Cluster/LocalLog/Log/Log.h>
#include <Common/ErrorCodes.h>
#include <Common/logger_useful.h>

#include <fmt/format.h>


namespace DB::ErrorCodes
{
extern const int OK;
extern const int RESOURCE_NOT_FOUND;
extern const int LOGICAL_ERROR;
}

namespace cluster
{


/// Pause a specific stream shard
Error NativeLog::pauseStreamShard(const StreamIDShard & stream_shard)
{
    try
    {
        auto log = getLog(stream_shard);
        if (!log)
        {
            return Error{DB::ErrorCodes::RESOURCE_NOT_FOUND, fmt::format("Stream shard {} not found", stream_shard.string())};
        }

        log->pause();
        LOG_INFO(logger, "Successfully paused stream shard {}", stream_shard.string());
        return Error{};
    }
    catch (const DB::Exception & e)
    {
        LOG_ERROR(logger, "Failed to pause stream shard {}: {}", stream_shard.string(), e.message());
        return Error{e.code(), e.message()};
    }
    catch (...)
    {
        return Error{DB::ErrorCodes::LOGICAL_ERROR, fmt::format("Unknown error while pausing stream shard {}", stream_shard.string())};
    }
}

/// Resume a specific stream shard
Error NativeLog::resumeStreamShard(const StreamIDShard & stream_shard)
{
    try
    {
        auto log = getLog(stream_shard);
        if (!log)
        {
            return Error{DB::ErrorCodes::RESOURCE_NOT_FOUND, fmt::format("Stream shard {} not found", stream_shard.string())};
        }

        log->resume();
        LOG_INFO(logger, "Successfully resumed stream shard {}", stream_shard.string());
        return Error{};
    }
    catch (const DB::Exception & e)
    {
        LOG_ERROR(logger, "Failed to resume stream shard {}: {}", stream_shard.string(), e.message());
        return Error{e.code(), e.message()};
    }
    catch (...)
    {
        return Error{DB::ErrorCodes::LOGICAL_ERROR, fmt::format("Unknown error while resuming stream shard {}", stream_shard.string())};
    }
}

/// Recover a specific stream shard
Error NativeLog::recoverStreamShard(const StreamIDShard & stream_shard)
{
    try
    {
        auto log = getLog(stream_shard);
        if (!log)
        {
            return Error{DB::ErrorCodes::RESOURCE_NOT_FOUND, fmt::format("Stream shard {} not found", stream_shard.string())};
        }

        log->recover();
        LOG_INFO(logger, "Successfully recovered stream shard {}", stream_shard.string());
        return Error{};
    }
    catch (const DB::Exception & e)
    {
        LOG_ERROR(logger, "Failed to recover stream shard {}: {}", stream_shard.string(), e.message());
        return Error{e.code(), e.message()};
    }
    catch (...)
    {
        return Error{DB::ErrorCodes::LOGICAL_ERROR, fmt::format("Unknown error while recovering stream shard {}", stream_shard.string())};
    }
}

}
