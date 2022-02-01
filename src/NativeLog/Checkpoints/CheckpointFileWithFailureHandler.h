#pragma once

#include "CheckpointFile.h"

#include <NativeLog/Common/LogDirFailureChannel.h>

namespace nlog
{
template<typename T>
class CheckpointFileWithFailureHandler final
{
public:
    CheckpointFileWithFailureHandler(const fs::path & file_, int32_t version, LogDirFailureChannelPtr log_dir_failure_channel_)
        : checkpoint(file_, version), log_dir_failure_channel(std::move(log_dir_failure_channel_)), log_dir(file_.parent_path()) { }

    void write(const std::vector<T> & entries)
    {
        try
        {
            checkpoint.write(entries);
        }
        catch (...)
        {
            /// FIXME exception code check
            log_dir_failure_channel->maybeAddOfflineDir(log_dir);
            throw;
        }
    }

    std::vector<T> read()
    {
        try
        {
            return checkpoint.read();
        }
        catch (...)
        {
            /// FIXME, exception code check
            log_dir_failure_channel->maybeAddOfflineDir(log_dir);
            throw;
        }
    }

private:
    CheckpointFile<T> checkpoint;
    LogDirFailureChannelPtr log_dir_failure_channel;
    fs::path log_dir;
};
}
