#pragma once

#include "CheckpointFileWithFailureHandler.h"

#include <NativeLog/Common/LogDirFailureChannel.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int INVALID_DATA;
}
}

namespace nlog
{
struct LeaderEpochOffset
{
    int32_t epoch;
    int64_t offset;

    void write(DB::WriteBuffer & out) const
    {
        DB::writeText(epoch, out);
        DB::writeText(SEP, out);
        DB::writeText(offset, out);
        DB::writeText('\n', out);
    }

    static LeaderEpochOffset read(const std::string & data)
    {
        LeaderEpochOffset offset;

        /// Leader epoch
        std::string::size_type pos = 0;
        auto rpos = data.find(SEP, pos);
        if (rpos == std::string::npos)
            throw DB::Exception("Failed to parse leader epoch", DB::ErrorCodes::INVALID_DATA);

        offset.epoch = DB::parseIntStrict<int32_t>(data, pos, rpos);

        /// Offset
        offset.offset = DB::parseIntStrict<int64_t>(data, rpos + 1, data.size());

        return offset;
    }

private:
    static const char SEP = '@';
};

/// This class persists a map of (LeaderEpoch => Offsets) to a file for a certain replica
/// The format in the offset checkpoint file is like this:
/// ------checkpoint file begin------
/// 1              <- LeaderEpochCheckpointFile.CURRENT_VERSION
/// 2              <- following entries size
/// epoch1 start_offset   <- the format is: leader_epoch(int32_t), start_offset(int64_t)
/// epoch2 start_offset2
/// ------checkpoint file end------
class LeaderEpochCheckpointFile final
{
public:
    explicit LeaderEpochCheckpointFile(const fs::path & file_, LogDirFailureChannelPtr log_dir_failure_channel_)
        : checkpoint(file_, CURRENT_VERSION, log_dir_failure_channel_)
    {
    }

    void write(const std::vector<LeaderEpochOffset> & epochs) { checkpoint.write(epochs); }

    std::vector<LeaderEpochOffset> read() { return checkpoint.read(); }

private:
    static const int32_t CURRENT_VERSION = 1;

private:
    CheckpointFileWithFailureHandler<LeaderEpochOffset> checkpoint;
};

using LeaderEpochCheckpointFilePtr = std::shared_ptr<LeaderEpochCheckpointFile>;
}
