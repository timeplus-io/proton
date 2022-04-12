#pragma once

#include "CheckpointFileWithFailureHandler.h"

#include <NativeLog/Common/LogDirFailureChannel.h>
#include <NativeLog/Common/StreamShard.h>

#include <filesystem>
#include <vector>

namespace DB
{
namespace ErrorCodes
{
    extern const int INVALID_DATA;
}
}

namespace nlog
{
struct StreamShardSequence
{
    StreamShardSequence() { }
    StreamShardSequence(const StreamShard & stream_shard_, int64_t offset_) : stream_shard(stream_shard_), offset(offset_) { }

    StreamShard stream_shard;
    int64_t offset;

    void write(DB::WriteBuffer & out) const
    {
        DB::writeText(stream_shard.stream.name, out);
        DB::writeText(SEP, out);
        DB::writeText(stream_shard.shard, out);
        DB::writeText(SEP, out);
        DB::writeText(offset, out);
        DB::writeText('\n', out);
    }

    static StreamShardSequence read(const std::string & data)
    {
        StreamShardSequence ts_offset;

        /// Stream
        std::string::size_type pos = 0;
        auto rpos = data.find(SEP, pos);
        if (rpos == std::string::npos)
            throw DB::Exception("Failed to parse stream", DB::ErrorCodes::INVALID_DATA);

        ts_offset.stream_shard.stream.name = std::string(data, pos, rpos - pos);

        /// Shard
        pos = rpos + 1;
        rpos = data.find(SEP, pos);
        if (rpos == std::string::npos)
            throw DB::Exception("Failed to parse shard", DB::ErrorCodes::INVALID_DATA);

        ts_offset.stream_shard.shard = DB::parseIntStrict<int32_t>(data, pos, rpos);

        /// Offset
        ts_offset.offset = DB::parseIntStrict<int64_t>(data, rpos + 1, data.size());

        return ts_offset;
    }

private:
    static const char SEP = '@';
};

/// This class persists a map of (Shard => Offsets) to a file for a certain replica
/// The format in the offset checkpoint file is like this:
/// ------checkpoint file begin------
/// 1              <- OffsetCheckpointFile.CURRENT_VERSION
/// 2              <- following entries size
/// tp1 shard1 1   <- the format is: STREAM SHARD OFFSET
/// tp2 shard2 2
/// ------checkpoint file end------
class OffsetCheckpointFile final
{
public:
    explicit OffsetCheckpointFile(const fs::path & file_, LogDirFailureChannelPtr log_dir_failure_channel_) : checkpoint(file_, CURRENT_VERSION, log_dir_failure_channel_) { }

    void write(const std::vector<StreamShardSequence> & offsets) { checkpoint.write(offsets); }

    std::vector<StreamShardSequence> read() { return checkpoint.read(); }

private:
    static const int32_t CURRENT_VERSION = 1;

private:
    CheckpointFileWithFailureHandler<StreamShardSequence> checkpoint;
};

using OffsetCheckpointFilePtr = std::shared_ptr<OffsetCheckpointFile>;
}
