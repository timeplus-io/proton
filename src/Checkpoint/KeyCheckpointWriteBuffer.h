#pragma once

#include <Checkpoint/Checkpoint.h>
#include <IO/WriteBufferFromFile.h>
#include <Common/Stopwatch.h>

namespace DB
{
/// It is used to splice and append multiple segments of the same key ckpt data based on seq_num.
/// Multiple segments number of the same key must be consecutive and start from 0.
class KeyCheckpointWriteBuffer
{
public:
    bool initialized() const;

    void init(std::string_view key_, VersionType version_, CheckpointType type_, std::unique_ptr<WriteBufferFromFileBase> file_buf, uint32_t next_seq_num_ = 0);

    /// \brief append partial ckpt data to key checkpoint file by idx
    /// \return success to append if the key ckpt write buffer is initialized and the seq_num is expected, otherwise return false
    bool append(uint32_t seq_num, std::string_view ckpt_data_v);

    void finalize();

    void reset();

    /// \brief dump the key ckpt write buffer (key, version, type, size, next_seq_num, elapsed_ms, etc.)
    String dump() const;

    String keyFileInfo() const;

    CheckpointType getCheckpointType() const { return type; }

    const String & getFileName() const { return file_name; }

    const String & getKey() const { return key; }

private:
    String key;
    VersionType version;
    CheckpointType type;
    std::unique_ptr<WriteBufferFromFileBase> file_buf;
    String file_name;
    uint32_t next_seq_num = 0;
    Stopwatch stopwatch{};
};
}
