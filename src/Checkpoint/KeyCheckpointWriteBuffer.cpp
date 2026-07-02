#include <Checkpoint/KeyCheckpointWriteBuffer.h>

#include <IO/WriteHelpers.h>
#include <Common/Exception.h>

namespace DB
{
bool KeyCheckpointWriteBuffer::initialized() const
{
    return file_buf != nullptr;
}

KeyCheckpointWriteBuffer::~KeyCheckpointWriteBuffer()
{
    try
    {
        reset();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void KeyCheckpointWriteBuffer::init(
    std::string_view key_,
    VersionType version_,
    CheckpointType type_,
    std::unique_ptr<WriteBufferFromFileBase> file_buf_,
    uint32_t next_seq_num_)
{
    chassert(!initialized());

    chassert(file_buf_);
    key = key_;
    version = version_;
    type = type_;
    file_buf = std::move(file_buf_);
    file_name = file_buf->getFileName();
    next_seq_num = next_seq_num_;
    stopwatch.restart();
}

bool KeyCheckpointWriteBuffer::append(uint32_t seq_num, std::string_view ckpt_data_v)
{
    if (!initialized() || seq_num != next_seq_num)
        return false;

    DB::writeString(ckpt_data_v, *file_buf);
    ++next_seq_num;
    return true;
}

void KeyCheckpointWriteBuffer::finalize()
{
    if (!initialized())
        return;

    stopwatch.stop();
    file_buf->finalize();
    file_buf.reset();
}

void KeyCheckpointWriteBuffer::reset()
{
    if (file_buf)
    {
        file_buf->cancel();
        file_buf.reset();
    }

    stopwatch.reset();
}

String KeyCheckpointWriteBuffer::dump() const
{
    if (!initialized())
        return "empty";

    return fmt::format(
        "key={}, version={}, type={}, size={}, next_seq_num={}, elapsed_ms={}",
        key,
        version,
        magic_enum::enum_name(type),
        file_buf->count(),
        next_seq_num,
        stopwatch.elapsedMilliseconds());
}

String KeyCheckpointWriteBuffer::keyFileInfo() const
{
    if (!initialized())
        return "empty";

    return fmt::format("file={}, current_file_size={}, elapsed_ms={}", file_name, file_buf->count(), stopwatch.elapsedMilliseconds());
}
}
