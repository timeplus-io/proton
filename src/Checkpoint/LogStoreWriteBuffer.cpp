#include <Checkpoint/LogStoreWriteBuffer.h>

#include <Bootstrap/Globals.h>
#include <Cluster/NativeLog/NativeLog.h>
#include <IO/WriteHelpers.h>
#include <Common/logger_useful.h>

namespace DB
{

void LogStoreWriteBuffer::nextImpl()
{
    auto unflushed_size = offset();
    assert(buffer.size() + unflushed_size <= buffer.capacity());
    buffer.resize(buffer.size() + unflushed_size);

    /// If current buffer is not full, reset \working_buffer to the rest of current buffer
    /// Otherwise, flush the full buffer to nativelog
    if (buffer.size() != buffer.capacity())
        set(reinterpret_cast<Position>(buffer.data() + buffer.size()), buffer.capacity() - buffer.size());
    else
        flush(/*last_flush=*/false);

    assert(offset() == 0);
}

void LogStoreWriteBuffer::finalizeImpl()
{
    auto unflushed_size = offset();
    assert(buffer.size() + unflushed_size <= buffer.capacity());
    buffer.resize(buffer.size() + unflushed_size);
    bytes += unflushed_size;

    flush(/*last_flush=*/true);
}

void LogStoreWriteBuffer::flush(bool last_flush)
{
    assert(buffer.size() >= PREFIX_FLAG_SIZE);

    assert(flag_getter);
    auto prefix_flag = flag_getter(/*seq_num=*/internal_segments_num, /*is_last_segment=*/last_flush);
    buffer.prefixWithFlag(prefix_flag);

    if (logger)
        LOG_DEBUG(
            logger,
            "Flush {} bytes buffer data (prefix_flag=0x{:x}, seq_num={}) to nativelog",
            buffer.size(),
            prefix_flag,
            internal_segments_num);

#ifdef ENABLE_UT_FLUSH
    assert(flush_func);
    auto err = flush_func(std::move(buffer));
#else
    /// TODO: Handle checkpoint append differently
    /// For now, just return success as checkpoints go to local filesystem
    int err = 0; /// Use int for error code instead of Error type
#endif

    if (last_flush)
    {
        set(nullptr, 0); /// Clear working buffer
    }
    else
    {
        ++internal_segments_num;
        resetBuffer(); /// Reset buffer for next flush
    }

    if (err != ErrorCodes::OK)
        throw Exception(err, "Failed to flush buffer to nativelog({})", log_stream_shard.string());
}

void LogStoreWriteBuffer::resetBuffer()
{
    /// The buffer capacity should be larger than the header size
    assert(buffer_cap >= PREFIX_FLAG_SIZE);
    buffer = cluster::ByteVector(buffer_cap);
    buffer.resize(PREFIX_FLAG_SIZE); /// padding for the header
    /// Set working buffer to the rest of the buffer
    set(reinterpret_cast<Position>(buffer.data() + buffer.size()), buffer.capacity() - buffer.size());
}
}
