#pragma once

#include <Cluster/Base/ByteVector.h>
#include <Cluster/Common/AckSemantic.h>
#include <Cluster/Common/StreamShard.h>
#include <IO/WriteBuffer.h>
#include <Common/logger_useful.h>

#include <memory>

namespace DB
{
/// The buffer is used to write to the log store, it will be flushed to the log store when:
/// 1) finalize() is called, it will take the current buffer as a segment (with 64bits prefix flag) and flush it to the log store
/// 2) If the buffer overflows (>= \buffer_cap), it will take the current buffer as a segment (with 64bits prefix flag) and flush it to the log store and reset the buffer
class LogStoreWriteBuffer final : public WriteBuffer
{
public:
    using PrefixFlagGetter = std::function<uint64_t(uint32_t /*seq_num*/, bool /*is_last_segment*/)>;
    static constexpr size_t PREFIX_FLAG_SIZE = sizeof(uint64_t);

    LogStoreWriteBuffer(
        const cluster::StreamShard & log_stream_shard_,
        size_t buffer_cap_,
        cluster::AckSemantic ack_semantic_ = cluster::AckSemantic::AfterProposal,
        size_t timeout_ms_ = 2000,
        LoggerPtr logger_ = nullptr)
        : WriteBuffer(nullptr, 0)
        , log_stream_shard(log_stream_shard_)
        , buffer_cap(buffer_cap_)
        , ack_semantic(ack_semantic_)
        , timeout_ms(timeout_ms_)
        , logger(logger_)
    {
        resetBuffer();
    }

    /// No need to call finalize() in dtor, since it makes no sense to flush to nativelog when an exception occurs
    // ~LogStoreWriteBuffer() override { finalize(); }

    void setPrefixFlagGetter(PrefixFlagGetter flag_getter_) { flag_getter.swap(flag_getter_); }

#ifdef ENABLE_UT_FLUSH
    using FlushFunc = std::function<int(cluster::ByteVector &&)>;

    void setFlushFunc(FlushFunc flush_func_) { flush_func.swap(flush_func_); }

private:
    FlushFunc flush_func{};
#endif

private:
    /// So far, we only support flush to nativelog
    void flush(bool last_flush);
    void resetBuffer();

    void nextImpl() override;
    void finalizeImpl() override;

    cluster::StreamShard log_stream_shard;
    size_t buffer_cap;
    [[maybe_unused]] cluster::AckSemantic ack_semantic;
    [[maybe_unused]] size_t timeout_ms;
    LoggerPtr logger;

    PrefixFlagGetter flag_getter{};
    cluster::ByteVector buffer;
    uint32_t internal_segments_num = 0;
};

using LogStoreWriteBufferPtr = std::unique_ptr<LogStoreWriteBuffer>;
}
