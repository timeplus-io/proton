#pragma once

#include "record.h"

#include <cassert>
#include <span>

namespace nlog::fbs
{
/// MemoryRecords is backed by a byte vector which is a serialization of Record
/// It provides a view / wrapper of Record.
/// NOTE: We can only update scalars fields of the underlying RecordBatch, otherwise
/// the length prefix will be broken since changing non-scalar fields can change total length.
/// We are assuming NativeLog clients always ingest data in RecordBatch and prefixed with a 32bit integer
/// which is the length of the whole batch.
/// [32bits-length-prefix][RecordBatch]
class MemoryRecords final
{
public:
    /// If this ctor is called, then MemoryRecord owns the underlying byte vector
    MemoryRecords(std::shared_ptr<uint8_t[]> reserved_buf_, size_t reserved_buf_size_, std::span<uint8_t> payload_)
        : reserved_buf(std::move(reserved_buf_)), reserved_buf_size(reserved_buf_size_), payload(std::move(payload_))
    {
        assert(payload.data());

        /// total length is the length of the payload + length of the prefix
        /// Here we are actually assuming flatbuffer put the length a the very beginning of the whole memory buffer which it does
        /// according to its code but not its memory layout specification. We are getting the prefix length
        /// in this hack way by assuming its memory layout which is not quite good but it's so far the only way to do this.
        length = flatbuffers::ReadScalar<flatbuffers::uoffset_t>(payload.data()) + sizeof(flatbuffers::uoffset_t);
        /// We also assume after the prefix, it is the payload (by reading flatbuffer implementation)
        batch = GetMutableRecordBatch(payload.data() + sizeof(flatbuffers::uoffset_t));

#ifndef NDEBUG
        flatbuffers::Verifier verifier(payload.data(), payload.size());
        bool success = VerifySizePrefixedRecordBatchBuffer(verifier);
        assert(success);
        (void)success;
#endif
        (void)reserved_buf_size;
        assert(isMagicValid());
    }

    /// If this ctor is called, then MemoryRecord doesn't own the underlying byte vector
    /// @param payload_ is a serialized RecordBatch byte vector
    explicit MemoryRecords(std::span<uint8_t> payload_) : MemoryRecords(nullptr, 0, std::move(payload_)) { }

    std::span<uint8_t> data() const { return std::span<uint8_t>(payload.data(), length); }

    void setBaseOffset(int64_t offset)
    {
        bool success = batch->mutate_base_offset(offset);
        assert(success);
        (void)success;
    }

    void setMaxTimestamp(int64_t max_timestamp)
    {
        bool success = batch->mutate_max_timestamp(max_timestamp);
        assert(success);
        (void)success;
    }

    void setShardLeaderEpoch(int32_t epoch)
    {
        bool success = batch->mutate_shard_leader_epoch(epoch);
        assert(success);
        (void)success;
    }

    void setAppendTimestamp(int64_t append_timestamp)
    {
        bool success = batch->mutate_append_timestamp(append_timestamp);
        assert(success);
        (void)success;
    }

    bool isValid() const
    {
        /// FIXME check CRC / recompute CRC ?
        return isMagicValid();
    }

    int64_t nextOffset() const { return lastOffset() + 1; }

    int64_t lastOffset() const { return batch->base_offset() + batch->last_offset_delta(); }

    int64_t baseOffset() const { return batch->base_offset(); }

    int32_t shardLeaderEpoch() const { return batch->shard_leader_epoch(); }

    /// Max event timestamp
    int64_t maxTimestamp() const { return batch->max_timestamp(); }

    int64_t appendTimestamp() const { return batch->append_timestamp(); }

    size_t sizeInBytes() const { return length; }

    /// Is the batch tombstoned
    bool removed() const { return batch->flags() & FLAGS_TOMBSTONE_MASK; }

    /// If this record is valid
    bool isMagicValid() const { return (batch->flags() & FLAGS_MAGIC_MASK) == FLAGS_MAGIC; }

    RecordVersion version() const { return toRecordVersion(batch->flags() & FLAGS_VERSION_MASK); }

    /// Number of records in this batch
    size_t size() const { return batch->records()->size(); }

    flatbuffers::Vector<flatbuffers::Offset<nlog::Record>> & mutableRecords() { return *batch->mutable_records(); }

    const flatbuffers::Vector<flatbuffers::Offset<nlog::Record>> & records() const { return *batch->records(); }

    nlog::RecordBatch * recordBatch() { return batch; }

public:
    static const uint64_t FLAGS_MAGIC = 0X4F80000000000000ull;

private:
    /// 64bit flags has the following layout
    /// [0-3bits][4-54bits][55-62bits][63bit    ]
    ///    |         |          |         |
    /// [version][reserved][magic=159][tombstone]
    /// The magic number is high 8 bits which is a sum of letter `t` (0x74) + letter `+` (0x2B)
    static const uint64_t FLAGS_VERSION_MASK = 0XF;
    static const uint64_t FLAGS_TOMBSTONE_MASK = 0X8000000000000000ull;
    static const uint64_t FLAGS_MAGIC_MASK = 0X7F80000000000000ull;

private:
    /// Total reserved buf: say 1024 bytes
    std::shared_ptr<uint8_t[]> reserved_buf;
    size_t reserved_buf_size;

    /// Payload is the valid bytes of stream object, say 58 bytes out of 1024 bytes
    /// Still there may be garbage at the end of a payload
    std::span<uint8_t> payload;

    /// Effective size of a RecordBatch. If payload contains garbage at its end, then the size of payload
    /// will be bigger the effective length
    uint32_t length;
    RecordBatch * batch;
};
}
