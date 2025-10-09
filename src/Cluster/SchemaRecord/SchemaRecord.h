#pragma once

#include <Cluster/Base/ByteVector.h>
#include <Cluster/Common/Stream.h>
#include <Cluster/Protocol/InternalProtocol.h>
#include <Cluster/Common/Entry.h>
#include <Compression/CompressionInfo.h>
#include <Core/Block.h>
#include <Cluster/Common/SerdeTag.h>

namespace DB
{
class ReadBuffer;
}

namespace cluster
{
struct SchemaRecord;
struct SchemaContext;

using SchemaRecordPtr = std::shared_ptr<SchemaRecord>;
using SchemaRecordPtrs = std::vector<SchemaRecordPtr>;

/// Record is a schema-ed presentation before serialization to network / file system or
/// It is a schema-ed presentation after deserialization from network / file system
/// cluster::Entry::data deserialize => Record
struct SchemaRecord
{
public:
    SchemaRecord(
        cluster::protocol::OpCode op_code, DB::Block && block_, uint16_t schema_version_, std::vector<uint16_t> column_positions_ = {})
        : flags(static_cast<uint64_t>(op_code) << OP_CODE_OFFSET)
        , schema_version(schema_version_)
        , column_positions(std::move(column_positions_))
        , block(std::move(block_))
    {
        setBlockFormat();
        setCompressionCodec(DB::CompressionMethodByte::NONE);
    }

    SchemaRecord(int64_t sn_, int64_t append_time) : sn(sn_)
    {
        setAppendTime(append_time);
        setCompressionCodec(DB::CompressionMethodByte::NONE);
    }

    explicit SchemaRecord(int64_t sn_) : sn(sn_) { }

    SchemaRecord() = default;

    SchemaRecordPtr clone(DB::Block && data) const
    {
        auto r{std::make_shared<SchemaRecord>(sn)};
        r->stream = stream;
        r->shard = shard;
        r->flags = flags;
        r->sn = sn;
        r->schema_version = schema_version;
        r->block_format = block_format;
        r->column_positions = column_positions;
        r->setBlock(std::move(data));

        return r;
    }

    ByteVector serialize() const;
    void deserialize(DB::ReadBuffer & rb, const SchemaContext & schema_ctx);

    static SchemaRecordPtr parse(DB::ReadBuffer & rb, const SchemaContext & schema_ctx);
    static SchemaRecordPtr parse(std::string_view data, const SchemaContext & schema_ctx);

    bool hasIdempotentKey() const noexcept { return !idempotent_key.empty(); }

    std::string & idempotentKey() noexcept { return idempotent_key; }

    const std::string & idempotentKey() const noexcept { return idempotent_key; }

    void setIdempotentKey(const std::string & key_) { idempotent_key = key_; }

    bool hasSchemaVersion() const noexcept { return schema_version != cluster::Nulls::NullVersion; }

    uint16_t schemaVersion() const noexcept { return schema_version; }

    void setSchemaVersion(uint16_t schema_version_) noexcept
    {
        schema_version = schema_version_;
        setBlockFormat();
    }

    void setColumnPositions(std::vector<uint16_t> positions) noexcept
    {
        column_positions.swap(positions);
        setBlockFormat();
    }

    bool empty() const noexcept { return block.rows() == 0; }

    void setStream(std::string stream_) noexcept { stream.swap(stream_); }
    const std::string & getStream() const noexcept { return stream; }

    void setShard(uint32_t shard_) noexcept { shard = shard_; }
    uint32_t getShard() const noexcept { return shard; }

    void setSN(int64_t sn_) noexcept { sn = sn_; }
    int64_t getSN() const noexcept { return sn; }

    void setBlock(DB::Block && block_) noexcept
    {
        auto append_time = block.info.appendTime();
        block.swap(block_);
        block.info.setAppendTime(append_time);
    }
    DB::Block & getBlock() noexcept { return block; }
    const DB::Block & getBlock() const noexcept { return block; }

    uint64_t getFlags() const noexcept { return flags; }
    const std::vector<uint16_t> & getColumnPositions() const noexcept { return column_positions; }

    int64_t getAppendTime() const noexcept { return block.info.hasAppendTime() ? block.info.appendTime() : 0; }
    void setAppendTime(int64_t append_time) noexcept { block.info.setAppendTime(append_time); }

    std::pair<int64_t, int64_t> minMaxEventTime() const;

private:
    inline void serializeData(DB::WriteBuffer & wb) const;
    inline void deserializeData(DB::ReadBuffer & rb, const SchemaContext & schema_ctx);

    inline void setBlockFormat()
    {
        if (!hasSchemaVersion())
            block_format = BlockFormat::Native;
        else if (column_positions.empty())
            block_format = BlockFormat::NativeInSchemaFull;
        else
            block_format = BlockFormat::NativeInSchemaPartial;
    }

public:
    constexpr static uint16_t DEFAULT_SECHEMA_VERSION = 1; /// 0-NullVersion

    /// Block in Record has schemas (maybe different per record / block). Used for read
    constexpr static uint16_t ANY_SCHEMA_VERSION = std::numeric_limits<uint16_t>::max();

private:
    /// [0-7]
    constexpr static uint64_t VERSION_MASK = 0xFFull;

    /// [8-23]
    constexpr static uint64_t OP_CODE_MASK = 0xFFFF00ull;

    /// [24-31]
    constexpr static uint64_t COMPRESSION_CODEC_MASK = 0xFF'00'00'00ull;

    constexpr static uint64_t OP_CODE_OFFSET = 8ull;
    constexpr static uint64_t COMPRESSION_CODEC_OFFSET = 24ull;

public:
    uint8_t version() const { return flags & VERSION_MASK; }

    void setVersion(uint8_t v)
    {
        flags &= ~VERSION_MASK;
        flags |= v;
    }

    DB::CompressionMethodByte compressionCodec() const
    {
        auto method = (flags & COMPRESSION_CODEC_MASK) >> COMPRESSION_CODEC_OFFSET;
        assert(method < 0xFFull);
        return static_cast<DB::CompressionMethodByte>(method);
    }

    void setCompressionCodec(DB::CompressionMethodByte codec)
    {
        flags &= ~COMPRESSION_CODEC_MASK;
        flags |= (static_cast<uint64_t>(codec) << COMPRESSION_CODEC_OFFSET);
    }

    cluster::protocol::OpCode opCode() const
    {
        auto opcode = (flags & OP_CODE_MASK) >> OP_CODE_OFFSET;
        return static_cast<cluster::protocol::OpCode>(opcode);
    }

    /// Ballpark size shall be called after all data member have been inited and populated
    /// otherwise the result will be less accurate.
    uint64_t approximateSerializedSize() const;

private:
    enum class BlockFormat : uint8_t
    {
        Native = 0x00, /// self-contained including column types
        NativeInSchemaFull = 0x01, /// full columns without column types
        NativeInSchemaPartial = 0x02, /// partial columns without column types
    };

private:
    /// For multiplexing routing
    std::string stream;

    /// When producing, shard indicates the target shard for this record to be appended to
    /// When consuming, shard indicates the source shard of this record
    uint32_t shard = cluster::Nulls::NullShardID;

    int64_t sn = -1;

    /// The following fields are serialized and persisted on filesystem
    /// 64bit flags has the following layout
    /// 0-7bits: record version
    /// 8-23bits: OpCode
    /// 24-31bits: Compression codec, 0x0000 means no compression
    SERDE uint64_t flags = 0;

    SERDE std::string idempotent_key; /// User provided key for this record. Used for record dedup. Optional

    /// Schema version of the block
    SERDE uint16_t schema_version = cluster::Nulls::NullVersion;

    /// Format of the block
    SERDE BlockFormat block_format = BlockFormat::Native;

    /// Column positions for block when NativeInSchemaPartial
    SERDE std::vector<uint16_t> column_positions;

    /// Append time is encoded in Block.info
    SERDE DB::Block block;
};
}
