#pragma once

#include "OpCodes.h"
#include "SchemaProvider.h"

#include <Compression/CompressionInfo.h>
#include <Core/Block.h>
#include <NativeLog/Base/ByteVector.h>

#include <string>
#include <unordered_map>
#include <vector>

namespace DB
{
class ReadBuffer;
}

namespace nlog
{
struct Record;

/// Record sequence number
using RecordSN = int64_t;
using Records = std::vector<Record>;
using RecordPtr = std::shared_ptr<Record>;
using RecordPtrs = std::vector<RecordPtr>;

constexpr static int64_t EARLIEST_SN = -2;
constexpr static int64_t LATEST_SN = -1;

inline RecordSN toSN(const std::string & sn_s)
{
    return sn_s == "earliest" ? EARLIEST_SN : LATEST_SN;
}

struct Record
{
public:
    Record(OpCode op_code, DB::Block && block_, uint16_t schema_version_, std::vector<uint16_t> column_positions_ = {})
        : flags((static_cast<uint16_t>(op_code) << OP_CODE_OFFSET) | (MAGIC << MAGIC_OFFSET))
        , sn(-1)
        , schema_version(schema_version_)
        , column_positions(std::move(column_positions_))
        , block(std::move(block_))
    {
        setBlockFormat();
        setCodec(DB::CompressionMethodByte::NONE);
    }

    explicit Record(RecordSN sn_) : flags(MAGIC << MAGIC_OFFSET), sn(sn_), block_format(BlockFormat::MAX_FORMAT)
    {
        setCodec(DB::CompressionMethodByte::NONE);
    }

    RecordPtr clone(DB::Block data) const
    {
        auto r{std::make_shared<Record>(sn)};
        r->setBlock(data);
        r->stream = stream;
        r->key = key;
        r->headers = headers;
        return r;
    }

    ByteVector serialize() const;

    /// Re-serialize sn to byte vector
    void serializeSN(ByteVector & byte_vec) const;
    /// Re-calculate CRC and serialize it to byte vector
    void serializeCRC(ByteVector & byte_vec) const;

    /// Serialize sn, append time and crc in one go
    void deltaSerialize(ByteVector & byte_vec) const;

    static RecordPtr deserialize(const char * data, size_t size, const SchemaContext & schema_ctx);
    static size_t deserialize(const char * data, size_t size, RecordPtrs & records, const SchemaContext & schema_ctx);
    static RecordPtr deserializeCommonMetadata(const char * data, size_t size);

    bool hasIdempotentKey() const { return headers.contains(IDEMPOTENT_KEY); }

    std::string & idempotentKey() { return headers.at(IDEMPOTENT_KEY); }

    void setIdempotentKey(const std::string & key_) { headers.insert_or_assign(IDEMPOTENT_KEY, key_); }

    bool hasSchema() const { return schema_version != NO_SCHEMA; }

    uint16_t schema() const { return schema_version; }

    void setSchemaVersion(uint16_t schema_version_)
    {
        schema_version = schema_version_;
        setBlockFormat();
    }

    void setColumnPositions(std::vector<uint16_t> positions)
    {
        column_positions.swap(positions);
        setBlockFormat();
    }

    void setIngestTime(int64_t ingest_time) { headers[INGEST_TIME_KEY] = std::to_string(ingest_time); }

    bool empty() const { return block.rows() == 0; }

    void setStream(std::string stream_) { stream = std::move(stream_); }
    const std::string & getStream() const { return stream; }

    void setShard(int16_t shard_) { shard = shard_; }
    int16_t getShard() const { return shard; }

    void setKey(std::string key_) { key = std::move(key_); }
    const std::string & getKey() const { return key; }

    void setSN(RecordSN sn_) { sn = sn_; }
    RecordSN getSN() const { return sn; }

    bool addHeader(std::string key_, std::string value_)
    {
        auto [_, inserted] = headers.emplace(std::move(key_), std::move(value_));
        return inserted;
    }

    bool hasHeader(const std::string & key_) const { return headers.contains(key_); }
    bool hasHeader(const char * key_) const { return headers.contains(key_); }
    const std::string & getHeader(const std::string & key_) const { return headers.at(key_); }
    const std::string & getHeader(const char * key_) const { return headers.at(key_); }
    auto & getHeaders() { return headers; }
    const auto & getHeaders() const { return headers; }

    void setBlock(DB::Block block_)
    {
        /// FIXME
        auto append_time = block.info.append_time;
        block.swap(block_);
        block.info.append_time = append_time;
    }
    DB::Block & getBlock() { return block; }
    const DB::Block & getBlock() const { return block; }

    uint64_t getFlags() const { return flags; }
    const std::vector<uint16_t> & getColumnPositions() const { return column_positions; }

    int64_t getAppendTime() const { return block.info.append_time; }
    void setAppendTime(int64_t append_time) { block.info.append_time = append_time; }
    void setConsumeTime(int64_t consume_time) { block.info.consume_time = consume_time; }

    std::pair<int64_t, int64_t> minMaxEventTime() const;

    bool isValid() const
    {
        /// FIXME, validate CRC
        return true;
    }

private:
    inline void serializeData(DB::WriteBuffer & wb) const;
    inline void serializeMetadataV0(DB::WriteBuffer & wb) const;
    inline void serializeInSchemaV0(DB::CompressionMethodByte codec, DB::WriteBuffer & wb) const;
    inline static RecordPtr doDeserializeCommonMetadata(DB::ReadBuffer & rb);
    inline static void deserializeMetadataV0(Record & record, DB::ReadBuffer & rb);
    inline static void deserializeInSchemaDataV0(Record & record, DB::ReadBuffer & rb, const SchemaContext & schema_ctx);

    inline void setBlockFormat()
    {
        if (!hasSchema())
            block_format = BlockFormat::NATIVE;
        else if (column_positions.empty())
            block_format = BlockFormat::NATIVE_IN_SCHEMA;
        else
            block_format = BlockFormat::NATIVE_IN_SCHEMA_PARTIAL;
    }

public:
    /// We like to make the keys as short as possible
    /// since they are bound to each batch
    inline const static std::string IDEMPOTENT_KEY = "__tp_id";
    inline const static std::string INGEST_TIME_KEY = "__tp_it";

private:
    /// [0-7]
    constexpr static uint64_t VERSION_MASK = 0xFFull;

    /// [24-31]
    constexpr static uint64_t CODEC_MASK = 0xFF'00'00'00ull;

    /// [8-23]
    constexpr static uint64_t OP_CODE_MASK = 0xFFFF00ull;

    /// 63
    constexpr static uint64_t TOMBSTONE_MASK = 0x80'00'00'00'00'00'00'00ull;

    constexpr static uint64_t MAGIC_OFFSET = 55ull;
    constexpr static uint64_t OP_CODE_OFFSET = 8ull;
    constexpr static uint64_t CODEC_OFFSET = 24ull;

    constexpr static uint64_t MAGIC = 0x9Full;

public:
    enum class Version : uint8_t
    {
        V0 = 0x00,
        MAX_VERSION,
    };

    Version version() const
    {
        auto v = flags & VERSION_MASK;
        assert(v < static_cast<uint64_t>(Version::MAX_VERSION));
        return static_cast<Version>(v);
    }

    void setVersion(Version v)
    {
        flags &= ~VERSION_MASK;
        flags |= static_cast<uint8_t>(v);
    }

    DB::CompressionMethodByte codec() const
    {
        auto method = (flags & CODEC_MASK) >> CODEC_OFFSET;
        assert(method < 0xFFull);
        return static_cast<DB::CompressionMethodByte>(method);
    }

    void setCodec(DB::CompressionMethodByte codec)
    {
        flags &= ~CODEC_MASK;
        flags |= (static_cast<uint8_t>(codec) << CODEC_OFFSET);
    }

    OpCode opcode() const
    {
        auto opcode = (flags & OP_CODE_MASK) >> OP_CODE_OFFSET;
        assert(opcode < static_cast<uint64_t>(OpCode::MAX_OPS_CODE));
        return static_cast<OpCode>(opcode);
    }

    uint8_t magic() const { return (flags >> MAGIC_OFFSET) & 0xFF; }

    bool tomebstoned() const { return (flags & TOMBSTONE_MASK) == 0; }

    void setTomebstone() { flags |= TOMBSTONE_MASK; }

    /// Ballpark size shall be called after all data member have been inited and populated
    /// otherwise the result will be less accurate.
    uint32_t ballparkSize() const;

    uint32_t serializedBytes() const { return prefix_length; }

    uint32_t totalSerializedBytes() const { return prefix_length + sizeof(prefix_length); }

    static constexpr size_t prefixLengthSize() { return sizeof(uint32_t); }

    static constexpr size_t commonMetadataBytes()
    {
        /// prefix_length + crc + flags + sn +  append_time + schema_version + block_format
        /// return 4 + 4 + 8 + 8 + 2 + 1 + 8;
        return sizeof(Record::prefix_length) + sizeof(Record::crc) + sizeof(Record::flags) + sizeof(Record::sn)
            + sizeof(DB::BlockInfo::append_time) + sizeof(Record::schema_version) + sizeof(Record::block_format);
    }

private:
    enum class BlockFormat : uint8_t
    {
        NATIVE = 0x00, /// self-contained including column types
        NATIVE_IN_SCHEMA = 0x01, /// full columns without column types
        NATIVE_IN_SCHEMA_PARTIAL = 0x02, /// partial columns without colun types

        MAX_FORMAT,
    };

private:
    std::string stream;

    /// When producing, shard indicates the target shard for this record to be appended to
    /// When consuming, shard indicates the source shard of this record
    int16_t shard = -1;

    mutable uint32_t ballpark_size = 0;

    /// When serialization, prefix a length which indicates
    /// the size of the serialized record in bytes. Note it doesn't include
    /// the `prefix_length` itself
    mutable uint32_t prefix_length = 0;

    /// The following fields are serialized and persisted on filesystem
    uint32_t crc = 0;

    /// 64bit flags has the following layout
    /// 0-7bits: record version
    /// 8-23bits: OpCode
    /// 24-31bits: Compression codec, 0x0000 means no compression
    /// 55-62bits: magic integer 159 which is a sum of letter `t` (0x74) + letter `+` (0x2B)
    /// 63bit : tombstone. 1 means tombstoned
    uint64_t flags;

    RecordSN sn;

    uint16_t schema_version = NO_SCHEMA; /// Schema version of the block
    BlockFormat block_format; /// Format of the block

    std::string key; /// User provided key for this record. Used for record dedup. Optional
    std::unordered_map<std::string, std::string> headers;

    std::vector<uint16_t> column_positions; /// Column positions for block when NATIVE_IN_SCHEMA_PARTIAL
    DB::Block block;
};
}
