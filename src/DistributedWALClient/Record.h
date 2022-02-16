#pragma once

#include "ByteVector.h"
#include "OpCodes.h"
#include "SchemaProvider.h"

#include <Compression/CompressionInfo.h>
#include <Core/Block.h>

#include <string>
#include <unordered_map>
#include <vector>

namespace DB
{
class ReadBufferFromMemory;
}

namespace DWAL
{
using RecordSN = int64_t;

struct Record
{
    /// We like to make the keys as short as possible
    /// since they are bound to each batch
    inline const static std::string IDEMPOTENT_KEY = "_id";
    inline const static std::string INGEST_TIME_KEY = "_it";

    enum class Version : uint8_t
    {
        NATIVE = 0x00,
        NATIVE_IN_SCHEMA = 0x01,

        MAX_VERSION,
    };

    /// Fields on the wire
    OpCode op_code = OpCode::MAX_OPS_CODE;

    std::unordered_map<std::string, std::string> headers;

    DB::Block block;
    /// Schema version of the block. NO_SCHEMA means the block is self-contained
    uint16_t schema_version = NO_SCHEMA;

    /// Fields which are not on the wire

    String topic;

    uint64_t partition_key = 0;

    RecordSN sn = -1;

    bool empty() const { return block.rows() == 0; }

    inline bool hasIdempotentKey() const { return headers.contains(IDEMPOTENT_KEY); }
    inline std::string & idempotentKey() { return headers.at(IDEMPOTENT_KEY); }
    inline void setIdempotentKey(const std::string & key) { headers[IDEMPOTENT_KEY] = key; }
    inline bool hasSchema() const { return schema_version != NO_SCHEMA; }
    inline uint16_t schema() const { return schema_version; }

    void setIngestTime(int64_t ingest_time) { headers[INGEST_TIME_KEY] = std::to_string(ingest_time); }

    /// flags bits distribution
    /// [0-7] : Version
    /// [8-23] : OpCode
    /// [24-31] : Compression codec, 0x0000 means no compression
    /// [32-63] : Reserved

    static uint64_t ALWAYS_INLINE flags(Version version, OpCode op_code, DB::CompressionMethodByte codec)
    {
        return static_cast<uint8_t>(version) | (static_cast<uint16_t>(op_code) << 8ull) | (static_cast<uint8_t>(codec) << 24ull);
    }

    static Version ALWAYS_INLINE version(uint64_t flags)
    {
        auto v = flags & 0xFF;
        assert(v < static_cast<uint64_t>(Version::MAX_VERSION));
        return static_cast<Version>(v);
    }

    static OpCode ALWAYS_INLINE opcode(uint64_t flags)
    {
        auto opcode = (flags >> 8ull) & 0xFFFF;
        assert(opcode < static_cast<uint64_t>(OpCode::MAX_OPS_CODE));
        return static_cast<OpCode>(opcode);
    }

    static ALWAYS_INLINE DB::CompressionMethodByte codec(uint64_t flags)
    {
        auto method = (flags >> 24ull) & 0xFF;
        assert(method < 0xFFull);
        return static_cast<DB::CompressionMethodByte>(method);
    }

    static ByteVector write(const Record & record, DB::CompressionMethodByte codec = DB::CompressionMethodByte::NONE);

    static std::shared_ptr<Record> read(const char * data, size_t size, const SchemaContext & schema_ctx);

    Record(OpCode op_code_, DB::Block && block_, uint16_t schema_version_)
        : op_code(op_code_), block(std::move(block_)), schema_version(schema_version_)
    {
    }
    explicit Record(RecordSN sn_) : sn(sn_) { }

private:
    static ByteVector writeInSchema(const Record & record, DB::CompressionMethodByte codec);
    static std::shared_ptr<Record> readInSchema(DB::ReadBufferFromMemory & rb, uint64_t flags, const SchemaContext & schema_ctx);
};

using Records = std::vector<Record>;
using RecordPtr = std::shared_ptr<Record>;
using RecordPtrs = std::vector<RecordPtr>;
}
