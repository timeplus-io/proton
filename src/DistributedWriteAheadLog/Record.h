#pragma once

#include "ByteVector.h"
#include "OpCodes.h"

#include <Core/Block.h>

#include <string>
#include <vector>
#include <unordered_map>

namespace DWAL
{

using RecordSN = int64_t;

struct Record
{
    inline const static std::string IDEMPOTENT_KEY = "_idem";
    constexpr static uint8_t VERSION = 1;

    /// Fields on the wire
    OpCode op_code = OpCode::UNKNOWN;

    std::unordered_map<std::string, std::string> headers;

    DB::Block block;

    /// Fields which are not on the wire

    String topic;

    uint64_t partition_key = 0;

    RecordSN sn = -1;

    bool empty() const { return block.rows() == 0; }

    bool hasIdempotentKey() const { return headers.contains(IDEMPOTENT_KEY) && !headers.at(IDEMPOTENT_KEY).empty(); }
    std::string & idempotentKey() { return headers.at(IDEMPOTENT_KEY); }
    void setIdempotentKey(const std::string & key) { headers[IDEMPOTENT_KEY] = key; }

    static uint8_t ALWAYS_INLINE version(uint64_t flags) { return flags & 0x1F; }

    static OpCode ALWAYS_INLINE opcode(uint64_t flags)
    {
        auto opcode = (flags >> 5ul) & 0x3F;
        if (likely(opcode < static_cast<uint64_t>(OpCode::MAX_OPS_CODE)))
        {
            return static_cast<OpCode>(opcode);
        }
        return OpCode::UNKNOWN;
    }

    static uint8_t ALWAYS_INLINE compression(uint64_t flags) { return (flags >> 11ul) & 0x01; }

    static ByteVector write(const Record & record, bool compressed = false);
    static std::shared_ptr<Record> read(const char * data, size_t size);

    Record(OpCode op_code_, DB::Block && block_) : op_code(op_code_), block(std::move(block_)) { }
    explicit Record(RecordSN sn_) : sn(sn_) { }
};

using Records = std::vector<Record>;
using RecordPtr = std::shared_ptr<Record>;
using RecordPtrs = std::vector<RecordPtr>;
}
