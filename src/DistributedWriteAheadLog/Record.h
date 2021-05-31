#pragma once

#include "ByteVector.h"
#include "OpCodes.h"

#include <Core/Block.h>

#include <string>
#include <vector>
#include <unordered_map>

namespace DB
{
namespace DWAL
{

using RecordSequenceNumber = int64_t;

struct Record
{
    inline const static std::string IDEMPOTENT_KEY = "_idem";
    constexpr static uint8_t VERSION = 1;

    /// fields on the wire
    OpCode op_code = OpCode::UNKNOWN;

    std::unordered_map<std::string, std::string> headers;

    Block block;

    /// fields which are not on the wire
    uint64_t partition_key = 0;

    RecordSequenceNumber sn = -1;

    bool empty() const { return block.rows() == 0; }

    bool hasIdempotentKey() const { return headers.contains(IDEMPOTENT_KEY) && !headers.at(IDEMPOTENT_KEY).empty(); }
    std::string & idempotentKey() { return headers.at(IDEMPOTENT_KEY); }
    void setIdempotentKey(const std::string & key) { headers[IDEMPOTENT_KEY] = key; }

    static uint8_t version(uint64_t flags) { return flags & 0x1F; }

    static OpCode opcode(uint64_t flags)
    {
        auto opcode = (flags >> 5ul) & 0x3F;
        if (likely(opcode < static_cast<uint64_t>(OpCode::MAX_OPS_CODE)))
        {
            return static_cast<OpCode>(opcode);
        }
        return OpCode::UNKNOWN;
    }

    static ByteVector write(const Record & record);
    static std::shared_ptr<Record> read(const char * data, size_t size);

    Record(OpCode op_code_, Block && block_) : op_code(op_code_), block(std::move(block_)) { }
    explicit Record(RecordSequenceNumber sn_) : sn(sn_) { }
};

using Records = std::vector<Record>;
using RecordPtr = std::shared_ptr<Record>;
using RecordPtrs = std::vector<RecordPtr>;
}
}
