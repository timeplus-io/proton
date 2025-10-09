#pragma once

#include <Cluster/Common/Nulls.h>
#include <Cluster/Common/SerdeTag.h>

#include <string>

namespace DB
{
class ReadBuffer;
class WriteBuffer;
}

namespace cluster
{
/// applied sequence tracking - simplified version without epoch
SERDE struct AppliedSequence
{
    AppliedSequence() = default;
    explicit AppliedSequence(int64_t sn_) : sn(sn_) { }

    void serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const;
    void deserialize(DB::ReadBuffer & rb, uint16_t /*version*/);

    size_t approximateSerializedSize() const noexcept { return sizeof(sn); }
    
    bool operator==(const AppliedSequence & other) const noexcept { return sn == other.sn; }
    bool operator<(const AppliedSequence & other) const noexcept { return sn < other.sn; }
    
    std::string string() const;

    int64_t sn = 0;
};

}
