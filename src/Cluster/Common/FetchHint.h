#pragma once

#include <Cluster/Common/Constants.h>
#include <Cluster/Common/SerdeTag.h>

#include <string>

namespace DB
{
class WriteBuffer;
class ReadBuffer;
}

namespace cluster
{
/// Server interpreters FetchHint in this way:
/// If std::optional<FetchHint> is not nullopt, then all of the three fields are expected to be populated.
/// and server will do more sanity test.
/// Client interpreters FetchHint in this way:
/// Client can receive sn only since segment base sn or file position is not available
SERDE struct FetchHint
{
    void serialize(DB::WriteBuffer & wb, uint16_t version) const;

    void deserialize(DB::ReadBuffer & rb, uint16_t version);

    size_t approximateSerializedSize() const noexcept { return sizeof(FetchHint); }

    std::string string() const;

    /// Empty just means no hint in another way
    bool empty() const noexcept { return sn == 0 && segment_base_sn == 0 && file_position == std::numeric_limits<uint64_t>::max(); }

    bool hasFilePosition() const noexcept { return file_position != std::numeric_limits<uint64_t>::max(); }

    bool operator==(const FetchHint & other) const noexcept
    {
        return sn == other.sn && segment_base_sn == other.segment_base_sn && file_position == other.file_position;
    }

    int64_t sn = 0;
    int64_t segment_base_sn = 0;
    uint64_t file_position = std::numeric_limits<uint64_t>::max();
};
}
