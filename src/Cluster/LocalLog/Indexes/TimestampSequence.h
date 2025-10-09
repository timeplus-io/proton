#pragma once

#include <Cluster/Common/Nulls.h>

namespace DB
{
class ReadBuffer;
class WriteBuffer;
}

namespace cluster::nlog
{
/// Timestamp to sequence number
/// Normal use case : give a timestamp, find the earliest entry which has less or
/// equal the given timestamp, return the sn of the entry
struct TimestampSequence
{
    TimestampSequence() : timestamp(0), sn(0) { }
    TimestampSequence(int64_t timestamp_, int64_t sn_) : timestamp(timestamp_), sn(sn_) { }

    void serialize(DB::WriteBuffer & wb, uint16_t version) const;
    void deserialize(DB::ReadBuffer & rb, uint16_t version);
    size_t approximateSerializedSize() const noexcept { return exactSerializedSize(); }
    static constexpr size_t exactSerializedSize() { return sizeof(timestamp) + sizeof(sn); }
    std::string string() const;

    bool operator==(const TimestampSequence & rhs) const noexcept { return timestamp == rhs.timestamp && sn == rhs.sn; }
    bool operator>(const TimestampSequence & rhs) const noexcept { return std::tie(timestamp, sn) > std::tie(rhs.timestamp, rhs.sn); }
    bool operator<(const TimestampSequence & rhs) const noexcept { return std::tie(timestamp, sn) < std::tie(rhs.timestamp, rhs.sn); }
    bool operator>=(const TimestampSequence & rhs) const noexcept { return !(*this < rhs); }
    bool operator<=(const TimestampSequence & rhs) const noexcept { return !(*this > rhs); }

    auto key() const noexcept { return timestamp; }
    auto value() const noexcept { return sn; }

    static const char * keyName() noexcept { return "timestamp"; }
    static const char * valueName() noexcept { return "sn"; }

    using KeyType = int64_t;
    using ValueType = int64_t;

    int64_t timestamp;
    int64_t sn;
};

}
