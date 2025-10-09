#pragma once

#include <Cluster/Common/Nulls.h>

namespace DB
{
class ReadBuffer;
class WriteBuffer;
}

namespace cluster::nlog
{
/// Sequence to file position
/// Normal use case : give a sequence number (sn), find the closest starting file position which contains the sn
struct SequencePosition
{
    SequencePosition() = default;
    SequencePosition(int64_t sn_, uint64_t file_position_) : sn(sn_), file_position(file_position_) { }

    void serialize(DB::WriteBuffer & wb, uint16_t version) const;
    void deserialize(DB::ReadBuffer & rb, uint16_t version);
    size_t approximateSerializedSize() const noexcept { return exactSerializedSize(); }
    static constexpr size_t exactSerializedSize() { return sizeof(sn) + sizeof(file_position); }
    std::string string() const;

    bool operator==(const SequencePosition & rhs) const noexcept { return sn == rhs.sn && file_position == rhs.file_position; }
    bool operator>(const SequencePosition & rhs) const noexcept
    {
        return std::tie(sn, file_position) > std::tie(rhs.sn, rhs.file_position);
    }
    bool operator<(const SequencePosition & rhs) const noexcept
    {
        return std::tie(sn, file_position) < std::tie(rhs.sn, rhs.file_position);
    }
    bool operator>=(const SequencePosition & rhs) const noexcept { return !(*this < rhs); }
    bool operator<=(const SequencePosition & rhs) const noexcept { return !(*this > rhs); }

    auto key() const noexcept { return sn; }
    auto value() const noexcept { return file_position; }
    static const char * keyName() noexcept { return "sn"; }
    static const char * valueName() noexcept { return "file_position"; }

    using KeyType = int64_t;
    using ValueType = uint64_t;

    int64_t sn = 0;
    uint64_t file_position = 0;
};

}
