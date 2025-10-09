#pragma once

#include <Cluster/Common/Nulls.h>

namespace DB
{
class ReadBuffer;
class WriteBuffer;
}

namespace cluster
{
/// Epoch is always 1, so we only track sequence numbers
struct EpochSequence
{
    EpochSequence() : epoch(1), sn(0) { }
    /// Epoch is always 1
    explicit EpochSequence(int64_t sn_) : epoch(1), sn(sn_) { }
    /// Compatibility constructor - epoch parameter ignored
    EpochSequence(uint64_t /*epoch_*/, int64_t sn_) : epoch(1), sn(sn_) { }

    void serialize(DB::WriteBuffer & wb, uint16_t version) const;
    void deserialize(DB::ReadBuffer & rb, uint16_t version);
    size_t approximateSerializedSize() const noexcept { return exactSerializedSize(); }
    static constexpr size_t exactSerializedSize() { return sizeof(epoch) + sizeof(sn); }
    std::string string() const;

    bool operator==(const EpochSequence & rhs) const noexcept { return epoch == rhs.epoch && sn == rhs.sn; }
    bool operator>(const EpochSequence & rhs) const noexcept { return epoch > rhs.epoch || (epoch == rhs.epoch && sn > rhs.sn); }
    bool operator<(const EpochSequence & rhs) const noexcept { return epoch < rhs.epoch || (epoch == rhs.epoch && sn < rhs.sn); }
    bool operator>=(const EpochSequence & rhs) const noexcept { return !(*this < rhs); }
    bool operator<=(const EpochSequence & rhs) const noexcept { return !(*this > rhs); }

    auto key() const noexcept { return epoch; }
    auto value() const noexcept { return sn; }
    static const char * keyName() noexcept { return "epoch"; }
    static const char * valueName() noexcept { return "sn"; }

    using KeyType = uint64_t;
    using ValueType = int64_t;

    uint64_t epoch;
    int64_t sn;
};

}
