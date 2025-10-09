#pragma once

#include <Cluster/Common/EpochSequence.h>

namespace cluster
{
struct CommittedSequences
{
    void serialize(DB::WriteBuffer & wb, uint16_t version) const;
    void deserialize(DB::ReadBuffer & rb, uint16_t version);
    size_t approximateSerializedSize() const noexcept;
    std::string string() const;

    std::vector<EpochSequence> epoch_sns;

    int64_t log_next_sn = 0;
};
}
