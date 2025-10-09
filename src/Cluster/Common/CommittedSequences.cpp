#include <Cluster/Common/CommittedSequences.h>
#include <Cluster/Common/serde.h>
#include <Cluster/Common/utils.h>

#include <IO/WriteHelpers.h>

#include <fmt/format.h>

namespace cluster
{
void CommittedSequences::serialize(DB::WriteBuffer & wb, uint16_t version) const
{
    cluster::serializeSerializables<false>(epoch_sns, wb, version);
    DB::writeVarInt(log_next_sn, wb);
}

void CommittedSequences::deserialize(DB::ReadBuffer & rb, uint16_t version)
{
    cluster::deserializeSerializables<false>(epoch_sns, rb, version);
    DB::readVarInt(log_next_sn, rb);
}

size_t CommittedSequences::approximateSerializedSize() const noexcept
{
    return cluster::totalApproximateSerializedSizeOf<false>(epoch_sns) + sizeof(log_next_sn);
}

std::string CommittedSequences::string() const
{
    return fmt::format("epoch_sns={}, log_next_sn={}", cluster::stringOfStringables<false>(epoch_sns), log_next_sn);
}
}
