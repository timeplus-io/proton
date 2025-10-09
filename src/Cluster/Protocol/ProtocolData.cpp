#include <Cluster/Common/serde.h>
#include <Cluster/Protocol/ProtocolData.h>

#include <magic_enum.hpp>

namespace cluster::protocol
{
std::string WireData::serialize(uint16_t version) const
{
    return cluster::serialize<std::string>(*this, version);
}

void WireData::deserialize(std::string_view data, uint16_t version)
{
    return cluster::deserialize(*this, data, version);
}

void WireData::deserialize(DB::ReadBuffer & rb, uint16_t version)
{
    assert(!initialized);

    doDeserialize(rb, version);

    initialized = true;
}

std::string ProtocolData::string() const
{
    return fmt::format("opcode={} {}", opCodeString(), doString());
}

std::string ProtocolData::opCodeString() const
{
    return fmt::format("{}", magic_enum::enum_name(opCode()));
}
}
