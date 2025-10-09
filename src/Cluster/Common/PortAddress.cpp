#include <Cluster/Common/Nulls.h>
#include <Cluster/Common/PortAddress.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <fmt/format.h>

namespace cluster
{
void TCPPort::serialize(DB::WriteBuffer & wb, uint16_t /*version*/) const
{
    DB::writeIntBinary(port, wb);
    DB::writeIntBinary(static_cast<uint8_t>(is_tls), wb);
}

void TCPPort::deserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    DB::readIntBinary(port, rb);

    uint8_t is_tls_i;
    DB::readIntBinary(is_tls_i, rb);
    is_tls = is_tls_i;
}

std::string TCPPort::string() const
{
    return fmt::format("port={} is_tls={}", port, is_tls);
}

void HostPort::deserialize(DB::ReadBuffer & rb, uint16_t /*version*/)
{
    DB::readStringBinary(host, rb);
    port.deserialize(rb, Nulls::NullVersion);
}

void HostPort::serialize(DB::WriteBuffer & wb, uint16_t version) const
{
    DB::writeStringBinary(host, wb);
    port.serialize(wb, version);
}

std::string HostPort::string() const
{
    return fmt::format("host={} {}", host, port.string());
}

std::string HostPort::hostPort() const
{
    return fmt::format("{}:{}", host, port.port);
}
}
