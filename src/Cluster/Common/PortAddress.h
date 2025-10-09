#pragma once

#include <Cluster/Common/SerdeTag.h>

#include <boost/functional/hash.hpp>

#include <functional>

namespace DB
{
class ReadBuffer;
class WriteBuffer;
}

namespace cluster
{

SERDE struct TCPPort
{
    TCPPort(uint16_t port_, bool is_tls_) : port(port_), is_tls(is_tls_) { }

    TCPPort() = default;

    void deserialize(DB::ReadBuffer & rb, uint16_t version);
    void serialize(DB::WriteBuffer & wb, uint16_t version) const;

    size_t approximateSerializedSize() const noexcept { return sizeof(TCPPort); }

    bool isValid() const noexcept { return port != 0; }
    std::string string() const;

    bool operator==(const TCPPort & other) const noexcept { return port == other.port && is_tls == other.is_tls; }

    uint16_t port = 0;
    bool is_tls = false;
};

SERDE struct HostPort
{
    HostPort(std::string host_, uint16_t port_, bool is_tls_ = false) : host(std::move(host_)), port(port_, is_tls_) { }

    HostPort(std::string host_, TCPPort port_) : host(std::move(host_)), port(port_) { }

    HostPort() = default;

    bool isTLS() const noexcept { return port.is_tls; }
    const std::string & getHost() const noexcept { return host; }
    uint16_t getPort() const noexcept { return port.port; }

    void deserialize(DB::ReadBuffer & rb, uint16_t version);
    void serialize(DB::WriteBuffer & wb, uint16_t version) const;

    size_t approximateSerializedSize() const noexcept { return sizeof(host.size()) + host.size() + port.approximateSerializedSize(); }

    bool operator==(const HostPort & other) const noexcept { return host == other.host && port == other.port; }

    bool isValid() const noexcept { return !host.empty() && port.isValid(); }

    std::string hostPort() const;

    std::string string() const;

    std::string host;
    TCPPort port;
};

using HostPortPtr = std::shared_ptr<HostPort>;
}

template <>
struct std::hash<cluster::HostPort>
{
    std::size_t operator()(const cluster::HostPort & host_port) const noexcept
    {
        size_t v = 0;
        boost::hash_combine(v, std::hash<std::string>{}(host_port.getHost()));
        boost::hash_combine(v, std::hash<uint16_t>{}(host_port.getPort()));
        return v;
    }
};
