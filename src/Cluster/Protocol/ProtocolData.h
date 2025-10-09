#pragma once

#include <Cluster/Protocol/InternalProtocol.h>

#include <Cluster/Common/SerdeTag.h>

#include <memory>

namespace DB
{
class WriteBuffer;
class ReadBuffer;
}

namespace cluster::protocol
{
class WireData;

using WireDataPtr = std::shared_ptr<WireData>;

/// Data will be sent over network / persistent on disk.
/// They are versioned.
SERDE class WireData
{
public:
    virtual ~WireData() = default;

    bool isInitialized() const noexcept { return initialized; }

    /// Marshal the request / response to string
    std::string serialize(uint16_t version) const;

    /// Unmarshal from read buffer to init the request / response object
    void deserialize(DB::ReadBuffer & rb, uint16_t version);

    /// Unmarshal the request / response object to binary string
    void deserialize(std::string_view data, uint16_t version);

    /// Marshal the request / response to write buffer
    virtual void serialize(DB::WriteBuffer & wb, uint16_t version) const = 0;

    virtual std::pair<uint16_t, uint16_t> supportedVersionsRange() const noexcept { return {0, 0}; }

    virtual size_t approximateSerializedSize() const noexcept = 0;

private:
    virtual void doDeserialize(DB::ReadBuffer & rb, uint16_t version) = 0;

    bool initialized = false;
};

/// ProtocolData is typed (containing a opcode, then we know how to
/// deserialize the data
class ProtocolData : public WireData
{
public:
    virtual protocol::OpCode opCode() const noexcept = 0;
    std::string opCodeString() const;

    /// string representation for logging / debug purpose
    std::string string() const;

private:
    virtual std::string doString() const { return ""; }
};

using ProtocolDataPtr = std::shared_ptr<ProtocolData>;
}
