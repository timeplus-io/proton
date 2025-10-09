#pragma once

#include <Cluster/Protocol/ProtocolData.h>

namespace cluster
{
/// Internal Request / Response base class
class RequestResponse
{
public:
    RequestResponse(uint16_t data_version_) : data_version(data_version_) { }

    virtual ~RequestResponse() = default;

    void serialize(DB::WriteBuffer & wb) const { return data().serialize(wb, data_version); }

    std::string serialize() const { return data().serialize(data_version); }

    void deserialize(DB::ReadBuffer & rb) { data().deserialize(rb, data_version); }

    void deserialize(std::string_view buffer) { data().deserialize(buffer, data_version); }

    protocol::OpCode opCode() const { return data().opCode(); }

    auto opCodeString() const { return data().opCodeString(); }

    uint16_t version() const noexcept { return data_version; }

    /// Request / Response protocol payload
    virtual protocol::ProtocolData & data() = 0;

    virtual const protocol::ProtocolData & data() const = 0;

    /// string representation for logging / debug purpose
    virtual std::string string() const { return data().string(); }

protected:
    /// The request / response data payload version
    uint16_t data_version;
};

using RequestResponsePtr = std::shared_ptr<RequestResponse>;
}
