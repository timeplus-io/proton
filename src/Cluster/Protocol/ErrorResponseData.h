#pragma once

#include <Cluster/Common/Error.h>
#include <Cluster/Protocol/ProtocolData.h>

#include <vector>

namespace cluster::protocol
{
struct ErrorResponseData final : public ProtocolData
{
public:
    /// Used for deserialization
    ErrorResponseData() = default;

    ErrorResponseData(int32_t error_code, std::string && error_message) : err(error_code, std::move(error_message)) { }
    explicit ErrorResponseData(Error && err_) : err(std::move(err_)) { }

    protocol::OpCode opCode() const noexcept override { return protocol::OpCode::Error; }

    std::pair<uint16_t, uint16_t> supportedVersionsRange() const noexcept override { return {1, 1}; }

    /// Marshal the request / response to write buffer
    void serialize(DB::WriteBuffer & wb, uint16_t version) const override;

    size_t approximateSerializedSize() const noexcept override;

    std::string doString() const override;

    bool hasError() const noexcept { return err.hasError(); }
    const Error & error() const noexcept { return err; }
    Error & error() noexcept { return err; }
    std::string errorString() const { return err.string(); }
    bool operator==(const ErrorResponseData & other) const noexcept { return err == other.err; }

private:
    void doDeserialize(DB::ReadBuffer & rb, uint16_t version) override;

public:
    Error err;
};

using ErrorResponseDataPtr = std::shared_ptr<ErrorResponseData>;
}
