#pragma once

#include <Cluster/Common/Error.h>
#include <Cluster/Protocol/ProtocolData.h>

namespace cluster::protocol
{
struct TruncateStreamResponseData final : public ProtocolData
{
public:
    /// Used for deserialization
    TruncateStreamResponseData() = default;

    TruncateStreamResponseData(int32_t error_code, std::string && error_message) : err(error_code, std::move(error_message)) { }

    protocol::OpCode opCode() const noexcept override { return protocol::OpCode::TruncateStream; }

    std::pair<uint16_t, uint16_t> supportedVersionsRange() const noexcept override { return {1, 1}; }

    void serialize(DB::WriteBuffer & wb, uint16_t version) const override;

    size_t approximateSerializedSize() const noexcept override { return err.approximateSerializedSize(); }

    std::string doString() const override { return err.string(); }

    bool hasError() const noexcept { return err.hasError(); }
    const Error & error() const noexcept { return err; }
    Error & error() noexcept { return err; }
    std::string errorString() const { return err.string(); }

private:
    void doDeserialize(DB::ReadBuffer & rb, uint16_t version) override;

public:
    Error err;
};

using TruncateStreamResponseDataPtr = std::shared_ptr<TruncateStreamResponseData>;
}
