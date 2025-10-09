#pragma once

#include <Cluster/Common/NodeID.h>
#include <Cluster/Common/Nulls.h>
#include <Cluster/Common/utils.h>
#include <Cluster/Protocol/ProtocolData.h>

#include <utility>

namespace cluster::protocol
{
struct DeleteAlertRequestData final : public ProtocolData
{
public:
    /// Used for deserialization
    DeleteAlertRequestData() = default;

    DeleteAlertRequestData(
        std::string && ns_, std::string && name_, DB::UUID && id_, cluster::NodeID initiator_, int64_t timeout_ms_)
        : ns(std::move(ns_))
        , name(std::move(name_))
        , id(std::move(id_))
        , initiator(initiator_)
        , timeout_ms(timeout_ms_)
    {
    }

    DeleteAlertRequestData(
        const std::string & ns_,
        const std::string & name_,
        const DB::UUID & id_,
        cluster::NodeID initiator_,
        int64_t timeout_ms_)
        : ns(ns_), name(name_), id(id_), initiator(initiator_), timeout_ms(timeout_ms_)
    {
    }

    [[nodiscard]] protocol::OpCode opCode() const noexcept override { return protocol::OpCode::DeleteAlert; }

    [[nodiscard]] std::pair<uint16_t, uint16_t> supportedVersionsRange() const noexcept override { return {1, 1}; }

    void serialize(DB::WriteBuffer & wb, uint16_t version) const override;

    [[nodiscard]] size_t approximateSerializedSize() const noexcept override;

    [[nodiscard]] std::string doString() const override;

private:
    void doDeserialize(DB::ReadBuffer & rb, uint16_t version) override;

public:
    std::string ns;
    std::string name;
    DB::UUID id;

    cluster::NodeID initiator = Nulls::NullNodeID;
    int64_t timeout_ms = 0;
};

using DeleteAlertRequestDataPtr = std::shared_ptr<DeleteAlertRequestData>;
}
