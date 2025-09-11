#pragma once

#include <Cluster/Protocol/AlterTaskRequestData.h>
#include <Cluster/Protocol/TaskDescriptor.h>
#include <Cluster/Requests/Request.h>


namespace cluster
{
struct AlterTaskRequest final : public Request
{
public:
    using Request::Request;

    /// Update task status
    AlterTaskRequest(
        std::string ns,
        std::string name,
        DB::UUID id,
        uint32_t data_version_,
        protocol::TaskStatus status,
        const std::string & created_by,
        NodeID initiator,
        int64_t timeout_ms,
        uint16_t request_version)
        : Request(request_version)
        , request_data(
              std::move(ns),
              std::move(name),
              std::move(id),
              data_version_,
              protocol::AlterTaskRequestData::AlterType::STATUS,
              status,
              created_by,
              initiator,
              timeout_ms)
    {
    }

    protocol::AlterTaskRequestData & data() override { return request_data; }

    [[nodiscard]] const protocol::AlterTaskRequestData & data() const override { return request_data; }

private:
    protocol::AlterTaskRequestData request_data;
};

using AlterTaskRequestPtr = std::shared_ptr<AlterTaskRequest>;
}
