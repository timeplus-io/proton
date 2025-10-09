#pragma once

#include <Cluster/Protocol/DiskDescriptor.h>
#include <Cluster/Protocol/ListDisksResponseData.h>
#include <Cluster/Requests/Response.h>

#include <vector>

namespace cluster
{
struct ListDisksResponse final : public Response
{
public:
    using Response::Response;

    ListDisksResponse(protocol::DiskDescriptorPtrs && disks, cluster::NodeID leader_replica_, int64_t sn_, uint16_t data_version_)
        : Response(data_version_), response_data(std::move(disks), leader_replica_, sn_)
    {
    }

    ListDisksResponse(protocol::DiskDescriptorPtr && disk, cluster::NodeID leader_replica_, int64_t sn_, int16_t data_version_)
        : Response(data_version_), response_data(protocol::DiskDescriptorPtrs{std::move(disk)}, leader_replica_, sn_)
    {
    }

    ListDisksResponse(int32_t error_code, std::string && error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::move(error_message))
    {
    }

    ListDisksResponse(int32_t error_code, std::string_view error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::string(error_message))
    {
    }

    ListDisksResponse(Error && error, uint16_t data_version_) : Response(data_version_), response_data(std::move(error)) { }

    protocol::ListDisksResponseData & data() override { return response_data; }

    const protocol::ListDisksResponseData & data() const override { return response_data; }

    bool hasError() const noexcept override { return response_data.hasError(); }
    const Error & error() const noexcept override { return response_data.error(); }
    Error & error() noexcept override { return response_data.error(); }

private:
    protocol::ListDisksResponseData response_data;
};

using ListDisksResponsePtr = std::shared_ptr<ListDisksResponse>;
}
