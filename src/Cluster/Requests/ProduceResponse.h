#pragma once

#include <Cluster/Protocol/ProduceResponseData.h>
#include <Cluster/Requests/Response.h>

namespace cluster
{
struct ProduceResponse final : public Response
{
public:
    using Response::Response;

    ProduceResponse(const StreamIDShard & stream_shard, int64_t sn, int64_t throttle_time_ms, NodeID replica_leader, uint16_t data_version_)
        : Response(data_version_), response_data(stream_shard, sn, throttle_time_ms, replica_leader)
    {
    }

    ProduceResponse(int32_t error_code, std::string && error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::move(error_message))
    {
    }

    ProduceResponse(Error && err_, uint16_t data_version_) : Response(data_version_), response_data(std::move(err_)) { }

    protocol::ProduceResponseData & data() override { return response_data; }

    const protocol::ProduceResponseData & data() const override { return response_data; }

    bool hasError() const noexcept override { return response_data.hasError(); }
    const Error & error() const noexcept override { return response_data.error(); }
    Error & error() noexcept override { return response_data.error(); }

private:
    protocol::ProduceResponseData response_data;
};

using ProduceResponsePtr = std::shared_ptr<ProduceResponse>;
}
