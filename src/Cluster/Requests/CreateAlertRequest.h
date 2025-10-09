#pragma once

#include <Cluster/Protocol/CreateAlertRequestData.h>
#include <Cluster/Protocol/ExistsOperation.h>
#include <Cluster/Requests/Request.h>

namespace cluster
{
/// Create a format schema
struct CreateAlertRequest final : public Request
{
public:
    using Request::Request;

    CreateAlertRequest(
        const DB::UUID & id,
        const std::string & ns,
        const std::string & name,
        const std::string & trigger_function,
        const std::string & select_query,
        const protocol::AlertDescriptor::PeriodicCount batch,
        const protocol::AlertDescriptor::PeriodicCount limit,
        protocol::ExistsOperation exists_op,
        const std::string & created_by,
        uint32_t data_version_,
        cluster::NodeID initiator_,
        int64_t timeout_ms_,
        uint16_t request_version_)
        : Request(request_version_)
        , request_data(
              protocol::AlertDescriptor{id, ns, name, trigger_function, select_query, batch, limit, data_version_},
              exists_op,
              created_by,
              initiator_,
              timeout_ms_)
    {
    }

    protocol::CreateAlertRequestData & data() override { return request_data; }

    [[nodiscard]] const protocol::CreateAlertRequestData & data() const override { return request_data; }

private:
    protocol::CreateAlertRequestData request_data;
};

using CreateAlertRequestPtr = std::shared_ptr<CreateAlertRequest>;
}
