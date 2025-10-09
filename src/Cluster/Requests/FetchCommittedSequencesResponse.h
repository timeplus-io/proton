#pragma once

#include <Cluster/Protocol/FetchCommittedSequencesResponseData.h>
#include <Cluster/Requests/Response.h>

namespace cluster
{

struct FetchCommittedSequencesResponse final : public Response
{
public:
    using Response::Response;

    FetchCommittedSequencesResponse(CommittedSequences && committed_sequences_, uint16_t data_version_)
        : Response(data_version_), response_data(std::move(committed_sequences_))
    {
    }

    FetchCommittedSequencesResponse(int32_t error_code, std::string && error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::move(error_message))
    {
    }

    protocol::FetchCommittedSequencesResponseData & data() override { return response_data; }

    const protocol::FetchCommittedSequencesResponseData & data() const override { return response_data; }

    bool hasError() const noexcept override { return response_data.hasError(); }
    const Error & error() const noexcept override { return response_data.error(); }
    Error & error() noexcept override { return response_data.error(); }

private:
    protocol::FetchCommittedSequencesResponseData response_data;
};

using FetchCommittedSequencesResponsePtr = std::shared_ptr<FetchCommittedSequencesResponse>;
}
