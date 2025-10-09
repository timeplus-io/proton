#pragma once

#include <Cluster/Protocol/ErrorResponseData.h>
#include <Cluster/Requests/Response.h>

namespace cluster
{
struct ErrorResponse final : public Response
{
public:
    using Response::Response;

    ErrorResponse(int32_t error_code, std::string && error_message, uint16_t data_version_)
        : Response(data_version_), response_data(error_code, std::move(error_message))
    {
    }

    explicit ErrorResponse(Error && err_, uint16_t data_version_) : Response(data_version_), response_data(std::move(err_)) { }

    protocol::ErrorResponseData & data() override { return response_data; }

    const protocol::ErrorResponseData & data() const override { return response_data; }

    bool hasError() const noexcept override { return response_data.hasError(); }
    const Error & error() const noexcept override { return response_data.error(); }
    Error & error() noexcept override { return response_data.error(); }
    bool operator==(const ErrorResponse & other) const noexcept { return response_data == other.response_data; }

private:
    protocol::ErrorResponseData response_data;
};

using ErrorResponsePtr = std::shared_ptr<ErrorResponse>;
}
