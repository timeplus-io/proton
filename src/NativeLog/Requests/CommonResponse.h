#pragma once

#include <fmt/format.h>

namespace DB::ErrorCodes
{
extern const int RESOURCE_NOT_FOUND;
}

namespace nlog
{
struct CommonResponse
{
public:
    explicit CommonResponse(int32_t api_version_) : api_version(api_version_) { }

    int32_t api_version;
    int32_t error_code = 0;
    std::string error_message;

    bool hasError() const { return error_code != 0; }
    bool hasUnretryableError() const { return error_code == DB::ErrorCodes::RESOURCE_NOT_FOUND; }

    std::string errString() const
    {
        return fmt::format("error_code={}, error_message={}", error_code, error_message);
    }

    virtual std::string string() const
    {
        return errString();
    }

    CommonResponse(const CommonResponse &) = default;
    virtual ~CommonResponse() = default;
};

}
