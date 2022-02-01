#pragma once

namespace nlog
{
struct CommonResponse
{
    int32_t error_code = 0;
    std::string error_message;

    bool hasError() const { return error_code != 0 || !error_message.empty(); }
};

}
