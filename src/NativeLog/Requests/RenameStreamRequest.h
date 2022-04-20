#pragma once

#include "CommonRequest.h"

namespace nlog
{
struct RenameStreamRequest final : public CommonRequest
{
public:
    explicit RenameStreamRequest(std::string existing_stream_, std::string new_stream_, int32_t api_version_ = 0)
        : CommonRequest(api_version_), stream(std::move(existing_stream_)), new_stream(std::move(new_stream_))
    {
    }

    std::string stream;
    std::string new_stream;
};
}
