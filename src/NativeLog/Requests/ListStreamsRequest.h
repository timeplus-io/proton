#pragma once

#include "CommonRequest.h"

#include <string>

namespace nlog
{
/// List a stream by name or all streams if stream name is empty
struct ListStreamsRequest final : public CommonRequest
{
public:
    explicit ListStreamsRequest(std::string stream_, int32_t api_version_ = 0) : CommonRequest(api_version_), stream(std::move(stream_)) { }

    std::string stream;
};
}
