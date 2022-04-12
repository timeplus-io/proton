#pragma once

#include "CommonRequest.h"

#include <NativeLog/Common/Stream.h>

namespace nlog
{
/// Delete a stream
struct DeleteStreamRequest final : public CommonRequest
{
public:
    DeleteStreamRequest(std::string stream_, const StreamID & stream_id_, int32_t api_version_ = 0) : CommonRequest(api_version_), stream(std::move(stream_)), stream_id(stream_id_) { }

    std::string stream;
    StreamID stream_id;
};
}
