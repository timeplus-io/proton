#pragma once

#include "CommonRequest.h"

#include <Compression/CompressionInfo.h>
#include <NativeLog/Common/Stream.h>

#include <string>
#include <map>

namespace nlog
{
/// Update a stream
struct UpdateStreamRequest final : public CommonRequest
{
    UpdateStreamRequest(
        std::string stream_,
        const StreamID & id_,
        std::map<std::string, int32_t> && flush_settings_,
        std::map<std::string, int64_t> && retention_settings_,
        int16_t api_version_ = 0)
        : CommonRequest(api_version_)
        , stream(std::move(stream_))
        , id(id_)
        , flush_settings(flush_settings_)
        , retention_settings(retention_settings_)
    {
    }

public:
    std::string stream;
    StreamID id;
    std::map<std::string, int32_t> flush_settings;
    std::map<std::string, int64_t> retention_settings;
};
}
