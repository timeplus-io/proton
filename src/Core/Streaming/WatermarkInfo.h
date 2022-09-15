#pragma once

#include <Common/Streaming/Substream/Common.h>

#include <unordered_map>
#include <base/types.h>

namespace DB
{
const Streaming::Substream::ID INVALID_SUBSTREAM_ID{};
namespace Streaming
{

struct WatermarkBound
{
    Substream::ID id{INVALID_SUBSTREAM_ID};
    /// watermark = 0 => no watermark setup
    /// watermark = -1 => force flush
    /// watermark > 0 => timestamp watermark
    Int64 watermark = 0;
    Int64 watermark_lower_bound = 0;

    bool valid() const { return watermark != 0; }
    operator bool() const { return valid(); }
    bool operator==(const WatermarkBound & rhs) const = default;
};
}

using SubstreamID = Streaming::Substream::ID;
using WatermarkBound = Streaming::WatermarkBound;
using WatermarkBounds = std::vector<WatermarkBound>;
template <typename T>
using SubstreamHashMap = std::unordered_map<SubstreamID, T>;
}
