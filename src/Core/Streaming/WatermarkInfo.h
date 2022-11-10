#pragma once

#include <Common/Streaming/Substream/Common.h>

#include <base/types.h>

#include <absl/container/flat_hash_map.h>

namespace DB
{
namespace Streaming
{
const Substream::ID INVALID_SUBSTREAM_ID{};

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
using SubstreamHashMap = absl::flat_hash_map<SubstreamID, T>;
}
