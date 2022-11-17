#pragma once

#include <base/types.h>
#include <Common/HashTable/Hash.h>

#include <absl/container/flat_hash_map.h>
#include <fmt/format.h>

namespace DB
{

class ReadBuffer;
class WriteBuffer;

namespace Streaming
{

using SubstreamID = UInt128;
const SubstreamID INVALID_SUBSTREAM_ID{};

struct WatermarkBound
{
    UInt128 id{INVALID_SUBSTREAM_ID};
    /// watermark = 0 => no watermark setup
    /// watermark = -1 => force flush
    /// watermark > 0 => timestamp watermark
    Int64 watermark = 0;
    Int64 watermark_lower_bound = 0;

    bool valid() const { return watermark != 0; }
    operator bool() const { return valid(); }
    bool operator==(const WatermarkBound & rhs) const = default;
};

using WatermarkBound = Streaming::WatermarkBound;
using WatermarkBounds = std::vector<WatermarkBound>;

template <typename T>
using SubstreamHashMap = absl::flat_hash_map<Streaming::SubstreamID, T, UInt128TrivialHash>;

void serialize(const SubstreamID & id, WriteBuffer & wb);
SubstreamID deserialize(ReadBuffer & rb);
}
}

template <>
struct fmt::formatter<DB::Streaming::SubstreamID>
{
    constexpr auto parse(format_parse_context & ctx)
    {
        auto it = ctx.begin();
        auto end = ctx.end();

        /// Only support {}.
        if (it != end && *it != '}')
            throw format_error("Invalid format");

        return it;
    }

    template <typename FormatContext>
    auto format(const DB::Streaming::SubstreamID & x, FormatContext & ctx)
    {
        return format_to(ctx.out(), "{{{}-{}}}", x.items[0], x.items[1]);
    }
};
