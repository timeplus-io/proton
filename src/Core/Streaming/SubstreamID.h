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

template <typename T>
using SubstreamHashMap = absl::flat_hash_map<Streaming::SubstreamID, T, UInt128TrivialHash>;

void serialize(const SubstreamID & id, WriteBuffer & wb);
void deserialize(SubstreamID & id, ReadBuffer & rb);
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
