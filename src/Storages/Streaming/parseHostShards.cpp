#include "parseHostShards.h"

#include <Common/Exception.h>
#include <Common/parseIntStrict.h>

#include <vector>
#include <assert.h>
namespace DB
{
namespace ErrorCodes
{
    extern const int INVALID_SHARD_ID;
}

/// host_shards='0,2'
std::vector<Int32> parseHostShards(const String & host_shards, Int32 shards)
{
    std::vector<Int32> results;
    results.reserve(shards);

    if (host_shards.empty())
    {
        for (Int32 shard = 0; shard < shards; ++shard)
            results.push_back(shard);

        return results;
    }

    String::size_type start_pos = 0;
    String::size_type end_pos = host_shards.size();
    if (host_shards.starts_with('\'') && host_shards.ends_with('\''))
    {
        start_pos = 1;
        end_pos -= 1;
    }

    if (start_pos == end_pos)
    {
        for (Int32 shard = 0; shard < shards; ++shard)
            results.push_back(shard);

        return results;
    }

    /// [left, right)
    auto trim_space = [](const String & s, String::size_type & left, String::size_type & right) {
        assert(left <= right && right <= s.size());

        for (; left < right && std::isspace(s[left]); ++left)
            ;

        /// If right is just beyond the end of s, we don't strip right
        if (right != s.size())
            for (; right > left + 1 && std::isspace(s[right]); --right)
                ;
    };

    auto do_parse_shard = [&](const String & s, String::size_type left, String::size_type right, Int32 total_shards) {
        trim_space(s, left, right);
        if (left != right)
        {
            auto shard = parseIntStrict<Int32>(s, left, right);
            if (shard < 0 || shard >= total_shards)
                throw Exception(ErrorCodes::INVALID_SHARD_ID, "Invalid shard ID in host_shards='{}' total_shards={}", s, total_shards);

            return shard;
        }
        else
            throw Exception(ErrorCodes::INVALID_SHARD_ID, "Invalid host_shards='{}'", s);
    };

    for (;;)
    {
        auto next_pos = host_shards.find(',', start_pos);
        if (next_pos == String::npos)
        {
            if (end_pos == host_shards.size() && std::isspace(host_shards.back()))
                next_pos = end_pos - 1;
            else if (std::isspace(host_shards[end_pos - 1]))
                next_pos = end_pos - 1;
            else
                next_pos = end_pos;

            results.push_back(do_parse_shard(host_shards, start_pos, next_pos, shards));
            break;
        }

        if (next_pos > start_pos)
        {
            /// `5 ,` for example
            auto npos = next_pos;
            if (std::isspace(host_shards[next_pos - 1]))
                next_pos -= 1;

            results.push_back(do_parse_shard(host_shards, start_pos, next_pos, shards));

            /// Move to next host shard ID
            start_pos = npos + 1;
        }
        else
            /// next_pos == start_pos, for example `0,,1`
            throw Exception(ErrorCodes::INVALID_SHARD_ID, "Invalid host_shards='{}'", host_shards);
    }

    if (results.empty())
        throw Exception(ErrorCodes::INVALID_SHARD_ID, "Invalid host_shards='{}'", host_shards);

    return results;
}
}
