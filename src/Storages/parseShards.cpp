#include <Storages/parseShards.h>

#include <Common/Exception.h>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>

#include <numeric>

namespace DB
{

namespace ErrorCodes
{
extern const int INVALID_SHARD_ID;
}

std::vector<uint64_t> parseShards(const std::string & shards_setting)
{
    std::vector<String> shard_strings;
    boost::split(shard_strings, shards_setting, boost::is_any_of(","));

    std::vector<uint64_t> result;
    result.reserve(shard_strings.size());
    for (const auto & shard_string : shard_strings)
    {
        try
        {
            result.push_back(std::stoull(shard_string));
        }
        catch (std::invalid_argument &)
        {
            throw Exception(ErrorCodes::INVALID_SHARD_ID, "Invalid shard : {}", shard_string);
        }
        catch (std::out_of_range &)
        {
            throw Exception(ErrorCodes::INVALID_SHARD_ID, "Shard {} is too big", shard_string);
        }

        if (result.back() > 1'000)
            throw Exception(ErrorCodes::INVALID_SHARD_ID, "Invalid shard: {}, too big shard ID", shard_string);
    }

    return result;
}

std::vector<uint64_t> parseQueryShards(const std::string & shards_setting, uint64_t total_shards)
{
    std::vector<uint64_t> shard_ids;
    if (!shards_setting.empty())
    {
        shard_ids = parseShards(shards_setting);
        /// Make sure they are valid.
        for (auto shard : shard_ids)
        {
            if (shard >= total_shards)
                throw Exception(
                    ErrorCodes::INVALID_SHARD_ID,
                    "Invalid shard_id={}. The stream has only {} shards and the max shard id is {}",
                    shard,
                    total_shards,
                    total_shards - 1);
        }
    }
    else
    {
        /// Query all available shards
        shard_ids.resize(total_shards);
        std::iota(shard_ids.begin(), shard_ids.end(), 0);
    }

    return shard_ids;
}

}
