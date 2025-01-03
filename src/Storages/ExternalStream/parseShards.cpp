#include <Storages/ExternalStream/parseShards.h>

#include <Common/Exception.h>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>

namespace DB
{

namespace ErrorCodes
{
extern const int INVALID_SETTING_VALUE;
}

std::vector<int32_t> parseShards(const std::string & shards_setting)
{
    std::vector<String> shard_strings;
    boost::split(shard_strings, shards_setting, boost::is_any_of(","));

    std::vector<int32_t> result;
    result.reserve(shard_strings.size());
    for (const auto & shard_string : shard_strings)
    {
        try
        {
            result.push_back(std::stoi(shard_string));
        }
        catch (std::invalid_argument &)
        {
            throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Invalid shard : {}", shard_string);
        }
        catch (std::out_of_range &)
        {
            throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Shard {} is too big", shard_string);
        }

        if (result.back() < 0)
            throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Invalid shard: {}", shard_string);
    }

    return result;
}

}
