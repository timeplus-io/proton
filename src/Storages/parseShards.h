#pragma once

#include <string>
#include <vector>

namespace DB
{
std::vector<int32_t> parseShards(const std::string & shards_setting);

std::vector<int32_t> getShardsToQuery(const std::string & shards_settings, int32_t shard_count);

}
