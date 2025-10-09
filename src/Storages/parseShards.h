#pragma once

#include <string>
#include <vector>

namespace DB
{
std::vector<uint64_t> parseShards(const std::string & shards_setting);

std::vector<uint64_t> parseQueryShards(const std::string & shards_settings, uint64_t shard_count);

}
