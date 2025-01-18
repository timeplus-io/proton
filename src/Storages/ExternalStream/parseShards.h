#pragma once

#include <vector>
#include <string>

namespace DB
{

std::vector<int32_t> parseShards(const std::string & shards_setting);

}
