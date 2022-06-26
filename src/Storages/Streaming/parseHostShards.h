#include <base/types.h>

namespace DB
{
/// host_shards='0,2'
std::vector<Int32> parseHostShards(const String & host_shards, Int32 shards);
}
