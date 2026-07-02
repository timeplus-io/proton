#include <Cluster/Common/StreamShard.h>
#include <Core/UUID.h>

#include <iostream>
#include <ranges>
#include <unordered_map>
#include <fmt/ranges.h>

int main(int /*argc*/, const char ** /*argv*/)
{
    for (auto pool_size : {4ull, 7ull, 8ull, 15ull, 16ull, 31ull, 32ull})
    {
        for (auto shard : {0ull, 1ull, 2ull, 3ull, 4ull, 5ull, 6ull, 7ull, 1001ull})
        {
            std::unordered_map<uint64_t, uint64_t> load_distributions;

            for (size_t i = 0; i < 1000; ++i)
            {
                auto uuid = DB::UUIDHelpers::generateV4();
                cluster::StreamIDShard stream_shard{uuid, static_cast<uint32_t>(shard)};
                auto key = std::hash<cluster::StreamIDShard>{}(stream_shard);
                auto target = key % pool_size;

                if (auto iter = load_distributions.find(target); iter != load_distributions.end())
                    ++iter->second;
                else
                    load_distributions.emplace(target, 1);
            }

            auto v = load_distributions | std::views::transform([](const auto & p) { return fmt::format("{}-{}", p.first, p.second); });
            std::cout << fmt::format("pool_size={} shard={} [{}]", pool_size, shard, fmt::join(v, ", ")) << std::endl;
        }
    }

    return 0;
}
