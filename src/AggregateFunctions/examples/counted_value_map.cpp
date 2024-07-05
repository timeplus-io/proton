#include "../Streaming/CountedValueMap.h"
#include "../Streaming/CountedValueMapStd.h"

#include <base/ClockUtils.h>

#include <random>

namespace
{
template<template <typename, bool> typename Map>
void perf(std::string_view map_name)
{
    for (uint64_t n : {100, 1000, 10'000, 100'000, 1000'000, 10'000'000})
    {
        Map<uint64_t, true> m(n);

        std::random_device rd;
        std::mt19937 gen(rd());

        std::uniform_int_distribution<> distrib(0, 2 * static_cast<int>(n));

        uint64_t loop = 10;
        uint64_t s = 0;

        auto start = DB::MonotonicNanoseconds::now();
        for (uint64_t j = 0; j < loop; ++j)
            for (uint64_t i = 0; i < n; ++i)
                s = distrib(gen);

        auto gen_duration = DB::MonotonicNanoseconds::now() - start;

        start = DB::MonotonicNanoseconds::now();
        for (uint32_t j = 0; j < loop; ++j)
            for (uint32_t i = 0; i < n; ++i)
                m.insert(distrib(gen));

        auto insert_duration = DB::MonotonicNanoseconds::now() - start - gen_duration;

        std::cout << "Use map=" << map_name << ", n=" << n << ", total insert=" << loop * n
                  << ", total cost=" << insert_duration << " ns, ops=" << (loop * n * 1000000) / (insert_duration / 1000)
                  << " insert/second, anti_opt=" << s << "\n";
    }
}


template<typename Key, bool Flag>
using CountedValueMap = DB::Streaming::CountedValueMap<Key, Flag, void>;
}

int main(int, char **)
{
    perf<CountedValueMap>("absl::btree_map");
    perf<DB::Streaming::CountedValueMapStd>("std::map");

    return 0;
}
