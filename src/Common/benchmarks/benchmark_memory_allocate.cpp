#include <Common/MemoryHelpers.h>

#include <benchmark/benchmark.h>

static void AlignedAlloc(benchmark::State & state)
{
    for (auto _ : state)
    {
        auto * p = aligned_alloc(8, 256);
        benchmark::DoNotOptimize(p);
        std::free(p);
    }
}
BENCHMARK(AlignedAlloc)->Iterations(100'000'000);

static void AllocateDataNoExcept(benchmark::State & state)
{
    for (auto _ : state)
    {
        auto p = DB::alignedAllocateNoExcept(256, 8);
        benchmark::DoNotOptimize(p);
    }
}
BENCHMARK(AllocateDataNoExcept)->Iterations(100'000'000);

static void AllocateData(benchmark::State & state)
{
    for (auto _ : state)
    {
        auto p = DB::alignedAllocate(256, 8);
        benchmark::DoNotOptimize(p);
    }
}
BENCHMARK(AllocateData)->Iterations(100'000'000);
