#include <benchmark/benchmark.h>

#include <Common/Exception.h>

namespace
{
std::string doStackUnwind()
{
    try
    {
        throw DB::Exception(1, "testing");
    }
    catch (...)
    {
        return DB::getCurrentExceptionMessage(
            /*with_stacktrace=*/true, /*check_embedded_stacktrace=*/false, /*with_extra_info=*/true);
    }
}

std::string doNoStackUnwind()
{
    try
    {
        throw DB::Exception(1, "testing");
    }
    catch (const DB::Exception & ex)
    {
        return ex.message();
    }
}

void stackUnwind(benchmark::State & state)
{
    for (auto _ : state)
        benchmark::DoNotOptimize(doStackUnwind());
}

void nostackUnwind(benchmark::State & state)
{
    for (auto _ : state)
        benchmark::DoNotOptimize(doNoStackUnwind());
}
}

BENCHMARK(stackUnwind);
BENCHMARK(nostackUnwind);
