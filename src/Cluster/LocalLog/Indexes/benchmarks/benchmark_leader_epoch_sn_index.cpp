#include <Cluster/LocalLog/Indexes/EpochSequenceIndex.h>
#include <Cluster/LocalLog/Indexes/LeaderEpochIndexRocks.h>

#include <Common/logger_useful.h>

#include <benchmark/benchmark.h>

namespace
{

class LogBasedFixture : public benchmark::Fixture
{
public:
    void SetUp(benchmark::State & state) override
    {
        uint64_t file_max_size = state.range(0); /// Argument idx 0
        {
            /// Clean up all logs files if there are
            cluster::nlog::EpochSequenceIndex remover(index_dir, file_max_size, true, getLogger("leader_epoch"));
            remover.remove();
        }
        index = std::make_unique<cluster::nlog::EpochSequenceIndex>(index_dir, file_max_size, true, getLogger("leader_epoch"));

        bool sync = state.range(1);
        bool is_query = state.range(2);
        if (is_query)
        {
            /// Populate index
            uint64_t i = 1;
            for (auto _ : state)
            {
                index->maybeIndexEpochStartSequence(cluster::EpochSequence(i, i), sync);
                ++i;
            }
        }
    }

    void TearDown(benchmark::State & /*state*/) override { index->remove(); }

protected:
    const char * index_dir = "./";
    std::unique_ptr<cluster::nlog::EpochSequenceIndex> index;
};

BENCHMARK_DEFINE_F(LogBasedFixture, Assign)(benchmark::State & state)
{
    bool sync = state.range(1); /// argument index 1

    for (uint64_t i = 1; auto _ : state)
    {
        index->maybeIndexEpochStartSequence(cluster::EpochSequence{i, static_cast<int64_t>(i)}, /*sync=*/sync);
        ++i;
    }
}

BENCHMARK_DEFINE_F(LogBasedFixture, Query)(benchmark::State & state)
{
    for (int64_t i = 1; auto _ : state)
    {
        index->leaderEpochSequenceFor(i);
        ++i;
    }
}

class RocksBasedFixture : public benchmark::Fixture
{
public:
    void SetUp(benchmark::State & state) override
    {
        std::error_code ec;
        std::filesystem::remove_all(index_dir, ec);
        index = std::make_unique<cluster::nlog::LeaderEpochIndexRocks>(index_dir, getLogger("leader_epoch"));

        bool is_query = state.range(0);
        if (is_query)
        {
            /// Populate index
            uint64_t i = 1;
            for (auto _ : state)
            {
                index->maybeIndexEpochStartSequence(i, i);
                ++i;
            }
        }
    }

    void TearDown(benchmark::State & /*state*/) override { index.reset(); }

protected:
    const char * index_dir = "./leader_epoch_rocks";
    std::unique_ptr<cluster::nlog::LeaderEpochIndexRocks> index;
};

BENCHMARK_DEFINE_F(RocksBasedFixture, Assign)(benchmark::State & state)
{
    for (uint64_t i = 1; auto _ : state)
    {
        index->maybeIndexEpochStartSequence(i, i);
        ++i;
    }
}

BENCHMARK_DEFINE_F(RocksBasedFixture, Query)(benchmark::State & state)
{
    for (int64_t i = 1; auto _ : state)
    {
        index->leaderEpochSequenceFor(i);
        ++i;
    }
}

}

BENCHMARK_REGISTER_F(LogBasedFixture, Assign)
    ->Args({/*file_max_size*/ 8 * 1024 * 1024, /*sync*/ true, /*query*/ false})
    ->Iterations(1'000'000);
BENCHMARK_REGISTER_F(LogBasedFixture, Assign)
    ->Args({/*file_max_size*/ 8 * 1024 * 1024, /*sync*/ false, /*query*/ false})
    ->Iterations(1'000'000);
BENCHMARK_REGISTER_F(LogBasedFixture, Assign)
    ->Args({/*file_max_size*/ 16 * 1024 * 1024, /*sync*/ true, /*query*/ false})
    ->Iterations(1'000'000);
BENCHMARK_REGISTER_F(LogBasedFixture, Assign)
    ->Args({/*file_max_size*/ 16 * 1024 * 1024, /*sync*/ false, /*query*/ false})
    ->Iterations(1'000'000);

BENCHMARK_REGISTER_F(RocksBasedFixture, Assign)->Args({/*query*/ false})->Iterations(10'000);

BENCHMARK_REGISTER_F(LogBasedFixture, Query)
    ->Args({/*file_max_size*/ 16 * 1024 * 1024, /*sync*/ false, /*query*/ true})
    ->Iterations(10'000);

BENCHMARK_REGISTER_F(RocksBasedFixture, Query)->Args({/*query*/ true})->Iterations(10'000);
