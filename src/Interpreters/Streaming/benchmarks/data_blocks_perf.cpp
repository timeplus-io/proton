#include <Interpreters/Streaming/RefCountDataBlockList.h>
#include <Interpreters/Streaming/RefCountDataBlockPages.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/LightChunk.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

#include <benchmark/benchmark.h>

namespace
{
DB::LightChunk prepareChunk(size_t num_columns, size_t rows)
{
    if (num_columns != 3 && num_columns != 5 && num_columns != 8)
        throw std::invalid_argument("invalid argument, only accept num_columns 3, 5 or 8");

    DB::Columns columns;
    /// UInt64
    auto add_uint64_col = [&]() {
        auto type = std::make_shared<DB::DataTypeUInt64>();
        auto mutable_col = type->createColumn();
        auto * col = typeid_cast<DB::ColumnUInt64 *>(mutable_col.get());
        col->reserve(rows);
        col->insertMany(123'456'789, rows);

        columns.push_back(std::move(mutable_col));
    };

    /// DateTime64
    auto add_datetime64_col = [&]() {
        auto type = std::make_shared<DB::DataTypeDateTime64>(3);
        auto mutable_col = type->createColumn();
        auto * col = typeid_cast<DB::ColumnDecimal<DB::DateTime64> *>(mutable_col.get());
        col->reserve(rows);
        col->insertMany(1'612'286'044'256, rows);
        columns.push_back(std::move(mutable_col));
    };

    auto add_string_col = [&](size_t string_length) {
        auto type = std::make_shared<DB::DataTypeString>();
        auto mutable_col = type->createColumn();
        auto * col = typeid_cast<DB::ColumnString *>(mutable_col.get());
        col->reserve(rows);

        std::string s(string_length, 'x');
        col->insertMany(s, rows);

        columns.push_back(std::move(mutable_col));
    };

    add_uint64_col();
    add_datetime64_col();
    add_string_col(16);

    if (num_columns == 3)
        return DB::LightChunk(std::move(columns));

    add_string_col(32);
    add_datetime64_col();

    if (num_columns == 5)
        return DB::LightChunk(std::move(columns));

    add_uint64_col();
    add_datetime64_col();
    add_string_col(64);

    /// num_columns == 8
    return DB::LightChunk(std::move(columns));
}

template <typename... Args>
void refCountDataBlockPages(benchmark::State & state, Args &&... args)
{
    auto args_tuple = std::make_tuple(std::move(args)...);

    auto chunk_columns = std::get<0>(args_tuple);
    auto chunk_rows = std::get<1>(args_tuple);
    auto chunks = std::get<2>(args_tuple);
    auto page_size = std::get<3>(args_tuple);

    using DataBlockPages = DB::Streaming::RefCountDataBlockPages<DB::LightChunk>;
    for (auto _ : state)
    {
        DB::Streaming::CachedBlockMetrics metrics;

        DataBlockPages data_blocks(page_size, metrics);

        for (int64_t chunk = 0; chunk < chunks; ++chunk)
            data_blocks.add(prepareChunk(chunk_columns, chunk_rows));

        benchmark::ClobberMemory();
    }
}

template <typename... Args>
void refCountDataBlockList(benchmark::State & state, Args &&... args)
{
    auto args_tuple = std::make_tuple(std::move(args)...);

    auto chunk_columns = std::get<0>(args_tuple);
    auto chunk_rows = std::get<1>(args_tuple);
    auto chunks = std::get<2>(args_tuple);

    using DataBlockList = DB::Streaming::RefCountDataBlockList<DB::LightChunk>;
    for (auto _ : state)
    {
        DB::Streaming::CachedBlockMetrics metrics;

        DataBlockList data_blocks(metrics);

        for (int64_t chunk = 0; chunk < chunks; ++chunk)
            data_blocks.add(prepareChunk(chunk_columns, chunk_rows));

        benchmark::ClobberMemory();
    }
}
}

BENCHMARK_CAPTURE(
    refCountDataBlockPages,
    Column3ChunkRows1Chunks1000PageSize512,
    /*chunk_columns=*/3,
    /*chunk_rows=*/1,
    /*chunks*=*/1'000,
    /*page_size=*/512)
    ->Iterations(1000);

BENCHMARK_CAPTURE(
    refCountDataBlockPages,
    Column3ChunkRows1Chunks10000PageSize512,
    /*chunk_columns=*/3,
    /*chunk_rows=*/1,
    /*chunks*=*/10'000,
    /*page_size=*/512)
    ->Iterations(100);

BENCHMARK_CAPTURE(
    refCountDataBlockPages,
    Column3ChunkRows1Chunks100000PageSize512,
    /*chunk_columns=*/3,
    /*chunk_rows=*/1,
    /*chunks*=*/100'000,
    /*page_size=*/512)
    ->Iterations(10);

BENCHMARK_CAPTURE(
    refCountDataBlockPages,
    Column3ChunkRows1Chunks1000000PageSize512,
    /*chunk_columns=*/3,
    /*chunk_rows=*/1,
    /*chunks*=*/1000'000,
    /*page_size=*/512)
    ->Iterations(10);

BENCHMARK_CAPTURE(
    refCountDataBlockPages,
    Column3ChunkRows1Chunks10000000PageSize512,
    /*chunk_columns=*/3,
    /*chunk_rows=*/1,
    /*chunks*=*/10'000'000,
    /*page_size=*/512)
    ->Iterations(1);

BENCHMARK_CAPTURE(
    refCountDataBlockPages,
    Column3ChunkRows1Chunks1000000PageSize1024,
    /*chunk_columns=*/3,
    /*chunk_rows=*/1,
    /*chunks*=*/1000'000,
    /*page_size=*/1024)
    ->Iterations(10);

BENCHMARK_CAPTURE(
    refCountDataBlockPages,
    Column3ChunkRows1Chunks10000000PageSize1024,
    /*chunk_columns=*/3,
    /*chunk_rows=*/1,
    /*chunks*=*/10'000'000,
    /*page_size=*/1024)
    ->Iterations(1);

BENCHMARK_CAPTURE(
    refCountDataBlockPages,
    Column3ChunkRows10Chunks1000PageSize512,
    /*chunk_columns=*/3,
    /*chunk_rows=*/10,
    /*chunks*=*/1'000,
    /*page_size=*/512)
    ->Iterations(1000);

BENCHMARK_CAPTURE(
    refCountDataBlockPages,
    Column3ChunkRows10Chunks10000PageSize512,
    /*chunk_columns=*/3,
    /*chunk_rows=*/10,
    /*chunks*=*/10'000,
    /*page_size=*/512)
    ->Iterations(100);

BENCHMARK_CAPTURE(
    refCountDataBlockPages,
    Column3ChunkRows10Chunks100000PageSize512,
    /*chunk_columns=*/3,
    /*chunk_rows=*/10,
    /*chunks*=*/100'000,
    /*page_size=*/512)
    ->Iterations(10);

BENCHMARK_CAPTURE(
    refCountDataBlockPages,
    Column3ChunkRows100Chunks1000PageSize512,
    /*chunk_columns=*/3,
    /*chunk_rows=*/100,
    /*chunks*=*/1'000,
    /*page_size=*/512)
    ->Iterations(1000);

BENCHMARK_CAPTURE(
    refCountDataBlockPages,
    Column3ChunkRows100Chunks10000PageSize512,
    /*chunk_columns=*/3,
    /*chunk_rows=*/100,
    /*chunks*=*/10'000,
    /*page_size=*/512)
    ->Iterations(100);

BENCHMARK_CAPTURE(
    refCountDataBlockPages,
    Column3ChunkRows100Chunks100000PageSize512,
    /*chunk_columns=*/3,
    /*chunk_rows=*/100,
    /*chunks*=*/100'000,
    /*page_size=*/512)
    ->Iterations(10);

BENCHMARK_CAPTURE(
    refCountDataBlockPages,
    Column3ChunkRows1000Chunks1000PageSize512,
    /*chunk_columns=*/3,
    /*chunk_rows=*/1000,
    /*chunks*=*/1'000,
    /*page_size=*/512)
    ->Iterations(1000);

BENCHMARK_CAPTURE(
    refCountDataBlockPages,
    Column3ChunkRows1000Chunks10000PageSize512,
    /*chunk_columns=*/3,
    /*chunk_rows=*/1000,
    /*chunks*=*/10'000,
    /*page_size=*/512)
    ->Iterations(100);

BENCHMARK_CAPTURE(
    refCountDataBlockPages,
    Column3ChunkRows1000Chunks100000PageSize512,
    /*chunk_columns=*/3,
    /*chunk_rows=*/1000,
    /*chunks*=*/100'000,
    /*page_size=*/512)
    ->Iterations(10);


/// List
BENCHMARK_CAPTURE(
    refCountDataBlockList,
    Column3ChunkRows1Chunks1000,
    /*chunk_columns=*/3,
    /*chunk_rows=*/1,
    /*chunks*=*/1'000)
    ->Iterations(1000);

BENCHMARK_CAPTURE(
    refCountDataBlockList,
    Column3ChunkRows1Chunks10000,
    /*chunk_columns=*/3,
    /*chunk_rows=*/1,
    /*chunks*=*/10'000)
    ->Iterations(100);

BENCHMARK_CAPTURE(
    refCountDataBlockList,
    Column3ChunkRows1Chunks100000,
    /*chunk_columns=*/3,
    /*chunk_rows=*/1,
    /*chunks*=*/100'000)
    ->Iterations(10);

BENCHMARK_CAPTURE(
    refCountDataBlockList,
    Column3ChunkRows1Chunks1000000,
    /*chunk_columns=*/3,
    /*chunk_rows=*/1,
    /*chunks*=*/1000'000)
    ->Iterations(10);

BENCHMARK_CAPTURE(
    refCountDataBlockList,
    Column3ChunkRows1Chunks10000000,
    /*chunk_columns=*/3,
    /*chunk_rows=*/1,
    /*chunks*=*/10'000'000)
    ->Iterations(1);

BENCHMARK_CAPTURE(
    refCountDataBlockList,
    Column3ChunkRows10Chunks1000,
    /*chunk_columns=*/3,
    /*chunk_rows=*/10,
    /*chunks*=*/1'000)
    ->Iterations(1000);

BENCHMARK_CAPTURE(
    refCountDataBlockList,
    Column3ChunkRows10Chunks10000,
    /*chunk_columns=*/3,
    /*chunk_rows=*/10,
    /*chunks*=*/10'000)
    ->Iterations(100);

BENCHMARK_CAPTURE(
    refCountDataBlockList,
    Column3ChunkRows10Chunks100000,
    /*chunk_columns=*/3,
    /*chunk_rows=*/10,
    /*chunks*=*/100'000)
    ->Iterations(10);

BENCHMARK_CAPTURE(
    refCountDataBlockList,
    Column3ChunkRows100Chunks1000,
    /*chunk_columns=*/3,
    /*chunk_rows=*/100,
    /*chunks*=*/1'000)
    ->Iterations(1000);

BENCHMARK_CAPTURE(
    refCountDataBlockList,
    Column3ChunkRows100Chunks10000,
    /*chunk_columns=*/3,
    /*chunk_rows=*/100,
    /*chunks*=*/10'000)
    ->Iterations(100);

BENCHMARK_CAPTURE(
    refCountDataBlockList,
    Column3ChunkRows100Chunks100000,
    /*chunk_columns=*/3,
    /*chunk_rows=*/100,
    /*chunks*=*/100'000)
    ->Iterations(10);

BENCHMARK_CAPTURE(
    refCountDataBlockList,
    Column3ChunkRows1000Chunks1000,
    /*chunk_columns=*/3,
    /*chunk_rows=*/1000,
    /*chunks*=*/1'000)
    ->Iterations(1000);

BENCHMARK_CAPTURE(
    refCountDataBlockList,
    Column3ChunkRows1000Chunks10000,
    /*chunk_columns=*/3,
    /*chunk_rows=*/1000,
    /*chunks*=*/10'000)
    ->Iterations(100);

BENCHMARK_CAPTURE(
    refCountDataBlockList,
    Column3ChunkRows1000Chunks100000,
    /*chunk_columns=*/3,
    /*chunk_rows=*/1000,
    /*chunks*=*/100'000)
    ->Iterations(10);

BENCHMARK_MAIN();
