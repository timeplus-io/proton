#include <Interpreters/Streaming/RefCountDataBlockList.h>
#include <Interpreters/Streaming/RefCountDataBlockPages.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/LightChunk.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

#include <benchmark/benchmark.h>

namespace
{
DB::LightChunk prepareChunk(size_t rows, size_t start_value)
{
    DB::Columns columns;
    {
        auto type = std::make_shared<DB::DataTypeUInt64>();
        auto mutable_col = type->createColumn();
        auto * col = typeid_cast<DB::ColumnUInt64 *>(mutable_col.get());

        for (size_t i = 0; i < rows; ++i)
            col->insertValue(start_value + i);

        columns.push_back(std::move(mutable_col));
    }

    {
        auto type = std::make_shared<DB::DataTypeString>();
        auto mutable_col = type->createColumn();
        auto * col = typeid_cast<DB::ColumnString *>(mutable_col.get());

        for (size_t i = 0; i < rows; ++i)
        {
            auto s = std::to_string(start_value + i);
            col->insertData(s.data(), s.size());
        }
        columns.push_back(std::move(mutable_col));
    }

    return DB::LightChunk(std::move(columns));
}

template <typename... Args>
void refCountDataBlockPages(benchmark::State & state, Args &&... args)
{
    auto args_tuple = std::make_tuple(std::move(args)...);
    auto keys = std::get<0>(args_tuple);
    auto chunk_size = std::get<1>(args_tuple);
    auto chunks = std::get<2>(args_tuple);
}

}

BENCHMARK_CAPTURE(refCountDataBlockPages, refCountDataBlockPagesWithChunkSize1, /*keys=*/1'000, /*chunk_size=*/1, /*chunks*=*/1'000);

BENCHMARK_MAIN();
