#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Streaming/ChunkSplitter.h>
#include <Processors/Transforms/Streaming/BlockSplitter.h>
#include <base/ClockUtils.h>

/// As 2022-11-15 10 AM PST on a c5a.8xlarge EC2 box
/// using ChunkMap = HashMap<UInt128, UInt32, UInt128TrivialHash>;
//Benchmarking case='Single 16 chars key + 1 row per chunk' took 2,666ms with total iterations=40,000,000, rps=20,000,000, total_chunks=40,000,000, cps=20,000,000
//Benchmarking case='Single 16 chars key + 10 rows per chunk with 3 subchunks' took 6,248ms with total iterations=4,000,000, rps=6,666,666, total_chunks=12,000,000, cps=2,000,000
//Benchmarking case='Single 16 chars key + 100 rows per chunk with 10 subchunks' took 34,566ms with total iterations=4,000,000, rps=11,764,705, total_chunks=40,000,000, cps=1,176,470
//Benchmarking case='Single 16 chars key + 1000 rows per chunk with 30 subchunks' took 71,608ms with total iterations=1,000,000, rps=14,084,507, total_chunks=30,000,000, cps=422,535
//Benchmarking case='Two 16 chars key + 1 row per chunk' took 3,650ms with total iterations=40,000,000, rps=13,333,333, total_chunks=40,000,000, cps=13,333,333
//Benchmarking case='Two 16 chars key + 10 row per chunk with 3 subchunks' took 10,973ms with total iterations=4,000,000, rps=4,000,000, total_chunks=12,000,000, cps=1,200,000
//Benchmarking case='Two 16 chars key + 100 row per chunk with 10 subchunks' took 60,993ms with total iterations=4,000,000, rps=6,666,666, total_chunks=40,000,000, cps=666,666
//Benchmarking case='Two 16 chars key + 1000 row per chunk with 30 subchunks' took 134,326ms with total iterations=1,000,000, rps=7,462,686, total_chunks=30,000,000, cps=223,880
//Benchmarking case='Three 16 chars key + 1 row per chunk' took 4,549ms with total iterations=40,000,000, rps=10,000,000, total_chunks=40,000,000, cps=10,000,000
//Benchmarking case='Three 16 chars key + 10 row per chunk with 3 subchunks' took 15,294ms with total iterations=4,000,000, rps=2,666,666, total_chunks=12,000,000, cps=800,000
//Benchmarking case='Three 16 chars key + 100 row per chunk with 10 subchunks' took 86,914ms with total iterations=4,000,000, rps=4,651,162, total_chunks=40,000,000, cps=465,116
//Benchmarking case='Three 16 chars key + 1000 row per chunk with 30 subchunks' took 207,641ms with total iterations=1,000,000, rps=4,830,917, total_chunks=30,000,000, cps=144,927
//Benchmarking case='Single uint64_t key + 1 row per chunk' took 2,205ms with total iterations=40,000,000, rps=20,000,000, total_chunks=40,000,000, cps=20,000,000
//Benchmarking case='Single uint64_t key + 10 rows per chunk with 3 subchunks' took 4,006ms with total iterations=4,000,000, rps=10,000,000, total_chunks=12,000,000, cps=3,000,000
//Benchmarking case='Single uint64_t key + 100 rows per chunk with 10 subchunks' took 20,314ms with total iterations=4,000,000, rps=20,000,000, total_chunks=40,000,000, cps=2,000,000
//Benchmarking case='Single uint64_t key + 1000 rows per chunk with 30 subchunks' took 39,939ms with total iterations=1,000,000, rps=25,641,025, total_chunks=30,000,000, cps=769,230
//Benchmarking case='Two uint64_t key + 1 row per chunk' took 2,448ms with total iterations=40,000,000, rps=20,000,000, total_chunks=40,000,000, cps=20,000,000
//Benchmarking case='Two uint64_t key + 10 rows per chunk with 3 subchunks' took 5,296ms with total iterations=4,000,000, rps=8,000,000, total_chunks=12,000,000, cps=2,400,000
//Benchmarking case='Two uint64_t key + 100 rows per chunk with 10 subchunks' took 29,828ms with total iterations=4,000,000, rps=13,793,103, total_chunks=40,000,000, cps=1,379,310
//Benchmarking case='Two uint64_t key + 1000 rows per chunk with 30 subchunks' took 53,740ms with total iterations=1,000,000, rps=18,867,924, total_chunks=30,000,000, cps=566,037
//Benchmarking case='One 16 chars key + one uint64_t key + 1 row per chunk' took 3,083ms with total iterations=40,000,000, rps=13,333,333, total_chunks=40,000,000, cps=13,333,333
//Benchmarking case='One 16 chars key + one uint64_t key + 10 rows per chunk with 3 subchunks' took 7,944ms with total iterations=4,000,000, rps=5,714,285, total_chunks=12,000,000, cps=1,714,285
//Benchmarking case='One 16 chars key + one uint64_t key + 100 rows per chunk with 10 subchunks' took 44,911ms with total iterations=4,000,000, rps=9,090,909, total_chunks=40,000,000, cps=909,090
//Benchmarking case='One 16 chars key + one uint64_t key + 1000 rows per chunk with 30 subchunks' took 98,123ms with total iterations=1,000,000, rps=10,204,081, total_chunks=30,000,000, cps=306,122

/// using ChunkMap = absl::flat_hash_map<UInt128, UInt32, UInt128TrivialHash>;
//Benchmarking case='Single 16 chars key + 1 row per chunk' took 2,667ms with total iterations=40,000,000, rps=20,000,000, total_chunks=40,000,000, cps=20,000,000
//Benchmarking case='Single 16 chars key + 10 rows per chunk with 3 subchunks' took 5,807ms with total iterations=4,000,000, rps=8,000,000, total_chunks=12,000,000, cps=2,400,000
//Benchmarking case='Single 16 chars key + 100 rows per chunk with 10 subchunks' took 35,171ms with total iterations=4,000,000, rps=11,428,571, total_chunks=40,000,000, cps=1,142,857
//Benchmarking case='Single 16 chars key + 1000 rows per chunk with 30 subchunks' took 73,400ms with total iterations=1,000,000, rps=13,698,630, total_chunks=30,000,000, cps=410,958
//Benchmarking case='Two 16 chars key + 1 row per chunk' took 3,651ms with total iterations=40,000,000, rps=13,333,333, total_chunks=40,000,000, cps=13,333,333
//Benchmarking case='Two 16 chars key + 10 row per chunk with 3 subchunks' took 9,604ms with total iterations=4,000,000, rps=4,444,444, total_chunks=12,000,000, cps=1,333,333
//Benchmarking case='Two 16 chars key + 100 row per chunk with 10 subchunks' took 60,077ms with total iterations=4,000,000, rps=6,666,666, total_chunks=40,000,000, cps=666,666
//Benchmarking case='Two 16 chars key + 1000 row per chunk with 30 subchunks' took 134,250ms with total iterations=1,000,000, rps=7,462,686, total_chunks=30,000,000, cps=223,880
//Benchmarking case='Three 16 chars key + 1 row per chunk' took 4,676ms with total iterations=40,000,000, rps=10,000,000, total_chunks=40,000,000, cps=10,000,000
//Benchmarking case='Three 16 chars key + 10 row per chunk with 3 subchunks' took 13,356ms with total iterations=4,000,000, rps=3,076,923, total_chunks=12,000,000, cps=923,076
//Benchmarking case='Three 16 chars key + 100 row per chunk with 10 subchunks' took 85,547ms with total iterations=4,000,000, rps=4,705,882, total_chunks=40,000,000, cps=470,588
//Benchmarking case='Three 16 chars key + 1000 row per chunk with 30 subchunks' took 208,155ms with total iterations=1,000,000, rps=4,807,692, total_chunks=30,000,000, cps=144,230
//Benchmarking case='Single uint64_t key + 1 row per chunk' took 2,156ms with total iterations=40,000,000, rps=20,000,000, total_chunks=40,000,000, cps=20,000,000
//Benchmarking case='Single uint64_t key + 10 rows per chunk with 3 subchunks' took 3,602ms with total iterations=4,000,000, rps=13,333,333, total_chunks=12,000,000, cps=4,000,000
//Benchmarking case='Single uint64_t key + 100 rows per chunk with 10 subchunks' took 21,727ms with total iterations=4,000,000, rps=19,047,619, total_chunks=40,000,000, cps=1,904,761
//Benchmarking case='Single uint64_t key + 1000 rows per chunk with 30 subchunks' took 43,548ms with total iterations=1,000,000, rps=23,255,813, total_chunks=30,000,000, cps=697,674
//Benchmarking case='Two uint64_t key + 1 row per chunk' took 2,447ms with total iterations=40,000,000, rps=20,000,000, total_chunks=40,000,000, cps=20,000,000
//Benchmarking case='Two uint64_t key + 10 rows per chunk with 3 subchunks' took 4,965ms with total iterations=4,000,000, rps=10,000,000, total_chunks=12,000,000, cps=3,000,000
//Benchmarking case='Two uint64_t key + 100 rows per chunk with 10 subchunks' took 30,722ms with total iterations=4,000,000, rps=13,333,333, total_chunks=40,000,000, cps=1,333,333
//Benchmarking case='Two uint64_t key + 1000 rows per chunk with 30 subchunks' took 54,823ms with total iterations=1,000,000, rps=18,518,518, total_chunks=30,000,000, cps=555,555
//Benchmarking case='One 16 chars key + one uint64_t key + 1 row per chunk' took 3,048ms with total iterations=40,000,000, rps=13,333,333, total_chunks=40,000,000, cps=13,333,333
//Benchmarking case='One 16 chars key + one uint64_t key + 10 rows per chunk with 3 subchunks' took 7,274ms with total iterations=4,000,000, rps=5,714,285, total_chunks=12,000,000, cps=1,714,285
//Benchmarking case='One 16 chars key + one uint64_t key + 100 rows per chunk with 10 subchunks' took 44,742ms with total iterations=4,000,000, rps=9,090,909, total_chunks=40,000,000, cps=909,090
//Benchmarking case='One 16 chars key + one uint64_t key + 1000 rows per chunk with 30 subchunks' took 95,056ms with total iterations=1,000,000, rps=10,526,315, total_chunks=30,000,000, cps=315,789

/// using ChunkMap = std::unordered_map<UInt128, UInt32, UInt128TrivialHash>;
//Benchmarking case='Single 16 chars key + 1 row per chunk' took 7,588ms with total iterations=40,000,000, rps=5,714,285, total_chunks=40,000,000, cps=5,714,285
//Benchmarking case='Single 16 chars key + 10 rows per chunk with 3 subchunks' took 6,380ms with total iterations=4,000,000, rps=6,666,666, total_chunks=12,000,000, cps=2,000,000
//Benchmarking case='Single 16 chars key + 100 rows per chunk with 10 subchunks' took 40,500ms with total iterations=4,000,000, rps=10,000,000, total_chunks=40,000,000, cps=1,000,000
//Benchmarking case='Single 16 chars key + 1000 rows per chunk with 30 subchunks' took 84,644ms with total iterations=1,000,000, rps=11,904,761, total_chunks=30,000,000, cps=357,142
//Benchmarking case='Two 16 chars key + 1 row per chunk' took 8,609ms with total iterations=40,000,000, rps=5,000,000, total_chunks=40,000,000, cps=5,000,000
//Benchmarking case='Two 16 chars key + 10 row per chunk with 3 subchunks' took 9,805ms with total iterations=4,000,000, rps=4,444,444, total_chunks=12,000,000, cps=1,333,333
//Benchmarking case='Two 16 chars key + 100 row per chunk with 10 subchunks' took 66,542ms with total iterations=4,000,000, rps=6,060,606, total_chunks=40,000,000, cps=606,060
//Benchmarking case='Two 16 chars key + 1000 row per chunk with 30 subchunks' took 149,584ms with total iterations=1,000,000, rps=6,711,409, total_chunks=30,000,000, cps=201,342
//Benchmarking case='Three 16 chars key + 1 row per chunk' took 9,497ms with total iterations=40,000,000, rps=4,444,444, total_chunks=40,000,000, cps=4,444,444
//Benchmarking case='Three 16 chars key + 10 row per chunk with 3 subchunks' took 13,941ms with total iterations=4,000,000, rps=3,076,923, total_chunks=12,000,000, cps=923,076
//Benchmarking case='Three 16 chars key + 100 row per chunk with 10 subchunks' took 92,367ms with total iterations=4,000,000, rps=4,347,826, total_chunks=40,000,000, cps=434,782
//Benchmarking case='Three 16 chars key + 1000 row per chunk with 30 subchunks' took 217,373ms with total iterations=1,000,000, rps=4,608,294, total_chunks=30,000,000, cps=138,248
//Benchmarking case='Single uint64_t key + 1 row per chunk' took 7,115ms with total iterations=40,000,000, rps=5,714,285, total_chunks=40,000,000, cps=5,714,285
//Benchmarking case='Single uint64_t key + 10 rows per chunk with 3 subchunks' took 4,168ms with total iterations=4,000,000, rps=10,000,000, total_chunks=12,000,000, cps=3,000,000
//Benchmarking case='Single uint64_t key + 100 rows per chunk with 10 subchunks' took 27,128ms with total iterations=4,000,000, rps=14,814,814, total_chunks=40,000,000, cps=1,481,481
//Benchmarking case='Single uint64_t key + 1000 rows per chunk with 30 subchunks' took 56,944ms with total iterations=1,000,000, rps=17,857,142, total_chunks=30,000,000, cps=535,714
//Benchmarking case='Two uint64_t key + 1 row per chunk' took 7,463ms with total iterations=40,000,000, rps=5,714,285, total_chunks=40,000,000, cps=5,714,285
//Benchmarking case='Two uint64_t key + 10 rows per chunk with 3 subchunks' took 5,466ms with total iterations=4,000,000, rps=8,000,000, total_chunks=12,000,000, cps=2,400,000
//Benchmarking case='Two uint64_t key + 100 rows per chunk with 10 subchunks' took 35,741ms with total iterations=4,000,000, rps=11,428,571, total_chunks=40,000,000, cps=1,142,857
//Benchmarking case='Two uint64_t key + 1000 rows per chunk with 30 subchunks' took 67,268ms with total iterations=1,000,000, rps=14,925,373, total_chunks=30,000,000, cps=447,761
//Benchmarking case='One 16 chars key + one uint64_t key + 1 row per chunk' took 8,135ms with total iterations=40,000,000, rps=5,000,000, total_chunks=40,000,000, cps=5,000,000
//Benchmarking case='One 16 chars key + one uint64_t key + 10 rows per chunk with 3 subchunks' took 7,768ms with total iterations=4,000,000, rps=5,714,285, total_chunks=12,000,000, cps=1,714,285
//Benchmarking case='One 16 chars key + one uint64_t key + 100 rows per chunk with 10 subchunks' took 51,191ms with total iterations=4,000,000, rps=7,843,137, total_chunks=40,000,000, cps=784,313
//Benchmarking case='One 16 chars key + one uint64_t key + 1000 rows per chunk with 30 subchunks' took 111,209ms with total iterations=1,000,000, rps=9,009,009, total_chunks=30,000,000, cps=270,270

/// use std::vector + std::find
//Benchmarking case='Single 16 chars key + 1 row per chunk' took 2,683ms with total iterations=40,000,000, rps=20,000,000, total_chunks=40,000,000, cps=20,000,000
//Benchmarking case='Single 16 chars key + 10 rows per chunk with 3 subchunks' took 4,997ms with total iterations=4,000,000, rps=10,000,000, total_chunks=12,000,000, cps=3,000,000
//Benchmarking case='Single 16 chars key + 100 rows per chunk with 10 subchunks' took 31,856ms with total iterations=4,000,000, rps=12,903,225, total_chunks=40,000,000, cps=1,290,322
//Benchmarking case='Single 16 chars key + 1000 rows per chunk with 30 subchunks' took 75,825ms with total iterations=1,000,000, rps=13,333,333, total_chunks=30,000,000, cps=400,000
//Benchmarking case='Two 16 chars key + 1 row per chunk' took 3,572ms with total iterations=40,000,000, rps=13,333,333, total_chunks=40,000,000, cps=13,333,333
//Benchmarking case='Two 16 chars key + 10 row per chunk with 3 subchunks' took 8,453ms with total iterations=4,000,000, rps=5,000,000, total_chunks=12,000,000, cps=1,500,000
//Benchmarking case='Two 16 chars key + 100 row per chunk with 10 subchunks' took 54,806ms with total iterations=4,000,000, rps=7,407,407, total_chunks=40,000,000, cps=740,740
//Benchmarking case='Two 16 chars key + 1000 row per chunk with 30 subchunks' took 138,033ms with total iterations=1,000,000, rps=7,246,376, total_chunks=30,000,000, cps=217,391
//Benchmarking case='Three 16 chars key + 1 row per chunk' took 4,492ms with total iterations=40,000,000, rps=10,000,000, total_chunks=40,000,000, cps=10,000,000
//Benchmarking case='Three 16 chars key + 10 row per chunk with 3 subchunks' took 11,928ms with total iterations=4,000,000, rps=3,636,363, total_chunks=12,000,000, cps=1,090,909
//Benchmarking case='Three 16 chars key + 100 row per chunk with 10 subchunks' took 78,355ms with total iterations=4,000,000, rps=5,128,205, total_chunks=40,000,000, cps=512,820
//Benchmarking case='Three 16 chars key + 1000 row per chunk with 30 subchunks' took 209,857ms with total iterations=1,000,000, rps=4,784,688, total_chunks=30,000,000, cps=143,540
//Benchmarking case='Single uint64_t key + 1 row per chunk' took 2,173ms with total iterations=40,000,000, rps=20,000,000, total_chunks=40,000,000, cps=20,000,000
//Benchmarking case='Single uint64_t key + 10 rows per chunk with 3 subchunks' took 2,945ms with total iterations=4,000,000, rps=20,000,000, total_chunks=12,000,000, cps=6,000,000
//Benchmarking case='Single uint64_t key + 100 rows per chunk with 10 subchunks' took 19,708ms with total iterations=4,000,000, rps=21,052,631, total_chunks=40,000,000, cps=2,105,263
//Benchmarking case='Single uint64_t key + 1000 rows per chunk with 30 subchunks' took 48,257ms with total iterations=1,000,000, rps=20,833,333, total_chunks=30,000,000, cps=625,000
//Benchmarking case='Two uint64_t key + 1 row per chunk' took 2,455ms with total iterations=40,000,000, rps=20,000,000, total_chunks=40,000,000, cps=20,000,000
//Benchmarking case='Two uint64_t key + 10 rows per chunk with 3 subchunks' took 4,276ms with total iterations=4,000,000, rps=10,000,000, total_chunks=12,000,000, cps=3,000,000
//Benchmarking case='Two uint64_t key + 100 rows per chunk with 10 subchunks' took 27,177ms with total iterations=4,000,000, rps=14,814,814, total_chunks=40,000,000, cps=1,481,481
//Benchmarking case='Two uint64_t key + 1000 rows per chunk with 30 subchunks' took 59,890ms with total iterations=1,000,000, rps=16,949,152, total_chunks=30,000,000, cps=508,474
//Benchmarking case='One 16 chars key + one uint64_t key + 1 row per chunk' took 3,064ms with total iterations=40,000,000, rps=13,333,333, total_chunks=40,000,000, cps=13,333,333
//Benchmarking case='One 16 chars key + one uint64_t key + 10 rows per chunk with 3 subchunks' took 6,411ms with total iterations=4,000,000, rps=6,666,666, total_chunks=12,000,000, cps=2,000,000
//Benchmarking case='One 16 chars key + one uint64_t key + 100 rows per chunk with 10 subchunks' took 40,923ms with total iterations=4,000,000, rps=10,000,000, total_chunks=40,000,000, cps=1,000,000
//Benchmarking case='One 16 chars key + one uint64_t key + 1000 rows per chunk with 30 subchunks' took 98,102ms with total iterations=1,000,000, rps=10,204,081, total_chunks=30,000,000, cps=306,122

namespace
{

template <typename ColumnType, typename IntegerType>
void doInsertColumnNumber(DB::Chunk & chunk, size_t salt, size_t rows, size_t chunks, DB::DataTypePtr data_type)
{
    auto col = data_type->createColumn();
    auto * col_ptr = typeid_cast<ColumnType *>(col.get());

    for (size_t i = 0; i < rows; ++i)
        col_ptr->insert(IntegerType((i + salt) % chunks));

    chunk.addColumn(std::move(col));
}

template <typename NumberType, typename ColumnType, typename IntegerType>
void insertColumnNumber(DB::Chunk & chunk, size_t salt, size_t rows, size_t chunks)
{
    doInsertColumnNumber<ColumnType, IntegerType>(chunk, salt, rows, chunks, std::make_shared<NumberType>());
}

void insertColumnUInt64(DB::Chunk & chunk, size_t salt, size_t rows, size_t chunks)
{
    insertColumnNumber<DB::DataTypeUInt64, DB::ColumnUInt64, UInt64>(chunk, salt, rows, chunks);
}

template <typename ColumnType>
void doInsertColumnString(DB::Chunk & chunk, size_t salt, size_t rows, size_t chunks, DB::DataTypePtr data_type)
{
    auto col = data_type->createColumn();
    auto * col_ptr = typeid_cast<ColumnType *>(col.get());

    for (size_t i = 0; i < rows; ++i)
        col_ptr->insert("this is a key " + std::to_string((i + salt) % chunks));

    chunk.addColumn(std::move(col));
}

void insertColumnString(DB::Chunk & chunk, size_t salt, size_t rows, size_t chunks)
{
    doInsertColumnString<DB::ColumnString>(chunk, salt, rows, chunks, std::make_shared<DB::DataTypeString>());
}

void doSplitChunkPerf(DB::Chunk chunk, std::vector<size_t> key_column_positions, std::string case_name, size_t iteration = 4'000'000)
{
    size_t rows = chunk.getNumRows();

    DB::Streaming::ChunkSplitter splitter(key_column_positions);

    auto start = DB::UTCMilliseconds ::now();

    size_t total = 0;
    for (size_t i = 0; i < iteration; ++i)
    {
        auto chunks = splitter.split(chunk);
        total += chunks.size();
    }

    auto cost = DB::UTCMilliseconds::now() - start;
    auto rps = (iteration * rows) / (cost / 1000);
    auto cps = total / (cost / 1000);

    std::cout << fmt::format(
        std::locale("en_US.UTF-8"),
        "Benchmarking case='{}' took {:L}ms with total iterations={:L}, rps={:L}, total_chunks={:L}, cps={:L}",
        case_name,
        cost,
        iteration,
        rps,
        total,
        cps) << std::endl;
}

void doSplitChunkPerfOneRow(
    DB::Chunk chunk, std::vector<size_t> key_column_positions, std::string case_name, size_t iterations = 40'000'000)
{
    size_t rows = chunk.getNumRows();

    DB::Streaming::ChunkSplitter splitter(key_column_positions);

    auto start = DB::UTCMilliseconds::now();

    size_t total = 0;
    for (size_t i = 0; i < iterations; ++i)
    {
        auto chunks = splitter.split(chunk);
        total += chunks.size();
        chunk.swap(chunks[0].chunk);
    }

    auto cost = DB::UTCMilliseconds::now() - start;
    auto rps = (iterations * rows) / (cost / 1000);
    auto cps = total / (cost / 1000);

    std::cout << fmt::format(
        std::locale("en_US.UTF-8"),
        "Benchmarking case='{}' took {:L}ms with total iterations={:L}, rps={:L}, total_chunks={:L}, cps={:L}",
        case_name,
        cost,
        iterations,
        rps,
        total,
        cps) << std::endl;
}

[[maybe_unused]] void splitChunkPerf()
{
    /// One 16 char string key
    {
        size_t rows = 1;
        size_t chunks = 1;
        DB::Chunk chunk;
        insertColumnString(chunk, 100, rows, chunks);

        std::vector<size_t> positions(chunk.getNumColumns());
        std::iota(positions.begin(), positions.end(), 0);

        doSplitChunkPerfOneRow(std::move(chunk), std::move(positions), "Single 16 chars key + 1 row per chunk");
    }

    {
        size_t rows = 10;
        size_t chunks = 3;

        DB::Chunk chunk;
        insertColumnString(chunk, 100, rows, chunks);

        std::vector<size_t> positions(chunk.getNumColumns());
        std::iota(positions.begin(), positions.end(), 0);

        doSplitChunkPerf(std::move(chunk), std::move(positions), "Single 16 chars key + 10 rows per chunk with 3 subchunks");
    }

    {
        size_t rows = 100;
        size_t chunks = 10;

        DB::Chunk chunk;
        insertColumnString(chunk, 100, rows, chunks);

        std::vector<size_t> positions(chunk.getNumColumns());
        std::iota(positions.begin(), positions.end(), 0);
        doSplitChunkPerf(std::move(chunk), std::move(positions), "Single 16 chars key + 100 rows per chunk with 10 subchunks");
    }

    {
        size_t rows = 1000;
        size_t chunks = 30;

        DB::Chunk chunk;
        insertColumnString(chunk, 100, rows, chunks);

        std::vector<size_t> positions(chunk.getNumColumns());
        std::iota(positions.begin(), positions.end(), 0);
        doSplitChunkPerf(std::move(chunk), std::move(positions), "Single 16 chars key + 1000 rows per chunk with 30 subchunks", 1'000'000);
    }

    /// Two 16 char string keys
    {
        size_t rows = 1;
        size_t chunks = 1;

        DB::Chunk chunk;
        insertColumnString(chunk, 100, rows, chunks);
        insertColumnString(chunk, 200, rows, chunks);

        std::vector<size_t> positions(chunk.getNumColumns());
        std::iota(positions.begin(), positions.end(), 0);

        doSplitChunkPerfOneRow(std::move(chunk), std::move(positions), "Two 16 chars key + 1 row per chunk");
    }

    {
        size_t rows = 10;
        size_t chunks = 3;

        DB::Chunk chunk;
        insertColumnString(chunk, 100, rows, chunks);
        insertColumnString(chunk, 200, rows, chunks);

        std::vector<size_t> positions(chunk.getNumColumns());
        std::iota(positions.begin(), positions.end(), 0);

        doSplitChunkPerf(std::move(chunk), std::move(positions), "Two 16 chars key + 10 row per chunk with 3 subchunks");
    }

    {
        size_t rows = 100;
        size_t chunks = 10;

        DB::Chunk chunk;
        insertColumnString(chunk, 100, rows, chunks);
        insertColumnString(chunk, 200, rows, chunks);

        std::vector<size_t> positions(chunk.getNumColumns());
        std::iota(positions.begin(), positions.end(), 0);

        doSplitChunkPerf(std::move(chunk), std::move(positions), "Two 16 chars key + 100 row per chunk with 10 subchunks");
    }

    {
        size_t rows = 1000;
        size_t chunks = 30;

        DB::Chunk chunk;
        insertColumnString(chunk, 100, rows, chunks);
        insertColumnString(chunk, 200, rows, chunks);

        std::vector<size_t> positions(chunk.getNumColumns());
        std::iota(positions.begin(), positions.end(), 0);

        doSplitChunkPerf(std::move(chunk), std::move(positions), "Two 16 chars key + 1000 row per chunk with 30 subchunks", 1'000'000);
    }

    /// Three 16 char string keys
    {
        size_t rows = 1;
        size_t chunks = 1;

        DB::Chunk chunk;
        insertColumnString(chunk, 100, rows, chunks);
        insertColumnString(chunk, 200, rows, chunks);
        insertColumnString(chunk, 300, rows, chunks);

        std::vector<size_t> positions(chunk.getNumColumns());
        std::iota(positions.begin(), positions.end(), 0);

        doSplitChunkPerfOneRow(std::move(chunk), std::move(positions), "Three 16 chars key + 1 row per chunk");
    }

    {
        size_t rows = 10;
        size_t chunks = 3;

        DB::Chunk chunk;
        insertColumnString(chunk, 100, rows, chunks);
        insertColumnString(chunk, 200, rows, chunks);
        insertColumnString(chunk, 300, rows, chunks);

        std::vector<size_t> positions(chunk.getNumColumns());
        std::iota(positions.begin(), positions.end(), 0);

        doSplitChunkPerf(std::move(chunk), std::move(positions), "Three 16 chars key + 10 row per chunk with 3 subchunks");
    }

    {
        size_t rows = 100;
        size_t chunks = 10;

        DB::Chunk chunk;
        insertColumnString(chunk, 100, rows, chunks);
        insertColumnString(chunk, 200, rows, chunks);
        insertColumnString(chunk, 300, rows, chunks);

        std::vector<size_t> positions(chunk.getNumColumns());
        std::iota(positions.begin(), positions.end(), 0);

        doSplitChunkPerf(std::move(chunk), std::move(positions), "Three 16 chars key + 100 row per chunk with 10 subchunks");
    }

    {
        size_t rows = 1000;
        size_t chunks = 30;

        DB::Chunk chunk;
        insertColumnString(chunk, 100, rows, chunks);
        insertColumnString(chunk, 200, rows, chunks);
        insertColumnString(chunk, 300, rows, chunks);

        std::vector<size_t> positions(chunk.getNumColumns());
        std::iota(positions.begin(), positions.end(), 0);

        doSplitChunkPerf(std::move(chunk), std::move(positions), "Three 16 chars key + 1000 row per chunk with 30 subchunks", 1'000'000);
    }

    /// One uint64_t key
    {
        size_t rows = 1;
        size_t chunks = 1;

        DB::Chunk chunk;
        insertColumnUInt64(chunk, 100, rows, chunks);

        std::vector<size_t> positions(chunk.getNumColumns());
        std::iota(positions.begin(), positions.end(), 0);

        doSplitChunkPerfOneRow(std::move(chunk), std::move(positions), "Single uint64_t key + 1 row per chunk");
    }

    {
        size_t rows = 10;
        size_t chunks = 3;

        DB::Chunk chunk;
        insertColumnUInt64(chunk, 100, rows, chunks);

        std::vector<size_t> positions(chunk.getNumColumns());
        std::iota(positions.begin(), positions.end(), 0);

        doSplitChunkPerf(std::move(chunk), std::move(positions), "Single uint64_t key + 10 rows per chunk with 3 subchunks");
    }

    {
        size_t rows = 100;
        size_t chunks = 10;

        DB::Chunk chunk;
        insertColumnUInt64(chunk, 100, rows, chunks);

        std::vector<size_t> positions(chunk.getNumColumns());
        std::iota(positions.begin(), positions.end(), 0);

        doSplitChunkPerf(std::move(chunk), std::move(positions), "Single uint64_t key + 100 rows per chunk with 10 subchunks");
    }

    {
        size_t rows = 1000;
        size_t chunks = 30;

        DB::Chunk chunk;
        insertColumnUInt64(chunk, 100, rows, chunks);

        std::vector<size_t> positions(chunk.getNumColumns());
        std::iota(positions.begin(), positions.end(), 0);
        doSplitChunkPerf(std::move(chunk), std::move(positions), "Single uint64_t key + 1000 rows per chunk with 30 subchunks", 1'000'000);
    }

    /// Two uint64_t key
    {
        size_t rows = 1;
        size_t chunks = 1;

        DB::Chunk chunk;
        insertColumnUInt64(chunk, 100, rows, chunks);
        insertColumnUInt64(chunk, 200, rows, chunks);

        std::vector<size_t> positions(chunk.getNumColumns());
        std::iota(positions.begin(), positions.end(), 0);

        doSplitChunkPerfOneRow(std::move(chunk), std::move(positions), "Two uint64_t key + 1 row per chunk");
    }

    {
        size_t rows = 10;
        size_t chunks = 3;

        DB::Chunk chunk;
        insertColumnUInt64(chunk, 100, rows, chunks);
        insertColumnUInt64(chunk, 200, rows, chunks);

        std::vector<size_t> positions(chunk.getNumColumns());
        std::iota(positions.begin(), positions.end(), 0);

        doSplitChunkPerf(std::move(chunk), std::move(positions), "Two uint64_t key + 10 rows per chunk with 3 subchunks");
    }

    {
        size_t rows = 100;
        size_t chunks = 10;

        DB::Chunk chunk;
        insertColumnUInt64(chunk, 100, rows, chunks);
        insertColumnUInt64(chunk, 200, rows, chunks);

        std::vector<size_t> positions(chunk.getNumColumns());
        std::iota(positions.begin(), positions.end(), 0);

        doSplitChunkPerf(std::move(chunk), std::move(positions), "Two uint64_t key + 100 rows per chunk with 10 subchunks");
    }

    {
        size_t rows = 1000;
        size_t chunks = 30;

        DB::Chunk chunk;
        insertColumnUInt64(chunk, 100, rows, chunks);
        insertColumnUInt64(chunk, 200, rows, chunks);

        std::vector<size_t> positions(chunk.getNumColumns());
        std::iota(positions.begin(), positions.end(), 0);
        doSplitChunkPerf(std::move(chunk), std::move(positions), "Two uint64_t key + 1000 rows per chunk with 30 subchunks", 1'000'000);
    }

    /// 16 chars + uint64_t keys
    {
        size_t rows = 1;
        size_t chunks = 1;

        DB::Chunk chunk;
        insertColumnString(chunk, 100, rows, chunks);
        insertColumnUInt64(chunk, 200, rows, chunks);

        std::vector<size_t> positions(chunk.getNumColumns());
        std::iota(positions.begin(), positions.end(), 0);

        doSplitChunkPerfOneRow(std::move(chunk), std::move(positions), "One 16 chars key + one uint64_t key + 1 row per chunk");
    }

    {
        size_t rows = 10;
        size_t chunks = 3;

        DB::Chunk chunk;
        insertColumnString(chunk, 100, rows, chunks);
        insertColumnUInt64(chunk, 200, rows, chunks);

        std::vector<size_t> positions(chunk.getNumColumns());
        std::iota(positions.begin(), positions.end(), 0);

        doSplitChunkPerf(
            std::move(chunk), std::move(positions), "One 16 chars key + one uint64_t key + 10 rows per chunk with 3 subchunks");
    }

    {
        size_t rows = 100;
        size_t chunks = 10;

        DB::Chunk chunk;
        insertColumnString(chunk, 100, rows, chunks);
        insertColumnUInt64(chunk, 200, rows, chunks);

        std::vector<size_t> positions(chunk.getNumColumns());
        std::iota(positions.begin(), positions.end(), 0);

        doSplitChunkPerf(
            std::move(chunk), std::move(positions), "One 16 chars key + one uint64_t key + 100 rows per chunk with 10 subchunks");
    }

    {
        size_t rows = 1000;
        size_t chunks = 30;

        DB::Chunk chunk;
        insertColumnString(chunk, 100, rows, chunks);
        insertColumnUInt64(chunk, 200, rows, chunks);

        std::vector<size_t> positions(chunk.getNumColumns());
        std::iota(positions.begin(), positions.end(), 0);
        doSplitChunkPerf(
            std::move(chunk),
            std::move(positions),
            "One 16 chars key + one uint64_t key + 1000 rows per chunk with 30 subchunks",
            1'000'000);
    }
}


void doSplitBlockPerf(DB::Chunk chunk, std::vector<size_t> key_column_positions, std::string case_name, size_t iteration = 10'000'000)
{
    auto & data_types = DB::DataTypeFactory::instance();
    DB::Block block;

    /// Convert chunk to block
    for (size_t i = 0; const auto & column : chunk.getColumns())
    {
        DB::ColumnWithTypeAndName col_with_name{column, data_types.get(column->getDataType()), fmt::format("c_{}_{}",column->getFamilyName(), i)};
        block.insert(col_with_name);
    }

    auto header = block.cloneEmpty();

    size_t rows = block.rows();

    auto start = DB::UTCMilliseconds ::now();

    size_t total = 0;
    for (size_t i = 0; i < iteration; ++i)
    {
        DB::Streaming::Substream::BlockSplitter splitter(header, key_column_positions);
        auto blocks = splitter.split(block);
        total += blocks.size();
    }

    auto cost = DB::UTCMilliseconds::now() - start;
    auto rps = (iteration * rows) / (cost / 1000);
    auto bps = total / (cost / 1000);

    std::cout << fmt::format(
        std::locale("en_US.UTF-8"),
        "Benchmarking case='{}' took {:L}ms with total iterations={:L}, rps={:L}, total_blocks={:L}, bps={:L}",
        case_name,
        cost,
        iteration,
        rps,
        total,
        bps) << std::endl;
}

//Benchmarking case='Single 16 chars key + 1 row per chunk' took 23,314ms with total iterations=10,000,000, rps=434,782, total_blocks=10,000,000, bps=434,782
//Benchmarking case='Single 16 chars key + 10 rows per chunk with 3 subchunks' took 44,859ms with total iterations=10,000,000, rps=2,272,727, total_blocks=30,000,000, bps=681,818
//Benchmarking case='Single 16 chars key + 100 rows per chunk with 10 subchunks' took 164,444ms with total iterations=10,000,000, rps=6,097,560, total_blocks=100,000,000, bps=609,756
//Benchmarking case='Single 16 chars key + 1000 rows per chunk with 30 subchunks' took 80,203ms with total iterations=1,000,000, rps=12,500,000, total_blocks=30,000,000, bps=375,000
//Benchmarking case='Two 16 chars key + 1 row per chunk' took 25,217ms with total iterations=10,000,000, rps=400,000, total_blocks=10,000,000, bps=400,000
//Benchmarking case='Two 16 chars key + 10 row per chunk with 3 subchunks' took 53,640ms with total iterations=10,000,000, rps=1,886,792, total_blocks=30,000,000, bps=566,037
//Benchmarking case='Two 16 chars key + 100 row per chunk with 10 subchunks' took 230,758ms with total iterations=10,000,000, rps=4,347,826, total_blocks=100,000,000, bps=434,782
//Benchmarking case='Two 16 chars key + 1000 row per chunk with 30 subchunks' took 132,571ms with total iterations=1,000,000, rps=7,575,757, total_blocks=30,000,000, bps=227,272
//Benchmarking case='Three 16 chars key + 1 row per chunk' took 29,665ms with total iterations=10,000,000, rps=344,827, total_blocks=10,000,000, bps=344,827
//Benchmarking case='Three 16 chars key + 10 row per chunk with 3 subchunks' took 68,441ms with total iterations=10,000,000, rps=1,470,588, total_blocks=30,000,000, bps=441,176
//Benchmarking case='Three 16 chars key + 100 row per chunk with 10 subchunks' took 278,245ms with total iterations=10,000,000, rps=3,597,122, total_blocks=100,000,000, bps=359,712
//Benchmarking case='Three 16 chars key + 1000 row per chunk with 30 subchunks' took 169,553ms with total iterations=1,000,000, rps=5,917,159, total_blocks=30,000,000, bps=177,514
//Benchmarking case='Single uint64_t key + 1 row per chunk' took 19,400ms with total iterations=10,000,000, rps=526,315, total_blocks=10,000,000, bps=526,315
//Benchmarking case='Single uint64_t key + 10 rows per chunk with 3 subchunks' took 36,362ms with total iterations=10,000,000, rps=2,777,777, total_blocks=30,000,000, bps=833,333
//Benchmarking case='Single uint64_t key + 100 rows per chunk with 10 subchunks' took 139,942ms with total iterations=10,000,000, rps=7,194,244, total_blocks=100,000,000, bps=719,424
//Benchmarking case='Single uint64_t key + 1000 rows per chunk with 30 subchunks' took 70,996ms with total iterations=1,000,000, rps=14,285,714, total_blocks=30,000,000, bps=428,571
//Benchmarking case='Two uint64_t key + 1 row per chunk' took 24,970ms with total iterations=10,000,000, rps=416,666, total_blocks=10,000,000, bps=416,666
//Benchmarking case='Two uint64_t key + 10 rows per chunk with 3 subchunks' took 44,749ms with total iterations=10,000,000, rps=2,272,727, total_blocks=30,000,000, bps=681,818
//Benchmarking case='Two uint64_t key + 100 rows per chunk with 10 subchunks' took 174,952ms with total iterations=10,000,000, rps=5,747,126, total_blocks=100,000,000, bps=574,712
//Benchmarking case='Two uint64_t key + 1000 rows per chunk with 30 subchunks' took 81,574ms with total iterations=1,000,000, rps=12,345,679, total_blocks=30,000,000, bps=370,370
//Benchmarking case='One 16 chars key + one uint64_t key + 1 row per chunk' took 22,733ms with total iterations=10,000,000, rps=454,545, total_blocks=10,000,000, bps=454,545
//Benchmarking case='One 16 chars key + one uint64_t key + 10 rows per chunk with 3 subchunks' took 48,319ms with total iterations=10,000,000, rps=2,083,333, total_blocks=30,000,000, bps=625,000
//Benchmarking case='One 16 chars key + one uint64_t key + 100 rows per chunk with 10 subchunks' took 193,452ms with total iterations=10,000,000, rps=5,181,347, total_blocks=100,000,000, bps=518,134
//Benchmarking case='One 16 chars key + one uint64_t key + 1000 rows per chunk with 30 subchunks' took 109,911ms with total iterations=1,000,000, rps=9,174,311, total_blocks=30,000,000, bps=275,229

[[maybe_unused]] void splitBlockPerf()
{
    /// One 16 char string key
    {
        size_t rows = 1;
        size_t chunks = 1;
        DB::Chunk chunk;
        insertColumnString(chunk, 100, rows, chunks);

        std::vector<size_t> positions(chunk.getNumColumns());
        std::iota(positions.begin(), positions.end(), 0);

        doSplitBlockPerf(std::move(chunk), std::move(positions), "Single 16 chars key + 1 row per chunk");
    }

    {
        size_t rows = 10;
        size_t chunks = 3;

        DB::Chunk chunk;
        insertColumnString(chunk, 100, rows, chunks);

        std::vector<size_t> positions(chunk.getNumColumns());
        std::iota(positions.begin(), positions.end(), 0);

        doSplitBlockPerf(std::move(chunk), std::move(positions), "Single 16 chars key + 10 rows per chunk with 3 subchunks");
    }

    {
        size_t rows = 100;
        size_t chunks = 10;

        DB::Chunk chunk;
        insertColumnString(chunk, 100, rows, chunks);

        std::vector<size_t> positions(chunk.getNumColumns());
        std::iota(positions.begin(), positions.end(), 0);
        doSplitBlockPerf(std::move(chunk), std::move(positions), "Single 16 chars key + 100 rows per chunk with 10 subchunks");
    }

    {
        size_t rows = 1000;
        size_t chunks = 30;

        DB::Chunk chunk;
        insertColumnString(chunk, 100, rows, chunks);

        std::vector<size_t> positions(chunk.getNumColumns());
        std::iota(positions.begin(), positions.end(), 0);
        doSplitBlockPerf(std::move(chunk), std::move(positions), "Single 16 chars key + 1000 rows per chunk with 30 subchunks", 1'000'000);
    }

    /// Two 16 char string keys
    {
        size_t rows = 1;
        size_t chunks = 1;

        DB::Chunk chunk;
        insertColumnString(chunk, 100, rows, chunks);
        insertColumnString(chunk, 200, rows, chunks);

        std::vector<size_t> positions(chunk.getNumColumns());
        std::iota(positions.begin(), positions.end(), 0);

        doSplitBlockPerf(std::move(chunk), std::move(positions), "Two 16 chars key + 1 row per chunk");
    }

    {
        size_t rows = 10;
        size_t chunks = 3;

        DB::Chunk chunk;
        insertColumnString(chunk, 100, rows, chunks);
        insertColumnString(chunk, 200, rows, chunks);

        std::vector<size_t> positions(chunk.getNumColumns());
        std::iota(positions.begin(), positions.end(), 0);

        doSplitBlockPerf(std::move(chunk), std::move(positions), "Two 16 chars key + 10 row per chunk with 3 subchunks");
    }

    {
        size_t rows = 100;
        size_t chunks = 10;

        DB::Chunk chunk;
        insertColumnString(chunk, 100, rows, chunks);
        insertColumnString(chunk, 200, rows, chunks);

        std::vector<size_t> positions(chunk.getNumColumns());
        std::iota(positions.begin(), positions.end(), 0);

        doSplitBlockPerf(std::move(chunk), std::move(positions), "Two 16 chars key + 100 row per chunk with 10 subchunks");
    }

    {
        size_t rows = 1000;
        size_t chunks = 30;

        DB::Chunk chunk;
        insertColumnString(chunk, 100, rows, chunks);
        insertColumnString(chunk, 200, rows, chunks);

        std::vector<size_t> positions(chunk.getNumColumns());
        std::iota(positions.begin(), positions.end(), 0);

        doSplitBlockPerf(std::move(chunk), std::move(positions), "Two 16 chars key + 1000 row per chunk with 30 subchunks", 1'000'000);
    }

    /// Three 16 char string keys
    {
        size_t rows = 1;
        size_t chunks = 1;

        DB::Chunk chunk;
        insertColumnString(chunk, 100, rows, chunks);
        insertColumnString(chunk, 200, rows, chunks);
        insertColumnString(chunk, 300, rows, chunks);

        std::vector<size_t> positions(chunk.getNumColumns());
        std::iota(positions.begin(), positions.end(), 0);

        doSplitBlockPerf(std::move(chunk), std::move(positions), "Three 16 chars key + 1 row per chunk");
    }

    {
        size_t rows = 10;
        size_t chunks = 3;

        DB::Chunk chunk;
        insertColumnString(chunk, 100, rows, chunks);
        insertColumnString(chunk, 200, rows, chunks);
        insertColumnString(chunk, 300, rows, chunks);

        std::vector<size_t> positions(chunk.getNumColumns());
        std::iota(positions.begin(), positions.end(), 0);

        doSplitBlockPerf(std::move(chunk), std::move(positions), "Three 16 chars key + 10 row per chunk with 3 subchunks");
    }

    {
        size_t rows = 100;
        size_t chunks = 10;

        DB::Chunk chunk;
        insertColumnString(chunk, 100, rows, chunks);
        insertColumnString(chunk, 200, rows, chunks);
        insertColumnString(chunk, 300, rows, chunks);

        std::vector<size_t> positions(chunk.getNumColumns());
        std::iota(positions.begin(), positions.end(), 0);

        doSplitBlockPerf(std::move(chunk), std::move(positions), "Three 16 chars key + 100 row per chunk with 10 subchunks");
    }

    {
        size_t rows = 1000;
        size_t chunks = 30;

        DB::Chunk chunk;
        insertColumnString(chunk, 100, rows, chunks);
        insertColumnString(chunk, 200, rows, chunks);
        insertColumnString(chunk, 300, rows, chunks);

        std::vector<size_t> positions(chunk.getNumColumns());
        std::iota(positions.begin(), positions.end(), 0);

        doSplitBlockPerf(std::move(chunk), std::move(positions), "Three 16 chars key + 1000 row per chunk with 30 subchunks", 1'000'000);
    }

    /// One uint64_t key
    {
        size_t rows = 1;
        size_t chunks = 1;

        DB::Chunk chunk;
        insertColumnUInt64(chunk, 100, rows, chunks);

        std::vector<size_t> positions(chunk.getNumColumns());
        std::iota(positions.begin(), positions.end(), 0);

        doSplitBlockPerf(std::move(chunk), std::move(positions), "Single uint64_t key + 1 row per chunk");
    }

    {
        size_t rows = 10;
        size_t chunks = 3;

        DB::Chunk chunk;
        insertColumnUInt64(chunk, 100, rows, chunks);

        std::vector<size_t> positions(chunk.getNumColumns());
        std::iota(positions.begin(), positions.end(), 0);

        doSplitBlockPerf(std::move(chunk), std::move(positions), "Single uint64_t key + 10 rows per chunk with 3 subchunks");
    }

    {
        size_t rows = 100;
        size_t chunks = 10;

        DB::Chunk chunk;
        insertColumnUInt64(chunk, 100, rows, chunks);

        std::vector<size_t> positions(chunk.getNumColumns());
        std::iota(positions.begin(), positions.end(), 0);

        doSplitBlockPerf(std::move(chunk), std::move(positions), "Single uint64_t key + 100 rows per chunk with 10 subchunks");
    }

    {
        size_t rows = 1000;
        size_t chunks = 30;

        DB::Chunk chunk;
        insertColumnUInt64(chunk, 100, rows, chunks);

        std::vector<size_t> positions(chunk.getNumColumns());
        std::iota(positions.begin(), positions.end(), 0);
        doSplitBlockPerf(std::move(chunk), std::move(positions), "Single uint64_t key + 1000 rows per chunk with 30 subchunks", 1'000'000);
    }

    /// Two uint64_t key
    {
        size_t rows = 1;
        size_t chunks = 1;

        DB::Chunk chunk;
        insertColumnUInt64(chunk, 100, rows, chunks);
        insertColumnUInt64(chunk, 200, rows, chunks);

        std::vector<size_t> positions(chunk.getNumColumns());
        std::iota(positions.begin(), positions.end(), 0);

        doSplitBlockPerf(std::move(chunk), std::move(positions), "Two uint64_t key + 1 row per chunk");
    }

    {
        size_t rows = 10;
        size_t chunks = 3;

        DB::Chunk chunk;
        insertColumnUInt64(chunk, 100, rows, chunks);
        insertColumnUInt64(chunk, 200, rows, chunks);

        std::vector<size_t> positions(chunk.getNumColumns());
        std::iota(positions.begin(), positions.end(), 0);

        doSplitBlockPerf(std::move(chunk), std::move(positions), "Two uint64_t key + 10 rows per chunk with 3 subchunks");
    }

    {
        size_t rows = 100;
        size_t chunks = 10;

        DB::Chunk chunk;
        insertColumnUInt64(chunk, 100, rows, chunks);
        insertColumnUInt64(chunk, 200, rows, chunks);

        std::vector<size_t> positions(chunk.getNumColumns());
        std::iota(positions.begin(), positions.end(), 0);

        doSplitBlockPerf(std::move(chunk), std::move(positions), "Two uint64_t key + 100 rows per chunk with 10 subchunks");
    }

    {
        size_t rows = 1000;
        size_t chunks = 30;

        DB::Chunk chunk;
        insertColumnUInt64(chunk, 100, rows, chunks);
        insertColumnUInt64(chunk, 200, rows, chunks);

        std::vector<size_t> positions(chunk.getNumColumns());
        std::iota(positions.begin(), positions.end(), 0);
        doSplitBlockPerf(std::move(chunk), std::move(positions), "Two uint64_t key + 1000 rows per chunk with 30 subchunks", 1'000'000);
    }

    /// 16 chars + uint64_t keys
    {
        size_t rows = 1;
        size_t chunks = 1;

        DB::Chunk chunk;
        insertColumnString(chunk, 100, rows, chunks);
        insertColumnUInt64(chunk, 200, rows, chunks);

        std::vector<size_t> positions(chunk.getNumColumns());
        std::iota(positions.begin(), positions.end(), 0);

        doSplitBlockPerf(std::move(chunk), std::move(positions), "One 16 chars key + one uint64_t key + 1 row per chunk");
    }

    {
        size_t rows = 10;
        size_t chunks = 3;

        DB::Chunk chunk;
        insertColumnString(chunk, 100, rows, chunks);
        insertColumnUInt64(chunk, 200, rows, chunks);

        std::vector<size_t> positions(chunk.getNumColumns());
        std::iota(positions.begin(), positions.end(), 0);

        doSplitBlockPerf(
            std::move(chunk), std::move(positions), "One 16 chars key + one uint64_t key + 10 rows per chunk with 3 subchunks");
    }

    {
        size_t rows = 100;
        size_t chunks = 10;

        DB::Chunk chunk;
        insertColumnString(chunk, 100, rows, chunks);
        insertColumnUInt64(chunk, 200, rows, chunks);

        std::vector<size_t> positions(chunk.getNumColumns());
        std::iota(positions.begin(), positions.end(), 0);

        doSplitBlockPerf(
            std::move(chunk), std::move(positions), "One 16 chars key + one uint64_t key + 100 rows per chunk with 10 subchunks");
    }

    {
        size_t rows = 1000;
        size_t chunks = 30;

        DB::Chunk chunk;
        insertColumnString(chunk, 100, rows, chunks);
        insertColumnUInt64(chunk, 200, rows, chunks);

        std::vector<size_t> positions(chunk.getNumColumns());
        std::iota(positions.begin(), positions.end(), 0);
        doSplitBlockPerf(
            std::move(chunk),
            std::move(positions),
            "One 16 chars key + one uint64_t key + 1000 rows per chunk with 30 subchunks",
            1'000'000);
    }
}
}

int main(int, char **)
{
    splitChunkPerf();

    splitBlockPerf();

    return 0;
}
