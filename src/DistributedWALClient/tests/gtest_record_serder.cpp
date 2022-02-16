#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DistributedWALClient/ByteVector.h>
#include <DistributedWALClient/OpCodes.h>
#include <DistributedWALClient/Record.h>
#include <DistributedWALClient/SchemaProvider.h>

#include <gtest/gtest.h>


using namespace DB;
using namespace DWAL;
using namespace std;

extern Block createBlockBig(size_t rows);

namespace
{
std::unique_ptr<Record> createRecord()
{
    Block block;

    auto uint64_type = make_shared<DataTypeUInt64>();
    auto float64_type = make_shared<DataTypeFloat64>();
    auto datetime64_type = make_shared<DataTypeDateTime64>(3);
    auto string_type = make_shared<DataTypeString>();

    auto id_col = uint64_type->createColumn();
    /// auto id_col = make_shared<ColumnInt64>();
    auto * id_col_inner = typeid_cast<ColumnUInt64 *>(id_col.get());
    id_col_inner->insertValue(102);
    id_col_inner->insertValue(101);
    id_col_inner->insertValue(100);

    ColumnWithTypeAndName id_col_with_type{std::move(id_col), uint64_type, "id"};
    block.insert(id_col_with_type);

    auto cpu_col = float64_type->createColumn();
    /// auto cpu_col = make_shared<ColumnFloat64>();
    auto * cpu_col_inner = typeid_cast<ColumnFloat64 *>(cpu_col.get());
    cpu_col_inner->insertValue(13.338);
    cpu_col_inner->insertValue(17.378);
    cpu_col_inner->insertValue(11.539);

    ColumnWithTypeAndName cpu_col_with_type(std::move(cpu_col), float64_type, "cpu");
    block.insert(cpu_col_with_type);

    auto raw_col = string_type->createColumn();
    raw_col->insertData("hello", 5);
    raw_col->insertData("world", 5);
    raw_col->insertData("你好啊", 6);

    ColumnWithTypeAndName raw_col_with_type(std::move(raw_col), string_type, "raw");
    block.insert(raw_col_with_type);

    auto time_col = datetime64_type->createColumn();
    /// auto time_col = make_shared<ColumnDecimal<DateTime64>>;
    auto * time_col_inner = typeid_cast<ColumnDecimal<DateTime64> *>(time_col.get());
    time_col_inner->insertValue(1612286044.256326);
    time_col_inner->insertValue(1612296044.256326);
    time_col_inner->insertValue(1612276044.256326);

    ColumnWithTypeAndName time_col_with_type(std::move(time_col), datetime64_type, "_tp_time");
    block.insert(time_col_with_type);

    return std::make_unique<Record>(OpCode::ADD_DATA_BLOCK, move(block), DWAL::NO_SCHEMA);
}

std::unique_ptr<Record> createRecordBig(size_t rows)
{
    Block block(createBlockBig(rows));
    return std::make_unique<Record>(OpCode::ADD_DATA_BLOCK, move(block), DWAL::NO_SCHEMA);
}

struct TestSchemaProvider : public DWAL::SchemaProvider
{
    TestSchemaProvider() : header(createRecord()->block.cloneEmpty()) { }

    const DB::Block & getSchema(uint16_t /*schema_version*/) const override { return header; }

    DB::Block header;
};

EmptySchemaProvider empty_schema_provider;
SchemaContext empty_schema_ctx(empty_schema_provider);
TestSchemaProvider test_schema_provider;
SchemaContext test_schema_ctx(test_schema_provider);

std::unordered_map<String, std::pair<int64_t, int64_t>> write_bench_results;
std::unordered_map<String, std::pair<int64_t, int64_t>> read_bench_results;

/// Change this base_iterations to 1000 to do benchmark for longer time
constexpr int32_t base_iterations = 10;

void writeBench(const Record & r, const String & test_case, int32_t n = 10000000)
{
    auto write_loop = [&](DB::CompressionMethodByte codec) {
        for (auto i = 0; i < n; i++)
            Record::write(r, codec);
    };

    auto t = std::chrono::high_resolution_clock::now();
    write_loop(DB::CompressionMethodByte::NONE);
    std::chrono::duration<double> duration0 = std::chrono::high_resolution_clock::now() - t;

    t = std::chrono::high_resolution_clock::now();
    write_loop(DB::CompressionMethodByte::LZ4);
    std::chrono::duration<double> duration1 = std::chrono::high_resolution_clock::now() - t;
    std::cout << test_case << ", write uncompressed: " << int32_t(n * r.block.rows() / duration0.count())
              << " eps, write compressed: " << int32_t(n * r.block.rows() / duration1.count()) << " eps" << std::endl;

    ASSERT_FALSE(write_bench_results.contains(test_case));
    write_bench_results[test_case] = {int64_t(n * r.block.rows() / duration0.count()), int64_t(n * r.block.rows() / duration1.count())};
}

void readBench(const Record & r, const SchemaContext & schema_ctx, const String & test_case, int32_t n = 1000000)
{
    auto read_loop = [&](DB::CompressionMethodByte codec) {
        ByteVector data{Record::write(r, codec)};
        for (auto i = 0; i < n; i++)
            Record::read(reinterpret_cast<char *>(data.data()), data.size(), schema_ctx);
    };

    auto t = std::chrono::high_resolution_clock::now();
    read_loop(DB::CompressionMethodByte::NONE);
    std::chrono::duration<double> duration0 = std::chrono::high_resolution_clock::now() - t;

    t = std::chrono::high_resolution_clock::now();
    read_loop(DB::CompressionMethodByte::LZ4);
    std::chrono::duration<double> duration1 = std::chrono::high_resolution_clock::now() - t;
    std::cout << test_case << ", read uncompressed: " << int32_t(n * r.block.rows() / duration0.count())
              << " eps, read compressed: " << int32_t(n * r.block.rows() / duration1.count()) << " eps" << std::endl;

    ASSERT_FALSE(read_bench_results.contains(test_case));
    read_bench_results[test_case] = {int64_t(n * r.block.rows() / duration0.count()), int64_t(n * r.block.rows() / duration1.count())};
}

void checkRecord(const std::vector<String> & cols, const Record & expect, const Record & actual)
{
    EXPECT_EQ(expect.op_code, actual.op_code);

    SipHash hash_expected;
    expect.block.updateHash(hash_expected);

    SipHash hash_got;
    actual.block.updateHash(hash_got);

    EXPECT_EQ(hash_expected.get64(), hash_got.get64());

    /// Compare columns

    for (const auto & col_name : cols)
    {
        const auto & col_expected = expect.block.getByName(col_name);
        const auto & col_got = actual.block.getByName(col_name);

        EXPECT_EQ(col_expected.name, col_got.name);
        EXPECT_EQ(col_expected.type->getTypeId(), col_got.type->getTypeId());
        EXPECT_EQ(col_expected.column->size(), 3);
        EXPECT_EQ(col_expected.column->size(), col_got.column->size());

        for (size_t i = 0; i < col_got.column->size(); ++i)
            EXPECT_EQ(col_expected.column->compareAt(i, i, *col_got.column, -1), 0);
    }
}
}

TEST(RecordSerde, Native)
{
    auto r = createRecord();
    ByteVector data{Record::write(*r)};
    auto rr = Record::read(reinterpret_cast<char *>(data.data()), data.size(), empty_schema_ctx);
    auto cols = std::vector<String>{"id", "cpu", "raw", "_tp_time"};

    checkRecord(cols, *r, *rr);
}

TEST(RecordSerde, NativeCompression)
{
    auto r = createRecord();
    ByteVector data{Record::write(*r, DB::CompressionMethodByte::LZ4)};

    auto rr = Record::read(reinterpret_cast<char *>(data.data()), data.size(), empty_schema_ctx);
    auto cols = std::vector<String>{"id", "cpu", "raw", "_tp_time"};

    checkRecord(cols, *r, *rr);
}

TEST(RecordSerde, NativeInSchema)
{
    auto r = createRecord();
    /// force a schema
    r->schema_version = 0;
    test_schema_ctx.column_positions.clear();

    ByteVector data{Record::write(*r)};
    auto rr = Record::read(reinterpret_cast<char *>(data.data()), data.size(), test_schema_ctx);
    auto cols = std::vector<String>{"id", "cpu", "raw", "_tp_time"};

    checkRecord(cols, *r, *rr);
}

TEST(RecordSerde, NativeInSchemaCompression)
{
    auto r = createRecord();
    /// force a schema
    r->schema_version = 0;
    test_schema_ctx.column_positions.clear();

    ByteVector data{Record::write(*r, DB::CompressionMethodByte::LZ4)};

    auto rr = Record::read(reinterpret_cast<char *>(data.data()), data.size(), test_schema_ctx);
    auto cols = std::vector<String>{"id", "cpu", "raw", "_tp_time"};

    checkRecord(cols, *r, *rr);
}

TEST(RecordSerde, WriteBenchNative)
{
    auto r = createRecord();
    writeBench(*r, "Native");
}

TEST(RecordSerde, ReadBenchmarkNative)
{
    auto r = createRecord();
    readBench(*r, empty_schema_ctx, "Native");
}

TEST(RecordSerde, WriteBenchmarkNativeInSchema)
{
    auto r = createRecord();
    /// force a schema
    r->schema_version = 0;
    writeBench(*r, "NativeInSchema");
}

TEST(RecordSerde, ReadBenchmarkNativeInSchema)
{
    auto r{createRecord()};
    /// force a schema
    r->schema_version = 0;
    test_schema_ctx.column_positions.clear();

    readBench(*r, test_schema_ctx, "NativeInSchema");
}

TEST(RecordSerde, ReadBenchmarkNativeInSchemaSkip)
{
    auto r{createRecord()};
    /// force a schema
    r->schema_version = 0;
    /// only read the second column
    test_schema_ctx.column_positions = {1};

    readBench(*r, test_schema_ctx, "NativeInSchemaSkip");
}

TEST(RecordSerde, WriteBenchNativeBig)
{
    std::vector<std::pair<int32_t, int32_t>> rows_iterations{
        {1, 100 * base_iterations}, {10, 100 * base_iterations}, {100, 100 * base_iterations}, {1000, 100 * base_iterations}};
    for (auto [rows, iterations] : rows_iterations)
    {
        auto r = createRecordBig(rows);
        writeBench(*r, "NativeBig_" + std::to_string(rows), iterations);
    }
}

TEST(RecordSerde, ReadBenchmarkNativeBig)
{
    std::vector<std::pair<int32_t, int32_t>> rows_iterations{
        {1, 10 * base_iterations}, {10, 10 * base_iterations}, {100, 10 * base_iterations}, {1000, 10 * base_iterations}};
    for (auto [rows, iterations] : rows_iterations)
    {
        auto r = createRecordBig(rows);
        readBench(*r, empty_schema_ctx, "NativeBig_" + std::to_string(rows), iterations);
    }
}

TEST(RecordSerde, WriteBenchmarkNativeInSchemaBig)
{
    std::vector<std::pair<int32_t, int32_t>> rows_iterations{
        {1, 100 * base_iterations}, {10, 100 * base_iterations}, {100, 100 * base_iterations}, {1000, 100 * base_iterations}};
    for (auto [rows, iterations] : rows_iterations)
    {
        auto r = createRecordBig(rows);
        /// force a schema
        r->schema_version = 0;
        writeBench(*r, "NativeInSchemaBig_" + std::to_string(rows), iterations);
    }
}

TEST(RecordSerde, ReadBenchmarkNativeInSchemaBig)
{
    std::vector<std::pair<int32_t, int32_t>> rows_iterations{
        {1, 100 * base_iterations}, {10, 100 * base_iterations}, {100, 100 * base_iterations}, {1000, 100 * base_iterations}};
    for (auto [rows, iterations] : rows_iterations)
    {
        auto r{createRecordBig(rows)};
        /// force a schema
        r->schema_version = 0;
        test_schema_ctx.column_positions.clear();
        readBench(*r, test_schema_ctx, "NativeInSchemaBig_" + std::to_string(rows), iterations);
    }
}

TEST(RecordSerde, ReadBenchmarkNativeInSchemaSkipBig)
{
    std::vector<std::pair<int32_t, int32_t>> rows_iterations{
        {1, 1000 * base_iterations}, {10, 1000 * base_iterations}, {100, 1000 * base_iterations}, {1000, 1000 * base_iterations}};
    for (auto [rows, iterations] : rows_iterations)
    {
        auto r{createRecordBig(rows)};
        /// force a schema
        r->schema_version = 0;
        /// only read the second column
        test_schema_ctx.column_positions = {1};

        readBench(*r, test_schema_ctx, "NativeInSchemaSkipBig_" + std::to_string(rows), iterations);
    }
}

TEST(RecordSerde, Summary)
{
    {
        ASSERT_TRUE(write_bench_results.contains("Native"));
        const auto & write_native = write_bench_results["Native"];

        ASSERT_TRUE(write_bench_results.contains("NativeInSchema"));
        const auto & write_native_in_schema = write_bench_results["NativeInSchema"];

        ASSERT_TRUE(read_bench_results.contains("Native"));
        const auto & read_native = read_bench_results["Native"];

        ASSERT_TRUE(read_bench_results.contains("NativeInSchema"));
        const auto & read_native_in_schema = read_bench_results["NativeInSchema"];

        ASSERT_TRUE(read_bench_results.contains("NativeInSchemaSkip"));
        const auto & read_native_in_schema_skip = read_bench_results["NativeInSchemaSkip"];

        auto r = createRecord();
        std::cout << fmt::format("Small block with rows={} columns={}:", r->block.rows(), r->block.columns()) << "\n"
                  << fmt::format(
                         "Uncompression Native-Write / Compression Native-Write: {}/{} = {:.2f}",
                         write_native.first,
                         write_native.second,
                         (write_native.first * 1.0) / write_native.second)
                  << "\n"
                  << fmt::format(
                         "Uncompression Native-Write-In-Schema / Compression Native-Write-In-Schema: {}/{} = {:.2f}",
                         write_native_in_schema.first,
                         write_native_in_schema.second,
                         (write_native_in_schema.first * 1.0) / write_native_in_schema.second)
                  << "\n"
                  << fmt::format(
                         "Uncompression Native-Write-In-Schema / Uncompression Native-Write: {}/{} = {:.2f}",
                         write_native_in_schema.first,
                         write_native.first,
                         (write_native_in_schema.first * 1.0) / write_native.first)
                  << "\n"
                  << fmt::format(
                         "Compression Native-Write-In-Schema / Compression Native-Write: {}/{} = {:.2f}",
                         write_native_in_schema.second,
                         write_native.second,
                         (write_native_in_schema.second * 1.0) / write_native.second)
                  << "\n----------\n"
                  << fmt::format(
                         "Uncompression Native-Read / Compression Native-Read: {}/{} = {:.2f}",
                         read_native.first,
                         read_native.second,
                         (read_native.first * 1.0) / read_native.second)
                  << "\n"
                  << fmt::format(
                         "Uncompression Native-Read-In-Schema / Compression Native-Read-In-Schema: {}/{} = {:.2f}",
                         read_native_in_schema.first,
                         read_native_in_schema.second,
                         (read_native_in_schema.first * 1.0) / read_native_in_schema.second)
                  << "\n"
                  << fmt::format(
                         "Uncompression Native-Read-In-Schema-Skip / Compression Native-Read-In-Schema-Skip: {}/{} = {:.2f}",
                         read_native_in_schema_skip.first,
                         read_native_in_schema_skip.second,
                         (read_native_in_schema_skip.first * 1.0) / read_native_in_schema_skip.second)
                  << "\n"
                  << fmt::format(
                         "Uncompression Native-Read-In-Schema / Uncompression Native-Read: {}/{} = {:.2f}",
                         read_native_in_schema.first,
                         read_native.first,
                         (read_native_in_schema.first * 1.0) / read_native.first)
                  << "\n"
                  << fmt::format(
                         "Uncompression Native-Read-In-Schema-Skip / Uncompression Native-Read: {}/{} = {:.2f}",
                         read_native_in_schema_skip.first,
                         read_native.first,
                         (read_native_in_schema_skip.first * 1.0) / read_native.first)
                  << "\n"
                  << fmt::format(
                         "Compression Native-Read-In-Schema / Compression Native-Read: {}/{} = {:.2f}",
                         read_native_in_schema.second,
                         read_native.second,
                         (read_native_in_schema.second * 1.0) / read_native.second)
                  << "\n"
                  << fmt::format(
                         "Compression Native-Read-In-Schema-Skip / Compression Native-Read: {}/{} = {:.2f}",
                         read_native_in_schema_skip.second,
                         read_native.second,
                         (read_native_in_schema_skip.second * 1.0) / read_native.second)
            << "\n***********\n";
    }

    {
        auto block{createBlockBig(1)};

        for (auto rows : {1, 10, 100, 1000})
        {
            auto rows_str = std::to_string(rows);

            ASSERT_TRUE(write_bench_results.contains("NativeBig_" + rows_str));
            const auto & write_native = write_bench_results["NativeBig_" + rows_str];

            ASSERT_TRUE(write_bench_results.contains("NativeInSchemaBig_" + rows_str));
            const auto & write_native_in_schema = write_bench_results["NativeInSchemaBig_" + rows_str];

            ASSERT_TRUE(read_bench_results.contains("NativeBig_" + rows_str));
            const auto & read_native = read_bench_results["NativeBig_" + rows_str];

            ASSERT_TRUE(read_bench_results.contains("NativeInSchemaBig_" + rows_str));
            const auto & read_native_in_schema = read_bench_results["NativeInSchemaBig_" + rows_str];

            ASSERT_TRUE(read_bench_results.contains("NativeInSchemaSkipBig_" + rows_str));
            const auto & read_native_in_schema_skip = read_bench_results["NativeInSchemaSkipBig_" + rows_str];

            std::cout << fmt::format("Big block with rows={} columns={}:", rows, block.columns()) << "\n"
                      << fmt::format(
                             "Uncompression Native-Write / Compression Native-Write: {}/{} = {:.2f}",
                             write_native.first,
                             write_native.second,
                             (write_native.first * 1.0) / write_native.second)
                      << "\n"
                      << fmt::format(
                             "Uncompression Native-Write-In-Schema / Compression Native-Write-In-Schema: {}/{} = {:.2f}",
                             write_native_in_schema.first,
                             write_native_in_schema.second,
                             (write_native_in_schema.first * 1.0) / write_native_in_schema.second)
                      << "\n"
                      << fmt::format(
                             "Uncompression Native-Write-In-Schema / Uncompression Native-Write: {}/{} = {:.2f}",
                             write_native_in_schema.first,
                             write_native.first,
                             (write_native_in_schema.first * 1.0) / write_native.first)
                      << "\n"
                      << fmt::format(
                             "Compression Native-Write-In-Schema / Compression Native-Write: {}/{} = {:.2f}",
                             write_native_in_schema.second,
                             write_native.second,
                             (write_native_in_schema.second * 1.0) / write_native.second)
                      << "\n----------\n"
                      << fmt::format(
                             "Uncompression Native-Read / Compression Native-Read: {}/{} = {:.2f}",
                             read_native.first,
                             read_native.second,
                             (read_native.first * 1.0) / read_native.second)
                      << "\n"
                      << fmt::format(
                             "Uncompression Native-Read-In-Schema / Compression Native-Read-In-Schema: {}/{} = {:.2f}",
                             read_native_in_schema.first,
                             read_native_in_schema.second,
                             (read_native_in_schema.first * 1.0) / read_native_in_schema.second)
                      << "\n"
                      << fmt::format(
                             "Uncompression Native-Read-In-Schema-Skip / Compression Native-Read-In-Schema-Skip: {}/{} = {:.2f}",
                             read_native_in_schema_skip.first,
                             read_native_in_schema_skip.second,
                             (read_native_in_schema_skip.first * 1.0) / read_native_in_schema_skip.second)
                      << "\n"
                      << fmt::format(
                             "Uncompression Native-Read-In-Schema / Uncompression Native-Read: {}/{} = {:.2f}",
                             read_native_in_schema.first,
                             read_native.first,
                             (read_native_in_schema.first * 1.0) / read_native.first)
                      << "\n"
                      << fmt::format(
                             "Uncompression Native-Read-In-Schema-Skip / Uncompression Native-Read: {}/{} = {:.2f}",
                             read_native_in_schema_skip.first,
                             read_native.first,
                             (read_native_in_schema_skip.first * 1.0) / read_native.first)
                      << "\n"
                      << fmt::format(
                             "Compression Native-Read-In-Schema / Compression Native-Read: {}/{} = {:.2f}",
                             read_native_in_schema.second,
                             read_native.second,
                             (read_native_in_schema.second * 1.0) / read_native.second)
                      << "\n"
                      << fmt::format(
                             "Compression Native-Read-In-Schema-Skip / Compression Native-Read: {}/{} = {:.2f}",
                             read_native_in_schema_skip.second,
                             read_native.second,
                             (read_native_in_schema_skip.second * 1.0) / read_native.second)
                      << "\n==========\n";
        }
    }
}
