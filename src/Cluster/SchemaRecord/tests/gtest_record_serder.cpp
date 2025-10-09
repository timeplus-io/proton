#include <Cluster/Base/ByteVector.h>
#include <Cluster/SchemaRecord/SchemaContext.h>
#include <Cluster/SchemaRecord/SchemaRecord.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

#include <gtest/gtest.h>

extern DB::Block createBlockBig(size_t rows);
extern DB::Block createBlockForCompress(size_t rows, bool only_string);

namespace
{
std::unique_ptr<cluster::SchemaRecord> createRecord()
{
    DB::Block block;

    auto uint64_type = std::make_shared<DB::DataTypeUInt64>();
    auto float64_type = std::make_shared<DB::DataTypeFloat64>();
    auto datetime64_type = std::make_shared<DB::DataTypeDateTime64>(3);
    auto string_type = std::make_shared<DB::DataTypeString>();

    auto id_col = uint64_type->createColumn();
    /// auto id_col = make_shared<ColumnInt64>();
    auto * id_col_inner = typeid_cast<DB::ColumnUInt64 *>(id_col.get());
    id_col_inner->insertValue(102);
    id_col_inner->insertValue(101);
    id_col_inner->insertValue(100);

    DB::ColumnWithTypeAndName id_col_with_type{std::move(id_col), uint64_type, "id"};
    block.insert(id_col_with_type);

    auto cpu_col = float64_type->createColumn();
    /// auto cpu_col = make_shared<ColumnFloat64>();
    auto * cpu_col_inner = typeid_cast<DB::ColumnFloat64 *>(cpu_col.get());
    cpu_col_inner->insertValue(13.338);
    cpu_col_inner->insertValue(17.378);
    cpu_col_inner->insertValue(11.539);

    DB::ColumnWithTypeAndName cpu_col_with_type(std::move(cpu_col), float64_type, "cpu");
    block.insert(cpu_col_with_type);

    auto raw_col = string_type->createColumn();
    raw_col->insertData("hello", 5);
    raw_col->insertData("world", 5);
    raw_col->insertData("你好啊", 6);

    DB::ColumnWithTypeAndName raw_col_with_type(std::move(raw_col), string_type, "raw");
    block.insert(raw_col_with_type);

    auto time_col = datetime64_type->createColumn();
    /// auto time_col = make_shared<ColumnDecimal<DateTime64>>;
    auto * time_col_inner = typeid_cast<DB::ColumnDecimal<DB::DateTime64> *>(time_col.get());
    time_col_inner->insertValue(static_cast<int64_t>(1612286044.256326));
    time_col_inner->insertValue(static_cast<int64_t>(1612296044.256326));
    time_col_inner->insertValue(static_cast<int64_t>(1612276044.256326));

    DB::ColumnWithTypeAndName time_col_with_type(std::move(time_col), datetime64_type, "_tp_time");
    block.insert(time_col_with_type);

    return std::make_unique<cluster::SchemaRecord>(cluster::protocol::OpCode::InsertData, std::move(block), cluster::Nulls::NullVersion);
}

std::unique_ptr<cluster::SchemaRecord> createRecordBig(size_t rows)
{
    DB::Block block(createBlockBig(rows));
    return std::make_unique<cluster::SchemaRecord>(cluster::protocol::OpCode::InsertData, std::move(block), cluster::Nulls::NullVersion);
}

struct TestSchemaProvider : public cluster::SchemaProvider
{
    TestSchemaProvider() : header(createRecord()->getBlock().cloneEmpty()) { }

    const DB::Block & getSchema(const std::string &, const DB::UUID &, uint16_t /*schema_version*/) const override { return header; }

    DB::Block header;
};

struct TestSchemaBigProvider : public cluster::SchemaProvider
{
    explicit TestSchemaBigProvider(size_t rows) : header(createRecordBig(rows)->getBlock().cloneEmpty()) { }

    const DB::Block & getSchema(const std::string &, const DB::UUID &, uint16_t /*schema_version*/) const override { return header; }

    DB::Block header;
};

cluster::EmptySchemaProvider empty_schema_provider;
cluster::SchemaContext empty_schema_ctx(empty_schema_provider);

TestSchemaProvider & getTestSchemaProvider()
{
    static TestSchemaProvider test_schema_provider;
    return test_schema_provider;
}

cluster::SchemaContext & getTestSchemaContext()
{
    static cluster::SchemaContext test_schema_ctx(getTestSchemaProvider());

    return test_schema_ctx;
}

TestSchemaBigProvider & getTestSchemaBigProvider()
{
    static TestSchemaBigProvider test_schema_big_provider(1);
    return test_schema_big_provider;
}

cluster::SchemaContext & getTestSchemaBigContext()
{
    static cluster::SchemaContext test_schema_big_ctx(getTestSchemaBigProvider());
    return test_schema_big_ctx;
}

std::unordered_map<String, std::pair<int64_t, int64_t>> write_bench_results;
std::unordered_map<String, std::pair<int64_t, int64_t>> read_bench_results;

/// Change this base_iterations to 1000 to do benchmark for longer time
constexpr int32_t base_iterations = 10;

void writeBench(cluster::SchemaRecord & r, const String & test_case, int32_t n = 10)
{
    auto write_loop = [&](DB::CompressionMethodByte codec) {
        r.setCompressionCodec(codec);
        for (auto i = 0; i < n; i++)
            r.serialize();
    };

    const auto & block = r.getBlock();

    auto t = std::chrono::high_resolution_clock::now();
    write_loop(DB::CompressionMethodByte::NONE);
    std::chrono::duration<double> duration0 = std::chrono::high_resolution_clock::now() - t;

    t = std::chrono::high_resolution_clock::now();
    write_loop(DB::CompressionMethodByte::LZ4);
    std::chrono::duration<double> duration1 = std::chrono::high_resolution_clock::now() - t;
    std::cout << test_case << ", write uncompressed: " << int32_t(n * block.rows() / duration0.count())
              << " eps, write compressed: " << int32_t(n * block.rows() / duration1.count()) << " eps" << std::endl;

    ASSERT_FALSE(write_bench_results.contains(test_case));
    write_bench_results[test_case] = {int64_t(n * block.rows() / duration0.count()), int64_t(n * block.rows() / duration1.count())};
}

void readBench(cluster::SchemaRecord & r, const cluster::SchemaContext & schema_ctx, const String & test_case, int32_t n = 10)
{
    auto read_loop = [&](DB::CompressionMethodByte codec) {
        r.setCompressionCodec(codec);
        cluster::ByteVector data{r.serialize()};
        for (auto i = 0; i < n; i++)
            cluster::SchemaRecord::parse(data.stringView(), schema_ctx);
    };

    const auto & block = r.getBlock();

    auto t = std::chrono::high_resolution_clock::now();
    read_loop(DB::CompressionMethodByte::NONE);
    std::chrono::duration<double> duration0 = std::chrono::high_resolution_clock::now() - t;

    t = std::chrono::high_resolution_clock::now();
    read_loop(DB::CompressionMethodByte::LZ4);
    std::chrono::duration<double> duration1 = std::chrono::high_resolution_clock::now() - t;
    std::cout << test_case << ", read uncompressed: " << int32_t(n * block.rows() / duration0.count())
              << " eps, read compressed: " << int32_t(n * block.rows() / duration1.count()) << " eps" << std::endl;

    ASSERT_FALSE(read_bench_results.contains(test_case));
    read_bench_results[test_case] = {int64_t(n * block.rows() / duration0.count()), int64_t(n * block.rows() / duration1.count())};
}

void checkRecord(const std::vector<String> & cols, const cluster::SchemaRecord & expect, const cluster::SchemaRecord & actual)
{
    EXPECT_EQ(expect.getFlags(), actual.getFlags());

    SipHash hash_expected;
    expect.getBlock().updateHash(hash_expected);

    SipHash hash_got;
    actual.getBlock().updateHash(hash_got);

    EXPECT_EQ(hash_expected.get64(), hash_got.get64());

    /// Compare columns

    for (const auto & col_name : cols)
    {
        const auto & col_expected = expect.getBlock().getByName(col_name);
        const auto & col_got = actual.getBlock().getByName(col_name);

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
    cluster::ByteVector data{r->serialize()};
    auto rr = cluster::SchemaRecord::parse(data.stringView(), empty_schema_ctx);
    auto cols = std::vector<String>{"id", "cpu", "raw", "_tp_time"};

    checkRecord(cols, *r, *rr);
}

TEST(RecordSerde, NativeCompression)
{
    auto r = createRecord();
    r->setCompressionCodec(DB::CompressionMethodByte::LZ4);
    cluster::ByteVector data{r->serialize()};

    auto rr = cluster::SchemaRecord::parse(data.stringView(), empty_schema_ctx);
    auto cols = std::vector<String>{"id", "cpu", "raw", "_tp_time"};

    checkRecord(cols, *r, *rr);
}

TEST(RecordSerde, NativeInSchema)
{
    auto r = createRecord();
    /// force a schema
    r->setSchemaVersion(0);
    getTestSchemaContext().column_positions_requested.clear();

    cluster::ByteVector data{r->serialize()};
    auto rr = cluster::SchemaRecord::parse(data.stringView(), getTestSchemaContext());
    auto cols = std::vector<String>{"id", "cpu", "raw", "_tp_time"};

    checkRecord(cols, *r, *rr);
}

TEST(RecordSerde, NativeInSchemaCompression)
{
    auto r = createRecord();
    /// force a schema
    r->setSchemaVersion(0);
    getTestSchemaContext().column_positions_requested.clear();

    r->setCompressionCodec(DB::CompressionMethodByte::LZ4);
    cluster::ByteVector data{r->serialize()};

    auto rr = cluster::SchemaRecord::parse(data.stringView(), getTestSchemaContext());
    auto cols = std::vector<String>{"id", "cpu", "raw", "_tp_time"};

    checkRecord(cols, *r, *rr);
}

//void batchDeserialize(DB::CompressionMethodByte codec, const cluster::SchemaContext & schema_ctx)
//{
//    auto r = createRecord();
//    r->setCodec(codec);
//
//    cluster::ByteVector data{r->serialize()};
//
//    auto cols = std::vector<String>{"id", "cpu", "raw", "_tp_time"};
//
//    {
//        cluster::SchemaRecordPtrs records;
//
//        cluster::SchemaRecord::parse(data.data(), 1, records, schema_ctx);
//        ASSERT_EQ(records.size(), 0);
//
//        cluster::SchemaRecord::deserialize(data.data(), cluster::SchemaRecord::metadataBytes(), records, schema_ctx);
//        ASSERT_EQ(records.size(), 0);
//
//        cluster::SchemaRecord::deserialize(data.data(), data.size(), records, schema_ctx);
//        ASSERT_EQ(records.size(), 1);
//        checkRecord(cols, *r, *records[0]);
//    }
//
//    {
//        for (auto n : {2, 3, 4})
//        {
//            std::vector<char> batch_data;
//            batch_data.reserve(data.size() * n);
//            for (auto i = 0; i < n; ++i)
//                for (size_t pos = 0; pos < data.size(); ++pos)
//                    batch_data.push_back(data[pos]);
//
//            if (n == 4)
//            {
//                /// push some garbage at end
//                batch_data.push_back('a');
//                batch_data.push_back('b');
//                batch_data.push_back('c');
//            }
//
//            cluster::SchemaRecordPtrs records;
//            auto deserialized = cluster::SchemaRecord::deserialize(batch_data.data(), batch_data.size(), records, schema_ctx);
//            ASSERT_EQ(records.size(), n);
//
//            if (n == 4)
//                ASSERT_EQ(deserialized + 3, batch_data.size());
//            else
//                ASSERT_EQ(deserialized, batch_data.size());
//
//            for (const auto & rr : records)
//                checkRecord(cols, *r, *rr);
//        }
//    }
//}
//
//TEST(RecordSerde, NativeBatch)
//{
//    batchDeserialize(DB::CompressionMethodByte::NONE, empty_schema_ctx);
//}
//
//TEST(RecordSerde, NativeCompressionBatch)
//{
//    batchDeserialize(DB::CompressionMethodByte::LZ4, empty_schema_ctx);
//}
//
//TEST(RecordSerde, NativeInSchemaBatch)
//{
//    auto r = createRecord();
//    /// force a schema
//    r->setSchemaVersion(0);
//    getTestSchemaContext().column_positions_requested.clear();
//
//    batchDeserialize(DB::CompressionMethodByte::NONE, getTestSchemaContext());
//}
//
//TEST(RecordSerde, NativeInSchemaCompressionBatch)
//{
//    auto r = createRecord();
//    /// force a schema
//    r->setSchemaVersion(0);
//    getTestSchemaContext().column_positions_requested.clear();
//    batchDeserialize(DB::CompressionMethodByte::LZ4, getTestSchemaContext());
//}

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
    r->setSchemaVersion(0);
    writeBench(*r, "NativeInSchema");
}

TEST(RecordSerde, ReadBenchmarkNativeInSchema)
{
    auto r{createRecord()};
    /// force a schema
    r->setSchemaVersion(0);
    getTestSchemaContext().column_positions_requested.clear();

    readBench(*r, getTestSchemaContext(), "NativeInSchema");
}

TEST(RecordSerde, ReadBenchmarkNativeInSchemaSkip)
{
    auto r{createRecord()};
    /// force a schema
    r->setSchemaVersion(0);
    /// only read the second column
    getTestSchemaContext().column_positions_requested = {1};

    readBench(*r, getTestSchemaContext(), "NativeInSchemaSkip");
}

TEST(RecordSerde, WriteBenchNativeBig)
{
    constexpr int32_t rows = 1, iterations = base_iterations;
    auto r = createRecordBig(rows);
    writeBench(*r, "NativeBig_" + std::to_string(rows), iterations);
}

TEST(RecordSerde, ReadBenchmarkNativeBig)
{
    constexpr int32_t rows = 1, iterations = base_iterations;

    auto r = createRecordBig(rows);
    readBench(*r, empty_schema_ctx, "NativeBig_" + std::to_string(rows), iterations);
}

TEST(RecordSerde, WriteBenchmarkNativeInSchemaBig)
{
    constexpr int32_t rows = 1, iterations = base_iterations;

    auto r = createRecordBig(rows);
    /// force a schema
    r->setSchemaVersion(0);
    writeBench(*r, "NativeInSchemaBig_" + std::to_string(rows), iterations);
}

TEST(RecordSerde, ReadBenchmarkNativeInSchemaBig)
{
    constexpr int32_t rows = 1, iterations = base_iterations;

    auto r{createRecordBig(rows)};
    /// force a schema
    r->setSchemaVersion(0);

    getTestSchemaBigContext().column_positions_requested.clear();
    readBench(*r, getTestSchemaBigContext(), "NativeInSchemaBig_" + std::to_string(rows), iterations);
}

TEST(RecordSerde, ReadBenchmarkNativeInSchemaSkipBig)
{
    constexpr int32_t rows = 1, iterations = base_iterations;
    auto r{createRecordBig(rows)};
    /// force a schema
    r->setSchemaVersion(0);
    /// only read the second column
    getTestSchemaBigContext().column_positions_requested = {1};
    readBench(*r, getTestSchemaBigContext(), "NativeInSchemaSkipBig_" + std::to_string(rows), iterations);
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
        std::cout << fmt::format("Small block with rows={} columns={}:", r->getBlock().rows(), r->getBlock().columns()) << "\n"
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

        for (auto rows : {1})
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

std::unique_ptr<cluster::SchemaRecord> createRecordForCompress(size_t rows, bool only_string)
{
    DB::Block block(createBlockForCompress(rows, only_string));
    return std::make_unique<cluster::SchemaRecord>(cluster::protocol::OpCode::InsertData, std::move(block), cluster::Nulls::NullVersion);
}

void compressTest(size_t rows, size_t n, bool only_string)
{
    auto r = createRecordForCompress(n, only_string);
    r->setCompressionCodec(DB::CompressionMethodByte::NONE);

    auto request_count = rows / n;
    auto count = request_count;
    size_t uncompressed_bytes = 0;
    auto t = std::chrono::high_resolution_clock::now();
    while (count--)
        uncompressed_bytes += r->serialize().size();

    std::chrono::duration<double> uncompressed_duration = std::chrono::high_resolution_clock::now() - t;
    auto uncompressed_eps = static_cast<uint64_t>(rows / uncompressed_duration.count());
    auto uncompressed_qps = static_cast<uint64_t>(request_count / uncompressed_duration.count());

    r->setCompressionCodec(DB::CompressionMethodByte::LZ4);
    count = request_count;
    size_t compressed_lz4_bytes = 0;
    t = std::chrono::high_resolution_clock::now();
    while (count--)
        compressed_lz4_bytes += r->serialize().size();

    std::chrono::duration<double> compressed_lz4_duration = std::chrono::high_resolution_clock::now() - t;
    auto lz4_ratio = static_cast<uint64_t>(static_cast<double>(compressed_lz4_bytes) * 10000 / uncompressed_bytes);
    auto compressed_lz4_eps = static_cast<uint64_t>(rows / compressed_lz4_duration.count());
    auto compressed_lz4_qps = static_cast<uint64_t>(request_count / compressed_lz4_duration.count());
    auto lz4_eps_ratio = static_cast<uint64_t>(static_cast<double>(compressed_lz4_eps) * 10000 / uncompressed_eps);
    auto lz4_qps_ratio = static_cast<uint64_t>(static_cast<double>(compressed_lz4_qps) * 10000 / uncompressed_qps);

    r->setCompressionCodec(DB::CompressionMethodByte::ZSTD);
    count = request_count;
    size_t compressed_zstd_bytes = 0;
    t = std::chrono::high_resolution_clock::now();
    while (count--)
        compressed_zstd_bytes += r->serialize().size();
    std::chrono::duration<double> compressed_zstd_duration = std::chrono::high_resolution_clock::now() - t;
    auto zstd_ratio = static_cast<uint64_t>(static_cast<double>(compressed_zstd_bytes) * 10000 / uncompressed_bytes);
    auto compressed_zstd_eps = static_cast<uint64_t>(rows / compressed_zstd_duration.count());
    auto zstd_eps_ratio = static_cast<uint64_t>(static_cast<double>(compressed_zstd_eps) * 10000 / uncompressed_eps);
    auto compressed_zstd_qps = static_cast<uint64_t>(request_count / compressed_zstd_duration.count());
    auto zstd_qps_ratio = static_cast<uint64_t>(static_cast<double>(compressed_zstd_qps) * 10000 / uncompressed_qps);

    std::cout << fmt::format("Total rows={}, per request block rows={}, ", rows, n) << "\n"
              << fmt::format(
                     " Non -compressed data total bytes: {}, eps: {}, qps: {}", uncompressed_bytes, uncompressed_eps, uncompressed_qps)
              << "\n"
              << fmt::format(
                     " LZ4 -compressed data total bytes: {}, eps: {}, qps: {}, compression ratio: {}.{}%, compression eps ratio: {}.{}%, "
                     "compression qps ratio: {}.{}%",
                     compressed_lz4_bytes,
                     compressed_lz4_eps,
                     compressed_lz4_qps,
                     lz4_ratio / 100,
                     lz4_ratio % 100,
                     lz4_eps_ratio / 100,
                     lz4_eps_ratio % 100,
                     lz4_qps_ratio / 100,
                     lz4_qps_ratio % 100)
              << "\n"
              << fmt::format(
                     " ZSTD-compressed data total bytes: {}, eps: {}, qps: {}, compression ratio: {}.{}%, compression eps ratio: {}.{}%, "
                     "compression qps ratio: {}.{}%",
                     compressed_zstd_bytes,
                     compressed_zstd_eps,
                     compressed_zstd_qps,
                     zstd_ratio / 100,
                     zstd_ratio % 100,
                     zstd_eps_ratio / 100,
                     zstd_eps_ratio % 100,
                     zstd_qps_ratio / 100,
                     zstd_qps_ratio % 100)
              << "\n\n";
}

TEST(RecordSerde, BenchmarkCompressionNative)
{
    /// Complex
    compressTest(100000, 100000, false);

    /// String
    compressTest(100000, 100000, true);
}
