#include <DistributedWALClient/ByteVector.h>
#include <DistributedWALClient/OpCodes.h>
#include <DistributedWALClient/Record.h>
#include <DistributedWALClient/SchemaProvider.h>

#include <gtest/gtest.h>

extern DB::Block createBlockBig(size_t rows);

namespace
{
std::unique_ptr<DWAL::Record> createRecordBig(size_t rows)
{
    DB::Block block(createBlockBig(rows));
    return std::make_unique<DWAL::Record>(DWAL::OpCode::ADD_DATA_BLOCK, std::move(block), DWAL::NO_SCHEMA);
}

struct TestSchemaProvider : public DWAL::SchemaProvider
{
    TestSchemaProvider() : header(createBlockBig(0)) { }

    const DB::Block & getSchema(uint16_t /*schema_version*/) const override { return header; }

    DB::Block header;
};

TestSchemaProvider test_schema_provider;
DWAL::SchemaContext test_schema_ctx(test_schema_provider);

void checkRecord(
    const DWAL::Record & expect,
    const DWAL::Record & actual,
    const std::vector<uint16_t> & positions,
    const std::vector<uint16_t> & default_positions = {})
{
    EXPECT_EQ(expect.op_code, actual.op_code);

    /// Compare columns
    for (auto pos : positions)
    {
        const auto & col_expected = expect.block.getByPosition(pos);
        const auto & col_got = actual.block.getByName(col_expected.name);

        for (size_t i = 0; i < col_got.column->size(); ++i)
            EXPECT_EQ(col_expected.column->compareAt(i, i, *col_got.column, -1), 0);
    }

    for (auto pos : default_positions)
    {
        const auto & origin = expect.block.getByPosition(pos);
        auto col_expected = origin.type->createColumn()->cloneResized(expect.block.rows());
        const auto & col_got = actual.block.getByName(origin.name);
        for (size_t i = 0; i < col_got.column->size(); ++i)
            EXPECT_EQ(col_expected->compareAt(i, i, *col_got.column, -1), 0);
    }
}

std::vector<uint16_t> column_positions = {5, 3, 10, 13, 7};

auto prepare()
{
    auto r = createRecordBig(10);
    /// force a schema
    r->schema_version = 0;

    EXPECT_TRUE(r->block.columns() > 13);

    r->column_positions = column_positions;

    DB::Block filtered;
    for (auto pos : r->column_positions)
        filtered.insert(r->block.getByPosition(pos));

    r->block.swap(filtered);
    DWAL::ByteVector data{DWAL::Record::write(*r)};
    /// swap back
    r->block.swap(filtered);
    return std::make_pair<std::unique_ptr<DWAL::Record>, DWAL::ByteVector>(std::move(r), std::move(data));
}
}

TEST(RecordSerde, NativeInSchemaSkipWriteReadAllWritten)
{
    /// Read back all positions written
    auto [r, data] = prepare();
    test_schema_ctx.column_positions = r->column_positions;
    auto rr = DWAL::Record::read(reinterpret_cast<char *>(data.data()), data.size(), test_schema_ctx);
    checkRecord(*r, *rr, test_schema_ctx.column_positions);
}

TEST(RecordSerde, NativeInSchemaSkipWriteReadPartialWritten)
{
    /// Read back partial positions written
    auto [r, data] = prepare();
    test_schema_ctx.column_positions = {3u, 13u};
    auto rr = DWAL::Record::read(reinterpret_cast<char *>(data.data()), data.size(), test_schema_ctx);
    checkRecord(*r, *rr, test_schema_ctx.column_positions);
}

TEST(RecordSerde, NativeInSchemaSkipWriteReadPartialWrittenAndPartialUnwritten)
{
    /// Read back partial positions written and partial unwritten positions
    auto [r, data] = prepare();
    test_schema_ctx.column_positions = {5u, 3u, 30u};
    auto rr = DWAL::Record::read(reinterpret_cast<char *>(data.data()), data.size(), test_schema_ctx);
    checkRecord(*r, *rr, {5u, 3u}, {30u});
}

TEST(RecordSerde, NativeInSchemaSkipWriteReadPartialWrittenAndPartialUnwritten2)
{
    /// Read back partial positions written and partial unwritten positions
    auto [r, data] = prepare();
    test_schema_ctx.column_positions = {5u, 3u, 30u, 15u};
    auto rr = DWAL::Record::read(reinterpret_cast<char *>(data.data()), data.size(), test_schema_ctx);
    checkRecord(*r, *rr, {5u, 3u}, {30u, 15u});
}

TEST(RecordSerde, NativeInSchemaSkipWriteReadPartialUnwritten)
{
    /// Read back partial unwritten positions
    auto [r, data] = prepare();
    test_schema_ctx.column_positions = {0u, 30u, 15u};
    auto rr = DWAL::Record::read(reinterpret_cast<char *>(data.data()), data.size(), test_schema_ctx);
    checkRecord(*r, *rr, {}, test_schema_ctx.column_positions);
}

TEST(RecordSerde, NativeInSchemaSkipWriteReadAll)
{
    /// Read back all
    auto [r, data] = prepare();
    test_schema_ctx.column_positions.clear();
    auto rr = DWAL::Record::read(reinterpret_cast<char *>(data.data()), data.size(), test_schema_ctx);

    std::vector<uint16_t> defaults;
    defaults.reserve(r->block.columns() - r->column_positions.size());
    for (uint16_t pos = 0; pos < r->block.columns(); ++pos)
        if (std::find(r->column_positions.begin(), r->column_positions.end(), pos) == r->column_positions.end())
            defaults.push_back(pos);

    checkRecord(*r, *rr, r->column_positions, defaults);
}
