#include <NativeLog/Base/ByteVector.h>
#include <NativeLog/Record/OpCodes.h>
#include <NativeLog/Record/Record.h>
#include <NativeLog/Record/SchemaProvider.h>

#include <gtest/gtest.h>

extern DB::Block createBlockBig(size_t rows);

namespace
{
std::unique_ptr<nlog::Record> createRecordBig(size_t rows)
{
    DB::Block block(createBlockBig(rows));
    return std::make_unique<nlog::Record>(nlog::OpCode::ADD_DATA_BLOCK, std::move(block), nlog::NO_SCHEMA);
}

struct TestSchemaProvider : public nlog::SchemaProvider
{
    TestSchemaProvider() : header(createBlockBig(0)) { }

    const DB::Block & getSchema(uint16_t /*schema_version*/) const override { return header; }

    DB::Block header;
};

TestSchemaProvider test_schema_provider;
nlog::SchemaContext test_schema_ctx(test_schema_provider);

void checkRecord(
    const nlog::Record & expect,
    const nlog::Record & actual,
    const std::vector<uint16_t> & positions,
    const std::vector<uint16_t> & default_positions = {})
{
    EXPECT_EQ(expect.getFlags(), actual.getFlags());

    /// Compare columns
    for (auto pos : positions)
    {
        const auto & col_expected = expect.getBlock().getByPosition(pos);
        const auto & col_got = actual.getBlock().getByName(col_expected.name);

        for (size_t i = 0; i < col_got.column->size(); ++i)
            EXPECT_EQ(col_expected.column->compareAt(i, i, *col_got.column, -1), 0);
    }

    for (auto pos : default_positions)
    {
        const auto & origin = expect.getBlock().getByPosition(pos);
        auto col_expected = origin.type->createColumn()->cloneResized(expect.getBlock().rows());
        const auto & col_got = actual.getBlock().getByName(origin.name);
        for (size_t i = 0; i < col_got.column->size(); ++i)
            EXPECT_EQ(col_expected->compareAt(i, i, *col_got.column, -1), 0);
    }
}
void checkRecordSub(
    const nlog::Record & expect,
    const nlog::Record & actual,
    const std::vector<uint16_t> & positions,
    const std::vector<uint16_t> & default_positions = {})
{
    EXPECT_EQ(expect.getFlags(), actual.getFlags());

    /// Compare columns
    for (auto pos : positions)
    {
        const auto & col_expected = expect.getBlock().getByPosition(pos);
        const auto & col_got = actual.getBlock().getByName(col_expected.name);


        for (size_t i = 0; i < col_got.column->size(); ++i)
            if (pos == *(positions.end() - 1))
                EXPECT_EQ(col_expected.column->compareAt(i, i, *col_got.column, -1), 1);
            else
                EXPECT_EQ(col_expected.column->compareAt(i, i, *col_got.column, -1), 0);
    }

    for (auto pos : default_positions)
    {
        const auto & origin = expect.getBlock().getByPosition(pos);
        auto col_expected = origin.type->createColumn()->cloneResized(expect.getBlock().rows());
        const auto & col_got = actual.getBlock().getByName(origin.name);

        for (size_t i = 0; i < col_got.column->size(); ++i)
            EXPECT_EQ(col_expected->compareAt(i, i, *col_got.column, -1), 0);
    }
}
std::vector<uint16_t> SERIALIZED_COLUMN_POSITIONS = {5, 3, 10, 13, 7};

auto prepare()
{
    auto r = createRecordBig(10);
    /// force a schema
    r->setSchemaVersion(0);
    r->setColumnPositions(SERIALIZED_COLUMN_POSITIONS);

    auto & block = r->getBlock();

    EXPECT_TRUE(block.columns() > 13);

    DB::Block filtered;
    for (auto pos : r->getColumnPositions())
        filtered.insert(block.getByPosition(pos));

    block.swap(filtered);
    nlog::ByteVector data{r->serialize()};
    /// swap back
    block.swap(filtered);
    return std::make_pair<std::unique_ptr<nlog::Record>, nlog::ByteVector>(std::move(r), std::move(data));
}
}

TEST(RecordSerde, NativeInSchemaSkipWriteReadAllWritten)
{
    /// Read back all positions written
    auto [r, data] = prepare();
    test_schema_ctx.column_positions = r->getColumnPositions();
    auto rr = nlog::Record::deserialize(reinterpret_cast<char *>(data.data()), data.size(), test_schema_ctx);
    checkRecord(*r, *rr, test_schema_ctx.column_positions.positions);
}

TEST(RecordSerde, NativeInSchemaSkipWriteReadPartialWritten)
{
    /// Read back partial positions written
    auto [r, data] = prepare();
    test_schema_ctx.column_positions = {3u, 13u};
    auto rr = nlog::Record::deserialize(reinterpret_cast<char *>(data.data()), data.size(), test_schema_ctx);
    checkRecord(*r, *rr, test_schema_ctx.column_positions.positions);
}

TEST(RecordSerde, NativeInSchemaSkipWriteReadPartialWrittenAndPartialUnwritten)
{
    /// Read back partial positions written and partial unwritten positions
    auto [r, data] = prepare();
    test_schema_ctx.column_positions = {5u, 3u, 30u};
    auto rr = nlog::Record::deserialize(reinterpret_cast<char *>(data.data()), data.size(), test_schema_ctx);
    checkRecord(*r, *rr, {5u, 3u}, {30u});
}

TEST(RecordSerde, NativeInSchemaSkipWriteReadPartialWrittenAndPartialUnwritten2)
{
    /// Read back partial positions written and partial unwritten positions
    auto [r, data] = prepare();
    test_schema_ctx.column_positions = {5u, 3u, 30u, 15u};
    auto rr = nlog::Record::deserialize(reinterpret_cast<char *>(data.data()), data.size(), test_schema_ctx);
    checkRecord(*r, *rr, {5u, 3u}, {30u, 15u});
}

TEST(RecordSerde, NativeInSchemaSkipWriteReadPartialUnwritten)
{
    /// Read back partial unwritten positions
    auto [r, data] = prepare();
    test_schema_ctx.column_positions = {0u, 30u, 15u};
    auto rr = nlog::Record::deserialize(reinterpret_cast<char *>(data.data()), data.size(), test_schema_ctx);
    checkRecord(*r, *rr, {}, test_schema_ctx.column_positions.positions);
}

TEST(RecordSerde, NativeInSchemaSkipWriteReadAll)
{
    /// Read back all
    auto [r, data] = prepare();
    test_schema_ctx.column_positions.clear();
    auto rr = nlog::Record::deserialize(reinterpret_cast<char *>(data.data()), data.size(), test_schema_ctx);

    const auto & col_positions = r->getColumnPositions();

    std::vector<uint16_t> defaults;
    defaults.reserve(r->getBlock().columns() - col_positions.size());
    for (uint16_t pos = 0; pos < r->getBlock().columns(); ++pos)
        if (std::find(col_positions.begin(), col_positions.end(), pos) == col_positions.end())
            defaults.push_back(pos);

    checkRecordSub(*r, *rr, col_positions, defaults);
}
