#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Chunk.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{
ColumnPtr makeUInt64Column(size_t n)
{
    auto col = std::make_shared<DataTypeUInt64>()->createColumn();
    auto * c = typeid_cast<ColumnUInt64 *>(col.get());
    for (size_t i = 0; i < n; ++i)
        c->insert(static_cast<UInt64>(i));
    return col;
}

ColumnPtr makeStringColumn(size_t n, const std::string & s)
{
    auto col = std::make_shared<DataTypeString>()->createColumn();
    auto * c = typeid_cast<ColumnString *>(col.get());
    for (size_t i = 0; i < n; ++i)
        c->insert(s);
    return col;
}

UInt64 sumByteSize(const Columns & cols)
{
    UInt64 res = 0;
    for (const auto & col : cols)
        res += col->byteSize();
    return res;
}
}

TEST(ChunkBytesCache, BasicCacheAndRepeat)
{
    Chunk ch;
    ch.addColumn(makeUInt64Column(10));
    ch.addColumn(makeStringColumn(10, "abc"));

    const auto expected = sumByteSize(ch.getColumns());
    ASSERT_EQ(ch.bytes(), expected);
    /// Second call should return the cached value (behavioral equality check)
    ASSERT_EQ(ch.bytes(), expected);
}

TEST(ChunkBytesCache, InvalidateOnSetColumns)
{
    Chunk ch;
    ch.addColumn(makeUInt64Column(5));
    const auto first = ch.bytes();

    Columns cols;
    cols.emplace_back(makeUInt64Column(5));
    cols.emplace_back(makeStringColumn(5, "xyz"));
    ch.setColumns(std::move(cols), 5);

    const auto second = ch.bytes();
    ASSERT_NE(first, second);
    ASSERT_EQ(second, sumByteSize(ch.getColumns()));
}

TEST(ChunkBytesCache, InvalidateOnAddAndErase)
{
    Chunk ch;
    ch.addColumn(makeUInt64Column(3));
    const auto a = ch.bytes();

    ch.addColumn(makeStringColumn(3, "qwerty"));
    const auto b = ch.bytes();
    ASSERT_GT(b, a);

    /// Erase the last column and expect bytes back to original
    ch.erase(0); /// remove first column
    const auto c = ch.bytes();
    ASSERT_LT(c, b);
}

TEST(ChunkBytesCache, InvalidateOnMutateAndDetach)
{
    Chunk ch;
    ch.addColumn(makeUInt64Column(4));
    ch.addColumn(makeStringColumn(4, "zz"));
    const auto before = ch.bytes();

    /// mutateColumns detaches storage; change size and set back
    auto mcols = ch.mutateColumns();
    /// After mutate, chunk becomes empty; bytes should be zero
    ASSERT_EQ(ch.bytes(), 0u);

    /// Insert one more value into first column
    auto * col0 = typeid_cast<ColumnUInt64 *>(mcols[0].get());
    col0->insert(static_cast<UInt64>(42));

    /// Keep all columns consistent in size: extend second column by one value
    auto * col1 = typeid_cast<ColumnString *>(mcols[1].get());
    col1->insert("zz");

    /// Adjust rows to new size (old size 4 -> now 5)
    ch.setColumns(std::move(mcols), 5);

    const auto after = ch.bytes();
    ASSERT_NE(after, before);
    ASSERT_EQ(after, sumByteSize(ch.getColumns()));

    /// Detach and ensure bytes() reflects emptiness
    auto detached = ch.detachColumns();
    (void)detached; /// silence unused warning
    ASSERT_EQ(ch.bytes(), 0u);
}

TEST(ChunkBytesCache, SwapAndMove)
{
    Chunk a;
    a.addColumn(makeUInt64Column(7));
    const auto a_bytes = a.bytes();

    Chunk b;
    b.addColumn(makeStringColumn(7, "long_string"));
    const auto b_bytes = b.bytes();

    ASSERT_NE(a_bytes, b_bytes);

    a.swap(b);
    ASSERT_EQ(a.bytes(), b_bytes);
    ASSERT_EQ(b.bytes(), a_bytes);

    /// Move construct
    Chunk c = std::move(a);
    ASSERT_EQ(c.bytes(), b_bytes);
    /// Moved-from 'a' is empty
    ASSERT_EQ(a.bytes(), 0u);
}

TEST(ChunkBytesCache, Append)
{
    Chunk a;
    a.addColumn(makeUInt64Column(3));

    Chunk b;
    b.addColumn(makeUInt64Column(3));

    const auto sum = a.bytes() + b.bytes();
    a.append(b.clone());
    ASSERT_EQ(a.bytes(), sum);
}
