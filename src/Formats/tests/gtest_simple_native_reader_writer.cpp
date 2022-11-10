#include <Formats/SimpleNativeReader.h>
#include <Formats/SimpleNativeWriter.h>
#include <IO/ReadBufferFromMemory.h>

#include <gtest/gtest.h>

extern DB::Block createBlockBig(size_t rows);

bool sameBlocks(const DB::Block & lhs, const DB::Block & rhs)
{
    if (!DB::blocksHaveEqualStructure(lhs, rhs))
        return false;

    /// Compare block info
    if (lhs.info != rhs.info)
        return false;

    /// Compare column by column
    for (size_t col = 0; col < lhs.columns(); ++col)
    {
        const auto & lhs_col = lhs.getByPosition(col);
        const auto & rhs_col = rhs.getByPosition(col);
        for (size_t row = 0; row < lhs.rows(); ++row)
        {
            if (lhs_col.column->compareAt(row, row, *rhs_col.column, -1))
                return false;
        }
    }

    return true;
}

TEST(SimpleNative, ReadWrite)
{
    auto block = createBlockBig(10);

    std::vector<char> data;
    DB::WriteBufferFromVector wb{data};

    DB::SimpleNativeWriter writer(wb, 0);

    writer.write(block);

    wb.next();
    wb.finalize();

    DB::ReadBufferFromMemory rb(data.data(), data.size());
    DB::SimpleNativeReader reader(rb, 0);

    auto recovered_block = reader.read();
    ASSERT_TRUE(sameBlocks(block, recovered_block));
}

TEST(SimpleNative, ReadWriteInfo)
{
    auto block = createBlockBig(10);
    block.info.bucket_num = 100;
    block.info.is_overflows = true;

    std::vector<char> data;
    DB::WriteBufferFromVector wb{data};

    DB::SimpleNativeWriter writer(wb, 1);

    writer.write(block);

    wb.next();
    wb.finalize();

    DB::ReadBufferFromMemory rb(data.data(), data.size());
    DB::SimpleNativeReader reader(rb, 1);

    auto recovered_block = reader.read();
    ASSERT_TRUE(sameBlocks(block, recovered_block));
}
