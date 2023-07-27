#include <Formats/SimpleNativeReader.h>
#include <Formats/SimpleNativeWriter.h>
#include <IO/ReadBufferFromMemory.h>
#include <Processors/Chunk.h>

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

TEST(SimpleNative, ReadWriteBlock)
{
    auto block = createBlockBig(10);
    auto header = block.cloneEmpty();

    std::vector<char> data;
    DB::WriteBufferFromVector wb{data};

    DB::SimpleNativeWriter<DB::Block> writer(wb, header, 0);

    writer.write(block);

    wb.next();
    wb.finalize();

    DB::ReadBufferFromMemory rb(data.data(), data.size());
    DB::SimpleNativeReader<DB::Block> reader(rb, header, 0);

    auto recovered_block = reader.read();
    ASSERT_TRUE(sameBlocks(block, recovered_block));
}

TEST(SimpleNative, ReadWriteBlockInfo)
{
    auto block = createBlockBig(10);
    auto header = block.cloneEmpty();
    block.info.bucket_num = 100;
    block.info.is_overflows = true;

    std::vector<char> data;
    DB::WriteBufferFromVector wb{data};

    DB::SimpleNativeWriter<DB::Block> writer(wb, header, 1);

    writer.write(block);

    wb.next();
    wb.finalize();

    DB::ReadBufferFromMemory rb(data.data(), data.size());
    DB::SimpleNativeReader<DB::Block> reader(rb, header, 1);

    auto recovered_block = reader.read();
    ASSERT_TRUE(sameBlocks(block, recovered_block));
}

TEST(SimpleNative, ReadWriteChunk)
{
    auto block = createBlockBig(10);
    auto header = block.cloneEmpty();

    std::vector<char> data;
    DB::WriteBufferFromVector wb{data};

    DB::SimpleNativeWriter<DB::Chunk> writer(wb, header, 0);

    writer.write(DB::Chunk{block.getColumns(), block.rows()});

    wb.next();
    wb.finalize();

    DB::ReadBufferFromMemory rb(data.data(), data.size());
    DB::SimpleNativeReader<DB::Chunk> reader(rb, header, 0);

    auto recovered_chunk = reader.read();
    auto recovered_block = header.cloneWithColumns(recovered_chunk.detachColumns());

    ASSERT_TRUE(sameBlocks(block, recovered_block));
}
