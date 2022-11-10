#include <Formats/NativeReader.h>
#include <Formats/NativeWriter.h>
#include <IO/ReadBufferFromMemory.h>

#include <gtest/gtest.h>

extern DB::Block createBlockBig(size_t rows);
extern bool sameBlocks(const DB::Block & lhs, const DB::Block & rhs);

TEST(Native, ReadWrite)
{
    auto block = createBlockBig(10);

    std::vector<char> data;
    DB::WriteBufferFromVector wb{data};

    DB::NativeWriter writer(wb, block.cloneEmpty(), 0);

    writer.write(block);

    wb.next();
    wb.finalize();

    DB::ReadBufferFromMemory rb(data.data(), data.size());
    DB::NativeReader reader(rb, 0);

    auto recovered_block = reader.read();
    ASSERT_TRUE(sameBlocks(block, recovered_block));
}

TEST(Native, ReadWriteInfo)
{
    auto block = createBlockBig(10);
    block.info.bucket_num = 100;
    block.info.is_overflows = true;

    std::vector<char> data;
    DB::WriteBufferFromVector wb{data};

    DB::NativeWriter writer(wb, block.cloneEmpty(), 1);

    writer.write(block);

    wb.next();
    wb.finalize();

    DB::ReadBufferFromMemory rb(data.data(), data.size());
    DB::NativeReader reader(rb, 1);

    auto recovered_block = reader.read();
    ASSERT_TRUE(sameBlocks(block, recovered_block));
}

