#include <Interpreters/Streaming/joinBlockList.h>

#include <Formats/SimpleNativeReader.h>
#include <Formats/SimpleNativeWriter.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/VersionRevision.h>

namespace DB
{
namespace Streaming
{
void JoinBlockList::serialize(WriteBuffer & wb, SerializedBlocksToIndices * serialized_blocks_to_indices) const
{
    DB::writeIntBinary(min_ts, wb);
    DB::writeIntBinary(max_ts, wb);
    DB::writeIntBinary(total_bytes, wb);

    DB::writeIntBinary<UInt32>(static_cast<UInt32>(blocks.size()), wb);

    SimpleNativeWriter writer(wb, ProtonRevision::getVersionRevision());
    for (UInt32 i = 0; const auto & block_with_ref : blocks)
    {
        writer.write(block_with_ref.block);
        DB::writeIntBinary(block_with_ref.refcnt, wb);

        if (serialized_blocks_to_indices)
            serialized_blocks_to_indices->emplace(reinterpret_cast<std::uintptr_t>(&block_with_ref.block), i);

        ++i;
    }
}

void JoinBlockList::deserialize(ReadBuffer & rb, DeserializedIndicesToBlocks * deserialized_indices_with_block)
{
    DB::readIntBinary(min_ts, rb);
    DB::readIntBinary(max_ts, rb);
    DB::readIntBinary(total_bytes, rb);

    UInt32 block_size;
    DB::readIntBinary<UInt32>(block_size, rb);

    SimpleNativeReader reader(rb, ProtonRevision::getVersionRevision());
    for (UInt32 i = 0; i < block_size; ++i)
    {
        Block block = reader.read();
        RefCountBlock elem{std::move(block)};
        DB::readIntBinary(elem.refcnt, rb);
        assert(elem.refcnt > 0);

        blocks.push_back(std::move(elem));

        if (deserialized_indices_with_block)
            deserialized_indices_with_block->emplace(i, lastBlockIter());
    }
}

}
}
