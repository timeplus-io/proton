#include <Interpreters/Streaming/RefCountDataBlockList.h>

#include <Formats/SimpleNativeReader.h>
#include <Formats/SimpleNativeWriter.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/VersionRevision.h>

namespace DB
{
namespace Streaming
{
template <typename DataBlock>
void RefCountDataBlockList<DataBlock>::serialize(
    WriteBuffer & wb, VersionType version, const Block & header, SerializedBlocksToIndices * serialized_blocks_to_indices) const
{
    if (version < STATE_V2_MIN_VERSION)
    {
        /// V1 layout [min_ts, max_ts, total_bytes]
        DB::writeIntBinary(min_ts, wb);
        DB::writeIntBinary(max_ts, wb);
        DB::writeIntBinary(total_data_bytes, wb);
    }

    UInt32 blocks_size = static_cast<UInt32>(blocks.size());
    DB::writeIntBinary<UInt32>(blocks_size, wb);

    if (blocks_size == 0)
        return;

    SimpleNativeWriter<DataBlock> writer(wb, header, version);
    for (UInt32 i = 0; const auto & block_with_ref : blocks)
    {
        writer.write(block_with_ref.block);
        DB::writeIntBinary(block_with_ref.refcnt, wb);

        if (serialized_blocks_to_indices)
            serialized_blocks_to_indices->emplace(reinterpret_cast<std::uintptr_t>(&block_with_ref.block), i);

        ++i;
    }
}

template <typename DataBlock>
void RefCountDataBlockList<DataBlock>::deserialize(
    ReadBuffer & rb, VersionType version, const Block & header, DeserializedIndicesToBlocks<DataBlock> * deserialized_indices_with_block)
{
    if (version < STATE_V2_MIN_VERSION)
    {
        /// V1 layout [min_ts, max_ts, total_bytes]
        DB::readIntBinary(min_ts, rb);
        DB::readIntBinary(max_ts, rb);
        DB::readIntBinary(total_data_bytes, rb);
    }

    UInt32 block_size;
    DB::readIntBinary<UInt32>(block_size, rb);

    if (block_size == 0)
        return;

    SimpleNativeReader<DataBlock> reader(rb, header, version);
    for (UInt32 i = 0; i < block_size; ++i)
    {
        auto data_block = reader.read();
        RefCountDataBlock<DataBlock> elem{std::move(data_block)};
        DB::readIntBinary(elem.refcnt, rb);
        assert(elem.refcnt > 0);

        /// Update metrics back via `updateMetrics`
        if (version >= STATE_V2_MIN_VERSION)
            updateMetrics(elem.block);

        blocks.push_back(std::move(elem));

        if (deserialized_indices_with_block)
            deserialized_indices_with_block->emplace(i, lastDataBlockIter());
    }
}

template struct RefCountDataBlockList<LightChunk>;
template struct RefCountDataBlockList<LightChunkWithTimestamp>;
}
}
