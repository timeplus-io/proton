#pragma once

#include <Interpreters/Streaming/HashJoin.h>
#include <Interpreters/Streaming/joinSerder_fwd.h>
namespace DB
{
class WriteBuffer;
class ReadBuffer;
class Arena;

namespace Streaming
{
/// For RowRef
void serialize(const RowRef & row_ref, const SerializedBlocksToIndices & serialized_blocks_to_indices, WriteBuffer & wb);
void deserialize(RowRef & row_ref, const DeserializedIndicesToBlocks & deserialized_indices_to_blocks, ReadBuffer & rb);

/// For RowRefList
void serialize(const RowRefList & row_ref, const SerializedBlocksToIndices & serialized_blocks_to_indices, WriteBuffer & wb);
void deserialize(RowRefList & row_ref, Arena & pool, const DeserializedIndicesToBlocks & deserialized_indices_to_blocks, ReadBuffer & rb);

/// For HashBlocks
void serialize(
    const HashBlocks & hash_blocks,
    const HashJoin & join,
    WriteBuffer & wb,
    SerializedRowRefListMultipleToIndices * serialized_row_ref_list_multiple_to_indices);
void deserialize(
    HashBlocks & hash_blocks,
    const HashJoin & join,
    ReadBuffer & rb,
    DeserializedIndicesToRowRefListMultiple * deserialized_indices_to_multiple_ref);

/// For JoinResults
void serialize(const HashJoin::JoinResults & join_results, const HashJoin & join, WriteBuffer & wb);
void deserialize(HashJoin::JoinResults & join_results, const HashJoin & join, ReadBuffer & rb);

/// For JoinData
void serialize(const HashJoin::JoinData & join_data, WriteBuffer & wb);
void deserialize(HashJoin::JoinData & join_data, ReadBuffer & rb);
}
}
