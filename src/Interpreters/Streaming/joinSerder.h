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
/// For HashBlocks
void serialize(
    const HashBlocks & hash_blocks,
    const Block & header,
    const HashJoin & join,
    WriteBuffer & wb,
    SerializedRowRefListMultipleToIndices * serialized_row_ref_list_multiple_to_indices);
void deserialize(
    HashBlocks & hash_blocks,
    const Block & header,
    const HashJoin & join,
    ReadBuffer & rb,
    DeserializedIndicesToRowRefListMultiple<JoinDataBlock> * deserialized_indices_to_multiple_ref);

/// For JoinResults
void serialize(const HashJoin::JoinResults & join_results, const HashJoin & join, WriteBuffer & wb);
void deserialize(HashJoin::JoinResults & join_results, const HashJoin & join, ReadBuffer & rb);

/// For JoinData
void serialize(const HashJoin::JoinData & join_data, WriteBuffer & wb);
void deserialize(HashJoin::JoinData & join_data, ReadBuffer & rb);

/// For JoinGlobalMetrics
void serialize(const HashJoin::JoinGlobalMetrics & join_metrics, WriteBuffer & wb);
void deserialize(HashJoin::JoinGlobalMetrics & join_metrics, ReadBuffer & rb);
}
}
