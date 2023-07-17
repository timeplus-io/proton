#pragma once

#include <base/types.h>

#include <list>

namespace DB
{
class WriteBuffer;
class ReadBuffer;
class Arena;

namespace Streaming
{
struct RefCountBlock;
using JoinBlockListIter = std::list<RefCountBlock>::iterator;

/// <Block *, serialized_index>
using SerializedBlocksToIndices = std::unordered_map<std::uintptr_t, UInt32>;
/// <deserialized_index, JoinBlockListIter>, here we use an iterator as mapped instaed of "Block *", which is required by `RowRefWithRefCount`
using DeserializedIndicesToBlocks = std::unordered_map<UInt32, JoinBlockListIter>;


struct RowRefListMultipleRef;

/// <RowRefWithRefCount *, serialized_index>
using SerializedRowRefListMultipleToIndices = std::unordered_map<std::uintptr_t, UInt32>;
/// <deserialized_index, RowRefListMultipleRef>
using DeserializedIndicesToRowRefListMultiple = std::unordered_map<UInt32, RowRefListMultipleRef>;
}
}
