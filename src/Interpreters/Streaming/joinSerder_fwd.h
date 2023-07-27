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
template <typename DataBlock>
struct RefCountBlock;

template <typename DataBlock>
using RefCountBlockListIter = typename std::list<RefCountBlock<DataBlock>>::iterator;

/// <Block *, serialized_index>
using SerializedBlocksToIndices = std::unordered_map<std::uintptr_t, UInt32>;
/// <deserialized_index, RefCountBlockListIter>, here we use an iterator as mapped instaed of "Block *", which is required by `RowRefWithRefCount`
template <typename DataBlock>
using DeserializedIndicesToBlocks = std::unordered_map<UInt32, RefCountBlockListIter<DataBlock>>;


struct RowRefListMultipleRef;

/// <RowRefWithRefCount *, serialized_index>
using SerializedRowRefListMultipleToIndices = std::unordered_map<std::uintptr_t, UInt32>;
/// <deserialized_index, RowRefListMultipleRef>
using DeserializedIndicesToRowRefListMultiple = std::unordered_map<UInt32, RowRefListMultipleRef>;
}
}
