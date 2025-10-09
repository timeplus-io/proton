#pragma once

#include <base/types.h>

#include <list>

namespace DB
{
struct LightChunk;

namespace Streaming
{
template <typename DataBlock>
struct RefCountDataBlock;

template <typename DataBlock>
using RefCountBlockListIter = typename std::list<RefCountDataBlock<DataBlock>>::iterator;

/// <Block *, serialized_index>
using SerializedBlocksToIndices = std::unordered_map<std::uintptr_t, UInt32>;
/// <deserialized_index, RefCountBlockListIter>, here we use an iterator as mapped instaed of "Block *", which is required by `RowRefWithRefCount`
template <typename DataBlock>
using DeserializedIndicesToBlocks = std::unordered_map<UInt32, RefCountBlockListIter<DataBlock>>;


template <typename DataBlock>
struct RowRefListMultipleRef;

/// <RowRefWithRefCount *, serialized_index>
using SerializedRowRefListMultipleToIndices = std::unordered_map<std::uintptr_t, UInt32>;
/// <deserialized_index, RowRefListMultipleRef>
template <typename DataBlock>
using DeserializedIndicesToRowRefListMultiple = std::unordered_map<UInt32, RowRefListMultipleRef<DataBlock>>;
}
}
