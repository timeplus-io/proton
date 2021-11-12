#pragma once

#include <Common/HashTable/Hash.h>
#include <Common/HashTable/StreamingTwoLevelHashMap.h>

namespace DB
{
/// using StreamingAggregatedDataWithUInt16Key = StreamingTwoLevelHashMap<FixedImplicitZeroHashMap<UInt16, AggregateDataPtr>>;
/// using StreamingAggregatedDataWithUInt32Key = StreamingTwoLevelHashMap<HashMap<UInt32, AggregateDataPtr, HashCRC32<UInt32>>>;
/// using StreamingAggregatedDataWithUInt64Key = StreamingTwoLevelHashMap<HashMap<UInt64, AggregateDataPtr, HashCRC32<UInt64>>>;
/// using StreamingAggregatedDataWithKeys128 = HashMap<UInt128, AggregateDataPtr, UInt128HashCRC32>;
/// using StreamingAggregatedDataWithKeys256 = HashMap<UInt256, AggregateDataPtr, UInt256HashCRC32>;

/// Single key
using StreamingAggregatedDataWithUInt16KeyTwoLevel = StreamingTwoLevelHashMap<UInt16, AggregateDataPtr, HashCRC32<UInt16>>;
using StreamingAggregatedDataWithUInt32KeyTwoLevel = StreamingTwoLevelHashMap<UInt32, AggregateDataPtr, HashCRC32<UInt32>>;
using StreamingAggregatedDataWithUInt64KeyTwoLevel = StreamingTwoLevelHashMap<UInt64, AggregateDataPtr, HashCRC32<UInt64>>;
using StreamingAggregatedDataWithStringKeyTwoLevel = StreamingTwoLevelHashMapWithSavedHash<StringRef, AggregateDataPtr>;

/// Multiple keys
using StreamingAggregatedDataWithKeys128TwoLevel = StreamingTwoLevelHashMap<UInt128, AggregateDataPtr, UInt128HashCRC32>;
using StreamingAggregatedDataWithKeys256TwoLevel = StreamingTwoLevelHashMap<UInt256, AggregateDataPtr, UInt256HashCRC32>;
}
