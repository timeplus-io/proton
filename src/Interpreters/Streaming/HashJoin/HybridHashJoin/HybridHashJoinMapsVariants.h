#pragma once

#include <Core/LightChunk.h>
#include <Interpreters/Streaming/HashJoin/HybridHashJoin/Rows.h>
#include <Interpreters/Streaming/OneRow.h>
#include <Common/HybridHashTable/HybridHashTableTemplate.h>

namespace DB::Streaming
{
using HybridMapsOne = HybridHashTableTemplateTagged<OneRow>;
using HybridMapsMultiple = HybridHashTableTemplateTagged<RowList>;

using AllRows = RowBatch<LightChunkWithTimestamp>;
using HybridMapsAll = HybridHashTableTemplateTagged<AllRows>;
using HybridMapsAsof = HybridHashTableTemplateTagged<AsofRows>;
using HybridMapsRangeAsof = HybridHashTableTemplateTagged<RangeAsofRows>;
using HybridMapsVariant = std::variant<HybridMapsMultiple, HybridMapsOne, HybridMapsAll, HybridMapsAsof, HybridMapsRangeAsof>;

using HybridHashJoinMapsVariants = std::vector<HybridMapsVariant>;
using HybridHashJoinMapsVariantsPtr = std::shared_ptr<HybridHashJoinMapsVariants>;


}
