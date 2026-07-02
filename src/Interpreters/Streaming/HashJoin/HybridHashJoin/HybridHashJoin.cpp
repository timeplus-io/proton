#include <Functions/FunctionHelpers.h>
#include <Interpreters/Streaming/ChooseHybridHashMethod.h>
#include <Interpreters/Streaming/HashJoin/HybridHashJoin/HybridHashJoin.h>
#include <Interpreters/Streaming/HashJoin/RowUtil.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
extern const int UNSUPPORTED;
extern const int NOT_IMPLEMENTED;
extern const int SYNTAX_ERROR;
}

namespace Streaming
{

HybridHashJoin::HybridHashJoin(
    std::shared_ptr<TableJoin> table_join_,
    JoinStreamDescriptionPtr left_join_stream_desc_,
    JoinStreamDescriptionPtr right_join_stream_desc_,
    const String & spill_dir_,
    size_t max_hot_key_count_,
    Int32 ttl_,
    const String & kv_options_)
    : HashJoin(std::move(table_join_), std::move(left_join_stream_desc_), std::move(right_join_stream_desc_), "HybridHashJoin")
    , right_data(right_join_ctx)
    , left_data(left_join_ctx)
{
    initRocksDBConfig(spill_dir_, max_hot_key_count_, ttl_, kv_options_);

    /// If right stream is key-value data stream semantic and our query plan pushes down the changelog transform
    /// initRightPrimaryKeyHashTable();
    initHashIndexes();

    chooseHashMethod();

    initHashMaps(right_data.index->getCurrentMapsVariants(), right_data.join_ctx.sample_block, right_data.index->id);

    LOG_INFO(
        logger,
        "({}) hash type: {}, kind: {}, strictness: {}, keys : {}, buffered_data_block_size={} right header: {}",
        fmt::ptr(this),
        toString(hash_method_type),
        toString(kind),
        toString(strictness),
        TableJoin::formatClauses(table_join->getClauses(), true),
        dataBlockSize(),
        right_data.join_ctx.join_stream_desc->input_header.dumpStructure());

    if (streaming_strictness == Strictness::Range)
        LOG_INFO(
            logger,
            "Range join: {} join_start_bucket={} join_stop_bucket={}",
            table_join->rangeAsofJoinContext().string(),
            left_data.index->join_start_bucket_offset,
            left_data.index->join_stop_bucket_offset);
}

HybridHashJoin::~HybridHashJoin() noexcept
{
    LOG_INFO(logger, "{}", metricsString());
}

void HybridHashJoin::chooseHashMethod()
{
    size_t disjuncts_num = table_join->getClauses().size();
    key_sizes.reserve(disjuncts_num);

    for (const auto & clause : table_join->getClauses())
    {
        const auto & key_names_right = clause.key_names_right;
        ColumnRawPtrs key_columns = JoinCommon::extractKeysForJoin(right_data.join_ctx.table_keys, key_names_right);

        if (streaming_strictness == Strictness::Range || streaming_strictness == Strictness::Asof)
        {
            chassert(disjuncts_num == 1);

            /// Note ASOF JOIN is not INNER. It's better avoid use of 'INNER ASOF' combination in messages.
            /// In fact INNER means 'LEFT SEMI ASOF' while LEFT means 'LEFT OUTER ASOF'.
            if (!isLeft(kind) && !isInner(kind))
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Wrong ASOF JOIN type. Only ASOF and LEFT ASOF joins are supported");

            if (key_columns.size() <= 1)
                throw Exception(ErrorCodes::SYNTAX_ERROR, "ASOF join needs at least one equal-join column");

            if (right_data.join_ctx.table_keys.getByName(key_names_right.back()).type->isNullable())
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ASOF join over right stream Nullable column is not implemented");

            size_t asof_size;
            asof_type = getAsofTypeSize(*key_columns.back(), asof_size);

            /// Remove the asof join key since we will handle it separately
            key_columns.pop_back();

            /// this is going to set up the appropriate hash table for the direct lookup part of the join
            /// However, this does not depend on the size of the asof join key (as that goes into the BST)
            /// Therefore, add it back in such that it can be extracted appropriately from the full stored
            /// key_columns and key_sizes
            auto & asof_key_sizes = key_sizes.emplace_back();
            auto hash_method = chooseHybridHashMethodForJoin(key_columns);
            hash_method_type = hash_method.type;
            has_nullable_key = hash_method.has_nullable_key;
            has_low_cardinality_key = hash_method.has_low_cardinality_key;
            asof_key_sizes.swap(hash_method.key_sizes);
            asof_key_sizes.push_back(asof_size);
        }
        else
        {
            /// Choose data structure to use for JOIN.
            auto & new_key_sizes = key_sizes.emplace_back();
            auto hash_method = chooseHybridHashMethodForJoin(key_columns);
            hash_method_type = hash_method.type;
            has_nullable_key = hash_method.has_nullable_key;
            has_low_cardinality_key = hash_method.has_low_cardinality_key;
            new_key_sizes.swap(hash_method.key_sizes);
        }
    }
}

void HybridHashJoin::initHashMaps(HybridHashJoinMapsVariants & maps_variants, const Block & sample_block_to_save, std::string_view table_id)
{
    maps_variants.resize(table_join->getClauses().size());

    for (size_t i = 0; auto & map_variants : maps_variants)
    {
        const auto & join_key_sizes = key_sizes[i];

        [[maybe_unused]] auto inited
            = hybridJoinDispatchInit(streaming_kind, streaming_strictness, map_variants, [&, this](auto & hash_table_template_tagged) {
                  using HashTableTagged = std::decay_t<decltype(hash_table_template_tagged)>;
                  using TaggedType = typename HashTableTagged::TaggedType;

                  /// Left hash table and right hash table are in different column families of the same RocksDB.
                  /// Differentiate by cf handle id
                  HybridHashTableConfig config;
                  config.base_conf = base_config.getSubConfig(fmt::format("{}_{}", table_id, i));

                  size_t row_size = sample_block_to_save.columns();
                  if (row_size == 0)
                  {
                      config.installNoOpCallbacks();
                      hash_table_template_tagged.table.init(hash_method_type, std::move(config), join_key_sizes, logger);
                      return;
                  }

                  config.value_object_size = sizeof(TaggedType);
                  config.align_value_object_size = alignof(TaggedType);

                  if constexpr (std::is_same_v<TaggedType, AsofRows> || std::is_same_v<TaggedType, RangeAsofRows>)
                  {
                      chassert(asof_type);
                      config.value_constructor = [asof_type_index = *asof_type](void * data) { new (data) TaggedType(asof_type_index); };
                  }
                  else
                  {
                      config.value_constructor = [](void * data) { new (data) TaggedType; };
                  }

                  config.value_destructor = [](void * data) {
                      auto * value = reinterpret_cast<TaggedType *>(data);
                      value->~TaggedType();
                  };

                  using RowBatchType = RowBatch<LightChunkWithTimestamp>;
                  config.value_serializer = [&](const void * data, WriteBuffer & wb) {
                      const auto * value = reinterpret_cast<const TaggedType *>(data);
                      if constexpr (std::is_same_v<TaggedType, AsofRows> || std::is_same_v<TaggedType, RangeAsofRows>)
                      {
                          chassert(asof_type);
                          value->serialize(wb, *asof_type);
                      }
                      else if constexpr (std::is_same_v<TaggedType, RowBatchType>)
                          value->serialize(wb, sample_block_to_save);
                      else
                          value->serialize(wb);
                      return DB::ErrorCodes::OK;
                  };

                  config.value_deserializer = [&](void * data, ReadBuffer & rb) {
                      auto * value = reinterpret_cast<TaggedType *>(data);
                      if constexpr (std::is_same_v<TaggedType, AsofRows> || std::is_same_v<TaggedType, RangeAsofRows>)
                      {
                          chassert(asof_type);
                          value->deserialize(rb, *asof_type);
                      }
                      else if constexpr (std::is_same_v<TaggedType, RowBatchType>)
                          value->deserialize(rb, sample_block_to_save);
                      else
                          value->deserialize(rb);
                      return DB::ErrorCodes::OK;
                  };

                  hash_table_template_tagged.table.init(hash_method_type, std::move(config), join_key_sizes, logger);
              });

        chassert(inited);
        ++i;
    }
}

IBlocksStreamPtr HybridHashJoin::getNonJoinedBlocks(
    const Block & /*left_sample_block*/, const Block & /*result_sample_block*/, UInt64 /*max_block_size*/) const
{
    /// FIXME, if there are left rows which are not joined with right data
    return nullptr;
}

String HybridHashJoin::metricsString() const
{
    return fmt::format(
        "Left stream metrics: {{{}}}, right stream metrics: {{{}}}, global join metrics: {{{}}}",
        left_data.index->metricsString(),
        right_data.index->metricsString(),
        join_metrics.string());
}

void HybridHashJoin::cancel()
{
}

size_t HybridHashJoin::getTotalRowCount() const
{
    return right_data.index->approximateCount() + left_data.index->approximateCount();
}

/// Sum size in bytes of all buffers, used for JOIN maps and for all memory pools.
size_t HybridHashJoin::getTotalByteCount() const
{
    return right_data.index->getBufferSizeInBytes() + left_data.index->getBufferSizeInBytes();
}

bool HybridHashJoin::alwaysReturnsEmptySet() const
{
    return false;
}

void HybridHashJoin::initHashIndexes()
{
    if (streaming_strictness == Strictness::Range)
    {
        const auto & right_asof_key_col = table_join->getOnlyClause().key_names_right.back();
        const auto & left_asof_key_col = table_join->getOnlyClause().key_names_left.back();

        const auto & range_join_ctx = table_join->rangeAsofJoinContext();

        right_data.index = std::make_shared<HashIndex>(this, "right", range_join_ctx, right_asof_key_col);
        left_data.index = std::make_shared<HashIndex>(this, "left", range_join_ctx, left_asof_key_col);
    }
    else
    {
        /// Although `append-only` joining `versioned-kv` doesn't need hold left data in memory
        /// we init the left_data to handle different case uniformly
        right_data.index = std::make_unique<HashIndex>(this, "right");
        left_data.index = std::make_unique<HashIndex>(this, "left");
    }

    /// right sample block was inited in HashJoin ctor, set the sample block to HashIndex now
    right_data.index->sample_block = right_data.join_ctx.sample_block;
}

void HybridHashJoin::checkLimits() const
{
    /// FIXME
}

}

}
