#include <Interpreters/Streaming/Aggregator/HybridAggregator/HybridAggregator.h>
#include <Interpreters/Streaming/Aggregator/HybridAggregator/HybridAggregator_Convert.h>
#include <Common/Exception.h>
#include <Common/HybridHashTable/HybridKeyGetter.h>
#include <Common/PackedTimeBucket.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace Streaming
{
Block HybridAggregator::spliceAndConvertToBlock(IAggregatedDataVariants & data_variants, const std::vector<Int64> & gcd_buckets) const
{
    return doSpliceAndConvertToBlock(data_variants, gcd_buckets);
}

Block HybridAggregator::spliceAndConvertUpdatesToBlock(
    IAggregatedDataVariants & data_variants, const std::vector<Int64> & gcd_buckets) const
{
    assert(params->tracking_updates_type == TrackingUpdatesType::Updates);
    return doSpliceAndConvertToBlock(data_variants, gcd_buckets);
}

Block HybridAggregator::doSpliceAndConvertToBlock(IAggregatedDataVariants & data_variants, const std::vector<Int64> & gcd_buckets) const
{
    assert(!gcd_buckets.empty());
    assert(data_variants.aggregatorType() == AggregatorType::Hybrid);
    auto & variants = static_cast<HybridAggregatedDataVariants &>(data_variants);
    assert(variants.isTwoLevel());

    Arena merge_arena;

    switch (method_chosen)
    {
#define M(NAME, IS_TWO_LEVEL) \
    case HybridHashType::NAME: \
    { \
        auto gcd_bucket_tables = variants.table.NAME->bucketTables(gcd_buckets); \
        assert(!gcd_bucket_tables.empty()); \
\
        if (has_nullable_key) \
        { \
            using KeyGetter = HybridKeyGetter<HybridHashType::NAME, /*nullable=*/true>; \
            if (gcd_bucket_tables.size() > 1) \
            { \
                HybridAggregatedDataVariants result{/*id_==*/"splice-convert"}; \
                initStates(result); \
\
                auto merged_bucket_table = result.table.NAME->newUnsharedBucketTable(result.getID()); \
                mergeGCDBuckets(*merged_bucket_table, gcd_bucket_tables, variants.updates.NAME.get(), merge_arena); \
                return convertOneBucketToBlockMerged<KeyGetter>(*merged_bucket_table); \
            } \
            else \
            { \
                auto iter = gcd_bucket_tables.begin(); \
                auto * bucket_updates = variants.updates.NAME ? variants.updates.NAME->bucketTable(iter->first) : nullptr; \
                return convertOneBucketToBlock<KeyGetter>(*iter->second, bucket_updates); \
            } \
        } \
        else \
        { \
            using KeyGetter = HybridKeyGetter<HybridHashType::NAME, /*nullable=*/false>; \
            if (gcd_bucket_tables.size() > 1) \
            { \
                HybridAggregatedDataVariants result{/*id_==*/"splice-convert"}; \
                initStates(result); \
\
                auto merged_bucket_table = result.table.NAME->newUnsharedBucketTable(result.getID()); \
                mergeGCDBuckets(*merged_bucket_table, gcd_bucket_tables, variants.updates.NAME.get(), merge_arena); \
                return convertOneBucketToBlockMerged<KeyGetter>(*merged_bucket_table); \
            } \
            else \
            { \
                auto iter = gcd_bucket_tables.begin(); \
                auto * bucket_updates = variants.updates.NAME ? variants.updates.NAME->bucketTable(iter->first) : nullptr; \
                return convertOneBucketToBlock<KeyGetter>(*iter->second, bucket_updates); \
            } \
        } \
    }
        APPLY_FOR_VARIANTS_TIME_BUCKET_TWO_LEVEL_HYBRID(M)
#undef M

        default:
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Invalid hash_method={} for window aggregation", magic_enum::enum_name(method_chosen));
        }
    }
}

Block HybridAggregator::mergeAndSpliceAndConvertToBlock(
    ManyIAggregatedDataVariants & many_data_variants, const std::vector<Int64> & gcd_buckets) const
{
    return doMergeAndSpliceAndConvertToBlock(many_data_variants, gcd_buckets);
}

Block HybridAggregator::mergeAndSpliceAndConvertUpdatesToBlock(
    ManyIAggregatedDataVariants & many_data_variants, const std::vector<Int64> & gcd_buckets) const
{
    assert(params->tracking_updates_type == TrackingUpdatesType::Updates);
    return doMergeAndSpliceAndConvertToBlock(many_data_variants, gcd_buckets);
}

Block HybridAggregator::doMergeAndSpliceAndConvertToBlock(
    ManyIAggregatedDataVariants & many_data_variants, const std::vector<Int64> & gcd_buckets) const
{
    assert(!gcd_buckets.empty());

    if (many_data_variants.size() == 1)
        return doSpliceAndConvertToBlock(*many_data_variants.back(), gcd_buckets);

    Arena merge_arena;

    switch (method_chosen)
    {
#define M(NAME, IS_TWO_LEVEL) \
    case HybridHashType::NAME: \
    { \
        HybridAggregatedDataVariants result{/*id_==*/"many-splice-convert"}; \
        initStates(result); \
        auto merged_bucket_table = result.table.NAME->newUnsharedBucketTable(result.getID()); \
\
        for (const auto & data_variants : many_data_variants) \
        { \
            chassert(data_variants->aggregatorType() == AggregatorType::Hybrid); \
            auto & variants = static_cast<HybridAggregatedDataVariants &>(*data_variants.get()); \
            if (variants.empty()) \
                continue; \
\
            auto gcd_bucket_tables = variants.table.NAME->bucketTables(gcd_buckets); \
\
            mergeGCDBuckets(*merged_bucket_table, gcd_bucket_tables, variants.updates.NAME.get(), merge_arena); \
        } \
\
        if (has_nullable_key) \
        { \
            using KeyGetter = HybridKeyGetter<HybridHashType::NAME, /*nullable=*/true>; \
            return convertOneBucketToBlockMerged<KeyGetter>(*merged_bucket_table); \
        } \
        else \
        { \
            using KeyGetter = HybridKeyGetter<HybridHashType::NAME, /*nullable=*/false>; \
            return convertOneBucketToBlockMerged<KeyGetter>(*merged_bucket_table); \
        } \
    }

        APPLY_FOR_VARIANTS_TIME_BUCKET_TWO_LEVEL_HYBRID(M)
#undef M

        default:
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Invalid hash_method={} for window aggregation", magic_enum::enum_name(method_chosen));
        }
    }
}

template <typename KeyGetter, typename Table>
ALWAYS_INLINE Block HybridAggregator::convertOneBucketToBlock(Table & table, Table * updates) const
{
    constexpr bool clear_updates = false; /// Don't reset updated, there may be overlapping gcd buckets
    auto blocks = convertToBlocksImpl<KeyGetter>(table, updates, /*retracts=*/static_cast<Table *>(nullptr), clear_updates);

    if (blocks.size() == 1)
        return std::move(blocks.front());

    return concatenateBlocks(blocks);
}

template <typename KeyGetter, typename Table>
ALWAYS_INLINE Block HybridAggregator::convertOneBucketToBlockMerged(Table & table) const
{
    auto blocks = convertToBlocksMerged<KeyGetter, Table>(table, /*retracts=*/static_cast<Table *>(nullptr));
    if (blocks.size() == 1)
        return std::move(blocks.front());

    return concatenateBlocks(blocks);
}

template <typename InnerTable, typename Table>
void HybridAggregator::mergeGCDBuckets(
    InnerTable & dst, const std::map<Int64, InnerTable *> & gcd_bucket_tables, Table * src_updates, Arena & arena) const
{
    assert(params->window_params);
    if (params->window_params->type == WindowType::Tumble)
    {
        /// Tumble window
        if (params->tracking_updates_type == TrackingUpdatesType::Updates)
            doMergeGCDBucketsForUpdates<InnerTable, Table, void *>(dst, gcd_bucket_tables, src_updates, nullptr, arena);
        else
            doMergeGCDBuckets<InnerTable, void *>(dst, gcd_bucket_tables, nullptr, arena);

        return;
    }

    auto window_keys_size = params->window_keys_num == 2 ? key_sizes[0] + key_sizes[1] : key_sizes[0];
    bool only_has_window_keys = key_sizes.size() == params->window_keys_num;

    /// In order to merge state with same other keys of different gcd buckets, reset the window group keys to zero
    /// create a new key, where the window key part is 0, and the other key parts are the same as the original value.
    /// For example:
    /// Key :           <window_start> +  <window_end> +  <other_keys...>
    /// New key :                0     +        0      +  <other_keys...>
    auto zero_out_bucket = [&, offset = bucket_key_offset.value()](const auto & key) -> std::decay_t<decltype(key)> {
        if constexpr (std::is_same_v<std::decay_t<decltype(key)>, std::string>)
        {
            /// Case-1: Serialized key, the window time keys always are always lower bits
            auto new_key = key;
            std::memset(new_key.data(), 0, window_keys_size);
            return new_key;
        }
        else
        {
            /// Case-2: SELECT ... FROM ... GROUP BY window_start or
            /// Case-2: SELECT ... FROM ... GROUP BY window_end or
            /// Case-2: SELECT ... FROM ... GROUP BY window_start, window_end or
            if (only_has_window_keys)
                return 0;

            /// Case-3: Fixed key
            return zeroOutTimeBucket(key, window_keys_size, offset);
        }
    };

    if (params->tracking_updates_type == TrackingUpdatesType::Updates)
        doMergeGCDBucketsForUpdates(dst, gcd_bucket_tables, src_updates, std::move(zero_out_bucket), arena);
    else
        doMergeGCDBuckets(dst, gcd_bucket_tables, std::move(zero_out_bucket), arena);
}

template <typename InnerTable, typename ZeroOutBucketForKey>
void HybridAggregator::doMergeGCDBuckets(
    InnerTable & dst, const std::map<Int64, InnerTable *> & gcd_bucket_tables, ZeroOutBucketForKey zero_out_bucket, Arena & arena) const
{
    for (auto [_, gcd_bucket_table] : gcd_bucket_tables)
    {
        /// FIXME, batch
        gcd_bucket_table->forEachKeyValue([&](const InnerTable::KeyType & key, auto value) {
            /// Remove GCD bucket from key since GCD bucket is encoded in the key
            HybridEmplaceResult emplace_result;
            if constexpr (std::is_same_v<ZeroOutBucketForKey, void *>)
            {
                emplace_result = dst.emplaceKey(key, /*disable_spill=*/true); /// disable spill for perf
            }
            else
            {
                auto key_without_bucket = zero_out_bucket(key);
                emplace_result = dst.emplaceKey(key_without_bucket, /*disable_spill=*/true); /// disable spill for perf
            }

            if (emplace_result.hasError())
                throw Exception::createRuntime(emplace_result.errorCode(), emplace_result.errorString());

            auto src_mapped = static_cast<ConstAggregateDataPtr>(value.getMapped());
            auto dst_mapped = static_cast<AggregateDataPtr>(emplace_result.getMutableMapped());
            mergeAggregateStates(dst_mapped, src_mapped, &arena);
        });

        dst.spillIfNecessary();
    }
}

template <typename InnerTable, typename Table, typename ZeroOutBucketForKey>
void HybridAggregator::doMergeGCDBucketsForUpdates(
    InnerTable & dst,
    const std::map<Int64, InnerTable *> & gcd_bucket_tables,
    Table * src_updates,
    ZeroOutBucketForKey zero_out_bucket,
    Arena & arena) const
{
    bool only_has_window_keys = key_sizes.size() == params->window_keys_num;
    if (only_has_window_keys)
    {
        /// Fast path, updates in any GCD bucket requires merge all GCD src bucket tables
        /// since there is only 1 key for each hop window
        bool has_updates = false;
        for (auto [bucket, _] : gcd_bucket_tables)
        {
            auto updates_bucket = src_updates->bucketTable(bucket);
            if (updates_bucket && !updates_bucket->empty())
            {
                has_updates = true;
                break;
            }
        }

        if (has_updates)
            doMergeGCDBuckets(dst, gcd_bucket_tables, std::move(zero_out_bucket), arena);
    }
    else
    {
        /// First collect all updated keys without the prefixing window since last
        src_updates->forEachKey([&](const Table::KeyType & key) {
            auto bucket = src_updates->timeBucket(key);
            if (!gcd_bucket_tables.contains(bucket))
                return;

            if constexpr (std::is_same_v<ZeroOutBucketForKey, void *>)
            {
                auto emplace_result = dst.emplaceKey(key, /*disable_spill=*/true); /// disable spill for perf
                if (emplace_result.hasError())
                    throw Exception::createRuntime(emplace_result.errorCode(), emplace_result.errorString());
            }
            else
            {
                auto key_without_bucket = zero_out_bucket(key);
                auto emplace_result = dst.emplaceKey(key_without_bucket, /*disable_spill=*/true); /// disable spill for perf
                if (emplace_result.hasError())
                    throw Exception::createRuntime(emplace_result.errorCode(), emplace_result.errorString());
            }
        });

        for (auto [_, gcd_bucket_table] : gcd_bucket_tables)
        {
            gcd_bucket_table->forEachKeyValue([&](const Table::KeyType & key, auto value) {
                HybridFindResult find_result;
                if constexpr (std::is_same_v<ZeroOutBucketForKey, void *>)
                {
                    find_result = dst.findKey(key, /*disable_spill=*/true);
                }
                else
                {
                    auto key_without_bucket = zero_out_bucket(key);
                    find_result = dst.findKey(key_without_bucket, /*disable_spill=*/true);
                }

                assert(!find_result.hasError()); /// Since we disable spill above, findkey can't fail
                if (find_result.isFound())
                {
                    auto * src_mapped = static_cast<ConstAggregateDataPtr>(value.getMapped());
                    auto * dst_mapped = static_cast<AggregateDataPtr>(find_result.getMutableMapped());
                    mergeAggregateStates(dst_mapped, src_mapped, &arena);
                }
            });
        }

        /// We didn't clear src_updates here since for hopping window, there is GCD window overlapping
        /// and updates for a GCD window can be used for the next hopping window
        /// src_updates->clear();
        dst.spillIfNecessary();
    }
}

}

}
