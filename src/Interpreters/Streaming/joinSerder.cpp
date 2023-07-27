#include <Interpreters/Streaming/joinSerder.h>

#include <Interpreters/Streaming/joinDispatch.h>

namespace DB
{
namespace ErrorCodes
{
extern const int RECOVER_CHECKPOINT_FAILED;
}

namespace Streaming
{
namespace
{
template <typename MapsTemplate>
void serializeHashJoinMap(
    MapsTemplate & maps_template,
    HashJoin::Type hash_method_type,
    const std::optional<TypeIndex> & asof_type,
    WriteBuffer & wb,
    [[maybe_unused]] SerializedBlocksToIndices * serialized_blocks_to_indices,
    [[maybe_unused]] SerializedRowRefListMultipleToIndices * serialized_row_ref_list_multiple_to_indices)
{
    switch (hash_method_type)
    {
#define M(TYPE) \
    case HashJoin::Type::TYPE: { \
        assert(maps_template.TYPE); \
        using Map = std::decay_t<decltype(*(maps_template.TYPE))>; \
        using Mapped = typename Map::mapped_type; \
        Map & map = *(maps_template.TYPE); \
        DB::writeIntBinary<size_t>(map.size(), wb); \
        map.forEachValue([&](const auto & key, auto & mapped) { \
            /* Key */ \
            DB::writeBinary(key, wb); \
            /* Mapped: only write the block index and row num */ \
            if constexpr (std::is_same_v<Mapped, RangeAsofRowRefs>) \
            { \
                assert(asof_type.has_value() && serialized_blocks_to_indices); \
                mapped.serialize(*asof_type, *serialized_blocks_to_indices, wb); \
            } \
            else if constexpr (std::is_same_v<Mapped, AsofRowRefs>) \
            { \
                assert(asof_type.has_value() && serialized_blocks_to_indices); \
                mapped.serialize(*asof_type, *serialized_blocks_to_indices, wb); \
            } \
            else if constexpr (std::is_same_v<Mapped, RowRefWithRefCount<Block>>) \
            { \
                assert(serialized_blocks_to_indices); \
                mapped.serialize(*serialized_blocks_to_indices, wb); \
            } \
            else if constexpr (std::is_same_v<Mapped, RowRefListMultiplePtr>) \
            { \
                assert(serialized_blocks_to_indices); \
                mapped->serialize(*serialized_blocks_to_indices, wb, serialized_row_ref_list_multiple_to_indices); \
            } \
            else if constexpr (std::is_same_v<Mapped, RowRefListMultipleRefPtr>) \
            { \
                assert(serialized_row_ref_list_multiple_to_indices); \
                mapped->serialize(*serialized_row_ref_list_multiple_to_indices, wb); \
            } \
            else \
            { \
                static_assert(std::is_same_v<Mapped, RowRefList>); \
                assert(serialized_blocks_to_indices); \
                serialize(mapped, *serialized_blocks_to_indices, wb); \
            } \
        }); \
        break; \
    }
        APPLY_FOR_JOIN_VARIANTS(M)
#undef M
    }
}

template <typename MapsTemplate>
void deserializeHashJoinMap(
    MapsTemplate & maps_template,
    HashJoin::Type hash_method_type,
    const std::optional<TypeIndex> & asof_type,
    Arena & pool,
    ReadBuffer & rb,
    [[maybe_unused]] RefCountBlockList<Block> * blocks,
    [[maybe_unused]] const DeserializedIndicesToBlocks<Block> * deserialized_indices_to_blocks,
    [[maybe_unused]] DeserializedIndicesToRowRefListMultiple * deserialized_indices_with_multiple_ref)
{
    switch (hash_method_type)
    {
#define M(TYPE) \
    case HashJoin::Type::TYPE: { \
        assert(maps_template.TYPE); \
        using Map = std::decay_t<decltype(*(maps_template.TYPE))>; \
        using Mapped = typename Map::mapped_type; \
        Map & map = *(maps_template.TYPE); \
        typename Map::key_type key; \
        typename Map::LookupResult lookup_result; \
        bool inserted; \
        size_t map_size; \
        DB::readIntBinary<size_t>(map_size, rb); \
        for (size_t i = 0; i < map_size; ++i) \
        { \
            /* Key */ \
            if constexpr (std::is_same_v<typename Map::key_type, StringRef>) \
            { \
                key = DB::readStringBinaryInto(pool, rb); \
                map.emplace(SerializedKeyHolder{key, pool}, lookup_result, inserted); \
            } \
            else \
            { \
                DB::readBinary(key, rb); \
                map.emplace(key, lookup_result, inserted); \
            } \
            assert(inserted); \
            /* Mapped: read the block index and row num ref */ \
            Mapped & mapped = lookup_result->getMapped(); \
            if constexpr (std::is_same_v<Mapped, RangeAsofRowRefs>) \
            { \
                assert(asof_type.has_value() && deserialized_indices_to_blocks); \
                mapped.deserialize(*asof_type, *deserialized_indices_to_blocks, rb); \
            } \
            else if constexpr (std::is_same_v<Mapped, AsofRowRefs>) \
            { \
                assert(asof_type.has_value() && blocks && deserialized_indices_to_blocks); \
                mapped.deserialize(*asof_type, blocks, *deserialized_indices_to_blocks, rb); \
            } \
            else if constexpr (std::is_same_v<Mapped, RowRefWithRefCount<Block>>) \
            { \
                assert(blocks && deserialized_indices_to_blocks); \
                mapped.deserialize(blocks, *deserialized_indices_to_blocks, rb); \
            } \
            else if constexpr (std::is_same_v<Mapped, RowRefListMultiplePtr>) \
            { \
                assert(blocks && deserialized_indices_to_blocks); \
                mapped = std::make_unique<RowRefListMultiple>(); \
                mapped->deserialize(blocks, *deserialized_indices_to_blocks, rb, deserialized_indices_with_multiple_ref); \
            } \
            else if constexpr (std::is_same_v<Mapped, RowRefListMultipleRefPtr>) \
            { \
                assert(deserialized_indices_with_multiple_ref); \
                mapped = std::make_unique<RowRefListMultipleRef>(); \
                mapped->deserialize(*deserialized_indices_with_multiple_ref, rb); \
            } \
            else \
            { \
                static_assert(std::is_same_v<Mapped, RowRefList>); \
                assert(deserialized_indices_to_blocks); \
                deserialize(mapped, pool, *deserialized_indices_to_blocks, rb); \
            } \
        } \
        break; \
    }
        APPLY_FOR_JOIN_VARIANTS(M)
#undef M
    }
}

void serializeHashJoinMapsVariants(
    const RefCountBlockList<Block> & blocks,
    const HashJoinMapsVariants & maps,
    const HashJoin & join,
    WriteBuffer & wb,
    SerializedRowRefListMultipleToIndices * serialized_row_ref_list_multiple_to_indices)
{
    SerializedBlocksToIndices serialized_blocks_to_indices;
    blocks.serialize(/*no use*/Block{}, wb, &serialized_blocks_to_indices);

    assert(maps.map_variants.size() >= 1);
    DB::writeIntBinary<UInt16>(static_cast<UInt16>(maps.map_variants.size()), wb);

    std::vector<const std::decay_t<decltype(maps.map_variants[0])> *> maps_vector;
    for (auto & map : maps.map_variants)
        maps_vector.push_back(&map);

    joinDispatch(
        join.getStreamingKind(), join.getStreamingStrictness(), maps_vector, [&](auto /*kind_*/, auto /*strictness_*/, const auto & mapv) {
            for (auto * map : mapv)
                serializeHashJoinMap(
                    *map,
                    join.getHashMethodType(),
                    join.getAsofType(),
                    wb,
                    &serialized_blocks_to_indices,
                    serialized_row_ref_list_multiple_to_indices);
        });
}

void deserializeHashJoinMapsVariants(
    RefCountBlockList<Block> & blocks,
    HashJoinMapsVariants & maps,
    Arena & pool,
    const HashJoin & join,
    ReadBuffer & rb,
    DeserializedIndicesToRowRefListMultiple * deserialized_indices_with_row_ref_list_multiple)
{
    DeserializedIndicesToBlocks<Block> deserialized_indices_to_blocks;
    blocks.deserialize(/*no use*/Block{}, rb, &deserialized_indices_to_blocks);

    UInt16 maps_size;
    DB::readIntBinary<UInt16>(maps_size, rb);
    if (maps_size != maps.map_variants.size())
        throw Exception(
            ErrorCodes::RECOVER_CHECKPOINT_FAILED,
            "Failed to recover hash join checkpoint. The number of hash join maps varitans are not the same, checkpointed={}, current={}",
            maps_size,
            maps.map_variants.size());
    assert(maps_size >= 1);

    std::vector<const std::decay_t<decltype(maps.map_variants[0])> *> maps_vector;
    for (auto & map : maps.map_variants)
        maps_vector.push_back(&map);

    joinDispatch(
        join.getStreamingKind(), join.getStreamingStrictness(), maps_vector, [&](auto /*kind_*/, auto /*strictness_*/, const auto & mapv) {
            for (auto * map : mapv)
                deserializeHashJoinMap(
                    *map,
                    join.getHashMethodType(),
                    join.getAsofType(),
                    pool,
                    rb,
                    &blocks,
                    &deserialized_indices_to_blocks,
                    deserialized_indices_with_row_ref_list_multiple);
        });
}
}

void serialize(const RowRef & row_ref, const SerializedBlocksToIndices & serialized_blocks_to_indices, WriteBuffer & wb)
{
    DB::writeIntBinary<UInt32>(serialized_blocks_to_indices.at(reinterpret_cast<std::uintptr_t>(row_ref.block)), wb);
    DB::writeBinary(row_ref.row_num, wb);
}

void deserialize(RowRef & row_ref, const DeserializedIndicesToBlocks<Block> & deserialized_indices_to_blocks, ReadBuffer & rb)
{
    UInt32 block_index;
    DB::readIntBinary<UInt32>(block_index, rb);
    row_ref.block = &(deserialized_indices_to_blocks.at(block_index)->block);
    DB::readBinary(row_ref.row_num, rb);
}

void serialize(const RowRefList & row_ref, const SerializedBlocksToIndices & serialized_blocks_to_indices, WriteBuffer & wb)
{
    /// Row list with same key.
    UInt32 size = 0;
    for (auto it = row_ref.begin(); it.ok(); ++it)
        ++size;

    /// At least has current one, first one always is itself
    assert(size > 0);

    DB::writeIntBinary<UInt32>(size, wb);

    for (auto it = row_ref.begin(); it.ok(); ++it)
        serialize(**it, serialized_blocks_to_indices, wb);
}

void deserialize(
    RowRefList & row_ref, Arena & pool, const DeserializedIndicesToBlocks<Block> & deserialized_indices_to_blocks, ReadBuffer & rb)
{
    UInt32 size;
    DB::readIntBinary<UInt32>(size, rb);
    if (size == 0)
        throw Exception(
            ErrorCodes::RECOVER_CHECKPOINT_FAILED, "Failed to recover hash join checkpoint. Got an invalid count of row ref list");

    /// First one always is itself.
    deserialize(static_cast<RowRef &>(row_ref), deserialized_indices_to_blocks, rb);

    /// Other row list with same key.
    RowRef other_row_ref;
    for (UInt32 i = 1; i < size; ++i)
    {
        deserialize(other_row_ref, deserialized_indices_to_blocks, rb);
        row_ref.insert(std::move(other_row_ref), pool);
    }
}

void serialize(
    const HashBlocks & hash_blocks,
    const HashJoin & join,
    WriteBuffer & wb,
    SerializedRowRefListMultipleToIndices * serialized_row_ref_list_multiple_to_indices)
{
    assert(hash_blocks.maps);
    serializeHashJoinMapsVariants(hash_blocks.blocks, *hash_blocks.maps, join, wb, serialized_row_ref_list_multiple_to_indices);

    /// FIXME: Useless for now
    // BlockNullmapList blocks_nullmaps;
}

void deserialize(
    HashBlocks & hash_blocks,
    const HashJoin & join,
    ReadBuffer & rb,
    DeserializedIndicesToRowRefListMultiple * deserialized_indices_to_multiple_ref)
{
    assert(hash_blocks.maps);
    deserializeHashJoinMapsVariants(
        hash_blocks.blocks, *hash_blocks.maps, hash_blocks.pool, join, rb, deserialized_indices_to_multiple_ref);

    /// FIXME: Useless for now
    // BlockNullmapList blocks_nullmaps;
}

void serialize(const HashJoin::JoinResults & join_results, const HashJoin & join, WriteBuffer & wb)
{
    std::scoped_lock lock(join_results.mutex);
    DB::writeBinary(join_results.block_id, wb);
    assert(join_results.maps);
    serializeHashJoinMapsVariants(join_results.blocks, *join_results.maps, join, wb, /* SerializedRowRefListMultipleToIndices* */ nullptr);
    join_results.metrics.serialize(wb);
}

void deserialize(HashJoin::JoinResults & join_results, const HashJoin & join, ReadBuffer & rb)
{
    std::scoped_lock lock(join_results.mutex);
    DB::readBinary(join_results.block_id, rb);
    assert(join_results.maps);
    deserializeHashJoinMapsVariants(
        join_results.blocks, *join_results.maps, join_results.pool, join, rb, /* DeserializedIndicesToRowRefListMultiple* */ nullptr);
    join_results.metrics.deserialize(rb);
}

void serialize(const HashJoin::JoinData & join_data, WriteBuffer & wb)
{
    bool has_data = static_cast<bool>(join_data.buffered_data);
    writeBoolText(has_data, wb);
    if (!has_data)
        return;

    bool has_primary_key_hash_table = static_cast<bool>(join_data.primary_key_hash_table);
    writeBoolText(has_primary_key_hash_table, wb);
    if (has_primary_key_hash_table)
    {
        SerializedRowRefListMultipleToIndices serialized_row_ref_list_multiple_to_indices;
        join_data.buffered_data->serialize(wb, &serialized_row_ref_list_multiple_to_indices);

        serializeHashJoinMap(
            join_data.primary_key_hash_table->map,
            join_data.primary_key_hash_table->hash_method_type,
            /*asof type*/ std::nullopt,
            wb,
            nullptr,
            &serialized_row_ref_list_multiple_to_indices);
    }
    else
        join_data.buffered_data->serialize(wb);
}

void deserialize(HashJoin::JoinData & join_data, ReadBuffer & rb)
{
    bool has_data = static_cast<bool>(join_data.buffered_data);
    bool recovered_has_data;
    readBoolText(recovered_has_data, rb);
    if (recovered_has_data != has_data)
        throw Exception(
            ErrorCodes::RECOVER_CHECKPOINT_FAILED,
            "Failed to recover hash join checkpoint. The data of hash join are not the same, checkpointed={}, current={}",
            recovered_has_data ? "Has JoinData" : "No JoinData",
            has_data ? "Has JoinData" : "No JoinData");

    if (!has_data)
        return;

    bool has_primary_key_hash_table = static_cast<bool>(join_data.primary_key_hash_table);
    bool recovered_has_primary_key_hash_table;
    readBoolText(recovered_has_primary_key_hash_table, rb);
    if (recovered_has_primary_key_hash_table != has_primary_key_hash_table)
        throw Exception(
            ErrorCodes::RECOVER_CHECKPOINT_FAILED,
            "Failed to recover hash join checkpoint. The primary key hash table of hash join are not the same, checkpointed={}, current={}",
            recovered_has_primary_key_hash_table ? "Has PrimaryKeyHashTable" : "No PrimaryKeyHashTable",
            has_primary_key_hash_table ? "Has PrimaryKeyHashTable" : "No PrimaryKeyHashTable");

    if (has_primary_key_hash_table)
    {
        DeserializedIndicesToRowRefListMultiple deserialized_indices_to_multiple_ref;
        join_data.buffered_data->deserialize(rb, &deserialized_indices_to_multiple_ref);

        deserializeHashJoinMap(
            join_data.primary_key_hash_table->map,
            join_data.primary_key_hash_table->hash_method_type,
            /* asof type */ std::nullopt,
            join_data.primary_key_hash_table->pool,
            rb,
            /* RefCountBlockList<Block>* */ nullptr,
            /* DeserializedIndicesToBlocks<Block>* */ nullptr,
            &deserialized_indices_to_multiple_ref);
    }
    else
        join_data.buffered_data->deserialize(rb);
}

void serialize(const HashJoin::JoinGlobalMetrics & join_metrics, WriteBuffer & wb)
{
    DB::writeBinary(join_metrics.total_join, wb);
    DB::writeBinary(join_metrics.left_block_and_right_range_bucket_no_intersection_skip, wb);
    DB::writeBinary(join_metrics.right_block_and_left_range_bucket_no_intersection_skip, wb);
}

void deserialize(HashJoin::JoinGlobalMetrics & join_metrics, ReadBuffer & rb)
{
    DB::readBinary(join_metrics.total_join, rb);
    DB::readBinary(join_metrics.left_block_and_right_range_bucket_no_intersection_skip, rb);
    DB::readBinary(join_metrics.right_block_and_left_range_bucket_no_intersection_skip, rb);
}

}
}
