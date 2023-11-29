#include <Interpreters/Streaming/joinSerder.h>

#include <Interpreters/Streaming/joinDispatch.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int RECOVER_CHECKPOINT_FAILED;
}

namespace Streaming
{
namespace
{
template <typename DataBlock>
void serializeHashJoinMapsVariants(
    const RefCountDataBlockList<DataBlock> & blocks,
    const Block & header,
    const HashJoinMapsVariants & maps,
    const HashJoin & join,
    WriteBuffer & wb,
    SerializedRowRefListMultipleToIndices * serialized_row_ref_list_multiple_to_indices = nullptr)
{
    SerializedBlocksToIndices serialized_blocks_to_indices;
    blocks.serialize(header, wb, &serialized_blocks_to_indices);

    assert(maps.map_variants.size() >= 1);
    DB::writeIntBinary<UInt16>(static_cast<UInt16>(maps.map_variants.size()), wb);

    std::vector<const std::decay_t<decltype(maps.map_variants[0])> *> maps_vector;
    for (const auto & map : maps.map_variants)
        maps_vector.push_back(&map);

    auto mapped_serializer = [&](const auto & mapped_, WriteBuffer & wb_) {
        using Mapped = std::decay_t<decltype(mapped_)>;
        if constexpr (std::is_same_v<Mapped, RangeAsofRowRefs<DataBlock>>)
        {
            assert(join.getAsofType().has_value());
            mapped_.serialize(*join.getAsofType(), serialized_blocks_to_indices, wb_);
        }
        else if constexpr (std::is_same_v<Mapped, AsofRowRefs<DataBlock>>)
        {
            assert(join.getAsofType().has_value());
            mapped_.serialize(*join.getAsofType(), serialized_blocks_to_indices, wb_);
        }
        else if constexpr (std::is_same_v<Mapped, RowRefWithRefCount<DataBlock>>)
        {
            mapped_.serialize(serialized_blocks_to_indices, wb_);
        }
        else if constexpr (std::is_same_v<Mapped, RowRefListMultiplePtr<DataBlock>>)
        {
            mapped_->serialize(serialized_blocks_to_indices, wb_, serialized_row_ref_list_multiple_to_indices);
        }
        else if constexpr (std::is_same_v<Mapped, RowRefList<DataBlock>>)
        {
            mapped_.serialize(serialized_blocks_to_indices, wb_);
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The mapped type {} of hash join map isn't supported", typeid(Mapped).name());
    };

    joinDispatch(
        join.getStreamingKind(), join.getStreamingStrictness(), maps_vector, [&](auto /*kind_*/, auto /*strictness_*/, const auto & mapv) {
            for (auto * map : mapv)
                map->serialize(mapped_serializer, wb);
        });
}

template <typename DataBlock>
void deserializeHashJoinMapsVariants(
    RefCountDataBlockList<DataBlock> & blocks,
    const Block & header,
    HashJoinMapsVariants & maps,
    Arena & pool,
    const HashJoin & join,
    ReadBuffer & rb,
    DeserializedIndicesToRowRefListMultiple<DataBlock> * deserialized_indices_to_multiple_ref = nullptr)
{
    DeserializedIndicesToBlocks<DataBlock> deserialized_indices_to_blocks;
    blocks.deserialize(header, rb, &deserialized_indices_to_blocks);

    UInt16 maps_size;
    DB::readIntBinary<UInt16>(maps_size, rb);
    if (maps_size != maps.map_variants.size())
        throw Exception(
            ErrorCodes::RECOVER_CHECKPOINT_FAILED,
            "Failed to recover hash join checkpoint. The number of hash join maps varitans are not the same, checkpointed={}, current={}",
            maps_size,
            maps.map_variants.size());
    assert(maps_size >= 1);

    std::vector<std::decay_t<decltype(maps.map_variants[0])> *> maps_vector;
    for (auto & map : maps.map_variants)
        maps_vector.push_back(&map);

    auto mapped_deserializer = [&](auto & mapped_, [[maybe_unused]] Arena & pool_, ReadBuffer & rb_) {
        using Mapped = std::decay_t<decltype(mapped_)>;
        if constexpr (std::is_same_v<Mapped, RangeAsofRowRefs<DataBlock>>)
        {
            assert(join.getAsofType().has_value());
            mapped_.deserialize(*join.getAsofType(), deserialized_indices_to_blocks, rb_);
        }
        else if constexpr (std::is_same_v<Mapped, AsofRowRefs<DataBlock>>)
        {
            assert(join.getAsofType().has_value());
            mapped_.deserialize(*join.getAsofType(), &blocks, deserialized_indices_to_blocks, rb_);
        }
        else if constexpr (std::is_same_v<Mapped, RowRefWithRefCount<DataBlock>>)
        {
            mapped_.deserialize(&blocks, deserialized_indices_to_blocks, rb_);
        }
        else if constexpr (std::is_same_v<Mapped, RowRefListMultiplePtr<DataBlock>>)
        {
            mapped_ = std::make_unique<RowRefListMultiple<DataBlock>>();
            mapped_->deserialize(&blocks, deserialized_indices_to_blocks, rb_, deserialized_indices_to_multiple_ref);
        }
        else if constexpr (std::is_same_v<Mapped, RowRefList<DataBlock>>)
        {
            mapped_.deserialize(pool_, deserialized_indices_to_blocks, rb_);
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The mapped type {} of hash join map isn't supported", typeid(Mapped).name());
    };

    joinDispatch(
        join.getStreamingKind(), join.getStreamingStrictness(), maps_vector, [&](auto /*kind_*/, auto /*strictness_*/, auto & mapv) {
            for (auto * map : mapv)
                map->deserialize(mapped_deserializer, pool, rb);
        });
}
}

void serialize(
    const HashBlocks & hash_blocks,
    const Block & header,
    const HashJoin & join,
    WriteBuffer & wb,
    SerializedRowRefListMultipleToIndices * serialized_row_ref_list_multiple_to_indices)
{
    assert(hash_blocks.maps);
    serializeHashJoinMapsVariants(hash_blocks.blocks, header, *hash_blocks.maps, join, wb, serialized_row_ref_list_multiple_to_indices);

    /// FIXME: Useless for now
    // BlockNullmapList blocks_nullmaps;
}

void deserialize(
    HashBlocks & hash_blocks,
    const Block & header,
    const HashJoin & join,
    ReadBuffer & rb,
    DeserializedIndicesToRowRefListMultiple<JoinDataBlock> * deserialized_indices_to_multiple_ref)
{
    assert(hash_blocks.maps);
    deserializeHashJoinMapsVariants(
        hash_blocks.blocks, header, *hash_blocks.maps, hash_blocks.pool, join, rb, deserialized_indices_to_multiple_ref);

    /// FIXME: Useless for now
    // BlockNullmapList blocks_nullmaps;
}

void serialize(const HashJoin::JoinResults & join_results, const HashJoin & join, WriteBuffer & wb)
{
    std::scoped_lock lock(join_results.mutex);
    assert(join_results.maps);
    serializeHashJoinMapsVariants(join_results.blocks, join_results.sample_block, *join_results.maps, join, wb);
    join_results.metrics.serialize(wb);
}

void deserialize(HashJoin::JoinResults & join_results, const HashJoin & join, ReadBuffer & rb)
{
    std::scoped_lock lock(join_results.mutex);
    assert(join_results.maps);
    deserializeHashJoinMapsVariants(join_results.blocks, join_results.sample_block, *join_results.maps, join_results.pool, join, rb);
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

        join_data.primary_key_hash_table->map.serialize(
            /*MappedSerializer*/
            [&](const RowRefListMultipleRefPtr<JoinDataBlock> & mapped, WriteBuffer & wb_) {
                mapped->serialize(serialized_row_ref_list_multiple_to_indices, wb_);
            },
            wb);
    }
    else
        join_data.buffered_data->serialize(wb, nullptr);
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
        DeserializedIndicesToRowRefListMultiple<JoinDataBlock> deserialized_indices_to_multiple_ref;
        join_data.buffered_data->deserialize(rb, &deserialized_indices_to_multiple_ref);

        join_data.primary_key_hash_table->map.deserialize(
            /*MappedDeserializer*/
            [&](RowRefListMultipleRefPtr<JoinDataBlock> & mapped, Arena &, ReadBuffer & rb_) {
                mapped = std::make_unique<RowRefListMultipleRef<JoinDataBlock>>();
                mapped->deserialize(deserialized_indices_to_multiple_ref, rb_);
            },
            join_data.primary_key_hash_table->pool,
            rb);
    }
    else
        join_data.buffered_data->deserialize(rb, nullptr);
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
