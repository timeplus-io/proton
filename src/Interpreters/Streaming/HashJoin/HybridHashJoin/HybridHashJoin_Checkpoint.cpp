#include <Interpreters/Streaming/HashJoin/HybridHashJoin/HybridHashJoin.h>

#include <Cluster/Common/serde.h>

namespace DB
{
namespace ErrorCodes
{
extern const int RECOVER_CHECKPOINT_FAILED;
}

namespace Streaming
{
RocksPtr HybridHashJoin::getOrCreateRocks()
{
    /// Initialize rocks on first use
    if (!rocks)
        rocks = Rocks::createOrLoadIfExists(
            base_config.getRocksOptions(), base_config.spill_dir_path, base_config.cleanup_on_disk_data, logger);

    return rocks;
}

void HybridHashJoin::shutdownRocks()
{
    if (rocks)
    {
        rocks->shutdown(true);
        rocks.reset();
    }
}

void HybridHashJoin::installRocks(const String & spill_dir_, size_t max_hot_key_count_)
{
    base_config.spill_dir_path = spill_dir_;
    base_config.max_hot_key_count = max_hot_key_count_;
    base_config.cleanup_on_disk_data = true;
    base_config.rocks_handler_getter = [this](const std::string & id) { return getOrCreateRocks()->getOrCreateHandler(id); };
}

void HybridHashJoin::reinstallRocks()
{
    if (rocks && !rocks->isShutdown())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Rocks is already installed");

    rocks
        = Rocks::createOrLoadIfExists(base_config.getRocksOptions(), base_config.spill_dir_path, base_config.cleanup_on_disk_data, logger);
}

void HybridHashJoin::serialize(WriteBuffer & wb, VersionType version) const
{
    /// Part-1: ON clauses
    DB::writeStringBinary(TableJoin::formatClauses(table_join->getClauses(), true), wb);

    /// Part-2: Description of left/right join stream
    DB::writeStringBinary(left_data.join_ctx.join_stream_desc->input_header.dumpStructure(), wb);
    cluster::serializeEnum(left_data.join_ctx.join_stream_desc->data_stream_semantic.semantic, wb);
    DB::writeVarUInt(left_data.join_ctx.join_stream_desc->keep_versions, wb);

    DB::writeStringBinary(right_data.join_ctx.join_stream_desc->input_header.dumpStructure(), wb);
    cluster::serializeEnum(right_data.join_ctx.join_stream_desc->data_stream_semantic.semantic, wb);
    DB::writeVarUInt(right_data.join_ctx.join_stream_desc->keep_versions, wb);

    /// Part-3: Join method
    cluster::serializeEnum(streaming_kind, wb);
    cluster::serializeEnum(streaming_strictness, wb);
    // DB::writeBinary(key_sizes, wb); /// No need after serialized `ON clauses` and left/right streams's header
    cluster::serializeEnum(hash_method_type, wb);

    /// Part-4: Buffered data of left/right join stream
    if (bidirectional_hash_join)
    {
        chassert(left_data.index);
        left_data.index->serialize(wb, version);
    }

    chassert(right_data.index);
    right_data.index->serialize(wb, version);

    /// Part-5: Asof type (Optional)
    bool need_asof = streaming_strictness == Strictness::Range || streaming_strictness == Strictness::Asof;
    if (need_asof)
    {
        chassert(asof_type.has_value());
        cluster::serializeEnum(*asof_type, wb);
        cluster::serializeEnum(asof_inequality, wb);
    }

    /// Part-6: Emit changelog (Optional)
    DB::writeBinary(join_results.has_value(), wb);
    if (join_results.has_value())
    {
        chassert(retract_push_down && emit_changelog);
        std::scoped_lock lock(join_results->mutex);
        serializeHybridHashJoinMapsVariants(*join_results->index, wb, version, *this);
    }

    /// Part-7: Others
    DB::writeIntBinary(combined_watermark.load(), wb);
    join_metrics.serialize(wb, version);
}

void HybridHashJoin::deserialize(ReadBuffer & rb, VersionType version)
{
    { /// Part-1: ON clauses
        auto clauses_str = TableJoin::formatClauses(table_join->getClauses(), true);
        String recovered_clauses_str;
        DB::readStringBinary(recovered_clauses_str, rb);
        if (recovered_clauses_str != clauses_str)
            throw Exception(
                ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                "Failed to recover hybrid hash join checkpoint. The ON clauses of join are not the same, checkpointed={}, current={}",
                recovered_clauses_str,
                clauses_str);
    }

    { /// Part-2: Description of left/right join stream
        String recovered_left_header_str;
        UInt64 recovered_left_keep_versions;
        DB::readStringBinary(recovered_left_header_str, rb);
        auto recovered_left_stream_semantic = cluster::deserializeEnum<DataStreamSemantic>(rb);
        DB::readVarUInt(recovered_left_keep_versions, rb);

        auto left_header_str = left_data.join_ctx.join_stream_desc->input_header.dumpStructure();
        if (recovered_left_header_str != left_header_str
            || recovered_left_stream_semantic != left_data.join_ctx.join_stream_desc->data_stream_semantic.semantic
            || recovered_left_keep_versions != left_data.join_ctx.join_stream_desc->keep_versions)
            throw Exception(
                ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                "Failed to recover hybrid hash join checkpoint. The description of left join stream are not the same, checkpointed: "
                "header={}, semantic={}, keep_versions={}, but current: header={}, semantic={}, keep_versions={}",
                recovered_left_header_str,
                recovered_left_stream_semantic,
                recovered_left_keep_versions,
                left_header_str,
                left_data.join_ctx.join_stream_desc->data_stream_semantic.semantic,
                left_data.join_ctx.join_stream_desc->keep_versions);

        String recovered_right_header_str;
        UInt64 recovered_right_keep_versions;
        DB::readStringBinary(recovered_right_header_str, rb);
        auto recovered_right_stream_semantic = cluster::deserializeEnum<DataStreamSemantic>(rb);
        DB::readVarUInt(recovered_right_keep_versions, rb);

        auto right_header_str = right_data.join_ctx.join_stream_desc->input_header.dumpStructure();
        if (recovered_right_header_str != right_header_str
            || recovered_right_stream_semantic != right_data.join_ctx.join_stream_desc->data_stream_semantic.semantic
            || recovered_right_keep_versions != right_data.join_ctx.join_stream_desc->keep_versions)
            throw Exception(
                ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                "Failed to recover hybrid hash join checkpoint. The description of right join stream are not the same, checkpointed: "
                "header={}, semantic={}, keep_versions={}, but current: header={}, semantic={}, keep_versions={}",
                recovered_right_header_str,
                recovered_right_stream_semantic,
                recovered_right_keep_versions,
                right_header_str,
                right_data.join_ctx.join_stream_desc->data_stream_semantic.semantic,
                right_data.join_ctx.join_stream_desc->keep_versions);
    }

    { /// Part-3: Join method
        auto recovered_streaming_kind = cluster::deserializeEnum<Kind>(rb);
        if (recovered_streaming_kind != streaming_kind)
            throw Exception(
                ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                "Failed to recover hybrid hash join checkpoint. The kind of join are not the same, checkpointed={}, current={}",
                magic_enum::enum_name(recovered_streaming_kind),
                magic_enum::enum_name(streaming_kind));

        auto recovered_streaming_strictness = cluster::deserializeEnum<Strictness>(rb);
        if (recovered_streaming_strictness != streaming_strictness)
            throw Exception(
                ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                "Failed to recover hybrid hash join checkpoint. The strictness of join are not the same, checkpointed={}, current={}",
                magic_enum::enum_name(recovered_streaming_strictness),
                magic_enum::enum_name(streaming_strictness));

        auto recovered_hash_method_type = cluster::deserializeEnum<HybridHashType>(rb);
        if (recovered_hash_method_type != hash_method_type)
            throw Exception(
                ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                "Failed to recover hybrid hash join checkpoint. The hash method type of join are not the same, checkpointed={}, current={}",
                magic_enum::enum_name(recovered_hash_method_type),
                magic_enum::enum_name(hash_method_type));
    }

    /// Part-4: Buffered data of left/right join stream
    if (bidirectional_hash_join)
    {
        chassert(left_data.index);
        left_data.index->deserialize(rb, version);
    }

    chassert(right_data.index);
    right_data.index->deserialize(rb, version);

    /// Part-5: Asof type (Optional)
    bool need_asof = streaming_strictness == Strictness::Range || streaming_strictness == Strictness::Asof;
    if (need_asof)
    {
        chassert(asof_type.has_value());
        auto recovered_asof_type = cluster::deserializeEnum<TypeIndex>(rb);
        if (recovered_asof_type != *asof_type)
            throw Exception(
                ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                "Failed to recover hybrid hash join checkpoint. The asof type of join are not the same, checkpointed={}, current={}",
                magic_enum::enum_name(recovered_asof_type),
                magic_enum::enum_name(*asof_type));

        auto recovered_asof_inequality = cluster::deserializeEnum<ASOFJoinInequality>(rb);
        if (recovered_asof_inequality != asof_inequality)
            throw Exception(
                ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                "Failed to recover hybrid hash join checkpoint. The asof inequality of join are not the same, checkpointed={}, current={}",
                magic_enum::enum_name(recovered_asof_inequality),
                magic_enum::enum_name(asof_inequality));
    }

    /// Part-6: Emit changelog (Optional)
    bool need_emit_changelog;
    DB::readBinary(need_emit_changelog, rb);
    if (need_emit_changelog)
    {
        if (!join_results.has_value())
            throw Exception(
                ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                "Failed to recover hybrid hash join checkpoint. The emit changelog strategy of join are not the same, checkpointed={}, "
                "current={}",
                need_emit_changelog,
                join_results.has_value());

        chassert(retract_push_down && emit_changelog);
        std::scoped_lock lock(join_results->mutex);
        deserializeHybridHashJoinMapsVariants(*join_results->index, rb, version, *this);
    }

    /// Part-7: Others
    int64_t recovered_combined_watermark;
    DB::readIntBinary(recovered_combined_watermark, rb);
    combined_watermark = recovered_combined_watermark;

    join_metrics.deserialize(rb, version);
}

void serializeHybridHashJoinMapsVariants(
    const HybridHashJoinMapsVariants & index, WriteBuffer & wb, VersionType /*version*/, const HybridHashJoin & join)
{
    chassert(!index.empty());

    DB::writeVarUInt(index.size(), wb);

    std::vector<const HybridMapsVariant *> maps_vector;
    maps_vector.reserve(index.size());
    for (size_t i = 0; i < index.size(); ++i)
        maps_vector.push_back(&index[i]);

    hybridJoinDispatch(
        join.getStreamingKind(),
        join.getStreamingStrictness(),
        maps_vector,
        [&](auto /*kind_*/, auto /*strictness_*/, const auto & maps_vector_) {
            for (const auto & map : maps_vector_)
                map->table.serialize(wb);
        });
}

void deserializeHybridHashJoinMapsVariants(
    HybridHashJoinMapsVariants & index, ReadBuffer & rb, VersionType /*version*/, const HybridHashJoin & join)
{
    chassert(!index.empty());

    size_t size = 0;
    DB::readVarUInt(size, rb);
    if (size != index.size())
        throw Exception(
            ErrorCodes::RECOVER_CHECKPOINT_FAILED,
            "Failed to recover hybrid hash join checkpoint. The number of hash join maps varitans are not the same, checkpointed={}, "
            "current={}",
            size,
            index.size());

    std::vector<HybridMapsVariant *> maps_vector;
    maps_vector.reserve(index.size());
    for (size_t i = 0; i < index.size(); ++i)
        maps_vector.push_back(&index[i]);

    hybridJoinDispatch(
        join.getStreamingKind(),
        join.getStreamingStrictness(),
        maps_vector,
        [&](auto /*kind_*/, auto /*strictness_*/, auto & maps_vector_) {
            for (const auto & map : maps_vector_)
                map->table.deserialize(rb);
        });
}

/// Write / read data to / from RocksDB
void HybridHashJoin::write(VersionType version)
{
    auto rocks_handler = getOrCreateRocks()->getOrCreateHandler();
    /// Part-1: ON clauses
    rocks_handler->put("on_clauses", TableJoin::formatClauses(table_join->getClauses(), true));

    /// Part-2: Description of left/right join stream
    rocks_handler->put("left_header", left_data.join_ctx.join_stream_desc->input_header.dumpStructure());
    rocks_handler->put("left_stream_semantic", left_data.join_ctx.join_stream_desc->data_stream_semantic.semantic);
    rocks_handler->put("left_keep_versions", left_data.join_ctx.join_stream_desc->keep_versions);

    rocks_handler->put("right_header", right_data.join_ctx.join_stream_desc->input_header.dumpStructure());
    rocks_handler->put("right_stream_semantic", right_data.join_ctx.join_stream_desc->data_stream_semantic.semantic);
    rocks_handler->put("right_keep_versions", right_data.join_ctx.join_stream_desc->keep_versions);

    /// Part-3: Join method
    rocks_handler->put("streaming_kind", streaming_kind);
    rocks_handler->put("streaming_strictness", streaming_strictness);
    /// rocks_handler->put("key_sizes", key_sizes); /// No need after serialized `ON clauses` and left/right streams's header
    rocks_handler->put("hash_method_type", hash_method_type);

    /// Part-4: Buffered data of left/right join stream
    rocks_handler->put("bidirectional_hash_join", bidirectional_hash_join);
    if (bidirectional_hash_join)
    {
        chassert(left_data.index);
        left_data.index->write(getOrCreateRocks()->getOrCreateHandler(left_data.index->id), version);
    }

    chassert(right_data.index);
    right_data.index->write(getOrCreateRocks()->getOrCreateHandler(right_data.index->id), version);

    /// Part-5: Asof type (Optional)
    bool need_asof = streaming_strictness == Strictness::Range || streaming_strictness == Strictness::Asof;
    rocks_handler->put("need_asof", need_asof);
    if (need_asof)
    {
        chassert(asof_type.has_value());
        rocks_handler->put("asof_type", *asof_type);
        rocks_handler->put("asof_inequality", asof_inequality);
    }

    /// Part-6: Emit changelog (Optional)
    rocks_handler->put("has_join_results", join_results.has_value());
    if (join_results.has_value())
    {
        chassert(retract_push_down && emit_changelog);
        std::scoped_lock lock(join_results->mutex);
        writeHybridHashJoinMapsVariants(*join_results->index, rocks_handler, "join_results", *this);
    }

    /// Part-7: Others
    rocks_handler->put("combined_watermark", combined_watermark.load());

    WriteBufferFromOwnString wb;
    join_metrics.serialize(wb, version);
    rocks_handler->put("join_metrics", wb.str());
}

void HybridHashJoin::read(VersionType version)
{
    auto rocks_handler = getOrCreateRocks()->getOrCreateHandler();
    { /// Part-1: ON clauses
        auto clauses_str = TableJoin::formatClauses(table_join->getClauses(), true);
        String recovered_clauses_str;
        rocks_handler->get("on_clauses", recovered_clauses_str);
        if (recovered_clauses_str != clauses_str)
            throw Exception(
                ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                "Failed to recover hybrid hash join checkpoint. The ON clauses of join are not the same, checkpointed={}, current={}",
                recovered_clauses_str,
                clauses_str);
    }

    { /// Part-2: Description of left/right join stream
        String recovered_left_header_str;
        DataStreamSemantic recovered_left_stream_semantic;
        UInt64 recovered_left_keep_versions;
        rocks_handler->get("left_header", recovered_left_header_str);
        rocks_handler->get("left_stream_semantic", recovered_left_stream_semantic);
        rocks_handler->get("left_keep_versions", recovered_left_keep_versions);

        auto left_header_str = left_data.join_ctx.join_stream_desc->input_header.dumpStructure();
        if (recovered_left_header_str != left_header_str
            || recovered_left_stream_semantic != left_data.join_ctx.join_stream_desc->data_stream_semantic.semantic
            || recovered_left_keep_versions != left_data.join_ctx.join_stream_desc->keep_versions)
            throw Exception(
                ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                "Failed to recover hybrid hash join checkpoint. The description of left join stream are not the same, checkpointed: "
                "header={}, semantic={}, keep_versions={}, but current: header={}, semantic={}, keep_versions={}",
                recovered_left_header_str,
                recovered_left_stream_semantic,
                recovered_left_keep_versions,
                left_header_str,
                left_data.join_ctx.join_stream_desc->data_stream_semantic.semantic,
                left_data.join_ctx.join_stream_desc->keep_versions);

        String recovered_right_header_str;
        DataStreamSemantic recovered_right_stream_semantic;
        UInt64 recovered_right_keep_versions;
        rocks_handler->get("right_header", recovered_right_header_str);
        rocks_handler->get("right_stream_semantic", recovered_right_stream_semantic);
        rocks_handler->get("right_keep_versions", recovered_right_keep_versions);

        auto right_header_str = right_data.join_ctx.join_stream_desc->input_header.dumpStructure();
        if (recovered_right_header_str != right_header_str
            || recovered_right_stream_semantic != right_data.join_ctx.join_stream_desc->data_stream_semantic.semantic
            || recovered_right_keep_versions != right_data.join_ctx.join_stream_desc->keep_versions)
            throw Exception(
                ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                "Failed to recover hybrid hash join checkpoint. The description of right join stream are not the same, checkpointed: "
                "header={}, semantic={}, keep_versions={}, but current: header={}, semantic={}, keep_versions={}",
                recovered_right_header_str,
                recovered_right_stream_semantic,
                recovered_right_keep_versions,
                right_header_str,
                right_data.join_ctx.join_stream_desc->data_stream_semantic.semantic,
                right_data.join_ctx.join_stream_desc->keep_versions);
    }

    { /// Part-3: Join method
        auto recovered_streaming_kind = streaming_kind;
        rocks_handler->get("streaming_kind", recovered_streaming_kind);
        if (recovered_streaming_kind != streaming_kind)
            throw Exception(
                ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                "Failed to recover hybrid hash join checkpoint. The kind of join are not the same, checkpointed={}, current={}",
                magic_enum::enum_name(recovered_streaming_kind),
                magic_enum::enum_name(streaming_kind));

        auto recovered_streaming_strictness = streaming_strictness;
        rocks_handler->get("streaming_strictness", recovered_streaming_strictness);
        if (recovered_streaming_strictness != streaming_strictness)
            throw Exception(
                ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                "Failed to recover hybrid hash join checkpoint. The strictness of join are not the same, checkpointed={}, current={}",
                magic_enum::enum_name(recovered_streaming_strictness),
                magic_enum::enum_name(streaming_strictness));

        auto recovered_hash_method_type = hash_method_type;
        rocks_handler->get("hash_method_type", recovered_hash_method_type);
        if (recovered_hash_method_type != hash_method_type)
            throw Exception(
                ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                "Failed to recover hybrid hash join checkpoint. The hash method type of join are not the same, checkpointed={}, current={}",
                magic_enum::enum_name(recovered_hash_method_type),
                magic_enum::enum_name(hash_method_type));
    }

    /// Part-4: Buffered data of left/right join stream
    bool recovered_bidirectional_join = false;
    rocks_handler->get("bidirectional_hash_join", recovered_bidirectional_join);
    if (recovered_bidirectional_join)
    {
        chassert(left_data.index);
        left_data.index->read(getOrCreateRocks()->getOrCreateHandler(left_data.index->id), version);
    }

    chassert(right_data.index);
    right_data.index->read(getOrCreateRocks()->getOrCreateHandler(right_data.index->id), version);

    /// Part-5: Asof type (Optional)
    bool need_asof = false;
    rocks_handler->get("need_asof", need_asof);
    if (need_asof)
    {
        chassert(asof_type.has_value());
        auto recovered_asof_type = *asof_type;
        rocks_handler->get("asof_type", recovered_asof_type);
        if (recovered_asof_type != *asof_type)
            throw Exception(
                ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                "Failed to recover hybrid hash join checkpoint. The asof type of join are not the same, checkpointed={}, current={}",
                magic_enum::enum_name(recovered_asof_type),
                magic_enum::enum_name(*asof_type));

        auto recovered_asof_inequality = asof_inequality;
        rocks_handler->get("asof_inequality", recovered_asof_inequality);
        if (recovered_asof_inequality != asof_inequality)
            throw Exception(
                ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                "Failed to recover hybrid hash join checkpoint. The asof inequality of join are not the same, checkpointed={}, current={}",
                magic_enum::enum_name(recovered_asof_inequality),
                magic_enum::enum_name(asof_inequality));
    }

    /// Part-6: Emit changelog (Optional)
    bool need_emit_changelog;
    rocks_handler->get("has_join_results", need_emit_changelog);
    if (need_emit_changelog)
    {
        if (!join_results.has_value())
            throw Exception(
                ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                "Failed to recover hybrid hash join checkpoint. The emit changelog strategy of join are not the same, checkpointed={}, "
                "current={}",
                need_emit_changelog,
                join_results.has_value());

        chassert(retract_push_down && emit_changelog);
        std::scoped_lock lock(join_results->mutex);
        readHybridHashJoinMapsVariants(*join_results->index, rocks_handler, "join_results", *this);
    }

    /// Part-7: Others
    int64_t recovered_combined_watermark;
    rocks_handler->get("combined_watermark", recovered_combined_watermark);
    combined_watermark = recovered_combined_watermark;

    String recovered_join_metrics_str;
    rocks_handler->get("join_metrics", recovered_join_metrics_str);
    ReadBufferFromString rb(recovered_join_metrics_str);
    join_metrics.deserialize(rb, version);
}

void writeHybridHashJoinMapsVariants(
    HybridHashJoinMapsVariants & index, RocksHandlerPtr rocks_handler, std::string_view id, const HybridHashJoin & join)
{
    chassert(!index.empty());

    size_t maps_size = index.size();
    rocks_handler->put(fmt::format("{}_maps_size", id), maps_size);

    std::vector<HybridMapsVariant *> maps_vector;
    maps_vector.reserve(maps_size);
    for (size_t i = 0; i < maps_size; ++i)
        maps_vector.push_back(&index[i]);

    /// Flush to rocksdb
    hybridJoinDispatch(
        join.getStreamingKind(),
        join.getStreamingStrictness(),
        maps_vector,
        [&](auto /*kind_*/, auto /*strictness_*/, auto & maps_vector_) {
            for (auto & map : maps_vector_)
                map->table.flush();
        });
}

void readHybridHashJoinMapsVariants(
    HybridHashJoinMapsVariants & index, RocksHandlerPtr rocks_handler, std::string_view id, const HybridHashJoin & join)
{
    chassert(!index.empty());

    size_t maps_size = 0;
    rocks_handler->get(fmt::format("{}_maps_size", id), maps_size);
    if (maps_size != index.size())
        throw Exception(
            ErrorCodes::RECOVER_CHECKPOINT_FAILED,
            "Failed to recover hybrid hash join checkpoint. The number of hash join maps varitans are not the same, checkpointed={}, "
            "current={}",
            maps_size,
            index.size());

    std::vector<HybridMapsVariant *> maps_vector;
    maps_vector.reserve(index.size());
    for (size_t i = 0; i < index.size(); ++i)
        maps_vector.push_back(&index[i]);

    /// Reload from rocksdb
    hybridJoinDispatch(
        join.getStreamingKind(),
        join.getStreamingStrictness(),
        maps_vector,
        [&](auto /*kind_*/, auto /*strictness_*/, auto & maps_vector_) {
            for (const auto & map : maps_vector_)
                map->table.reload();
        });
}
}
}
