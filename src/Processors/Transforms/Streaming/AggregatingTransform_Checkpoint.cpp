#include <Processors/Transforms/Streaming/AggregatingTransform.h>

#include <Checkpoint/CheckpointContext.h>
#include <Checkpoint/CheckpointCoordinator.h>
#include <Checkpoint/FileCheckpoint.h>
#include <Checkpoint/RocksCheckpoint.h>
#include <Interpreters/Streaming/Aggregator/AggregatedDataMetrics.h>
#include <Interpreters/Streaming/Aggregator/HybridAggregator/HybridAggregator.h>
#include <Interpreters/Streaming/Aggregator/HybridAggregator/HybridAggregatorParams.h>
#include <Common/ProtonCommon.h>
#include <Common/Rocks/RocksDB.h>

namespace DB
{
namespace ErrorCodes
{
extern const int CREATE_CHECKPOINT_FAILED;
extern const int RECOVER_CHECKPOINT_FAILED;
}

namespace Streaming
{
void AggregatingTransform::initRocksDBConfig()
{
    chassert(variants.aggregatorType() == AggregatorType::Hybrid);
    auto & hybrid_variants = static_cast<HybridAggregatedDataVariants &>(variants);
    auto hybrid_params = std::static_pointer_cast<HybridAggregatorParams>(params->params);

    /// Init the base config according to HybridAggregatorParams
    hybrid_variants.config.spill_dir_path = fmt::format("{}_{}", hybrid_params->spill_dir_path, hybrid_variants.getID());
    hybrid_variants.config.max_hot_key_count = hybrid_params->max_hot_key_count;
    hybrid_variants.config.cleanup_on_disk_data = true;
    hybrid_variants.config.ttl = hybrid_params->aggregate_state_ttl;
    hybrid_variants.config.kv_options = hybrid_params->kv_options;

    /// This is the callback when HybridHashTable needs init its rocks part
    hybrid_variants.config.rocks_cf_handler_getter = [this](const HybridConfig & config) {
        return getOrCreateRocksDB(config)->getOrCreateColumnFamilyHandler(config.cf_handle_id, config.ttl);
    };
}

RocksDBPtr AggregatingTransform::getOrCreateRocksDB(const HybridConfig & config)
{
    auto & rocks = many_data->rocks[current_variant];
    if (!rocks)
        /// Initialize rocks on first use
        rocks = RocksDB::createOrLoadIfExists(
            config.getRocksOptions(), config.spill_dir_path, config.ttl, config.cleanup_on_disk_data, logger);

    return rocks;
}

void AggregatingTransform::doCheckpoint(CheckpointContextPtr ckpt_ctx)
{
    chassert(ckpt_ctx->request_ctx && ckpt_ctx->request_ctx->settings);
    const auto & settings = ckpt_ctx->request_ctx->settings;

    CheckpointType ckpt_type = CheckpointType::File;
    if (params->aggregator->type() == AggregatorType::Hybrid && settings->type != CheckpointType::File)
        ckpt_type = CheckpointType::Rocks;
    else if (params->aggregator->type() == AggregatorType::Memory && settings->type == CheckpointType::Rocks)
        throw Exception(ErrorCodes::CREATE_CHECKPOINT_FAILED, "Memory aggregation does not support checkpoint type 'rocks'");

    CheckpointPtr ckpt;
    if (ckpt_type == CheckpointType::Rocks)
        ckpt = createRocksCheckpoint();
    else
        ckpt = createFileCheckpoint();

    ckpt_ctx->coordinator->checkpoint(getLogicID(), std::move(ckpt), ckpt_ctx);
}

void AggregatingTransform::doRecover(CheckpointContextPtr ckpt_ctx)
{
    auto ckpt = ckpt_ctx->coordinator->recover(getLogicID(), ckpt_ctx);
    switch (ckpt->type())
    {
        case CheckpointType::File:
            return recoverFileCheckpoint(std::move(ckpt));
        case CheckpointType::Rocks:
            return recoverRocksCheckpoint(std::move(ckpt));
        default:
            throw Exception(ErrorCodes::RECOVER_CHECKPOINT_FAILED, "Unknown checkpoint type {}", ckpt->type());
    }

    UNREACHABLE();
}

CheckpointPtr AggregatingTransform::createFileCheckpoint()
{
    return std::make_shared<FileCheckpoint>(getVersion(), [this](WriteBuffer & wb) {
        bool is_last_checkpointing_transform = (this == many_data->last_checkpointing_transform.load());
        DB::writeBoolText(is_last_checkpointing_transform, wb);

        /// Serializing shared data (only do it on last checkpointing transform)
        if (is_last_checkpointing_transform)
        {
            UInt16 num_variants = many_data->variants.size();
            DB::writeIntBinary(num_variants, wb);

            DB::writeIntBinary(many_data->finalized_watermark.load(), wb);
            DB::writeIntBinary(many_data->finalized_window_end.load(), wb);
            DB::writeIntBinary(many_data->emitted_version.load(), wb);

            assert(num_variants == many_data->rows_since_last_finalizations.size());
            for (const auto & last_row : many_data->rows_since_last_finalizations)
                writeIntBinary<UInt64>(last_row->load(), wb);

            bool has_field = many_data->hasField();
            DB::writeBoolText(has_field, wb);
            if (has_field)
                many_data->any_field.serializer(many_data->any_field.field, wb, getVersion());
        }

        /// Serializing no shared data
        variants.serialize(wb, *params->aggregator);

        DB::writeIntBinary(watermark, wb);

        /// After the local checkpoint is processed, the `propagated_watermark` may still be updated,
        /// because other transforms may have new finalizing processing.
        /// But it doesn't matter, we will update according to the recovered `finalized_watermark` later
        DB::writeIntBinary(propagated_watermark, wb);
    });
}

void AggregatingTransform::recoverFileCheckpoint(CheckpointPtr ckpt)
{
    auto file_ckpt = std::static_pointer_cast<FileCheckpoint>(ckpt);
    file_ckpt->recover([version_ = file_ckpt->getVersion(), this](ReadBuffer & rb) {
        bool is_last_checkpointing_transform;
        DB::readBoolText(is_last_checkpointing_transform, rb);

        /// Serializing shared data
        if (is_last_checkpointing_transform)
        {
            UInt16 num_variants = 0;
            DB::readIntBinary(num_variants, rb);
            if (num_variants != many_data->variants.size())
                throw Exception(
                    ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                    "Failed to recover aggregation checkpoint. Number of data variants are not the same, checkpointed={}, "
                    "current={}",
                    num_variants,
                    many_data->variants.size());

            Int64 last_finalized_watermark;
            DB::readIntBinary(last_finalized_watermark, rb);
            many_data->finalized_watermark = last_finalized_watermark;

            Int64 last_finalized_window_end;
            DB::readIntBinary(last_finalized_window_end, rb);
            many_data->finalized_window_end = last_finalized_window_end;

            Int64 last_version = 0;
            DB::readIntBinary(last_version, rb);
            many_data->emitted_version = last_version;

            assert(num_variants == many_data->rows_since_last_finalizations.size());
            for (auto & rows_since_last_finalization : many_data->rows_since_last_finalizations)
            {
                UInt64 last_rows = 0;
                DB::readIntBinary<UInt64>(last_rows, rb);
                /// In case when for we had global aggregated some data, but done checkpoint request before finializing
                if (last_rows > 0) [[unlikely]]
                    LOG_WARNING(logger, "Last checkpoint state don't be finalized, rows_since_last_finalization={}", last_rows);

                *rows_since_last_finalization = last_rows;
            }

            bool has_field;
            DB::readBoolText(has_field, rb);
            if (has_field)
                many_data->any_field.deserializer(many_data->any_field.field, rb, version_);
        }

        variants.deserialize(rb, *params->aggregator);

        DB::readIntBinary(watermark, rb);

        DB::readIntBinary(propagated_watermark, rb);
    });
}

CheckpointPtr AggregatingTransform::createRocksCheckpoint()
{
    chassert(variants.aggregatorType() == AggregatorType::Hybrid);
    auto rocks = getOrCreateRocksDB(static_cast<const HybridAggregatedDataVariants &>(variants).config);

    /// Checkpoint metadata to default column family without TTL
    auto cf_handler = rocks->getDefaultColumnFamilyHandler();

    bool is_last_checkpointing_transform = (this == many_data->last_checkpointing_transform.load());
    cf_handler->put("is_last", is_last_checkpointing_transform);

    /// Serializing shared data (only do it on last checkpointing transform)
    if (is_last_checkpointing_transform)
    {
        UInt16 num_variants = many_data->variants.size();
        cf_handler->put("num_variants", num_variants);
        cf_handler->put("finalized_watermark", many_data->finalized_watermark.load());
        cf_handler->put("finalized_window_end", many_data->finalized_window_end.load());
        cf_handler->put("emitted_version", many_data->emitted_version.load());

        assert(num_variants == many_data->rows_since_last_finalizations.size());
        for (UInt16 i = 0; const auto & rows : many_data->rows_since_last_finalizations)
            cf_handler->put(fmt::format("rows_{}", i++), rows->load());

        if (many_data->hasField())
        {
            WriteBufferFromOwnString wb;
            many_data->any_field.serializer(many_data->any_field.field, wb, getVersion());
            cf_handler->put("any_field", wb.str());
        }
    }

    /// Serializing no shared data
    cf_handler->put("watermark", watermark);

    /// After the local checkpoint is processed, the `propagated_watermark` may still be updated,
    /// because other transforms may have new finalizing processing.
    /// But it doesn't matter, we will update according to the recovered `finalized_watermark` later
    cf_handler->put("propagated_watermark", propagated_watermark);

    /// Checkpoint variants metadata to `variants` column family
    assert_cast<HybridAggregatedDataVariants &>(variants).write(
        rocks->getOrCreateColumnFamilyHandler("variants", /*ttl_sec=*/0), *params->aggregator);

    return std::make_shared<RocksCheckpoint>(getVersion(), rocks);
}

void AggregatingTransform::recoverRocksCheckpoint(CheckpointPtr ckpt)
{
    auto rocks_ckpt = std::static_pointer_cast<RocksCheckpoint>(ckpt);

    /// NOTE: All rocks handlers are invalid after shutdown
    auto & rocks = many_data->rocks[current_variant];
    if (rocks)
    {
        rocks->shutdown(/*cleanup=*/true);
        rocks.reset();
    }

    chassert(variants.aggregatorType() == AggregatorType::Hybrid);
    auto & hybrid_variants = assert_cast<HybridAggregatedDataVariants &>(variants);
    const auto & config = hybrid_variants.config;

    rocks_ckpt->recover(config.spill_dir_path);

    /// Reinstall recovered rocks
    rocks = RocksDB::createOrLoadIfExists(config.getRocksOptions(), config.spill_dir_path, config.ttl, config.cleanup_on_disk_data, logger);
    variants.reset();

    /// Recover from default column family to restore metadata
    auto cf_handler = rocks->getDefaultColumnFamilyHandler();

    bool is_last_checkpointing_transform = false;
    cf_handler->get("is_last", is_last_checkpointing_transform);
    if (is_last_checkpointing_transform)
    {
        UInt16 num_variants = 0;
        cf_handler->get("num_variants", num_variants);
        if (num_variants != many_data->variants.size())
            throw Exception(
                ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                "Failed to recover aggregation checkpoint. Number of data variants are not the same, checkpointed={}, "
                "current={}",
                num_variants,
                many_data->variants.size());

        Int64 last_finalized_watermark;
        cf_handler->get("finalized_watermark", last_finalized_watermark);
        many_data->finalized_watermark = last_finalized_watermark;

        Int64 last_finalized_window_end;
        cf_handler->get("finalized_window_end", last_finalized_window_end);
        many_data->finalized_window_end = last_finalized_window_end;

        Int64 last_version = 0;
        cf_handler->get("emitted_version", last_version);
        many_data->emitted_version = last_version;

        assert(num_variants == many_data->rows_since_last_finalizations.size());
        for (UInt16 i = 0; const auto & rows : many_data->rows_since_last_finalizations)
        {
            UInt64 last_rows = 0;
            cf_handler->get(fmt::format("rows_{}", i++), last_rows);
            /// In case when for we had global aggregated some data, but done checkpoint request before finalization
            if (last_rows > 0)
                LOG_WARNING(logger, "Last checkpoint state don't be finalized, rows_since_last_finalization={}", last_rows);

            *rows = last_rows;
        }

        std::string_view field;
        if (cf_handler->tryGet("any_field", field))
        {
            ReadBufferFromString rb(field);
            many_data->any_field.deserializer(many_data->any_field.field, rb, getVersion());
        }
    }

    cf_handler->get("watermark", watermark);

    cf_handler->get("propagated_watermark", propagated_watermark);

    hybrid_variants.read(rocks->getOrCreateColumnFamilyHandler("variants", /*ttl_sec=*/0), *params->aggregator);
}
}
}
