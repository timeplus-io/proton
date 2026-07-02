#include <Processors/Transforms/Streaming/HybridVersionsFilterTransform.h>

#include <Checkpoint/CheckpointContext.h>
#include <Checkpoint/CheckpointCoordinator.h>
#include <Checkpoint/FileCheckpoint.h>
#include <Checkpoint/RocksCheckpoint.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Streaming/ChooseHybridHashMethod.h>
#include <base/ClockUtils.h>
#include <Common/HybridHashTable/HybridKeyGetter.h>
#include <Common/ProtonCommon.h>
#include <Common/logger_useful.h>
#include <fmt/ranges.h>

namespace DB
{
namespace ErrorCodes
{
extern const int UNSUPPORTED;
extern const int RECOVER_CHECKPOINT_FAILED;
}

namespace Streaming
{

HybridVersionsFilterTransform::HybridVersionsFilterTransform(
    const DB::Block & input_header,
    const DB::Block & output_header,
    std::vector<std::string> key_column_names,
    const std::string & version_column_name,
    bool late_insert_overrides_,
    std::string spill_dir,
    size_t max_hot_key_count,
    const std::string & kv_options,
    bool backfill_key_unique_)
    : ISimpleTransform(input_header, output_header, false, ProcessorID::HybridVersionsFilterTransformID)
    , late_insert_overrides(late_insert_overrides_)
    , backfill_key_unique(backfill_key_unique_)
    , output_chunk_header(outputs.front().getHeader().getColumns(), 0)
    , last_log_ts(MonotonicMilliseconds::now())
    , logger(getLogger("HybridVersionsFilterTransform"))
{
    chassert(!key_column_names.empty());
    chassert(!version_column_name.empty());
    chassert(input_header.has(version_column_name));

    output_column_positions.reserve(output_header.columns());
    for (const auto & col : output_header)
        output_column_positions.push_back(input_header.getPositionByName(col.name));

    DataTypes key_column_types;
    key_column_types.reserve(key_column_names.size());
    key_column_positions.reserve(key_column_names.size());
    for (const auto & key_col : key_column_names)
    {
        key_column_positions.push_back(input_header.getPositionByName(key_col));
        key_column_types.push_back(input_header.getByPosition(key_column_positions.back()).type);
    }

    version_column_position = input_header.getPositionByName(version_column_name);
    version_column_serialization = input_header.getByPosition(version_column_position).type->getDefaultSerialization();

    createHashTable(key_column_types, std::move(spill_dir), max_hot_key_count, kv_options);

    LOG_INFO(
        logger,
        "Prepare versions filtering by keys_num={}, keys_size={} (each key size: {}), input_header={}",
        key_sizes.size(),
        std::accumulate(key_sizes.begin(), key_sizes.end(), static_cast<size_t>(0)),
        fmt::join(key_sizes, ", "),
        input_header.dumpStructure());
}

void HybridVersionsFilterTransform::transform(Chunk & chunk)
{
    /// Propagate empty chunk since it acts like a heartbeat
    auto rows = chunk.rows();
    auto columns = chunk.detachColumns();
    if (rows)
    {
        /// Constant columns are not supported directly during hashing keys. To make them work anyway, we materialize them.
        Columns materialized_columns;
        materialized_columns.reserve(key_column_positions.size() + 1);

        ColumnRawPtrs key_columns;
        key_columns.reserve(key_column_positions.size());

        for (auto key_col_pos : key_column_positions)
        {
            /// Materialize Sparse/Const/LowCardinality columns
            materialized_columns.push_back(columns[key_col_pos]->convertToFullIfNeeded());
            key_columns.push_back(materialized_columns.back().get());
        }

        materialized_columns.push_back(columns[version_column_position]->convertToFullIfNeeded());
        const auto & version_column = *materialized_columns.back();

        switch (latest_version_map.getType())
        {
            case HybridHashType::Empty:
                [[fallthrough]];
            case HybridHashType::WithoutKey:
                [[fallthrough]];
            case HybridHashType::key_hashed:
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR, "HybridHashTable shall have a key column, but empty or without or hashed key type is found");

#define M(TYPE, IS_TWO_LEVEL) \
    case HybridHashType::TYPE: \
        if (has_nullable_key) \
            doFilter<HybridKeyGetter<HybridHashType::TYPE, /*nullable=*/true>>( \
                rows, columns, key_columns, version_column, *latest_version_map.TYPE); \
        else \
            doFilter<HybridKeyGetter<HybridHashType::TYPE, /*nullable=*/false>>( \
                rows, columns, key_columns, version_column, *latest_version_map.TYPE); \
        break;
                APPLY_FOR_HASH_KEY_VARIANTS_HYBRID(M)
#undef M
        }

        chunk.setColumns(columns, rows);
    }
    else
    {
        /// MarkSource is always generating the historical data start / end mark in a separate and empty chunk
        if (!backfill_done)
        {
            if (!backfill_started)
                backfill_started |= chunk.isHistoricalDataStart();
            else
                backfill_done |= chunk.isHistoricalDataEnd();
        }

        chunk.setColumns(output_chunk_header.cloneEmptyColumns(), 0);
    }

    /// Every 30 seconds, log metrics
    if (auto now = MonotonicMilliseconds::now(); now - last_log_ts > log_metrics_interval_ms)
    {
        LOG_INFO(
            logger,
            "Hybrid hashtable metrics={{{}}} approximate_keys={}",
            latest_version_map.metrics().string(),
            latest_version_map.approximateCount());
        last_log_ts = now;
    }
}

template <typename KeyGetter, typename Table>
void HybridVersionsFilterTransform::doFilter(
    UInt64 & rows, Columns & columns, const ColumnRawPtrs & key_columns, const IColumn & version_column, Table & table)
{
    KeyGetter key_getter(key_columns, key_sizes);

    HybridEmplaceResults emplace_results;
    {
        std::vector<typename KeyGetter::KeyType> keys;
        keys.reserve(rows);

        for (size_t row = 0; row < rows; ++row)
            keys.emplace_back(key_getter.getKeyHolder(row));

        auto results = backfillingNewKeys() ? table.emplaceNewKeys(keys) : table.emplaceKeys(keys);
        if (results.hasError())
            throw Exception::createRuntime(results.errorCode(), results.errorString());

        emplace_results.results.swap(results.results);
    }

    /// Fast path during backfilling
    if (backfillingNewKeys())
    {
        for (size_t row = 0; row < rows; ++row)
        {
            auto & result = emplace_results.results[row];
            chassert(result.isInserted());
            *static_cast<Field *>(result.getMutableMapped()) = version_column[row];
        }

        transformToOutputColumns(columns);
        return;
    }

    auto original_rows = rows;
    IColumn::Filter filter(original_rows, 1);
    for (size_t row = 0; row < original_rows; ++row)
    {
        /// Please note, when value_column_positions is empty which means there is no value columns cached in
        /// the hybrid hash table, so there is no OneRow record there neither. (SELECT count() FROM mutable_stream).
        /// In the following code, we still cast `mapped` to `OneRow *` no matter value_column_positions is empty or not.
        /// It is still safe to do so even when `mapped` is not pointing to `OneRow` when value_column_positions is empty,
        /// because after casting to `OneRow *`, we will never access it in this case.
        /// In this way, we don't need branching according value_column_positions.empty() which simplifies the code
        /// but indeed introduces lots of subtleties
        auto & result = emplace_results.results[row];
        auto current_version = version_column[row];
        if (result.isInserted())
        {
            /// This is a new key
            *static_cast<Field *>(result.getMutableMapped()) = current_version;
        }
        else
        {
            const Field & latest_version = *static_cast<const Field *>(result.getMapped());
            if (current_version > latest_version)
            {
                /// Update the new version
                *static_cast<Field *>(result.getMutableMapped()) = std::move(current_version);
            }
            else if (late_insert_overrides && current_version == latest_version)
            {
                /// Do nothing, keep the existing version
            }
            else [[unlikely]]
            {
                /// This is a late row, we can skip it
                filter[row] = 0;
                --rows;
            }
        }
    }

    late_rows += original_rows - rows;

    transformToOutputColumns(columns);

    if (rows == original_rows)
    {
        /// No filtering
    }
    else if (rows != 0)
    {
        for (auto & column : columns)
            column = column->filter(filter, rows);
    }
    else
    {
        for (auto & column : columns)
            column = column->cloneEmpty();
    }
}

void HybridVersionsFilterTransform::transformToOutputColumns(Columns & columns) const
{
    Columns output_columns;
    output_columns.reserve(output_column_positions.size());
    for (auto pos : output_column_positions)
        output_columns.push_back(std::move(columns[pos]));

    columns.swap(output_columns);
}

void HybridVersionsFilterTransform::createHashTable(
    const DataTypes & key_column_types, std::string spill_dir, size_t max_hot_key_count, const std::string & kv_options)
{
    /// init latest_version_map hash table
    auto hash_method = chooseHybridHashMethod(key_column_types, /*needs_time_bucket=*/false);
    has_nullable_key = hash_method.has_nullable_key;

    config.base_conf.spill_dir_path.swap(spill_dir);
    config.base_conf.max_hot_key_count = max_hot_key_count;
    config.base_conf.kv_options = kv_options;
    config.value_object_size = sizeof(Field);
    config.align_value_object_size = alignof(Field);
    config.value_constructor = [](void * data) { new (data) Field; };
    config.value_destructor = [](void * data) {
        auto * row = static_cast<Field *>(data);
        row->~Field();
    };

    config.value_serializer = [this](const void * data, WriteBuffer & wb) {
        version_column_serialization->serializeBinary(*static_cast<const Field *>(data), wb, format_settings);
        return DB::ErrorCodes::OK;
    };

    config.value_deserializer = [this](void * data, ReadBuffer & rb) {
        version_column_serialization->deserializeBinary(*static_cast<Field *>(data), rb, format_settings);
        return DB::ErrorCodes::OK;
    };

    /// Install rocks handler getter
    config.base_conf.rocks_cf_handler_getter = [this](const HybridConfig & hybrid_config) {
        return getOrCreateRocksDB(hybrid_config)->getOrCreateColumnFamilyHandler(hybrid_config.cf_handle_id, hybrid_config.ttl);
    };
    latest_version_map.init(hash_method.type, config, hash_method.key_sizes, logger);
    key_sizes.swap(hash_method.key_sizes);
}

RocksDBPtr HybridVersionsFilterTransform::getOrCreateRocksDB(const HybridConfig & hybrid_config)
{
    /// Initialize rocks on first use
    if (!rocks)
        rocks = RocksDB::createOrLoadIfExists(
            hybrid_config.getRocksOptions(), hybrid_config.spill_dir_path, /*ttl=*/0, hybrid_config.cleanup_on_disk_data, logger);

    return rocks;
}

void HybridVersionsFilterTransform::doCheckpoint(CheckpointContextPtr ckpt_ctx)
{
    chassert(ckpt_ctx->request_ctx && ckpt_ctx->request_ctx->settings);
    auto & settings = ckpt_ctx->request_ctx->settings;
    CheckpointPtr ckpt;
    switch (settings->type)
    {
        case CheckpointType::Auto:
        case CheckpointType::Rocks:
        {
            chassert(!latest_version_map.isTwoLevel());
            latest_version_map.flush();
            getOrCreateRocksDB()->getDefaultColumnFamilyHandler()->put("__late_rows", late_rows);
            ckpt = std::make_shared<RocksCheckpoint>(getVersion(), rocks);
            break;
        }
        case CheckpointType::File:
        {
            ckpt = std::make_shared<FileCheckpoint>(getVersion(), [this](WriteBuffer & wb) {
                latest_version_map.serialize(wb);
                DB::writeVarUInt(late_rows, wb);
            });
            break;
        }
    }

    ckpt_ctx->coordinator->checkpoint(getLogicID(), std::move(ckpt), ckpt_ctx);
}

void HybridVersionsFilterTransform::doRecover(CheckpointContextPtr ckpt_ctx)
{
    auto ckpt = ckpt_ctx->coordinator->recover(getLogicID(), ckpt_ctx);
    switch (ckpt->type())
    {
        case CheckpointType::File:
        {
            auto file_ckpt = std::static_pointer_cast<FileCheckpoint>(ckpt);
            file_ckpt->recover([this](ReadBuffer & rb) {
                latest_version_map.deserialize(rb);
                DB::readVarUInt(late_rows, rb);
            });
            break;
        }
        case CheckpointType::Rocks:
        {
            auto rocks_ckpt = std::static_pointer_cast<RocksCheckpoint>(ckpt);

            /// NOTE: All rocks handlers are invalid after shutdown
            if (rocks)
            {
                rocks->shutdown(/*cleanup=*/true);
                rocks.reset();
            }

            rocks_ckpt->recover(config.base_conf.spill_dir_path);

            /// Reinstall recovered rocks
            rocks = RocksDB::createOrLoadIfExists(
                config.getRocksOptions(), config.base_conf.spill_dir_path, /*ttl=*/0, config.base_conf.cleanup_on_disk_data, logger);
            rocks->getDefaultColumnFamilyHandler()->get("__late_rows", late_rows);
            latest_version_map.reload();
            break;
        }
        default:
        {
            throw Exception(ErrorCodes::RECOVER_CHECKPOINT_FAILED, "Unsupported checkpoint type: {}", ckpt->type());
        }
    }
}
}
}
