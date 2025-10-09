#include <Interpreters/Streaming/HashJoin/HybridHashJoin/HashIndex.h>
#include <Interpreters/Streaming/HashJoin/HybridHashJoin/HybridHashJoin.h>

namespace DB::Streaming
{
void HashIndex::serialize(WriteBuffer & wb, VersionType version) const
{
    std::scoped_lock lock(mutex);

    range_asof_join_ctx.serialize(wb);

    DB::writeIntBinary(bucket_size, wb);
    DB::writeIntBinary(join_start_bucket_offset, wb);
    DB::writeIntBinary(join_stop_bucket_offset, wb);

    DB::writeIntBinary(current_watermark.load(), wb);

    chassert(current_hash_index);
    serializeHybridHashJoinMapsVariants(*current_hash_index, wb, version, *join);

    DB::writeVarUInt(range_bucket_hash_indexes.size(), wb);
    for (const auto & [bucket, index_with_timestamps] : range_bucket_hash_indexes)
    {
        DB::writeVarInt(bucket, wb);

        chassert(index_with_timestamps.index);
        serializeHybridHashJoinMapsVariants(*index_with_timestamps.index, wb, version, *join);

        DB::writeIntBinary(index_with_timestamps.min_ts, wb);
        DB::writeIntBinary(index_with_timestamps.max_ts, wb);
    }

    if (version <= CachedBlockMetrics::SERDE_REQUIRED_MAX_VERSION)
        metrics.serialize(wb, version);
}

void HashIndex::deserialize(ReadBuffer & rb, VersionType version)
{
    std::scoped_lock lock(mutex);

    range_asof_join_ctx.deserialize(rb);

    DB::readIntBinary(bucket_size, rb);
    DB::readIntBinary(join_start_bucket_offset, rb);
    DB::readIntBinary(join_stop_bucket_offset, rb);

    Int64 recovered_watermark;
    DB::readIntBinary(recovered_watermark, rb);
    current_watermark = recovered_watermark;

    chassert(current_hash_index);
    deserializeHybridHashJoinMapsVariants(*current_hash_index, rb, version, *join);

    size_t size;
    Int64 bucket;
    DB::readVarUInt(size, rb);
    for (size_t i = 0; i < size; ++i)
    {
        DB::readVarInt(bucket, rb);

        auto index = std::make_shared<HybridHashJoinMapsVariants>();
        chassert(join && sample_block);
        join->initHashMaps(*index, sample_block, fmt::format("{}-range-bucket-{}", id, bucket));
        deserializeHybridHashJoinMapsVariants(*index, rb, version, *join);

        Int64 min_ts;
        Int64 max_ts;
        DB::readIntBinary(min_ts, rb);
        DB::readIntBinary(max_ts, rb);

        [[maybe_unused]] auto [_, inserted]
            = range_bucket_hash_indexes.emplace(bucket, IndexWithTimestamps{std::move(index), min_ts, max_ts});
        chassert(inserted);
    }

    if (version <= CachedBlockMetrics::SERDE_REQUIRED_MAX_VERSION)
        metrics.deserialize(rb, version);
}

/// Write / read data to / from RocksDB
void HashIndex::write(RocksHandlerPtr rocks_handler, VersionType version)
{
    std::scoped_lock lock(mutex);

    {
        DB::WriteBufferFromOwnString wb;
        range_asof_join_ctx.serialize(wb);
        rocks_handler->put("range_asof_join_ctx", wb.str());
    }

    rocks_handler->put("bucket_size", bucket_size);
    rocks_handler->put("join_start_bucket_offset", join_start_bucket_offset);
    rocks_handler->put("join_stop_bucket_offset", join_stop_bucket_offset);
    rocks_handler->put("current_watermark", current_watermark.load());

    chassert(current_hash_index);
    writeHybridHashJoinMapsVariants(*current_hash_index, rocks_handler, "current_hash_index", *join);

    {
        std::vector<Int64> range_buckets;
        range_buckets.reserve(range_bucket_hash_indexes.size());
        for (const auto & [bucket, _] : range_bucket_hash_indexes)
            range_buckets.push_back(bucket);
        rocks_handler->put("range_buckets", range_buckets);
    }

    for (const auto & [bucket, index_with_timestamps] : range_bucket_hash_indexes)
    {
        chassert(index_with_timestamps.index);
        auto prefix_id = fmt::format("range_bucket_{}", bucket);
        writeHybridHashJoinMapsVariants(*index_with_timestamps.index, rocks_handler, prefix_id, *join);

        rocks_handler->put(fmt::format("{}_min_ts", prefix_id), index_with_timestamps.min_ts);
        rocks_handler->put(fmt::format("{}_max_ts", prefix_id), index_with_timestamps.max_ts);
    }

    if (version <= CachedBlockMetrics::SERDE_REQUIRED_MAX_VERSION)
    {
        DB::WriteBufferFromOwnString wb;
        metrics.serialize(wb, version);
        rocks_handler->put("metrics", wb.str());
    }
}

void HashIndex::read(RocksHandlerPtr rocks_handler, VersionType version)
{
    std::scoped_lock lock(mutex);

    {
        String range_asof_join_ctx_str;
        rocks_handler->get("range_asof_join_ctx", range_asof_join_ctx_str);
        DB::ReadBufferFromString rb(range_asof_join_ctx_str);
        range_asof_join_ctx.deserialize(rb);
    }

    rocks_handler->get("bucket_size", bucket_size);
    rocks_handler->get("join_start_bucket_offset", join_start_bucket_offset);
    rocks_handler->get("join_stop_bucket_offset", join_stop_bucket_offset);

    Int64 recovered_watermark;
    rocks_handler->get("current_watermark", recovered_watermark);
    current_watermark = recovered_watermark;

    chassert(current_hash_index);
    readHybridHashJoinMapsVariants(*current_hash_index, rocks_handler, "current_hash_index", *join);

    std::vector<Int64> range_buckets;
    rocks_handler->get("range_buckets", range_buckets);
    for (auto bucket : range_buckets)
    {
        auto index = std::make_shared<HybridHashJoinMapsVariants>();
        chassert(join && sample_block);
        join->initHashMaps(*index, sample_block, fmt::format("{}-range-bucket-{}", id, bucket));

        auto prefix_id = fmt::format("range_bucket_{}", bucket);
        readHybridHashJoinMapsVariants(*index, rocks_handler, prefix_id, *join);

        Int64 min_ts;
        Int64 max_ts;
        rocks_handler->get(fmt::format("{}_min_ts", prefix_id), min_ts);
        rocks_handler->get(fmt::format("{}_max_ts", prefix_id), max_ts);

        [[maybe_unused]] auto [_, inserted]
            = range_bucket_hash_indexes.emplace(bucket, IndexWithTimestamps{std::move(index), min_ts, max_ts});
        chassert(inserted);
    }

    if (version <= CachedBlockMetrics::SERDE_REQUIRED_MAX_VERSION)
    {
        String metrics_str;
        rocks_handler->get("metrics", metrics_str);
        DB::ReadBufferFromString rb(metrics_str);
        metrics.deserialize(rb, version);
    }
}
}
