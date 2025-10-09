#pragma once

#include <Interpreters/Streaming/Aggregator/AggregatingConvertParams.h>
#include <Interpreters/Streaming/Aggregator/IAggregator.h>
#include <Common/serde.h>

#include <any>

namespace DB
{

class Rocks;
using RocksPtr = std::shared_ptr<Rocks>;

namespace Streaming
{
class AggregatingTransform;

SERDE struct ManyAggregatedData
{
    /// Reference to all transforms
    std::vector<AggregatingTransform *> aggregating_transforms;

    /// A optimization is used for emit updates/changelog only
    std::vector<KeyColumns> processed_keys_columns;

    std::vector<std::unique_ptr<std::timed_mutex>> variants_mutexes;
    SERDE ManyIAggregatedDataVariants variants;

    SERDE std::vector<RocksPtr> rocks_holders;

    /// Watermarks for all variants
    /// Acquire lock when update current watermark and find min watermark from all transform
    std::mutex watermarks_mutex;
    SERDE std::vector<Int64> watermarks TSA_GUARDED_BY(watermarks_mutex);

    std::mutex finalizing_mutex;

    /// `finalized_watermark` is capturing the max watermark we have progressed
    SERDE std::atomic<Int64> finalized_watermark = INVALID_WATERMARK;
    SERDE std::atomic<Int64> finalized_window_end = INVALID_WATERMARK;

    SERDE std::atomic<Int64> emitted_version = 0;

    SERDE std::vector<std::unique_ptr<std::atomic<UInt64>>> rows_since_last_finalizations;

    std::atomic<UInt32> ckpt_requested = 0;
    std::atomic<AggregatingTransform *> last_checkpointing_transform = nullptr;

    std::atomic<Int64> last_log_ts = MonotonicMilliseconds::now();

    /// Stuff additional data context to it if needed
    SERDE struct AnyField
    {
        SERDE std::any field;
        std::function<void(const std::any &, WriteBuffer &, VersionType)> serializer;
        std::function<void(std::any &, ReadBuffer &, VersionType)> deserializer;
    } any_field;

    ManyAggregatedData(AggregatorType aggregator_type, const std::string & id);
    ManyAggregatedData(AggregatorType aggregator_type, size_t num_threads);

    bool hasField() const { return any_field.field.has_value(); }

    void setField(AnyField && field_) { any_field = std::move(field_); }

    template <typename T>
    T & getField()
    {
        return std::any_cast<T &>(any_field.field);
    }

    template <typename T>
    const T & getField() const
    {
        return std::any_cast<const T &>(any_field.field);
    }

    bool hasNewData() const
    {
        return std::any_of(
            rows_since_last_finalizations.begin(), rows_since_last_finalizations.end(), [](const auto & rows) { return *rows > 0; });
    }

    void resetRowCounts()
    {
        for (auto & rows : rows_since_last_finalizations)
            *rows = 0;
    }

    void addRowCount(size_t rows, size_t current_variant) { *rows_since_last_finalizations[current_variant] += rows; }
};

using ManyAggregatedDataPtr = std::shared_ptr<ManyAggregatedData>;

}
}
