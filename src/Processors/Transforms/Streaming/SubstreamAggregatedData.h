#pragma once

#include <Core/Streaming/SubstreamID.h>
#include <Interpreters/Streaming/Aggregator/IAggregator.h>
#include <Common/serde.h>

#include <any>

namespace DB::Streaming
{

class AggregatingTransformWithSubstream;

SERDE struct SubstreamAggregatedData
{
    static constexpr VersionType version = 1;

    /// Reference to current transform
    AggregatingTransformWithSubstream * aggregating_transform;

    /// A optimization is used for emit updates/changelog only
    bool keys_cache_enabled = false;
    KeyColumns processed_keys_columns;

    SERDE SubstreamID id;

    SERDE IAggregatedDataVariantsPtr variants;

    /// `finalized_watermark` is capturing the max watermark we have progressed and
    /// it is used to garbage collect time bucketed memory : time buckets which
    /// are below this watermark can be safely GCed.
    SERDE Int64 finalized_watermark = INVALID_WATERMARK;

    SERDE Int64 emitted_version = 0;

    SERDE UInt64 rows_since_last_finalization = 0;

    /// Stuff additional data context to it if needed
    SERDE struct AnyField
    {
        std::any field;
        std::function<void(const std::any &, WriteBuffer &, VersionType)> serializer;
        std::function<void(std::any &, ReadBuffer &, VersionType)> deserializer;
    } any_field;

    SubstreamAggregatedData(AggregatorType aggregator_type_, AggregatingTransformWithSubstream * aggr, const SubstreamID & id_ = {});

    void serialize(WriteBuffer & wb) const;

    void deserialize(ReadBuffer & rb);

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

    bool hasNewData() const { return rows_since_last_finalization > 0; }
    void resetRowCounts() { rows_since_last_finalization = 0; }
    void addRowCount(size_t rows) { rows_since_last_finalization += rows; }
};

using SubstreamAggregatedDataPtr = std::shared_ptr<SubstreamAggregatedData>;

}
