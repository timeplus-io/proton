#include <Processors/Transforms/Streaming/AggregatingTransformWithSubstream.h>
#include <Processors/Transforms/Streaming/SubstreamAggregatedData.h>

#include <Interpreters/Streaming/Aggregator/HybridAggregator/HybridAggregatedDataVariants.h>
#include <Interpreters/Streaming/Aggregator/MemoryAggregator/MemoryAggregatedDataVariants.h>

namespace DB::ErrorCodes
{
extern const int BAD_VERSION;
}

namespace DB::Streaming
{

SubstreamAggregatedData::SubstreamAggregatedData(
    AggregatorType aggregator_type, AggregatingTransformWithSubstream * aggr, const SubstreamID & id_)
    : aggregating_transform(aggr), id(id_)
{
    assert(aggregating_transform);

    switch (aggregator_type)
    {
        case AggregatorType::Memory:
        {
            variants = std::make_shared<MemoryAggregatedDataVariants>();
            break;
        }
        case AggregatorType::Hybrid:
        {
            variants = std::make_shared<HybridAggregatedDataVariants>(/*id_=*/DB::toString(id));
            break;
        }
    }
}

void SubstreamAggregatedData::serialize(WriteBuffer & wb) const
{
    DB::writeIntBinary(version, wb);

    DB::Streaming::serialize(id, wb);

    /// Serializing local or stable data during checkpointing
    variants->serialize(wb, *aggregating_transform->params->aggregator);

    DB::writeIntBinary(finalized_watermark, wb);

    DB::writeIntBinary(emitted_version, wb);

    DB::writeIntBinary(rows_since_last_finalization, wb);

    bool has_field = hasField();
    DB::writeBoolText(has_field, wb);
    if (has_field)
        any_field.serializer(any_field.field, wb, version);
}

void SubstreamAggregatedData::deserialize(ReadBuffer & rb)
{
    VersionType recovered_version;
    DB::readIntBinary(recovered_version, rb);
    /// Check version compatibility
    if (recovered_version > version)
        throw Exception(
            ErrorCodes::BAD_VERSION,
            "Failed to recover substream aggregated state, incompatible version: {}, expected version: {}",
            recovered_version,
            version);

    DB::Streaming::deserialize(id, rb);

    variants->deserialize(rb, *aggregating_transform->params->aggregator);

    DB::readIntBinary(finalized_watermark, rb);

    DB::readIntBinary(emitted_version, rb);

    DB::readIntBinary(rows_since_last_finalization, rb);

    bool has_field;
    DB::readBoolText(has_field, rb);
    if (has_field)
        any_field.deserializer(any_field.field, rb, recovered_version);
}
}
