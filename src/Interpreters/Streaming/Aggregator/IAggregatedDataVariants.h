#pragma once

#include <Interpreters/Streaming/Aggregator/AggregatedDataMetrics.h>
#include <Interpreters/Streaming/Aggregator/AggregatorType.h>
#include <Common/Arena.h>

namespace DB
{
using AggregateDataPtr = char *;

class ReadBuffer;
class WriteBuffer;

namespace Streaming
{

class IAggregator;

struct IAggregatedDataVariants
{
    virtual ~IAggregatedDataVariants() = default;

    virtual std::string_view getID() const noexcept = 0; /// The current variant ID
    virtual bool isLowCardinality() const noexcept = 0;
    virtual bool isTwoLevel() const noexcept = 0;
    virtual bool empty() const noexcept = 0;
    virtual void reset() = 0;
    virtual void updateMetrics(AggregatedDataMetrics & metrics, size_t total_size_of_aggregate_states) const = 0;
    virtual AggregatorType aggregatorType() const noexcept = 0;

    virtual void serialize(WriteBuffer & wb, const IAggregator &) const = 0;
    virtual void deserialize(ReadBuffer & rb, const IAggregator &) = 0;
};

using IAggregatedDataVariantsPtr = std::shared_ptr<IAggregatedDataVariants>;
using ManyIAggregatedDataVariants = std::vector<IAggregatedDataVariantsPtr>;

}

}
