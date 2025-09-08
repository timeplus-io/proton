#include <Interpreters/Streaming/Aggregator/HybridAggregator/HybridAggregatedDataVariants.h>
#include <Interpreters/Streaming/Aggregator/MemoryAggregator/MemoryAggregatedDataVariants.h>
#include <Processors/Transforms/Streaming/ManyAggregatedData.h>

namespace DB::Streaming
{
ManyAggregatedData::ManyAggregatedData(AggregatorType aggregator_type, const std::string & id_)
    : id(id_), variants(1), watermarks(1, INVALID_WATERMARK)
{
    switch (aggregator_type)
    {
        case AggregatorType::Memory:
        {
            variants[0] = std::make_shared<MemoryAggregatedDataVariants>(id_);
            break;
        }
        case AggregatorType::Hybrid:
        {
            variants[0] = std::make_shared<HybridAggregatedDataVariants>(id_);
            break;
        }
    }

    rows_since_last_finalizations.emplace_back(std::make_unique<std::atomic<UInt64>>(0));
    variants_mutexes.emplace_back(std::make_unique<std::timed_mutex>());
    aggregating_transforms.resize(variants.size());
    rocks_holders.resize(variants.size());
}

ManyAggregatedData::ManyAggregatedData(AggregatorType aggregator_type, size_t num_threads)
    : variants(num_threads), watermarks(num_threads, INVALID_WATERMARK)
{
    for (size_t i = 0; auto & elem : variants)
    {
        switch (aggregator_type)
        {
            case AggregatorType::Memory:
            {
                elem = std::make_shared<MemoryAggregatedDataVariants>(fmt::format("{}", i++));
                break;
            }
            case AggregatorType::Hybrid:
            {
                elem = std::make_shared<HybridAggregatedDataVariants>(fmt::format("{}", i++));
                break;
            }
        }
    }

    for (size_t i = 0; i < num_threads; ++i)
    {
        rows_since_last_finalizations.emplace_back(std::make_unique<std::atomic<UInt64>>(0));
        variants_mutexes.emplace_back(std::make_unique<std::timed_mutex>());
    }

    aggregating_transforms.resize(variants.size());
    rocks_holders.resize(variants.size());
}

}
