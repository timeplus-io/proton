#include "StreamingStoreSource.h"
#include "StorageDistributedMergeTree.h"
#include "StreamingBlockReader.h"

#include <base/ClockUtils.h>

namespace DB
{
StreamingStoreSource::StreamingStoreSource(
    std::shared_ptr<IStorage> storage_,
    const Block & header,
    ContextPtr context_,
    Int32 shard_,
    DWAL::KafkaWALSimpleConsumerPtr consumer_,
    Poco::Logger * log_)
    : SourceWithProgress(header)
    , storage(std::move(storage_))
    , context(std::move(context_))
    , column_names(header.getNames())
    , header_chunk(header.getColumns(), 0)
    , shard(shard_)
    , consumer(std::move(consumer_))
    , log(log_)
{
    reader = std::make_unique<StreamingBlockReader>(storage->getStorageID(), context, shard, consumer, log);
    iter = result_chunks.begin();

    last_flush_ms = MonotonicMilliseconds::now();
}

Chunk StreamingStoreSource::generate()
{
    assert(reader);

    if (isCancelled())
    {
        return {};
    }

    /// std::unique_lock lock(result_blocks_mutex);
    if (result_chunks.empty() || iter == result_chunks.end())
    {
        readAndProcess();

        if (isCancelled())
        {
            return {};
        }

        /// After processing blocks, check again to see if there are new results
        if (result_chunks.empty() || iter == result_chunks.end())
        {
            /// Act as a heart beat and flush
            last_flush_ms = MonotonicMilliseconds::now();
            return header_chunk.clone();
        }

        /// result_blocks is not empty, fallthrough
    }

    if (MonotonicMilliseconds::now() - last_flush_ms >= flush_interval_ms)
    {
        last_flush_ms = MonotonicMilliseconds::now();
        return header_chunk.clone();
    }

    return std::move(*iter++);
}

void StreamingStoreSource::readAndProcess()
{
    auto records = reader->read(flush_interval_ms);
    if (records.empty())
        return;

    /// 1) Insert raw blocks to in-memory aggregation table
    /// 2) Select the final result from the aggregated table
    /// 3) Update result_blocks and iterator
    result_chunks.clear();
    result_chunks.reserve(records.size());

    for (auto & record : records)
    {
       Block block;
       /// Only select required columns.
       /// FIXME, move the columns filtered logic to deserialization ?

       for (const auto & name : column_names)
           block.insert(std::move(record->block.getByName(name)));

       result_chunks.emplace_back(block.getColumns(), block.rows());
    }
    iter = result_chunks.begin();
}
}
