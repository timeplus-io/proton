#include "StreamingStoreSource.h"
#include "StorageDistributedMergeTree.h"
#include "StreamingBlockReader.h"

#include <base/ClockUtils.h>
#include <Common/StreamingCommon.h>

namespace DB
{
StreamingStoreSource::StreamingStoreSource(
    std::shared_ptr<IStorage> storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    const Names & column_names_,
    ContextPtr context_,
    Int32 shard_,
    DWAL::KafkaWALSimpleConsumerPtr consumer_,
    Poco::Logger * log_)
    : SourceWithProgress(metadata_snapshot_->getSampleBlockForColumns(column_names, storage_->getVirtuals(), storage_->getStorageID()))
    , storage(std::move(storage_))
    , context(context_)
    , column_names(column_names_)
    , header(outputs.front().getHeader())
    , shard(shard_)
    , consumer(consumer_)
    , log(log_)
{
    wend_type = header.findByName(STREAMING_WINDOW_END);
    if (!wend_type)
        wend_type = header.findByName(STREAMING_WINDOW_START);

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
            return Chunk(header.getColumns(), 0);
        }

        /// result_blocks is not empty, fallthrough
    }

    if (MonotonicMilliseconds::now() - last_flush_ms >= flush_interval_ms)
    {
        last_flush_ms = MonotonicMilliseconds::now();
        return Chunk(header.getColumns(), 0);
    }

    return std::move(*iter++);
}

void StreamingStoreSource::readAndProcess()
{
    auto records = reader->read(flush_interval_ms);
    if (records.empty())
    {
        return;
    }

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
       auto rows = record->block.rows();

       for (const auto & name : column_names)
       {
           if (name == STREAMING_WINDOW_START || name == STREAMING_WINDOW_END)
           {
               assert (wend_type);
               ColumnWithTypeAndName col{wend_type->cloneEmpty()};
               col.name = name;
               col.column->assumeMutable()->insertManyDefaults(rows);
               block.insert(std::move(col));
           }
           else
               block.insert(std::move(record->block.getByName(name)));
       }

       result_chunks.emplace_back(block.getColumns(), block.rows());
    }
    iter = result_chunks.begin();
}
}
