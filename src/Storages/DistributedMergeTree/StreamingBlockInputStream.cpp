#include "StreamingBlockInputStream.h"
#include "StorageDistributedMergeTree.h"
#include "StreamingBlockReader.h"

#include <common/ClockUtils.h>

namespace DB
{
StreamingBlockInputStream::StreamingBlockInputStream(
    const std::shared_ptr<IStorage> & storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    const Names & column_names_,
    ContextPtr context_,
    Int32 shard_,
    DWAL::KafkaWALSimpleConsumerPtr consumer_,
    Poco::Logger * log_)
    : storage(storage_)
    , context(context_)
    , column_names(column_names_)
    , shard(shard_)
    , consumer(consumer_)
    , log(log_)
    , header(metadata_snapshot_->getSampleBlockForColumns(column_names, storage->getVirtuals(), storage->getStorageID()))
{
    wend_type = header.findByName("wend");
    if (!wend_type)
        wend_type = header.findByName("wstart");
}

Block StreamingBlockInputStream::getHeader() const
{
    return header;
}

void StreamingBlockInputStream::readPrefixImpl()
{
    assert(!reader);

    reader = std::make_unique<StreamingBlockReader>(storage->getStorageID(), context, shard, consumer, log);
    iter = result_blocks.begin();

    last_flush_ms = MonotonicMilliseconds::now();
}

Block StreamingBlockInputStream::readImpl()
{
    assert(reader);

    if (isCancelled())
    {
        return {};
    }

    /// std::unique_lock lock(result_blocks_mutex);
    if (result_blocks.empty() || iter == result_blocks.end())
    {
        readAndProcess();

        if (isCancelled())
        {
            return {};
        }

        /// After processing blocks, check again to see if there are new results
        if (result_blocks.empty() || iter == result_blocks.end())
        {
            /// Act as a heart beat and flush
            last_flush_ms = MonotonicMilliseconds::now();
            return header;
        }

        /// result_blocks is not empty, fallthrough
    }

    if (MonotonicMilliseconds::now() - last_flush_ms >= flush_interval_ms)
    {
        last_flush_ms = MonotonicMilliseconds::now();
        return header;
    }

    return std::move(*iter++);
}

void StreamingBlockInputStream::readAndProcess()
{
    auto records = reader->read(flush_interval_ms);
    if (records.empty())
    {
        return;
    }

    /// 1) Insert raw blocks to in-memory aggregation table
    /// 2) Select the final result from the aggregated table
    /// 3) Update result_blocks and iterator
    result_blocks.clear();
    result_blocks.reserve(records.size());

    for (auto & record : records)
    {
       Block block;
       /// Only select required columns.
       /// FIXME, move the columns filtered logic to deserialization ?
       auto rows = record->block.rows();

       for (const auto & name : column_names)
       {
           if (name == "wstart" || name == "wend")
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

       result_blocks.push_back(std::move(block));
    }
    iter = result_blocks.begin();
}
}
