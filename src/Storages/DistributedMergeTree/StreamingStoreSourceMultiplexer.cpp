#include "StreamingStoreSourceMultiplexer.h"
#include "StorageDistributedMergeTree.h"

#include <DistributedWALClient/KafkaWALPool.h>
#include <Storages/IStorage.h>
#include <base/ClockUtils.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int RESOURCE_NOT_FOUND;
    extern const int DWAL_FATAL_ERROR;
}

StreamingStoreSourceMultiplexer::StreamingStoreSourceMultiplexer(
    UInt32 id_, Int32 shard_, std::shared_ptr<IStorage> storage_, ContextPtr global_context, Poco::Logger * log_)
    : id(id_), shard(shard_), storage(std::move(storage_)), poller(1), last_metrics_log_time(MonotonicMilliseconds::now()), log(log_)
{
    auto distributed = storage->as<StorageDistributedMergeTree>();
    assert(distributed);

    auto consumer = DWAL::KafkaWALPool::instance(global_context).getOrCreateStreaming(distributed->streamingStorageClusterId());
    reader = std::make_shared<StreamingBlockReader>(storage, shard, -1 /*latest*/, std::vector<uint16_t>{}, std::move(consumer), log);

    poller.scheduleOrThrowOnError([this] { backgroundPoll(); });
}

StreamingStoreSourceMultiplexer::~StreamingStoreSourceMultiplexer()
{
    doShutdown();

    poller.wait();

    LOG_INFO(
        log,
        "StreamingStoreSourceMultiplexer id={} for shard={} is dtored with {} channels, fan out total_time={}ns, total_count={}, avg={}ns",
        id,
        shard,
        channels.size(),
        metrics.total_time,
        metrics.total_count,
        metrics.total_time / (metrics.total_count == 0 ? 1 : metrics.total_count));
}

void StreamingStoreSourceMultiplexer::backgroundPoll()
{
    while (!shutdown)
    {
        try
        {
            auto records = reader->read(1000, 100);
            auto start = MonotonicNanoseconds::now();

            if (!records.empty())
            {
                fanOut(std::move(records));

                metrics.total_time = MonotonicNanoseconds::now() - start;
                ++metrics.total_count;
            }

            if (start - last_metrics_log_time >= 5000000000)
            {
                LOG_INFO(
                    log,
                    "StreamingStoreSourceMultiplexer id={} serving={} channels, average fan out time={}ns, total_time={}ns, total_count={}",
                    id,
                    totalChannels(),
                    metrics.total_time / (metrics.total_count == 0 ? 1 : metrics.total_count),
                    metrics.total_time,
                    metrics.total_count);

                last_metrics_log_time = start;
            }
        }
        catch (DB::Exception & e)
        {
            if (e.code() == ErrorCodes::RESOURCE_NOT_FOUND || e.code() == ErrorCodes::DWAL_FATAL_ERROR)
            {
                LOG_ERROR(
                    log,
                    "StreamingStoreSourceMultiplexer id={} for shard {} failed to poll, fatal error={}. Shutting down source multiplexer",
                    id,
                    shard,
                    e.message());
                doShutdown();
            }
            else
                LOG_ERROR(log, "StreamingStoreSourceMultiplexer id={} for shard {} failed to poll, error={}.", id, shard, e.message());
        }
        catch (...)
        {
            LOG_ERROR(log, "StreamingStoreSourceMultiplexer id={} for shard {} failed to poll, unknown error", id, shard);
        }
    }
}

void StreamingStoreSourceMultiplexer::doShutdown()
{
    shutdown = true;

    /// Send an empty sentinel to channels to tell them shutdown
    fanOut({});
}

void StreamingStoreSourceMultiplexer::fanOut(DWAL::RecordPtrs records)
{
    std::vector<StreamingStoreSourceChannelPtr> fanout_channels;

    {
        std::lock_guard lock{channels_mutex};
        fanout_channels.reserve(channels.size());
        for (auto & shard_channel : channels)
        {
            auto channel = shard_channel.second.lock();
            /// FIXME, audit per channel metrics here: how much time does it take to add records to channel
            if (channel)
                fanout_channels.push_back(std::move(channel));
        }
    }

    for (auto & channel : fanout_channels)
        /// Copy over the records
        channel->add(records);
}

StreamingStoreSourceChannelPtr StreamingStoreSourceMultiplexer::createChannel(
    const Names & column_names, const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context)
{
    auto header{metadata_snapshot->getSampleBlockForColumns(column_names, storage->getVirtuals(), storage->getStorageID())};
    auto channel
        = std::make_shared<StreamingStoreSourceChannel>(shared_from_this(), std::move(header), metadata_snapshot, std::move(query_context));

    std::lock_guard lock{channels_mutex};
    auto [_, inserted] = channels.emplace(channel->getID(), channel);
    assert(inserted);
    (void)inserted;

    return channel;
}

void StreamingStoreSourceMultiplexer::removeChannel(UInt32 channel_id)
{
    bool need_shutdown = false;
    LOG_INFO(log, "Removing streaming store channel id={}", channel_id);
    {
        std::lock_guard lock{channels_mutex};
        auto erased = channels.erase(channel_id);
        assert(erased == 1);
        (void)erased;

        if (channels.empty())
            need_shutdown = true;
    }

    if (need_shutdown)
    {
        LOG_INFO(log, "Empty channels in source multiplexer id={}. Shut it down", id);
        doShutdown();
    }
}

size_t StreamingStoreSourceMultiplexer::totalChannels() const
{
    std::lock_guard lock{channels_mutex};
    return channels.size();
}

StreamingStoreSourceMultiplexers::StreamingStoreSourceMultiplexers(
    std::shared_ptr<IStorage> storage_, ContextPtr global_context_, Poco::Logger * log_)
    : storage(std::move(storage_)), global_context(std::move(global_context_)), log(log_)
{
}

StreamingStoreSourceChannelPtr StreamingStoreSourceMultiplexers::createChannel(
    Int32 shard, const Names & column_names, const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context)
{
    std::lock_guard lock{multiplexers_mutex};

    auto iter = multiplexers.find(shard);
    if (iter == multiplexers.end())
    {
        multiplexers.emplace(
            shard,
            StreamingStoreSourceMultiplexerPtrs{std::make_shared<StreamingStoreSourceMultiplexer>(0, shard, storage, global_context, log)});
        iter = multiplexers.find(shard);
    }

    /// Find the multiplexer which has the least channels in the multiplexer array
    size_t min_channels = std::numeric_limits<size_t>::max();
    StreamingStoreSourceMultiplexerPtr best_multiplexer;

    auto it = iter->second.begin();
    for (; it != iter->second.end(); )
    {
        if ((*it)->isShutdown())
        {
            it = iter->second.erase(it);
            continue;
        }

        auto channels = (*it)->totalChannels();
        if (channels < min_channels)
        {
            min_channels = channels;
            best_multiplexer = *it;
        }
        ++it;
    }

    if (best_multiplexer)
    {
        /// Found one
        /// If min channels is greater than > 20, create another multiplexer for this shard
        /// FIXME, make this configurable
        if (min_channels > global_context->getSettingsRef().max_channels_per_resource_group.value)
        {
            best_multiplexer = std::make_shared<StreamingStoreSourceMultiplexer>(iter->second.size(), shard, storage, global_context, log);
            iter->second.push_back(best_multiplexer);
        }

        return best_multiplexer->createChannel(column_names, metadata_snapshot, query_context);
    }
    else
    {
        /// All multiplexers are shutdown
        auto multiplexer{std::make_shared<StreamingStoreSourceMultiplexer>(iter->second.size(), shard, storage, global_context, log)};
        iter->second.push_back(multiplexer);
        return multiplexer->createChannel(column_names, metadata_snapshot, query_context);
    }
}
}
