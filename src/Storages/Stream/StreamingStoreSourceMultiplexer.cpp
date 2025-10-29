#include <Storages/Stream/StorageStream.h>
#include <Storages/Stream/StreamShardStore.h>
#include <Storages/Stream/StreamingBlockReaderKafka.h>
#include <Storages/Stream/StreamingStoreSourceMultiplexer.h>

#include <Bootstrap/Globals.h>
#include <Cluster/Common/Constants.h>
#include <Cluster/KafkaLog/KafkaWALPool.h>
#include <Interpreters/Context.h>
#include <base/ClockUtils.h>

namespace DB
{
namespace ErrorCodes
{
extern const int RESOURCE_NOT_FOUND;
extern const int DWAL_FATAL_ERROR;
}

StreamingStoreSourceMultiplexer::StreamingStoreSourceMultiplexer(
    UInt32 id_, StreamShardStorePtr stream_shard_store_, ContextPtr /*global_context*/, LoggerPtr logger_)
    : id(id_)
    , stream_shard_store(std::move(stream_shard_store_))
    , poller(std::make_unique<ThreadPool>(CurrentMetrics::LocalThread, CurrentMetrics::LocalThreadActive, 1))
    , last_metrics_log_time(MonotonicMilliseconds::now())
    , logger(logger_)
{
    // UInt32 record_consume_batch_count = 1000;
    // Int32 record_consume_timeout_ms = 100;
    // const auto & settings = global_context->getSettingsRef();
    // if (settings.record_consume_batch_count.value != 0)
    //     record_consume_batch_count = static_cast<UInt32>(settings.record_consume_batch_count.value);

    // if (settings.record_consume_timeout_ms.value != 0)
    //     record_consume_timeout_ms = static_cast<Int32>(settings.record_consume_timeout_ms.value);

    auto sample_block = stream_shard_store_->storageStream().getInMemoryMetadataPtr()->getSampleBlock();
    CachedSchemaProvider schema_provider(&stream_shard_store_->storageStream());

    // auto consumer = Globals::getKafkaWALPool()->getOrCreateStreaming(stream_shard_store->logStoreClusterId());
    // reader = std::make_unique<StreamingBlockReaderKafka>(
    //     cluster::Constants::LatestSN,
    //     record_consume_timeout_ms,
    //     record_consume_batch_count,
    //     /*stream_shard_=*/stream_shard_store->streamShard(),
    //     /*storage=*/&stream_shard_store_->storageStream(),
    //     /*sample_block=*/std::move(sample_block),
    //     /*query_schema_version=*/cluster::SchemaRecord::DEFAULT_SECHEMA_VERSION,
    //     cluster::SourceColumnsDescription::PhysicalColumnPositions{},
    //     std::move(consumer),
    //     /*is_stopped_=*/[shard_store = std::move(stream_shard_store)] { return shard_store->isStopped(); },
    //     logger);

    poller->scheduleOrThrowOnError([this] { backgroundPoll(); });
}

StreamingStoreSourceMultiplexer::~StreamingStoreSourceMultiplexer()
{
    doShutdown();

    poller->wait();
    poller.reset();

    LOG_INFO(
        logger,
        "StreamingStoreSourceMultiplexer id={} for {} is dtored with {} channels, fan out total_time={}ns, total_count={}, avg={}ns",
        id,
        stream_shard_store->streamShard().string(),
        channels.size(),
        metrics.total_time,
        metrics.total_count,
        metrics.total_time / (metrics.total_count == 0 ? 1 : metrics.total_count));
}

void StreamingStoreSourceMultiplexer::backgroundPoll()
{
    /*while (!shutdown)
    {
        try
        {
            auto records = reader->read();
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
                    logger,
                    "StreamingStoreSourceMultiplexer id={} serving={} channels, average fan out time={}ns, total_time={}ns, total_count={}",
                    id,
                    totalChannels(),
                    metrics.total_time / (metrics.total_count == 0 ? 1 : metrics.total_count),
                    metrics.total_time,
                    metrics.total_count);

                last_metrics_log_time = start;
            }
        }
        catch (const DB::Exception & e)
        {
            if (e.code() == ErrorCodes::RESOURCE_NOT_FOUND || e.code() == ErrorCodes::DWAL_FATAL_ERROR)
            {
                LOG_ERROR(
                    logger,
                    "StreamingStoreSourceMultiplexer id={} for {} failed to poll, fatal error={}. Shutting down source multiplexer",
                    id,
                    stream_shard_store->streamShard().string(),
                    e.message());
                doShutdown();
            }
            else
            {
                LOG_ERROR(
                    logger,
                    "StreamingStoreSourceMultiplexer id={} for {} failed to poll, error={}.",
                    id,
                    stream_shard_store->streamShard().string(),
                    e.message());
            }
        }
        catch (...)
        {
            LOG_ERROR(
                logger,
                "StreamingStoreSourceMultiplexer id={} for {} failed to poll, unknown error",
                id,
                stream_shard_store->streamShard().string());
        }
    }*/
}

void StreamingStoreSourceMultiplexer::doShutdown()
{
    shutdown = true;

    /// Send an empty sentinel to channels to tell them shutdown
    fanOut({});
}

void StreamingStoreSourceMultiplexer::fanOut(cluster::SchemaRecordPtrs records)
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
    const Names & column_names, const StorageSnapshotPtr & storage_snapshot, ContextPtr query_context)
{
    auto header{storage_snapshot->getSampleBlockForColumns(column_names)};
    auto channel = std::make_shared<StreamingStoreSourceChannel>(
        shared_from_this(), std::move(header), storage_snapshot, std::move(query_context), logger);

    std::lock_guard lock{channels_mutex};
    [[maybe_unused]] auto [_, inserted] = channels.emplace(channel->getID(), channel);
    assert(inserted);

    return channel;
}

void StreamingStoreSourceMultiplexer::removeChannel(UInt32 channel_id)
{
    bool need_shutdown = false;
    LOG_INFO(logger, "Removing streaming store channel id={}", channel_id);
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
        LOG_INFO(logger, "Empty channels in source multiplexer id={}. Shut it down", id);
        doShutdown();
    }
}

size_t StreamingStoreSourceMultiplexer::totalChannels() const
{
    std::lock_guard lock{channels_mutex};
    return channels.size();
}


std::pair<String, Int32> StreamingStoreSourceMultiplexer::getStreamShard() const
{
    return stream_shard_store->getStreamShard();
}

const cluster::StreamShard & StreamingStoreSourceMultiplexer::streamShard() const
{
    return stream_shard_store->streamShard();
}

StreamingStoreSourceMultiplexers::StreamingStoreSourceMultiplexers(
    StreamShardStorePtr stream_shard_store_, ContextPtr global_context_, LoggerPtr logger_)
    : stream_shard_store(std::move(stream_shard_store_)), global_context(std::move(global_context_)), logger(logger_)
{
}

StreamingStoreSourceChannelPtr StreamingStoreSourceMultiplexers::createChannel(
    Int32 shard, const Names & column_names, const StorageSnapshotPtr & storage_snapshot, ContextPtr query_context)
{
    std::lock_guard lock{multiplexers_mutex};

    auto iter = multiplexers.find(shard);
    if (iter == multiplexers.end())
    {
        multiplexers.emplace(
            shard,
            StreamingStoreSourceMultiplexerPtrs{
                std::make_shared<StreamingStoreSourceMultiplexer>(0, stream_shard_store, global_context, logger)});
        iter = multiplexers.find(shard);
    }

    /// Find the multiplexer which has the least channels in the multiplexer array
    size_t min_channels = std::numeric_limits<size_t>::max();
    StreamingStoreSourceMultiplexerPtr best_multiplexer;

    auto it = iter->second.begin();
    for (; it != iter->second.end();)
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
            best_multiplexer
                = std::make_shared<StreamingStoreSourceMultiplexer>(iter->second.size(), stream_shard_store, global_context, logger);
            iter->second.push_back(best_multiplexer);
        }

        return best_multiplexer->createChannel(column_names, storage_snapshot, query_context);
    }
    else
    {
        /// All multiplexers are shutdown
        auto multiplexer{
            std::make_shared<StreamingStoreSourceMultiplexer>(iter->second.size(), stream_shard_store, global_context, logger)};
        iter->second.push_back(multiplexer);
        return multiplexer->createChannel(column_names, storage_snapshot, query_context);
    }
}
}
