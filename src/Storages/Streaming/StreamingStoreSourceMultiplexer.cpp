#include <Storages/Streaming/StreamingStoreSourceMultiplexer.h>

#include <KafkaLog/KafkaWALPool.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/Streaming/StreamShard.h>
#include <Storages/Streaming/StreamingStoreSourceChannel.h>
#include <base/ClockUtils.h>

namespace DB
{
namespace ErrorCodes
{
extern const int RESOURCE_NOT_FOUND;
extern const int DWAL_FATAL_ERROR;
}

StreamingStoreSourceMultiplexer::StreamingStoreSourceMultiplexer(
    UInt32 id_,
    std::shared_ptr<StreamShard> stream_shard_,
    ContextPtr query_context,
    Poco::Logger * log_,
    StreamingStoreSourceMultiplexer::AttachToSharedGroupFunc attach_to_shared_group_func_)
    : id(id_)
    , stream_shard(std::move(stream_shard_))
    , attach_to_shared_group_func(attach_to_shared_group_func_)
    , poller(std::make_unique<ThreadPool>(1))
    , last_metrics_log_time(MonotonicMilliseconds::now())
    , log(log_)
{
    const auto & settings = query_context->getSettingsRef();
    if (settings.record_consume_batch_count.value != 0)
        record_consume_batch_count = static_cast<UInt32>(settings.record_consume_batch_count.value);

    if (settings.record_consume_timeout_ms.value != 0)
        record_consume_timeout_ms = static_cast<Int32>(settings.record_consume_timeout_ms.value);

    if (stream_shard->isLogStoreKafka())
    {
        auto consumer = klog::KafkaWALPool::instance(query_context).getOrCreateStreaming(stream_shard->logStoreClusterId());
        assert(consumer);
        kafka_reader = std::make_unique<StreamingBlockReaderKafka>(
            stream_shard, nlog::LATEST_SN, SourceColumnsDescription::PhysicalColumnPositions{}, std::move(consumer), log);
    }
    else
    {
        auto fetch_buffer_size = query_context->getSettingsRef().fetch_buffer_size;
        fetch_buffer_size = std::min<UInt64>(64 * 1024 * 1024, fetch_buffer_size);
        nativelog_reader = std::make_unique<StreamingBlockReaderNativeLog>(
            stream_shard,
            nlog::LATEST_SN,
            record_consume_timeout_ms,
            fetch_buffer_size,
            /*schema_provider*/ nullptr,
            /*schema_version*/ 0,
            SourceColumnsDescription::PhysicalColumnPositions{},
            log);
    }

    /// So far, the `attach_to_shared_group_func` is only set in an independent multiplexer, which requires lazy start up
    bool need_lazy_startup = static_cast<bool>(attach_to_shared_group_func);
    if (!need_lazy_startup)
        startup();
}

StreamingStoreSourceMultiplexer::~StreamingStoreSourceMultiplexer()
{
    doShutdown();

    poller->wait();
    poller.reset();

    LOG_INFO(
        log,
        "StreamingStoreSourceMultiplexer id={} for shard={} is dtored with {} channels, fan out total_time={}ns, total_count={}, avg={}ns",
        id,
        stream_shard->getShard(),
        channels.size(),
        metrics.total_time,
        metrics.total_count,
        metrics.total_time / (metrics.total_count == 0 ? 1 : metrics.total_count));
}

void StreamingStoreSourceMultiplexer::startup()
{
    if (started.test_and_set())
        return;

    poller->scheduleOrThrowOnError([this] { backgroundPoll(); });
}

void StreamingStoreSourceMultiplexer::resetSequenceNumber(Int64 start_sn)
{
    assert(!started.test());
    if (nativelog_reader)
        nativelog_reader->resetSequenceNumber(start_sn);
    else
        kafka_reader->resetOffset(start_sn);
}

nlog::RecordPtrs StreamingStoreSourceMultiplexer::read()
{
    if (nativelog_reader)
        return nativelog_reader->read();
    else
        return kafka_reader->read(record_consume_batch_count, record_consume_timeout_ms);
}

void StreamingStoreSourceMultiplexer::backgroundPoll()
{
    while (!shutdown)
    {
        try
        {
            auto records = read();
            auto start = MonotonicNanoseconds::now();

            if (!records.empty())
            {
                fanOut(std::move(records));

                metrics.total_time = MonotonicNanoseconds::now() - start;
                ++metrics.total_count;
            }
            else if (attach_to_shared_group_func)
            {
                /// Assume that the latest record has been read, we can try attach to shared group,
                /// After attached, means its all channels will consume from shared group
                attach_to_shared_group_func(shared_from_this());
                attach_to_shared_group_func = {};

                LOG_INFO(log, "StreamingStoreSourceMultiplexer id={} for shard {} attached to shared group.", id, stream_shard->getShard());
            }

            if (start - last_metrics_log_time >= 30000000000)
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
        catch (const DB::Exception & e)
        {
            if (e.code() == ErrorCodes::RESOURCE_NOT_FOUND || e.code() == ErrorCodes::DWAL_FATAL_ERROR)
            {
                LOG_ERROR(
                    log,
                    "StreamingStoreSourceMultiplexer id={} for shard {} failed to poll, fatal error={}. Shutting down source multiplexer",
                    id,
                    stream_shard->getShard(),
                    e.message());
                doShutdown();
            }
            else
                LOG_ERROR(
                    log,
                    "StreamingStoreSourceMultiplexer id={} for shard {} failed to poll, error={}.",
                    id,
                    stream_shard->getShard(),
                    e.message());
        }
        catch (...)
        {
            LOG_ERROR(
                log, "StreamingStoreSourceMultiplexer id={} for shard {} failed to poll, unknown error", id, stream_shard->getShard());
        }
    }
}

void StreamingStoreSourceMultiplexer::doShutdown()
{
    shutdown = true;

    /// Send an empty sentinel to channels to tell them shutdown
    fanOut({});
}

void StreamingStoreSourceMultiplexer::fanOut(nlog::RecordPtrs records)
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

        /// Update fanout max sn
        auto iter = std::find_if(records.rbegin(), records.rend(), [](const auto & record) { return !record->empty(); });
        if (iter != records.rend())
            fanout_sn = (*iter)->getSN();
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
        shared_from_this(), std::move(header), storage_snapshot, std::move(query_context), log);

    std::lock_guard lock{channels_mutex};
    [[maybe_unused]] auto [_, inserted] = channels.emplace(channel->getID(), channel);
    assert(inserted);

    return channel;
}

bool StreamingStoreSourceMultiplexer::tryDetachChannelsInto(std::shared_ptr<StreamingStoreSourceMultiplexer> new_multiplexer)
{
    /// NOTE: `fanout_sn` is only updated during locking channels_mutex in `fanOut()`
    {
        std::lock_guard lock1{channels_mutex};
        {
            std::lock_guard lock2{new_multiplexer->channels_mutex};
            if (fanout_sn < new_multiplexer->fanout_sn)
                return false;

            for (auto & shard_channel : channels)
            {
                auto channel = shard_channel.second.lock();
                if (channel)
                {
                    channel->attachTo(new_multiplexer);
                    [[maybe_unused]] auto [_, inserted] = new_multiplexer->channels.emplace(shard_channel.first, std::move(channel));
                    assert(inserted);
                }
            }
        }
        channels.clear();
    }
    doShutdown();
    return true;
}

void StreamingStoreSourceMultiplexer::removeChannel(UInt32 channel_id)
{
    bool need_shutdown = false;
    LOG_INFO(log, "Removing streaming store channel id={}", channel_id);
    {
        std::lock_guard lock{channels_mutex};
        [[maybe_unused]] auto erased = channels.erase(channel_id);
        assert(erased == 1);

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


std::pair<String, Int32> StreamingStoreSourceMultiplexer::getStreamShard() const
{
    return stream_shard->getStreamShard();
}

StreamingStoreSourceMultiplexers::StreamingStoreSourceMultiplexers(ContextPtr global_context_, Poco::Logger * log_)
    : global_context(std::move(global_context_)), log(log_)
{
}

StreamingStoreSourceChannelPtr StreamingStoreSourceMultiplexers::createChannel(
    std::shared_ptr<StreamShard> stream_shard,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    ContextPtr query_context,
    Int64 start_sn)
{
    /// In following scenarios, we need independent channel to read some past data
    /// 1) Recover from checkpointed queries
    /// 2) Queries which seek to a specific timestamp or earliest
    if (query_context->getSettingsRef().exec_mode == ExecuteMode::RECOVER)
        return createIndependentChannelForRecover(stream_shard, column_names, storage_snapshot, query_context);
    else if (start_sn != nlog::LATEST_SN)
        return createIndependentChannelWithSeekTo(stream_shard, column_names, storage_snapshot, query_context, start_sn);

    assert(start_sn == nlog::LATEST_SN);

    std::lock_guard lock{multiplexers_mutex};

    auto shard = stream_shard->getShard();
    auto iter = multiplexers.find(shard);
    if (iter == multiplexers.end())
    {
        multiplexers.emplace(
            shard,
            StreamingStoreSourceMultiplexerPtrs{std::make_shared<StreamingStoreSourceMultiplexer>(0, stream_shard, global_context, log)});
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
        /// If min channels is greater than > 20(default value), create another multiplexer for this shard
        if (min_channels > global_context->getSettingsRef().max_channels_per_resource_group.value)
        {
            best_multiplexer = std::make_shared<StreamingStoreSourceMultiplexer>(iter->second.size(), stream_shard, global_context, log);
            iter->second.push_back(best_multiplexer);
        }

        return best_multiplexer->createChannel(column_names, storage_snapshot, query_context);
    }
    else
    {
        /// All multiplexers are shutdown
        auto multiplexer{std::make_shared<StreamingStoreSourceMultiplexer>(iter->second.size(), stream_shard, global_context, log)};
        iter->second.push_back(multiplexer);
        return multiplexer->createChannel(column_names, storage_snapshot, query_context);
    }
}

StreamingStoreSourceChannelPtr StreamingStoreSourceMultiplexers::createIndependentChannelForRecover(
    std::shared_ptr<StreamShard> stream_shard,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    ContextPtr query_context)
{
    /// will startup after `StreamingStoreSourceChannel::recover()` and reset recovered sn
    /// The `multiplexer` is cached in created StreamingStoreSourceChannel, we can release this one
    auto multiplexer = std::make_shared<StreamingStoreSourceMultiplexer>(
        0, std::move(stream_shard), global_context, log, [this](auto multiplexer_) { attachToSharedGroup(multiplexer_); });
    return multiplexer->createChannel(column_names, storage_snapshot, query_context);
}

StreamingStoreSourceChannelPtr StreamingStoreSourceMultiplexers::createIndependentChannelWithSeekTo(
    std::shared_ptr<StreamShard> stream_shard,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    ContextPtr query_context,
    Int64 start_sn)
{
    /// The `multiplexer` is cached in created StreamingStoreSourceChannel, we can release this one
    auto multiplexer = std::make_shared<StreamingStoreSourceMultiplexer>(
        0, std::move(stream_shard), global_context, log, [this](auto multiplexer_) { attachToSharedGroup(multiplexer_); });
    auto channel = multiplexer->createChannel(column_names, storage_snapshot, query_context);
    multiplexer->resetSequenceNumber(start_sn);
    multiplexer->startup();
    return channel;
}

void StreamingStoreSourceMultiplexers::attachToSharedGroup(StreamingStoreSourceMultiplexerPtr multiplexer)
{
    std::lock_guard lock{multiplexers_mutex};

    /// First release the detached multiplexers
    detached_multiplexers.clear();

    auto & multiplexer_list = multiplexers[multiplexer->stream_shard->getShard()];
    for (auto & shared_multiplexer : multiplexer_list)
    {
        /// Skip multiplexer that already have too many channels
        if (shared_multiplexer->totalChannels() > global_context->getSettingsRef().max_channels_per_resource_group.value)
            continue;

        if (multiplexer->tryDetachChannelsInto(shared_multiplexer))
        {
            /// keep the detached multiplexer for a while since we cannot release itself in his own background polling thread,
            /// we will release it on next call (another background polling thread),
            detached_multiplexers.emplace_back(std::move(multiplexer));
            return;
        }
    }

    /// Not detach channels into any existed shared multiplexer, so we reuse it and join in shared groups
    multiplexer->id = multiplexer_list.size(); /// it's thread safe
    multiplexer_list.emplace_back(std::move(multiplexer));
}
}
