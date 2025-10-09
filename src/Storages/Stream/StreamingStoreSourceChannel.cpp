#include <Storages/Stream/StreamingStoreSourceChannel.h>
#include <Storages/Stream/StreamingStoreSourceMultiplexer.h>

#include <DataTypes/ObjectUtils.h>
#include <Interpreters/inplaceBlockConversions.h>

namespace DB
{
StreamingStoreSourceChannel::StreamingStoreSourceChannel(
    std::shared_ptr<StreamingStoreSourceMultiplexer> multiplexer_,
    Block header,
    StorageSnapshotPtr storage_snapshot_,
    ContextPtr query_context_,
    LoggerPtr log_)
    : StreamingStoreSourceBase(
          header,
          storage_snapshot_,
          std::move(query_context_),
          log_,
          ProcessorID::StreamingStoreSourceChannelID) /// NOLINT(performance-move-const-arg)
    , id(sequence_id++)
    , multiplexer(std::move(multiplexer_))
    , records_queue(1000)
{
    const auto & stream_shard = multiplexer->streamShard();
    setDescription(fmt::format("channel_id={},stream={}.{},shard={}", id, stream_shard.ns, stream_shard.name, stream_shard.shard()));
}

std::atomic<uint32_t> StreamingStoreSourceChannel::sequence_id = 0;

StreamingStoreSourceChannel::~StreamingStoreSourceChannel()
{
    multiplexer->removeChannel(id);
}

void StreamingStoreSourceChannel::readAndProcess()
{
    cluster::SchemaRecordPtrs records;
    auto got_records = records_queue.tryPop(records, 100);
    if (!got_records)
        return;

    /// Got shutdown signal
    if (records.empty())
    {
        cancel();
        return;
    }

    process(records);
}

void StreamingStoreSourceChannel::add(cluster::SchemaRecordPtrs records)
{
    [[maybe_unused]] auto added = records_queue.emplace(std::move(records));
    assert(added);
}

std::pair<String, Int32> StreamingStoreSourceChannel::getStreamShard() const
{
    return multiplexer->getStreamShard();
}
}
