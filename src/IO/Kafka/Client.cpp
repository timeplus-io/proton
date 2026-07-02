#include <IO/Kafka/Client.h>

#include <IO/Kafka/Connection.h>
#include <IO/Kafka/Handle.h>
#include <IO/Kafka/mapErrorCode.h>
#include <Common/logger_useful.h>
#include <Common/scope_guard_safe.h>

#include <optional>


namespace DB
{

namespace ErrorCodes
{
extern const int RESOURCE_NOT_FOUND;
}

namespace Kafka
{

Client::Client(const std::shared_ptr<Handle> & handle_, const std::string & topic)
    : topic_name(topic)
    , handle(handle_)
    , topic_handle({rd_kafka_topic_new(handle->get(), topic.c_str(), nullptr), rd_kafka_topic_destroy})
    , logger(getLogger(fmt::format("{}-{}", name(), topicName())))
{
}

rd_kafka_t * Client::getHandle() const
{
    return handle->get();
}

rd_kafka_topic_t * Client::getTopicHandle() const
{
    return topic_handle.get();
}

std::string Client::name() const
{
    return handle->getName();
}

const std::string & Client::topicName() const
{
    return topic_name;
}

int32_t Client::getPartitionCount(uint64_t timeout_ms) const
{
    const struct rd_kafka_metadata * metadata = nullptr;

    auto err = rd_kafka_metadata(getHandle(), 0, getTopicHandle(), &metadata, static_cast<int>(timeout_ms));
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
        throw Exception(
            mapErrorCode(err),
            "Failed to get partition count of topic {}, timeout_ms={}, error_code={}, error_msg={}",
            topicName(),
            timeout_ms,
            err,
            rd_kafka_err2str(err));

    SCOPE_EXIT({ rd_kafka_metadata_destroy(metadata); });

    if (metadata->topic_cnt < 1)
        throw Exception(ErrorCodes::RESOURCE_NOT_FOUND, "Could not find topic {}", topicName());

    assert(metadata->topic_cnt == 1);

    /// It is possible the rd_kafka_metadata returns RD_KAFKA_RESP_ERR_NO_ERROR as a whole,
    /// but error for separate topics
    if (metadata->topics[0].err != RD_KAFKA_RESP_ERR_NO_ERROR)
        throw Exception(mapErrorCode(metadata->topics[0].err), "Failed to get partition count for topic={}", topicName());

    return metadata->topics[0].partition_cnt;
}

WatermarkOffsets Client::queryWatermarkOffsets(int32_t partition, uint64_t timeout_ms) const
{
    int64_t low = 0;
    int64_t high = 0;
    auto err = rd_kafka_query_watermark_offsets(handle->get(), topicName().c_str(), partition, &low, &high, static_cast<int>(timeout_ms));
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
    {
        LOG_INFO(
            logger,
            "Failed to query watermark offsets topic={} partition={} timeout_ms={} error={}",
            topicName(),
            partition,
            timeout_ms,
            rd_kafka_err2str(err));

        throw Exception(
            mapErrorCode(err),
            "Failed to query watermark offsets topic={} partition={} timeout_ms={} error={}",
            topicName(),
            partition,
            timeout_ms,
            rd_kafka_err2str(err));
    }

    return {low, high};
}

WatermarkOffsets Client::getWatermarkOffsets(int32_t partition) const
{
    int64_t low = 0;
    int64_t high = 0;

    /// rd_kafka_get_watermark_offsets returns cached start_offset and end_offset
    /// end_offset is the next offset to be assigned, so it is [start_offset, end_offset) range
    auto err = rd_kafka_get_watermark_offsets(getHandle(), topicName().c_str(), partition, &low, &high);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
    {
        LOG_INFO(logger, "Failed to get watermark offsets topic={} partition={} error={}", topicName(), partition, rd_kafka_err2str(err));

        throw Exception(
            mapErrorCode(err),
            "Failed to get watermark offsets topic={} partition={} error={}",
            topicName(),
            partition,
            rd_kafka_err2str(err));
    }

    return {low, high};
}

Consumer::Consumer(const ConnectionPtr & owner_, const ConsumerHandlePtr & handle_, const std::string & topic)
    : Client(handle_, topic), owner(owner_)
{
}

void Consumer::initialize(const std::vector<uint64_t> & partitions)
{
    partitions_progress.reserve(partitions.size());
    for (auto p : partitions)
        partitions_progress.emplace(p, WatermarkOffsets{});
}

void Consumer::startConsume(int32_t partition, int64_t offset)
{
    if (!partitions_progress.contains(partition))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Consumer {} was not supposed to consume partition {}", name(), partition);

    doStartConsume(partition, offset);

    partitions_progress[partition].high = offset - 1;
}

void Consumer::doStartConsume(int32_t partition, int64_t offset)
{
    LOG_INFO(logger, "Start consuming {}-{} from {}", topicName(), partition, offset);

    handle->startPolling();

    auto res = rd_kafka_consume_start(getTopicHandle(), partition, offset);
    if (res < 0)
    {
        auto err = rd_kafka_last_error();
        throw Exception(
            mapErrorCode(err),
            "Failed to start consuming topic={} partition={} offset={} error={}",
            topicName(),
            partition,
            offset,
            rd_kafka_err2str(err));
    }
}

void Consumer::stopConsume(int32_t partition)
{
    if (!partitions_progress.contains(partition))
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Unexpected call of stopConsume on partition {}, {} was not consuming it", partition, name());

    doStopConsume(partition);
}

void Consumer::doStopConsume(int32_t partition)
{
    LOG_INFO(logger, "Stop consuming {}-{}", topicName(), partition);

    auto res = rd_kafka_consume_stop(getTopicHandle(), partition);
    if (res < 0)
    {
        auto err = rd_kafka_last_error();
        LOG_ERROR(logger, "Failed to stop consuming topic={} partition={} error={}", topicName(), partition, rd_kafka_err2str(err));
    }
}

bool Consumer::recreate(UInt64 cooldown_ms)
{
    /// Make sure that no one can consume data (by calling consumeBatch) during recreation.
    std::unique_lock lock{mutex};

    /// The last recreation was within cool down time, ignore it.
    if (creation_time.elapsedMilliseconds() < cooldown_ms)
        return false;

    for (const auto & p : partitions_progress)
        doStopConsume(p.first);

    owner->updateConsumer(shared_from_this());

    for (const auto & p : partitions_progress)
        doStartConsume(p.first, p.second.high + 1);

    creation_time.restart();

    return true;
}

void Consumer::updateFromWithLockHeld(Consumer && other)
{
    /// Lock is already held in Consumer::recreate()
    ///   Consumer::recreate() -> Connection::updateConsumer() -> Consumer::updateFromWithLockHeld()
    handle.swap(other.handle);
    topic_handle.swap(other.topic_handle);
    logger = other.logger;
}

void Consumer::consumeBatch(int32_t partition, uint32_t count, int32_t timeout_ms, Callback callback, ErrorCallback error_callback)
{
    std::unique_ptr<rd_kafka_message_t *, decltype(free) *> rkmessages{
        static_cast<rd_kafka_message_t **>(malloc(sizeof(rd_kafka_message_t *) * count)), free};

    auto & progress = partitions_progress[partition];

    Int64 res{0};
    std::optional<ssize_t> first_error_message_idx;

    {
        /// Allows all sources which use the same consumer can consume data at the same time.
        std::shared_lock lock{mutex};
        res = rd_kafka_consume_batch(getTopicHandle(), partition, timeout_ms, rkmessages.get(), count);

        if (res < 0)
        {
            error_callback(rd_kafka_last_error(), std::string_view{});
            return;
        }

        if (res > 0)
        {
            std::optional<int64_t> first_offset;
            std::optional<int64_t> last_offset;

            for (ssize_t idx = 0; idx < res; ++idx)
            {
                auto * rkmessage = rkmessages.get()[idx];
                if (unlikely(rkmessage->err != RD_KAFKA_RESP_ERR_NO_ERROR))
                {
                    first_error_message_idx = idx;
                    break;
                }

                /// Properly record the progress so that it can skip posion data (in MV) properly.
                if (likely(rkmessage->offset != RD_KAFKA_OFFSET_INVALID))
                {
                    if (!first_offset)
                        first_offset = rkmessage->offset;
                    last_offset = rkmessage->offset;
                }
            }

            if (!first_error_message_idx && first_offset)
            {
                progress.low = first_offset.value();
                progress.high = last_offset.value();
            }
        }
    }

    SCOPE_EXIT_SAFE(for (ssize_t idx = 0; idx < res; ++idx) rd_kafka_message_destroy(rkmessages.get()[idx]););

    if (first_error_message_idx)
    {
        auto * rkmessage = rkmessages.get()[first_error_message_idx.value()];
        error_callback(rkmessage->err, std::string_view{reinterpret_cast<const char *>(rkmessage->payload), rkmessage->len});
        return;
    }

    callback(rkmessages.get(), res, nullptr);
}

WatermarkOffsets Consumer::getLastBatchOffsets(int32_t partition) const
{
    return partitions_progress.at(partition);
}

std::pair<int64_t /* last_consumed_offset */, int64_t /* latest_offset */> Consumer::getProgress(int32_t partition) const
{
    return {partitions_progress.at(partition).high, getWatermarkOffsets(partition).high};
}

std::string Consumer::name() const
{
    std::shared_lock lock{mutex};
    return Client::name();
}

int32_t Consumer::getPartitionCount(uint64_t timeout_ms) const
{
    std::shared_lock lock{mutex};
    return Client::getPartitionCount(timeout_ms);
}

WatermarkOffsets Consumer::queryWatermarkOffsets(int32_t partition, uint64_t timeout_ms) const
{
    std::shared_lock lock{mutex};
    return Client::queryWatermarkOffsets(partition, timeout_ms);
}

WatermarkOffsets Consumer::getWatermarkOffsets(int32_t partition) const
{
    std::shared_lock lock{mutex};
    return Client::getWatermarkOffsets(partition);
}

Producer::Producer(const ProducerHandlePtr & handle_, const std::string & topic) : Client(handle_, topic)
{
}

void Producer::start(bool need_poll)
{
    auto * producer_handle = dynamic_cast<ProducerHandle *>(handle.get());
    if (need_poll)
        producer_handle->startPolling();
    producer_handle->increaseUseCount();
}

void Producer::stop()
{
    auto * producer_handle = dynamic_cast<ProducerHandle *>(handle.get());
    producer_handle->decreaseUseCount();
}

}

}
