#include <IO/Kafka/Client.h>

#include <IO/Kafka/Connection.h>
#include <IO/Kafka/Handle.h>
#include <IO/Kafka/mapErrorCode.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
extern const int RESOURCE_NOT_FOUND;
}

namespace Kafka
{

Client::Client(const std::shared_ptr<Handle> & handle_, const std::string & topic)
    : handle(handle_)
    , topic_handle({rd_kafka_topic_new(handle->get(), topic.c_str(), nullptr), rd_kafka_topic_destroy})
    , logger(&Poco::Logger::get(fmt::format("{}-{}", name(), topicName())))
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

std::string Client::topicName() const
{
    return rd_kafka_topic_name(topic_handle.get());
}

int32_t Client::getPartitionCount() const
{
    const struct rd_kafka_metadata * metadata = nullptr;

    auto err = rd_kafka_metadata(getHandle(), 0, getTopicHandle(), &metadata, /*timeout_ms=*/5000);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
        throw Exception(
            mapErrorCode(err),
            "Failed to get partition count of topic {}, error_code={}, error_msg={}",
            topicName(),
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

WatermarkOffsets Client::queryWatermarkOffsets(int32_t partition) const
{
    int64_t low, high;
    auto err = rd_kafka_query_watermark_offsets(handle->get(), topicName().c_str(), partition, &low, &high, /*timeout_ms=*/5000);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
    {
        LOG_INFO(logger, "Failed to query watermark offsets topic={} partition={} error={}", topicName(), partition, rd_kafka_err2str(err));

        throw Exception(
            mapErrorCode(err),
            "Failed to query watermark offsets topic={} partition={} error={}",
            topicName(),
            partition,
            rd_kafka_err2str(err));
    }

    return {low, high};
}

WatermarkOffsets Client::getWatermarkOffsets(int32_t partition) const
{
    int64_t low = 0, high = 0;

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

void Consumer::initialize(const std::vector<int32_t> & partitions)
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

Consumer::Version Consumer::recreate(Version version)
{
    /// Make sure that no one can consume data (by calling consumeBatch) during recreation.
    std::unique_lock lock{mutex};

    /// The request is out-of-date, ignore it.
    if (version < ver)
        return ver;

    for (const auto & p : partitions_progress)
        doStopConsume(p.first);

    owner->updateConsumer(shared_from_this());

    for (const auto & p : partitions_progress)
        doStartConsume(p.first, p.second.high + 1);

    return ++ver;
}

void Consumer::updateFrom(Consumer && other)
{
    handle.swap(other.handle);
    topic_handle.swap(other.topic_handle);
    logger = other.logger;
}

void Consumer::consumeBatch(int32_t partition, uint32_t count, int32_t timeout_ms, Callback callback, ErrorCallback error_callback)
{
    std::unique_ptr<rd_kafka_message_t *, decltype(free) *> rkmessages{
        static_cast<rd_kafka_message_t **>(malloc(sizeof(rd_kafka_message_t *) * count)), free};

    Int64 res{0};
    {
        /// Allows all sources which use the same consumer can consume data at the same time.
        std::shared_lock lock{mutex};
        res = rd_kafka_consume_batch(getTopicHandle(), partition, timeout_ms, rkmessages.get(), count);

        if (res < 0)
        {
            error_callback(rd_kafka_last_error());
            return;
        }

        if (res > 0)
        {
            auto & progress = partitions_progress[partition];
            progress.low = rkmessages.get()[0]->offset;
            progress.high = rkmessages.get()[res - 1]->offset;
        }
    }

    for (ssize_t idx = 0; idx < res; ++idx)
    {
        auto * rkmessage = rkmessages.get()[idx];

        try
        {
            if (rkmessage->err != RD_KAFKA_RESP_ERR_NO_ERROR)
                error_callback(rkmessage->err);
            else
                callback(rkmessage, res, nullptr);
        }
        catch (...)
        {
            /// just log the error to make sure the messages get destroyed
            LOG_ERROR(
                logger,
                "Uncaught exception during consuming topic={} partition={} error={}",
                topicName(),
                partition,
                DB::getCurrentExceptionMessage(true, true));
        }

        rd_kafka_message_destroy(rkmessage);
    }
}

WatermarkOffsets Consumer::getLastBatchOffsets(int32_t partition) const
{
    return partitions_progress.at(partition);
}

std::pair<int64_t /* last_consumed_offset */, int64_t /* latest_offset */> Consumer::getProgress(int32_t partition) const
{
    return {partitions_progress.at(partition).high, getWatermarkOffsets(partition).high};
}

Producer::Producer(const ProducerHandlePtr & handle_, const std::string & topic) : Client(handle_, topic)
{
}

void Producer::start()
{
    auto * producer_handle = dynamic_cast<ProducerHandle *>(handle.get());
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
