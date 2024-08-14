#include <KafkaLog/KafkaWALCommon.h>
#include <Storages/ExternalStream/Kafka/Topic.h>

namespace DB
{

namespace ErrorCodes
{
extern const int RESOURCE_NOT_FOUND;
}

namespace RdKafka
{

Topic::Topic(rd_kafka_t & rk, const std::string & name) : rdk(rk)
{
    /// rd_kafka_topic_new takes ownership of topic_conf
    rkt.reset(rd_kafka_topic_new(&rk, name.c_str(), /*conf=*/nullptr));
    if (!rkt)
    {
        auto err_code = rd_kafka_last_error();
        throw Exception(klog::mapErrorCode(err_code), "failed to create topic handler for {}, err_code={}, error_msg={}", name, err_code, rd_kafka_err2str(err_code));
    }
}

int Topic::getPartitionCount() const
{
    const struct rd_kafka_metadata * metadata = nullptr;

    auto err = rd_kafka_metadata(&rdk, 0, rkt.get(), &metadata, /*timeout_ms=*/5000);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
        throw Exception(klog::mapErrorCode(err), "failed to describe topic {}, error_code={}, error_msg={}", name(), err, rd_kafka_err2str(err));

    if (metadata->topic_cnt < 1)
    {
        rd_kafka_metadata_destroy(metadata);
        throw Exception(ErrorCodes::RESOURCE_NOT_FOUND, "Could not find topic {}", name());
    }

    assert(metadata->topic_cnt == 1);

    auto partition_cnt = metadata->topics[0].partition_cnt;
    rd_kafka_metadata_destroy(metadata);
    if (partition_cnt > 0)
        return partition_cnt;
    else
        throw Exception(ErrorCodes::RESOURCE_NOT_FOUND, "Describe topic of {} returned 0 partitions", name());
}

WatermarkOffsets Topic::queryWatermarks(Int32 partition) const
{
    int64_t low, high;
    auto err = rd_kafka_query_watermark_offsets(&rdk, name().data(), partition, &low, &high, /*timeout_ms=*/1000);

    if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
      throw Exception(klog::mapErrorCode(err), "Failed to query watermark offsets topic={} parition={} error={}", name(), partition, rd_kafka_err2str(err));

    return {low, high};
}

}

}
