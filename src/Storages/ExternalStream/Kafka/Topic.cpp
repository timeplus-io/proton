#include <KafkaLog/KafkaWALCommon.h>
#include <Storages/ExternalStream/Kafka/Topic.h>

namespace DB
{

namespace ErrorCodes
{
extern const int INVALID_SETTING_VALUE;
}

namespace RdKafka
{

Topic::Topic(rd_kafka_t * rk, const std::string & name, void * opaque)
{
    rd_kafka_topic_conf_t * topic_conf = nullptr;

    if (opaque)
    {
        const auto * conf  = rd_kafka_conf(rk);
        auto * topic_conf_ = rd_kafka_conf_get_default_topic_conf(const_cast<rd_kafka_conf_t *>(conf));
        topic_conf = rd_kafka_topic_conf_dup(topic_conf_);

        rd_kafka_topic_conf_set_opaque(topic_conf, opaque);
    }
    /// rd_kafka_topic_new takes ownership of topic_conf
    rkt.reset(rd_kafka_topic_new(rk, name.c_str(), topic_conf));
    if (!rkt)
    {
        auto err_code = rd_kafka_last_error();
        throw Exception(klog::mapErrorCode(err_code), "failed to create topic handler for {}, err_code={}, error_msg={}", name, err_code, rd_kafka_err2str(err_code));
    }
}

}

}
