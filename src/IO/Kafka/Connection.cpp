#include <IO/Kafka/Connection.h>

#include <IO/Kafka/Handle.h>
#include <IO/Kafka/mapErrorCode.h>
#include <Common/logger_useful.h>

#include <sstream>

namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_REFRESH_KAFKA_OAUTHBEARER_TOKEN;
extern const int INVALID_SETTING_VALUE;
extern const int LIMIT_EXCEEDED;
}

namespace Kafka
{

namespace
{

#if USE_AWS_MSK_IAM
/// "AWS_MSK_IAM" is the named used by the AWS libraries
const String SASL_MECHANISM_AWS_MSK_IAM = "AWS_MSK_IAM";
#endif

void normailizeConf(Conf & conf [[maybe_unused]])
{
    auto sasl_mechanism = conf.getSaslMechanism();
#if USE_AWS_MSK_IAM
    if (sasl_mechanism == SASL_MECHANISM_AWS_MSK_IAM)
    {
        conf.set("sasl.mechanism", "OAUTHBEARER");
        conf.setOauthbearerTokenRefreshCallback(&Connection::onOauthBearerTokenRefresh);
    }
    else
#endif
    {
        if (!sasl_mechanism.empty())
            conf.set("sasl.mechanism", sasl_mechanism);
    }
}

std::vector<int64_t> getOffsetsForTimestampsImpl(
    struct rd_kafka_s * rd_handle,
    const std::string & topic,
    const std::vector<PartitionTimestamp> & partition_timestamps,
    int32_t timeout_ms)
{
    assert(rd_handle);

    using RdKafkaTopicPartitionListPtr
        = std::unique_ptr<rd_kafka_topic_partition_list_t, decltype(rd_kafka_topic_partition_list_destroy) *>;

    RdKafkaTopicPartitionListPtr offsets{
        rd_kafka_topic_partition_list_new(static_cast<int>(partition_timestamps.size())), rd_kafka_topic_partition_list_destroy};

    for (size_t i = 0; const auto & partition_timestamp : partition_timestamps)
    {
        memset(&offsets->elems[i], 0, sizeof(offsets->elems[i]));

        /// We will need duplicate the topic string since destroy function will free it
        offsets->elems[i].topic = strdup(topic.c_str());
        offsets->elems[i].partition = partition_timestamp.partition;
        offsets->elems[i].offset = partition_timestamp.timestamp;
        ++i;
    }

    offsets->cnt = static_cast<int>(partition_timestamps.size());

    auto err = rd_kafka_offsets_for_times(rd_handle, offsets.get(), timeout_ms);
    if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
        throw DB::Exception(mapErrorCode(err), "Failed to fetch offsets for timestamps");

    std::vector<int64_t> results;
    results.reserve(partition_timestamps.size());

    for (size_t i = 0; i < partition_timestamps.size(); ++i)
    {
        if (offsets->elems[i].err != RD_KAFKA_RESP_ERR_NO_ERROR)
            throw DB::Exception(mapErrorCode(err), "Failed to fetch offsets for timestamps on partition {}", offsets->elems[i].partition);

        results.push_back(offsets->elems[i].offset);
    }

    return results;
}

#if USE_AWS_MSK_IAM
String extractRegion(const String & brokers)
{
    std::string_view url{brokers};
    /// If there are more than one brokers, use the first one.
    if (auto pos = url.find_first_of(','); pos != String::npos)
        url = url.substr(pos);

    /// Remove port
    if (auto pos = url.find_last_of(':'); pos != String::npos)
        url.remove_suffix(url.size() - pos);

    /// Remove suffix
    static const String url_suffix = ".amazonaws.com";
    if (!url.ends_with(url_suffix))
        return "";
    url.remove_suffix(url_suffix.size());

    if (auto pos = url.find_last_of('.'); pos != String::npos)
        return String(url.substr(pos + 1, url.size()));

    return "";
}

std::unique_ptr<AwsMskIamSinger> createAwsAuthSigner(const Conf & conf)
{
    auto region = conf.getRegion();
    if (region.empty())
        region = extractRegion(conf.getBrokers());

    if (region.empty())
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Cannot extracted region from brokers, please use the `region` setting");

    return std::make_unique<AwsMskIamSinger>(region);
}
#endif

}

std::string Connection::Limits::toString() const
{
    std::stringstream out;
    out << "{max_alive_consumer_handles='" << max_alive_consumer_handles << "'," << "consumer_handle_share_soft_cap='"
        << consumer_handle_share_soft_cap << "'," << "max_alive_producer_handles='" << max_alive_producer_handles << "',"
        << "producer_handle_share_soft_cap='" << producer_handle_share_soft_cap << "'}";

    return out.str();
}

#if USE_AWS_MSK_IAM
void Connection::onOauthBearerTokenRefresh(rd_kafka_t * rk, const char * /*oauthbearer_config*/, void * opaque)
{
    auto * conn = static_cast<Connection *>(opaque);
    conn->setOauthBearerToken(rk);
}
#endif

Connection::Connection(const Conf & conf_, const Limits & limits_)
    : conf(conf_.getConf()), limits(limits_), logger(&Poco::Logger::get("KafkaConnection"))
{
    rd_kafka_conf_set_opaque(conf.get(), this);
#if USE_AWS_MSK_IAM
    if (conf_.getSaslMechanism() == SASL_MECHANISM_AWS_MSK_IAM)
        aws_iam_signer = createAwsAuthSigner(conf_);
#endif
}

ConsumerPtr Connection::getConsumer(const std::string & topic)
{
    std::lock_guard lock{consumers_mutex};

    /// getConsumerHandle and markTopicConsumed in consumeFrom shall be
    /// in one transaction (under one mutex), otherwise there is race condition
    /// https://github.com/timeplus-io/proton-enterprise/issues/7606
    return getConsumerHandleWithLockHeld(topic)->consumeFrom(topic);
}

void Connection::updateConsumer(ConsumerPtr consumer)
{
    auto topic = consumer->topicName();

    std::lock_guard lock{consumers_mutex};

    /// getConsumerHandle and markTopicConsumed in consumeFrom shall be
    /// in one transaction (under one mutex), otherwise there is race condition
    /// https://github.com/timeplus-io/proton-enterprise/issues/7606
    auto new_handle = getConsumerHandleWithLockHeld(topic);
    consumer->updateFrom(Consumer{shared_from_this(), new_handle, topic});
    new_handle->markTopicConsumed(topic, consumer);
}

ConsumerHandlePtr Connection::getConsumerHandleWithLockHeld(const std::string & topic)
{
    std::optional<ConsumerHandlePtr> least_busy_handle;
    auto expired_ptr_idx = consumer_handles.size();

    /// First, try to find an available consumer handle.
    /// If there are multiple handles available, find the least busy one.
    /// Meanwhile, remember which slot in the consumers pool is available (i.e. the weak pointer expired).
    for (size_t i{0}; auto & ref : consumer_handles)
    {
        if (auto handle = ref.lock())
        {
            if (handle->isConsuming(topic))
                LOG_DEBUG(logger, "Skipped consumer handle {} as it is already consuming topic {}", handle->getName(), topic);
            else if (!least_busy_handle || least_busy_handle.value()->useCount() > handle->useCount())
                least_busy_handle = handle;
        }
        else if (expired_ptr_idx >= consumer_handles.size())
        {
            LOG_INFO(logger, "Found an expired consumer handle at index {}", i);
            expired_ptr_idx = i;
        }

        ++i;
    }

    /// Case 1 - an existing handle is available, a few things to consider:
    if (least_busy_handle)
    {
        /// Case 1.1 - if the use count is still below the throttle, simply reuse it.
        if (least_busy_handle.value()->useCount() < limits.consumer_handle_share_soft_cap)
        {
            LOG_INFO(
                logger,
                "Reuse consumer handle {} to consume topic {} use_count={}",
                least_busy_handle.value()->getName(),
                topic,
                least_busy_handle.value()->useCount());
            return least_busy_handle.value();
        }

        /// Case 1.2 - the handle already reached the soft cap.
        /// Case 1.2.a - if the max alive handle limit is reached, we still should reuse the handle.
        if (expired_ptr_idx >= consumer_handles.size() && consumer_handles.size() >= limits.max_alive_consumer_handles)
        {
            LOG_WARNING(
                logger,
                "Consumer handle {} has reached share soft cap {}, reuse it regardless due to max_alive_consumer_handles limit is reached",
                least_busy_handle.value()->getName(),
                limits.consumer_handle_share_soft_cap);
            return least_busy_handle.value();
        }

        /// Case 1.2.b - there is still available slot(s), create a new handle (fallthrough to case 2 below)
        LOG_INFO(logger, "All existing consumer handles have reached share soft cap {}", limits.consumer_handle_share_soft_cap);
    }

    /// Case 2 - it is still allowed to create more handles
    if (expired_ptr_idx < consumer_handles.size() || consumer_handles.size() < limits.max_alive_consumer_handles)
    {
        auto handle = std::make_shared<ConsumerHandle>(shared_from_this(), rd_kafka_conf_dup(conf.get()));
        setOauthBearerToken(handle->get());

        std::weak_ptr<ConsumerHandle> ref = handle;

        if (expired_ptr_idx < consumer_handles.size())
            consumer_handles[expired_ptr_idx].swap(ref);
        else
            consumer_handles.push_back(std::move(ref));

        LOG_INFO(
            logger,
            "New consumer handle {} created expired_ptr_idx={} consumer_handles.size={}",
            handle->getName(),
            expired_ptr_idx,
            consumer_handles.size());

        return handle;
    }

    /// Lastly, the limits are all exceeded.
    assert(!least_busy_handle); /// this case has been handled in case 1.2.a

    throw Exception(
        ErrorCodes::LIMIT_EXCEEDED,
        "The number of alive consumer handles has exceeded the limit {}, consumer_handle_share_soft_cap={}",
        limits.max_alive_consumer_handles,
        limits.consumer_handle_share_soft_cap);
}

ProducerPtr Connection::getProducer(const std::string & topic)
{
    std::lock_guard lock{producers_mutex};

    std::optional<ProducerHandlePtr> less_busy_in_use_ptr;
    auto expired_ptr_idx = producer_handles.size();

    /// Try to find a ProducerHandle that can be used to produce data to the topic.
    for (size_t i = 0; auto & ref : producer_handles)
    {
        if (auto handle = ref.lock())
        {
            if (static_cast<uint64_t>(handle->useCount()) >= limits.producer_handle_share_soft_cap)
            {
                LOG_DEBUG(
                    logger,
                    "Skipped producer handle {} due to share soft cap {}",
                    handle->getName(),
                    limits.producer_handle_share_soft_cap);

                if (!less_busy_in_use_ptr || less_busy_in_use_ptr->use_count() > handle.use_count())
                    less_busy_in_use_ptr = handle;
            }
            else
            {
                LOG_INFO(logger, "Reuse producer handle {} use_count='{}'", handle->getName(), handle->useCount());
                return handle->produceTo(topic);
            }
        }
        else
        {
            if (expired_ptr_idx >= producer_handles.size())
            {
                LOG_INFO(logger, "Found expired producer handle at index {}", i);
                expired_ptr_idx = i;
            }
        }

        ++i;
    }

    if (expired_ptr_idx >= producer_handles.size() && producer_handles.size() >= limits.max_alive_producer_handles)
    {
        LOG_WARNING(
            logger,
            "The number of alive producer handles has exceeded the limit {}, and all in-use handle reached the share soft cap {}. Will "
            "reuse handle {} use_count={}",
            limits.max_alive_producer_handles,
            limits.producer_handle_share_soft_cap,
            less_busy_in_use_ptr.value()->getName(),
            less_busy_in_use_ptr.value()->useCount());

        assert(less_busy_in_use_ptr);

        return less_busy_in_use_ptr.value()->produceTo(topic);
    }

    /// No reusable handle, create a new one.
    auto handle = std::make_shared<ProducerHandle>(rd_kafka_conf_dup(conf.get()));
    setOauthBearerToken(handle->get());

    std::weak_ptr<ProducerHandle> ref = handle;

    /// If there is an expired handle weak pointer, then replace it to avoid infinite growth.
    if (expired_ptr_idx < producer_handles.size())
        producer_handles[expired_ptr_idx].swap(ref);
    else
        producer_handles.push_back(std::move(ref));

    LOG_INFO(logger, "New producer handle created for topic {} producer_handles.size='{}'", topic, producer_handles.size());
    return handle->produceTo(topic);
}

int32_t Connection::getPartitionCount(const std::string & topic)
{
    return getProducer(topic)->getPartitionCount();
}

std::vector<Int64> Connection::getOffsetsForTimestamps(
    const std::string & topic, const std::vector<PartitionTimestamp> & partition_timestamps, int32_t timeout_ms)
{
    std::vector<Int64> results;

    withAnyHandle([&](auto * rkt) { results = getOffsetsForTimestampsImpl(rkt, topic, partition_timestamps, timeout_ms); });

    return results;
}

WatermarkOffsets Connection::getWatermarkOffsets(const std::string & topic, int32_t partition)
{
    return getProducer(topic)->getWatermarkOffsets(partition);
}

/// Find a handle to call the function \param func .
/// If no available handle, create a new one.
void Connection::withAnyHandle(std::function<void(rd_kafka_t *)> func)
{
    /// Since we just need a handle for some temporary jobs, we don't care about the limits.
    for (const auto & ref : producer_handles)
        if (auto handle = ref.lock())
            return func(handle->get());

    for (const auto & ref : consumer_handles)
        if (auto handle = ref.lock())
            return func(handle->get());

    auto handle = std::make_shared<ConsumerHandle>(shared_from_this(), rd_kafka_conf_dup(conf.get()));
    setOauthBearerToken(handle->get());
    func(handle->get());
}

void Connection::setOauthBearerToken(rd_kafka_t * rk [[maybe_unused]])
{
#if USE_AWS_MSK_IAM
    if (!aws_iam_signer)
        return;

    rd_kafka_resp_err_t err{RD_KAFKA_RESP_ERR_NO_ERROR};
    char errmsg[512]{'\0'};

    std::lock_guard<std::mutex> lock{oauth_token_mutex};

    auto now_epoch_ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    if (!oauth_token.token.empty() && oauth_token.expiration_ms > now_epoch_ms)
    {
        err = rd_kafka_oauthbearer_set_token(rk, oauth_token.token.c_str(), oauth_token.expiration_ms, "", nullptr, 0, errmsg, 512);
        if (err == RD_KAFKA_RESP_ERR_NO_ERROR)
        {
            LOG_INFO(logger, "Reused existing token");
            return;
        }

        /// Just in case, if the cached token has already expired, just carry on. Otherwise, throw exception.
        if (!String(errmsg).contains("Must supply an unexpired token"))
            throw Exception(
                ErrorCodes::CANNOT_REFRESH_KAFKA_OAUTHBEARER_TOKEN,
                "Failed to set kafka oauthbearer token, error_code={}, error_msg={}",
                rd_kafka_err2str(err),
                errmsg);
    }

    oauth_token = aws_iam_signer->generateToken();

    err = rd_kafka_oauthbearer_set_token(rk, oauth_token.token.c_str(), oauth_token.expiration_ms, "", nullptr, 0, errmsg, 512);

    if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
        throw Exception(
            ErrorCodes::CANNOT_REFRESH_KAFKA_OAUTHBEARER_TOKEN,
            "Failed to set kafka oauthbearer token, error_code={}, error_msg={}",
            rd_kafka_err2str(err),
            errmsg);

    LOG_INFO(logger, "New token generated");
#else
    /// no-op
#endif
}

ConnectionFactory & ConnectionFactory::instance()
{
    static ConnectionFactory instance;
    return instance;
}

ConnectionFactory::ConnectionFactory() : logger(&Poco::Logger::get("KafkaConnectionFactory"))
{
}

ConnectionPtr ConnectionFactory::getConnection(Conf && conf)
{
    std::lock_guard lock{connections_mutex};

    normailizeConf(conf);

    if (connections.contains(conf))
        if (auto conn = connections.at(conf).lock())
        {
            LOG_INFO(logger, "Reuse existing connection for {}", conf.toString());
            return conn;
        }

    LOG_INFO(logger, "Create new connection for {}", conf.toString());
    auto conn = std::make_shared<Connection>(conf, limits);
    connections[std::move(conf)] = conn;
    return conn;
}

void ConnectionFactory::setConnectionLimits(Connection::Limits limits_)
{
    LOG_INFO(logger, "Set connection limits: {}", limits_.toString());
    limits = std::move(limits_);
}

}

}
