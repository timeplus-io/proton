#pragma once

#include <IO/Kafka/AwsMskIamSigner.h>
#include <IO/Kafka/Client.h>
#include <IO/Kafka/Common.h>
#include <IO/Kafka/Conf.h>

#include <mutex>

namespace DB
{

namespace Kafka
{

class Connection final: public std::enable_shared_from_this<Connection>
{
    friend Consumer::Version Consumer::recreate(Consumer::Version);

public:
#if USE_AWS_MSK_IAM
    static void onOauthBearerTokenRefresh(rd_kafka_t * rk, const char * oauthbearer_config, void * opaque);
#endif

    struct Limits
    {
        uint64_t max_alive_consumer_handles{100'000};
        uint64_t consumer_handle_share_soft_cap{10};
        uint64_t max_alive_producer_handles{100'000};
        uint64_t producer_handle_share_soft_cap{10};

        std::string toString() const;
    };

    Connection(const Conf &, const Limits &);

    /// \return a Consumer that consumes messages in the topic.
    /// A consumer contains a consumer handle, and one consumer handle can be shared amoung different topics.
    /// However, it cannot consume the same topic multiple times at the same time,
    /// This function will firstly try to find an existing consumer handle that can be used to consume messages in the topic, if none is available, it creates a new one.
    ConsumerPtr getConsumer(const std::string & topic);

    /// \return a Producer that produces messages to the topic.
    /// Since a producer can be used to produce messages to any number of topics at the same time, this function will always return an existing producer if there is one.
    ProducerPtr getProducer(const std::string & topic);

    int32_t getPartitionCount(const std::string & topic);

    std::vector<int64_t> getOffsetsForTimestamps(
        const std::string & topic, const std::vector<PartitionTimestamp> & partition_timestamps, int32_t timeout_ms = 5000);

    WatermarkOffsets getWatermarkOffsets(const std::string & topic, int32_t partition);

    /// Recreate new internal handles to update existing consumer
    void updateConsumer(ConsumerPtr consumer);

private:
    ConsumerHandlePtr getConsumerHandleWithLockHeld(const std::string & topic);

    void withAnyHandle(std::function<void(rd_kafka_t *)>);

    void setOauthBearerToken(rd_kafka_t *);

    RdkConfPtr conf;

    Limits limits;

    std::mutex consumers_mutex;
    std::vector<std::weak_ptr<ConsumerHandle>> consumer_handles;

    std::mutex producers_mutex;
    std::vector<std::weak_ptr<ProducerHandle>> producer_handles;

#if USE_AWS_MSK_IAM
    std::unique_ptr<const AwsMskIamSinger> aws_iam_signer;
    std::mutex oauth_token_mutex;
    AwsMskIamSinger::Token oauth_token;
#endif

    Poco::Logger * logger;
};

using ConnectionPtr = std::shared_ptr<Connection>;

class ConnectionFactory final
{
public:
    static ConnectionFactory & instance();

    /// \return a connection created with conf.
    /// If there is an existing connection that was created by the same conf,
    /// i.e. all configs in that conf are the same, then just return the existing connection.
    ConnectionPtr getConnection(Conf && conf);

    void setConnectionLimits(Connection::Limits limits_);

private:
    ConnectionFactory();

    Connection::Limits limits;

    std::mutex connections_mutex;
    /// Connections are actually owned by external streams. Because if all the external streams that use the connection are deleted,
    /// then the connection should be deleted as well. Thus, here it only holds a weak pointer to the connection.
    /// So, when a new external stream is created, if there is a matched connection still alive, it can reuse the connection,
    /// otherwise, a new connection will be created.
    std::unordered_map<Conf, std::weak_ptr<Connection>, Conf::Hasher, Conf::EqualTo> connections;

    Poco::Logger * logger;
};

}

}
