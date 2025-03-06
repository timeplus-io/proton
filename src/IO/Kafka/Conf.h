#pragma once

#include <Common/SipHash.h>

#include <librdkafka/rdkafka.h>

#include <map>

namespace DB
{

namespace Kafka
{

using RdkConfPtr = std::unique_ptr<rd_kafka_conf_t, decltype(rd_kafka_conf_destroy) *>;

class Conf final
{
public:
    struct Hasher
    {
        size_t operator()(const Conf & conf_) const
        {
            SipHash s;

            if (!conf_.region.empty())
                s.update(conf_.region);

            for (const auto & [property, value] : conf_.configs)
            {
                s.update(property);
                s.update(value);
            }
            return s.get64();
        }
    };

    struct EqualTo
    {
        constexpr bool operator()(const Conf & lhs, const Conf & rhs) const
        {
            if (lhs.configs.size() != rhs.configs.size())
                return false;

            Hasher hasher;
            return hasher(lhs) == hasher(rhs);
        }
    };

    friend Hasher;
    friend EqualTo;

    Conf() : conf{rd_kafka_conf_new(), rd_kafka_conf_destroy} { }

    void setBrokers(const std::string &);
    std::string getBrokers() const;

    void setRegion(const std::string &);
    std::string getRegion() const;

    void setSaslMechanism(const std::string &);
    std::string getSaslMechanism() const;

    void set(std::string property, std::string value);
    std::optional<const std::string> get(const std::string & property) const;

    void setLogCallback(void (*log_cb)(const rd_kafka_t * rk, int level, const char * fac, const char * buf));
    void setErrorCallback(void (*error_cb)(rd_kafka_t * rk, int err, const char * reason, void * opaque));
    void setThrottleCallback(
        void (*throttle_cb)(rd_kafka_t * rk, const char * broker_name, int32_t broker_id, int throttle_time_ms, void * opaque));
    void setDrMsgCallback(void (*dr_msg_cb)(rd_kafka_t * rk, const rd_kafka_message_t * rkmessage, void * opaque));
    void setStatsCallback(int (*stats_cb)(rd_kafka_t * rk, char * json, size_t json_len, void * opaque));
    void setOauthbearerTokenRefreshCallback(
        void (*oauthbearer_token_refresh_cb)(rd_kafka_t * rk, const char * oauthbearer_config, void * opaque));

    RdkConfPtr getConf() const;

    std::string toString() const;

private:
    RdkConfPtr conf;
    std::string region;
    std::string sasl_mechanism;
    /// Order is critical.
    std::map<std::string, std::string> configs;
    char errstr[512]{'\0'};
};

}

}
