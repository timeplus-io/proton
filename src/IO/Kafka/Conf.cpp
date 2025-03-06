#include <IO/Kafka/Conf.h>

#include <Common/Exception.h>

#include <sstream>

namespace DB
{

namespace ErrorCodes
{
extern const int INVALID_CONFIG_PARAMETER;
}

namespace Kafka
{

void Conf::set(std::string property, std::string value)
{
    auto err = rd_kafka_conf_set(conf.get(), property.c_str(), value.c_str(), errstr, sizeof(errstr));
    if (err != RD_KAFKA_CONF_OK)
    {
        throw Exception(
            ErrorCodes::INVALID_CONFIG_PARAMETER,
            "Failed to set kafka property `{}` with value `{}` error_code={} error_msg={}",
            property,
            value,
            err,
            errstr);
    }

    configs[property] = value;
}

void Conf::setBrokers(const std::string & brokers)
{
    set("bootstrap.servers", brokers);
}

std::string Conf::getBrokers() const
{
    return get("bootstrap.servers").value_or("");
}

void Conf::setRegion(const std::string & region_)
{
    region = region_;
}

std::string Conf::getRegion() const
{
    return region;
}

void Conf::setSaslMechanism(const std::string & sasl_mechanism_)
{
    sasl_mechanism = sasl_mechanism_;
}

std::string Conf::getSaslMechanism() const
{
    return sasl_mechanism;
}

void Conf::setLogCallback(void (*log_cb)(const rd_kafka_t * rk, int level, const char * fac, const char * buf))
{
    rd_kafka_conf_set_log_cb(conf.get(), log_cb);
}

void Conf::setErrorCallback(void (*error_cb)(rd_kafka_t * rk, int err, const char * reason, void * opaque))
{
    rd_kafka_conf_set_error_cb(conf.get(), error_cb);
}

void Conf::setThrottleCallback(
    void (*throttle_cb)(rd_kafka_t * rk, const char * broker_name, int32_t broker_id, int throttle_time_ms, void * opaque))
{
    rd_kafka_conf_set_throttle_cb(conf.get(), throttle_cb);
}

void Conf::setDrMsgCallback(void (*dr_msg_cb)(rd_kafka_t * rk, const rd_kafka_message_t * rkmessage, void * opaque))
{
    rd_kafka_conf_set_dr_msg_cb(conf.get(), dr_msg_cb);
}

void Conf::setStatsCallback(int (*stats_cb)(rd_kafka_t * rk, char * json, size_t json_len, void * opaque))
{
    rd_kafka_conf_set_stats_cb(conf.get(), stats_cb);
}

void Conf::setOauthbearerTokenRefreshCallback(
    void (*oauthbearer_token_refresh_cb)(rd_kafka_t * rk, const char * oauthbearer_config, void * opaque))
{
    rd_kafka_conf_set_oauthbearer_token_refresh_cb(conf.get(), oauthbearer_token_refresh_cb);
}

std::optional<const std::string> Conf::get(const std::string & property) const
{
    /// Firstly, get the value size of the property.
    size_t size{0};
    auto err = rd_kafka_conf_get(conf.get(), property.c_str(), nullptr, &size);
    if (err != RD_KAFKA_CONF_OK)
        return std::nullopt;

    /// With the correct size, we can get the value properly.
    char value[size];
    rd_kafka_conf_get(conf.get(), property.c_str(), value, &size);
    return std::string{value, size};
}

RdkConfPtr Conf::getConf() const
{
    /// When create a consumer or producer, it will take the ownership of the config object,
    /// thus we need to dup before return the conf object.
    return {rd_kafka_conf_dup(conf.get()), rd_kafka_conf_destroy};
}

std::string Conf::toString() const
{
    std::stringstream out;
    out << "Kafka::Conf{";
    bool first{true};
    for (const auto & [k, v] : configs)
    {
        auto secret = k.contains("password") || k.contains(".pem") || k.contains("secret") || k == "ssl_key" || k == "ssl_certificate";
        out << (first ? "" : "', ") << k << "='" << (secret ? "***" : v);
        first = false;
    }
    out << "'}";

    return out.str();
}

}

}
