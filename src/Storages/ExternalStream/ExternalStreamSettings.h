#pragma once

#include <Core/BaseSettings.h>
#include <Core/Settings.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/Context.h>

#include <boost/algorithm/string/predicate.hpp>

namespace DB
{
class ASTStorage;

#define KAFKA_EXTERNAL_STREAM_SETTINGS(M) \
    M(String, brokers, "", "A comma-separated list of brokers, for example Kafka brokers.", 0) \
    M(String, topic, "", "topic, for example Kafka topic name.", 0) \
    M(String, security_protocol, "plaintext", "The protocol to connection to Kafka.", 0) \
    M(String, username, "", "User name.", 0) \
    M(String, password, "", "User password", 0) \
    M(String, sasl_mechanism, "", "SASL mechanism to use for authentication. Supported: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512. Default to PLAIN when SASL is enabled.", 0) \
    M(String, ssl_ca_cert_file, "", "The path of ssl ca cert file", 0) \
    M(String, ssl_ca_pem, "", "CA certificate string (PEM format) for verifying the server's key.", 0) \
    M(Bool, skip_ssl_cert_check, false, "If set to true, the server's certification won't be verified.", 0) \
    M(String, properties, "", "A semi-colon-separated key-value pairs for configuring the kafka client used by the external stream. A key-value pair is separated by a equal sign. Example: 'client.id=my-client-id;group.id=my-group-id'. Note, not all properties are supported, please check the document for supported properties.", 0) \
    M(UInt64, poll_waittime_ms, 500, "How long (in milliseconds) should poll waits.", 0) \
    M(String, sharding_expr, "", "An expression which will be evaluated on each row of data returned by the query to calculate the an integer which will be used to determine the ID of the partition to which the row of data will be sent. If not set, data are sent to any partition randomly.", 0) \
    M(String, message_key, "", "(Deprecated) An expression which will be evaluated on each row of data returned by the query to compute a string which will be used as the message key. This setting is deprecated, please define a `_tp_message_key` column in the external stream instead.", 0) \
    M(Bool, one_message_per_row, false, "If set to true, when send data to the Kafka external stream with row-based data format like `JSONEachRow`, it will produce one message per row.", 0) \
    M(String, region, "", "The AWS region to target.", 0) \
    M(Bool, log_stats, false, "If set to true, print statistics to the logs. Note that, the statistics could contain quite a lot of data. The frequency of the statistics logs is control by the statistics.interval.ms property.", 0) \
    M(Milliseconds, consumer_stall_timeout_ms, 60 * 1000, "Define the amount of time when a consumer is not making any progress, then consider the consumer stalled, and then a new consumer will be created. Adjust the value based on how busy a topic is. Use small values for a busy topic to avoid big latency. Use big values for less busy topics to avoid disruption. Set to 0 to disable the behavior.", 0)

#define LOG_FILE_EXTERNAL_STREAM_SETTINGS(M) \
    M(String, log_files, "", "A comma-separated list of log files", 0) \
    M(String, log_dir, "", "log root directory", 0) \
    M(String, timestamp_regex, "", "Regex to extract log timestamp", 0) \
    M(UInt64, hash_bytes, 1024, "File bytes to hash to decide if the same file", 0) \
    /* those are mapped to format factory settings */ \
    M(String, data_format, "", "The message format, for example JSONEachRow", 0) \
    M(String, row_delimiter, "\n", "The string to be considered as a delimiter in raw message.", 0) \
    M(UInt64, max_row_length, 4096, "Max row length", 0)

#define TIMEPLUS_EXTERNAL_STREAM_SETTINGS(M) \
    M(String, hosts, "", "A remote server address or an expression that generates multiple addresses of remote servers. Format: host or host:port.", 0) \
    M(String, db, "default", "Database name.", 0) \
    M(String, stream, "", "Stream name.", 0) \
    M(String, user, "", "User name. If not specified, `default` is be used.", 0) \
    M(Bool, secure, false, "Use secure connection.", 0)

#define PULSAR_EXTERNAL_STREAM_SETTINGS(M) \
    M(String, service_url, "", "The Pulsar protocol URL", 0) \
    M(Bool, skip_server_cert_check, false, "If set to true, it will accept untrusted TLS certificates from brokers", 0) \
    M(Bool, validate_hostname, false, "Configure whether it allows validating hostname verification when a client connects to a broker over TLS", 0) \
    M(String, ca_cert, "", "The CA certificate (PEM format), which will be used to verify the server's certificate.", 0) \
    M(String, client_cert, "", "The certificate (PEM format) for the client to use mTLS authentication.", 0) \
    M(String, client_key, "", "The private key (PEM format) for the client to use mTLS authentication.", 0) \
    M(String, jwt, "", "The JSON web token for the client to use JWT authentication.", 0) \
    M(UInt64, connections_per_broker, 1, "Sets the max number of connection that this external stream will open to a single broker. By default, the connection pool will use a single connection for all the producers and consumers. Increasing this parameter may improve throughput when using many producers over a high latency connection.", 0) \
    M(UInt64, memory_limit, 0, "Configure a limit on the amount of memory that will be allocated by this external stream. Setting this to 0 will disable the limit. By default this is disabled.", 0) \
    M(UInt64, io_threads, 1, "Set the number of IO threads to be used by the Pulsar client. Default is 1 thread.", 0)

#define ALL_EXTERNAL_STREAM_SETTINGS(M) \
    M(String, type, "", "External stream type", 0) \
    M(String, config_file, "", "External stream configuration file path", 0) \
    KAFKA_EXTERNAL_STREAM_SETTINGS(M) \
    LOG_FILE_EXTERNAL_STREAM_SETTINGS(M) \
    TIMEPLUS_EXTERNAL_STREAM_SETTINGS(M) \
    PULSAR_EXTERNAL_STREAM_SETTINGS(M)

#define LIST_OF_EXTERNAL_STREAM_SETTINGS(M) \
    ALL_EXTERNAL_STREAM_SETTINGS(M) \
    FORMAT_FACTORY_SETTINGS(M)

DECLARE_SETTINGS_TRAITS(KafkaExternalStreamSettingsTraits, KAFKA_EXTERNAL_STREAM_SETTINGS)

struct KafkaExternalStreamSettings : public BaseSettings<KafkaExternalStreamSettingsTraits>
{
    bool usesSASL() const
    {
        return boost::istarts_with(security_protocol.value, "SASL_");
    }

    /// "SASL_SSL" or "SSL"
    bool usesSecureConnection() const
    {
        return boost::iends_with(security_protocol.value, "SSL");
    }
};

DECLARE_SETTINGS_TRAITS(ExternalStreamSettingsTraits, LIST_OF_EXTERNAL_STREAM_SETTINGS)

/** Settings for the ExternalStream engine.
  * Could be loaded from a CREATE EXTERNAL STREAM query (SETTINGS clause).
  */
struct ExternalStreamSettings : public BaseSettings<ExternalStreamSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def, bool throw_on_unknown = true);

    /// Load settings from configure file when 'config_file' is set.
    /// Only the unchanged settings are updated so user may explicitly override
    /// configuration file settings in the query settings clause.
    void loadFromConfigFile();

    KafkaExternalStreamSettings getKafkaSettings() const
    {
        KafkaExternalStreamSettings settings {};
#define SET_CHANGED_SETTINGS(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS) \
        if ((NAME).changed) \
            settings.NAME = (NAME);

        KAFKA_EXTERNAL_STREAM_SETTINGS(SET_CHANGED_SETTINGS)

#undef SET_CHANGED_SETTINGS
        return settings;
    }

    FormatSettings getFormatSettings(const ContextPtr & context) const
    {
        FormatFactorySettings settings {};
        const auto & settings_from_context = context->getSettingsRef();

        /// settings from context have higher priority
#define SET_CHANGED_SETTINGS(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS) \
        if (settings_from_context.NAME.changed) \
            settings.NAME = settings_from_context.NAME; \
        else if ((NAME).changed) \
            settings.NAME = (NAME);

        FORMAT_FACTORY_SETTINGS(SET_CHANGED_SETTINGS)

#undef SET_CHANGED_SETTINGS

        return DB::getFormatSettings(context, settings);
    }
};

using ExternalStreamSettingsPtr = std::unique_ptr<ExternalStreamSettings>;

}
