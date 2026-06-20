#pragma once

#include <Core/BaseSettings.h>
#include <Core/Settings.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/Context.h>

#include <boost/algorithm/string/predicate.hpp>

namespace DB
{
class ASTStorage;

#define KAFKA_EXTERNAL_STREAM_SETTINGS(M, ALIAS) \
    M(String, brokers, "", "A comma-separated list of brokers, for example Kafka brokers.", 0) \
    M(String, topic, "", "topic, for example Kafka topic name.", 0) \
    M(String, security_protocol, "plaintext", "The protocol to connection to Kafka.", 0) \
    M(String, username, "", "User name.", 0) \
    M(String, password, "", "User password", 0) \
    M(String, sasl_mechanism, "", "SASL mechanism to use for authentication. Supported: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512. Default to PLAIN when SASL is enabled.", 0) \
    M(String, ssl_ca_cert_file, "", "The path of ssl ca cert file", 0) \
    M(String, ssl_ca_pem, "", "CA certificate string (PEM format) for verifying the server's key.", 0) \
    M(Bool, skip_ssl_cert_check, false, "If set to true, the server's certification won't be verified.", 0) \
    M(String, schema_subject_name, "", "Avro / Protobuf schema subject name in registry. Used to fetch schema in the registry when writing", 0) \
    M(String, subject_name_strategy, "", "Avro / Protobuf subject name strategy in registry. Default is TopicNameStrategy if not configured. RecordNameStrategy, TopicRecordNameStrategy can be used to mix different type of records in the same topic", 0) \
    M(String, properties, "", "A semi-colon-separated key-value pairs for configuring the kafka client used by the external stream. A key-value pair is separated by a equal sign. Example: 'client.id=my-client-id;group.id=my-group-id'. Note, not all properties are supported, please check the document for supported properties.", 0) \
    M(UInt64, poll_waittime_ms, 500, "How long (in milliseconds) should poll waits.", 0) \
    M(String, sharding_expr, "", "An expression which will be evaluated on each row of data returned by the query to calculate the an integer which will be used to determine the ID of the partition to which the row of data will be sent. If not set, data are sent to any partition randomly.", 0) \
    M(String, message_key, "", "(Deprecated) An expression which will be evaluated on each row of data returned by the query to compute a string which will be used as the message key. This setting is deprecated, please define a `_tp_message_key` column in the external stream instead.", 0) \
    M(Bool, one_message_per_row, false, "If set to true, when send data to the Kafka external stream with row-based data format like `JSONEachRow`, it will produce one message per row.", 0) \
    M(String, region, "", "The AWS region to target.", 0) \
    M(String, access_key_id, "", "The access key ID.", 0) \
    M(String, secret_access_key, "", "The secret access key.", 0) \
    M(String, session_token, "", "The session token for authentication.", 0) \
    M(Bool, use_environment_credentials, false, "Use credentials from environment, where it's applicable", 0) \
    M(Bool, log_stats, false, "If set to true, print statistics to the logs. Note that, the statistics could contain quite a lot of data. The frequency of the statistics logs is control by the statistics.interval.ms property.", 0) \
    M(Milliseconds, consumer_stall_timeout_ms, 60 * 1000, "Define the amount of time when a consumer is not making any progress, then consider the consumer stalled, and then a new consumer will be created. Adjust the value based on how busy a topic is. Use small values for a busy topic to avoid big latency. Use big values for less busy topics to avoid disruption. Set to 0 to disable the behavior.", 0) \
    M(Milliseconds, connection_timeout_ms, 10 * 1000, "Timeout in milliseconds for establishing a connection to a broker.", 0)

#define LOG_FILE_EXTERNAL_STREAM_SETTINGS(M, ALIAS) \
    M(String, log_files, "", "A comma-separated list of log files", 0) \
    M(String, log_dir, "", "log root directory", 0) \
    M(String, timestamp_regex, "", "Regex to extract log timestamp", 0) \
    M(UInt64, hash_bytes, 1024, "File bytes to hash to decide if the same file", 0) \
    /* those are mapped to format factory settings */ \
    M(String, data_format, "", "The message format, for example JSONEachRow", 0) \
    M(String, row_delimiter, "\n", "The string to be considered as a delimiter in raw message.", 0) \
    M(UInt64, max_row_length, 4096, "Max row length", 0)

#define TIMEPLUS_EXTERNAL_STREAM_SETTINGS(M, ALIAS) \
    M(String, hosts, "", "A remote server address or an expression that generates multiple addresses of remote servers. Format: host or host:port.", 0) \
    M(String, db, "default", "Database name.", 0) \
    M(String, stream, "", "Stream name.", 0) \
    M(String, user, "", "User name. If not specified, `default` is be used.", 0) \
    M(Bool, secure, false, "Use secure connection.", 0)

#define PULSAR_EXTERNAL_STREAM_SETTINGS(M, ALIAS) \
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

#define ICEBERG_EXTERNAL_STREAM_SETTINGS(M, ALIAS) \
    M(String, iceberg_storage_endpoint, "", "Endpoint for data storage.", 0) \
    M(String, http_client, "", "HTTP Client type to make requests. Supported values: gcp_oauth", 0) \
    M(String, service_account, "", "GCP service account email or 'default' to use the instance's service account for authentication", 0) \
    M(String, metadata_service, "", "GCP metadata service endpoint (default: metadata.google.internal)", 0) \
    M(String, \
      request_token_path, \
      "", \
      "Path to the GCP service account token on the metadata service (default: computeMetadata/v1/instance/service-accounts)", \
      0)

/// HTTP also uses:
///    M(String, ssl_ca_cert_file, "", "The path of ssl ca cert file", 0)
///    M(String, ssl_ca_pem, "", "CA certificate string (PEM format) for verifying the server's key.", 0)
///    M(Bool, skip_ssl_cert_check, false, "If set to true, the server's certification won't be verified.", 0)
///    M(String, client_key, "", "The private key (PEM format) for the client to use mTLS authentication.", 0)
#define HTTP_EXTERNAL_STREAM_SETTINGS(M, ALIAS) \
    M(String, url, "", "HTTP endpoint for both read and write.", 0) \
    M(String, read_url, "", "HTTP endpoint for read.", 0) \
    M(String, write_url, "", "HTTP endpoint for write.", 0) \
    M(String, read_method, "GET", "HTTP method to be used to fetch data from the URL", 0) \
    M(String, write_method, "POST", "HTTP verb to be used to send data to the URL", 0) \
    M(String, compression_method, "none", "Indicates that whether the HTTP body should be compressed. If the compression is enabled, the HTTP packets sent by the URL engine contain 'Content-Encoding' header to indicate which compression method is used.", 0) \
    M(Bool, use_chunked_encoding, true, "Indicates that whether to use the Chunked Transfer Encoding. If not, INSERT will create new HTTP request for each batch of data", 0)

#define NATS_JETSTREAM_EXTERNAL_STREAM_SETTINGS(M, ALIAS) \
    M(String, subject, "", "NATS JetStream subject filter for subscribing. Supports wildcards: orders.> or orders.*", 0) \
    M(String, stream_name, "", "JetStream stream name. The stream must already exist on the NATS server.", 0) \
    M(String, consumer_name, "", "Durable consumer name. If empty, auto-generated from the query ID.", 0) \
    M(Bool, durable, true, "Create a durable consumer. Durable consumers persist across restarts.", 0) \
    M(String, ack_policy, "explicit", "Ack policy: none (fire-and-forget), all (ack all up to latest), explicit (per-message ack for at-least-once)", 0) \
    M(String, deliver_policy, "all", "Deliver policy: all, last, new, by_start_sequence, by_start_time", 0) \
    M(UInt64, max_ack_pending, 1024, "Maximum number of outstanding acks before the server pauses delivery", 0) \
    M(UInt64, batch_size, 256, "Number of messages to fetch per pull request", 0) \
    M(UInt64, fetch_timeout_ms, 5000, "Timeout in milliseconds for each pull fetch request", 0) \
    M(Milliseconds, nats_stall_timeout_ms, 60 * 1000, "Time in milliseconds without progress before the consumer subscription is considered stalled and recreated. Set to 0 to disable.", 0) \
    M(String, nats_username, "", "NATS username for user/password authentication", 0) \
    M(String, nats_password, "", "NATS password for user/password authentication", 0) \
    M(String, nats_token, "", "NATS auth token for token-based authentication", 0) \
    M(String, nats_nkey_seed, "", "NATS NKey seed for NKey-based authentication", 0) \
    M(String, nats_creds_file, "", "Path to NATS credentials file for decentralized JWT authentication", 0) \
    M(Bool, nats_tls, false, "Enable TLS. Auto-detected if URL starts with tls:// or nats+tls://", 0) \
    M(String, nats_ca_file, "", "Path to TLS CA certificate file for server verification", 0) \
    M(String, nats_cert_file, "", "Path to TLS client certificate for mTLS authentication", 0) \
    M(String, nats_key_file, "", "Path to TLS client private key for mTLS authentication", 0) \
    M(UInt64, reconnect_wait_ms, 2000, "Wait time in milliseconds between reconnect attempts", 0) \
    M(Int64, max_reconnects, 60, "Maximum number of reconnect attempts. Set to -1 for unlimited.", 0) \
    M(UInt64, start_sequence, 0, "Start sequence for deliver_policy=by_start_sequence", 0) \
    M(String, start_time, "", "Start time (Unix timestamp in nanoseconds) for deliver_policy=by_start_time", 0)

#define ALL_EXTERNAL_STREAM_SETTINGS(M, ALIAS) \
    M(String, type, "", "External stream type", 0) \
    M(String, config_file, "", "External stream configuration file path", 0) \
    M(String, named_collection, "", "External stream named collection configuration", 0) \
    M(Bool, local, false, "In a distributed env, local=true means it is a stream which is local to that node only and is not visible to other nodes in the cluster", 0) \
    M(String, read_function_name, "", "Python external stream entrypoint name, defaults to stream name", 0) \
    M(String, write_function_name, "", "Python external stream sink function name, defaults to read_function_name", 0) \
    M(String, init_function_name, "", "Python external stream initialization hook name, called once before read/write processing", 0) \
    M(String, init_function_parameters, "", "Optional Python external stream initialization parameters passed as a string to init()", 0) \
    M(String, deinit_function_name, "", "Python external stream cleanup hook name, called once after read/write processing", 0) \
    M(String, flush_function_name, "", "Python sink flush hook, called on checkpoints and before cleanup", 0) \
    M(String, mode, "", "Python external stream execution mode: 'auto', 'streaming', or 'batch' (empty defaults to auto)", 0) \
    KAFKA_EXTERNAL_STREAM_SETTINGS(M, ALIAS) \
    LOG_FILE_EXTERNAL_STREAM_SETTINGS(M, ALIAS) \
    TIMEPLUS_EXTERNAL_STREAM_SETTINGS(M, ALIAS) \
    PULSAR_EXTERNAL_STREAM_SETTINGS(M, ALIAS) \
    ICEBERG_EXTERNAL_STREAM_SETTINGS(M, ALIAS) \
    HTTP_EXTERNAL_STREAM_SETTINGS(M, ALIAS) \
    NATS_JETSTREAM_EXTERNAL_STREAM_SETTINGS(M, ALIAS)

#define LIST_OF_EXTERNAL_STREAM_SETTINGS(M, ALIAS) \
    ALL_EXTERNAL_STREAM_SETTINGS(M, ALIAS) \
    FORMAT_FACTORY_SETTINGS(M, ALIAS)

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
    void apply(const SettingChange & change, bool throw_on_unknown);

    KafkaExternalStreamSettings getKafkaSettings() const
    {
        KafkaExternalStreamSettings settings {};
#define SET_CHANGED_SETTINGS(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS) \
        if ((NAME).changed) \
            settings.NAME = (NAME);

        KAFKA_EXTERNAL_STREAM_SETTINGS(SET_CHANGED_SETTINGS, ALIAS)

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

        FORMAT_FACTORY_SETTINGS(SET_CHANGED_SETTINGS, ALIAS)

#undef SET_CHANGED_SETTINGS

        auto format_settings = DB::getFormatSettings(context, settings);

        /// This is needed otherwise using an external stream with ProtobufSingle format as the target stream
        /// of a MV (or in `INSERT ... SELECT ...`), i.e. more than one rows sent to the stream, exception will be thrown.
        format_settings.protobuf.allow_multiple_rows_without_delimiter = true;

        /// Derive subject name according to schema subject name strategy
        /// https://developer.confluent.io/courses/schema-registry/schema-subjects/
        if (subject_name_strategy.value.empty() || subject_name_strategy.value == "TopicNameStrategy")
        {
            /// Ingore subject_name_strategy setting when using TopicNameStrategy, because the subject name is derived from topic name.
            format_settings.kafka_schema_registry.subject_name = fmt::format("{}-value", topic.value);
        }
        else if (subject_name_strategy.value == "TopicRecordNameStrategy")
        {
            if (schema_subject_name.value.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "schema_subject_name must be provided when using TopicRecordNameStrategy");

            format_settings.kafka_schema_registry.subject_name = fmt::format("{}-{}", topic.value, schema_subject_name.value);
            format_settings.kafka_schema_registry.consume_single_schema = true;
        }
        else
        {
            /// subject_name_strategy.value == "RecordNameStrategy" or CustomNameStrategy

            if (schema_subject_name.value.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "schema_subject_name must be provided when using {}", subject_name_strategy.value);

            format_settings.kafka_schema_registry.subject_name = schema_subject_name.value;
            format_settings.kafka_schema_registry.consume_single_schema = true;
        }

        /// Kafka schema registry may have multiple historical schema versions. Allow old data missed some fields and read as default value.
        format_settings.avro.allow_missing_fields = true;

        return format_settings;
    }
};

using ExternalStreamSettingsPtr = std::unique_ptr<ExternalStreamSettings>;

}
