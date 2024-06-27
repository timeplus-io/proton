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
    M(String, sasl_mechanism, "PLAIN", "SASL mechanism to use for authentication. Supported: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512. Default to PLAIN when SASL is enabled.", 0) \
    M(String, ssl_ca_cert_file, "", "The path of ssl ca cert file", 0) \
    M(String, ssl_ca_pem, "", "CA certificate string (PEM format) for verifying the server's key.", 0) \
    M(Bool, skip_ssl_cert_check, false, "If set to true, the server's certification won't be verified.", 0) \
    M(String, properties, "", "A semi-colon-separated key-value pairs for configuring the kafka client used by the external stream. A key-value pair is separated by a equal sign. Example: 'client.id=my-client-id;group.id=my-group-id'. Note, not all properties are supported, please check the document for supported properties.", 0) \
    M(UInt64, poll_waittime_ms, 500, "How long (in milliseconds) should poll waits.", 0) \
    M(String, sharding_expr, "", "An expression which will be evaluated on each row of data returned by the query to calculate the an integer which will be used to determine the ID of the partition to which the row of data will be sent. If not set, data are sent to any partition randomly.", 0) \
    M(String, message_key, "", "An expression which will be evaluated on each row of data returned by the query to compute a string which will be used as the message key.", 0) \
    M(Bool, one_message_per_row, false, "If set to true, when send data to the Kafka external stream with row-based data format like `JSONEachRow`, it will produce one message per row.", 0)

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
    M(String, stream, "", "Stream name. If not specified, The external stream name is used.", 0) \
    M(String, user, "", "User name. If not specified, `default` is be used.", 0) \
    M(Bool, secure, false, "Use secure connection.", 0)

#define ALL_EXTERNAL_STREAM_SETTINGS(M) \
    M(String, type, "", "External stream type", 0) \
    KAFKA_EXTERNAL_STREAM_SETTINGS(M) \
    LOG_FILE_EXTERNAL_STREAM_SETTINGS(M) \
    TIMEPLUS_EXTERNAL_STREAM_SETTINGS(M)

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
    void loadFromQuery(ASTStorage & storage_def);

    KafkaExternalStreamSettings getKafkaSettings()
    {
        KafkaExternalStreamSettings settings {};
#define SET_CHANGED_SETTINGS(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS) \
        if ((NAME).changed) \
            settings.NAME = (NAME);

        KAFKA_EXTERNAL_STREAM_SETTINGS(SET_CHANGED_SETTINGS)

#undef SET_CHANGED_SETTINGS
        return settings;
    }

    FormatSettings getFormatSettings(const ContextPtr & context)
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

}
