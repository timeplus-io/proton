#pragma once

#include <Core/BaseSettings.h>
#include <Core/Settings.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/Context.h>

namespace DB
{
class ASTStorage;

#define EXTERNAL_STREAM_RELATED_SETTINGS(M) \
    M(String, type, "", "External stream type", 0) \
    /* those are kafka related settings */ \
    M(String, brokers, "", "A comma-separated list of brokers, for example Kafka brokers.", 0) \
    M(String, topic, "", "topic, for example Kafka topic name.", 0) \
    M(String, security_protocol, "plaintext", "The protocol to connection external logstore", 0) \
    M(String, username, "", "The username of external logstore", 0) \
    M(String, password, "", "The password of external logstore", 0) \
    M(String, sasl_mechanism, "PLAIN", "SASL mechanism to use for authentication. Supported: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512. Default to PLAIN when SASL is enabled.", 0) \
    M(String, ssl_ca_cert_file, "", "The path of ssl ca cert file", 0) \
    M(String, properties, "", "A semi-colon-separated key-value pairs for configuring the kafka client used by the external stream. A key-value pair is separated by a equal sign. Example: 'client.id=my-client-id;group.id=my-group-id'. Note, not all properties are supported, please check the document for supported properties.", 0) \
    M(String, sharding_expr, "", "An expression which will be evaluated on each row of data returned by the query to calculate the an integer which will be used to determine the ID of the partition to which the row of data will be sent. If not set, data are sent to any partition randomly.", 0) \
    M(String, message_key, "", "An expression which will be evaluated on each row of data returned by the query to compute a string which will be used as the message key.", 0) \
    M(Bool, one_message_per_row, false, "If set to true, when send data to the Kafka external stream with row-based data format like `JSONEachRow`, it will produce one message per row.", 0) \
    /* those are log related settings */ \
    M(String, log_files, "", "A comma-separated list of log files", 0) \
    M(String, log_dir, "", "log root directory", 0) \
    M(String, timestamp_regex, "", "Regex to extract log timestamp", 0) \
    M(UInt64, hash_bytes, 1024, "File bytes to hash to decide if the same file", 0) \
    /* those are mapped to format factory settings */ \
    M(String, data_format, "", "The message format, for example JSONEachRow", 0) \
    M(String, row_delimiter, "\n", "The string to be considered as a delimiter in raw message.", 0) \
    M(UInt64, max_row_length, 4096, "Max row length", 0)

#define LIST_OF_EXTERNAL_STREAM_SETTINGS(M) \
    EXTERNAL_STREAM_RELATED_SETTINGS(M) \
    FORMAT_FACTORY_SETTINGS(M)

DECLARE_SETTINGS_TRAITS(ExternalStreamSettingsTraits, LIST_OF_EXTERNAL_STREAM_SETTINGS)

/** Settings for the ExternalStream engine.
  * Could be loaded from a CREATE EXTERNAL STREAM query (SETTINGS clause).
  */
struct ExternalStreamSettings : public BaseSettings<ExternalStreamSettingsTraits>
{
    void loadFromQuery(ASTStorage & storage_def);

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
