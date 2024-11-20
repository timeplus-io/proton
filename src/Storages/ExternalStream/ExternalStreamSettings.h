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
    M(String, security_protocol, "plaintext", "The protocol to connection external logstore", 0) \
    M(String, username, "", "The username of external logstore", 0) \
    M(String, password, "", "The password of external logstore", 0) \
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

#define NATS_EXTERNAL_STREAM_SETTINGS(M) \
    M(String, nats_servers, "", "A comma-separated list of NATS servers.", 0) \
    M(String, nats_subject, "", "NATS subject name.", 0) \
    M(String, nats_queue_group, "", "NATS queue group name.", 0) \
    M(String, nats_durable_name, "", "NATS durable name.", 0) \
    M(String, nats_ack_wait, "30s", "NATS acknowledgment wait time.", 0) \
    M(UInt64, nats_max_inflight, 1024, "Maximum number of inflight messages for NATS.", 0) \
    M(String, nats_start_sequence, "", "NATS start sequence.", 0) \
    M(String, nats_start_time, "", "NATS start time.", 0) \
    M(String, nats_deliver_policy, "all", "NATS deliver policy.", 0) \
    M(String, nats_ack_policy, "explicit", "NATS acknowledgment policy.", 0) \
    M(String, nats_replay_policy, "instant", "NATS replay policy.", 0) \
    M(String, nats_flow_control, "false", "Enable NATS flow control.", 0) \
    M(String, nats_max_waiting, "512", "Maximum number of waiting messages for NATS.", 0) \
    M(String, nats_max_deliver, "5", "Maximum number of delivery attempts for NATS.", 0) \
    M(String, nats_backoff, "", "NATS backoff intervals.", 0) \
    M(String, nats_filter_subject, "", "NATS filter subject.", 0) \
    M(String, nats_replay_rate, "", "NATS replay rate.", 0) \
    M(String, nats_max_ack_pending, "", "NATS maximum acknowledgment pending.", 0) \
    M(String, nats_idle_heartbeat, "", "NATS idle heartbeat interval.", 0) \
    M(String, nats_flow_control_subject, "", "NATS flow control subject.", 0) \
    M(String, nats_max_consumers, "", "NATS maximum number of consumers.", 0) \
    M(String, nats_max_messages, "", "NATS maximum number of messages.", 0) \
    M(String, nats_max_bytes, "", "NATS maximum number of bytes.", 0) \
    M(String, nats_max_age, "", "NATS maximum age of messages.", 0) \
    M(String, nats_max_msg_size, "", "NATS maximum message size.", 0) \
    M(String, nats_max_msg_size_bytes, "", "NATS maximum message size in bytes.", 0) \
    M(String, nats_max_msg_size_kb, "", "NATS maximum message size in kilobytes.", 0) \
    M(String, nats_max_msg_size_mb, "", "NATS maximum message size in megabytes.", 0) \
    M(String, nats_max_msg_size_gb, "", "NATS maximum message size in gigabytes.", 0) \
    M(String, nats_max_msg_size_tb, "", "NATS maximum message size in terabytes.", 0) \
    M(String, nats_max_msg_size_pb, "", "NATS maximum message size in petabytes.", 0) \
    M(String, nats_max_msg_size_eb, "", "NATS maximum message size in exabytes.", 0) \
    M(String, nats_max_msg_size_zb, "", "NATS maximum message size in zettabytes.", 0) \
    M(String, nats_max_msg_size_yb, "", "NATS maximum message size in yottabytes.", 0) \
    M(String, nats_max_msg_size_bb, "", "NATS maximum message size in brontobytes.", 0) \
    M(String, nats_max_msg_size_geopb, "", "NATS maximum message size in geopbytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String, nats_max_msg_size_hellabyte, "", "NATS maximum message size in hellabytes.", 0) \
    M(String,
