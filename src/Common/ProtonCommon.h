#pragma once

#include <base/types.h>
#include <Common/IntervalKind.h>

#include <map>
#include <vector>

namespace DB
{
namespace ProtonConsts
{
/// Reserved column names / aliases for streaming processing
const String STREAMING_WINDOW_START = "window_start";
const String STREAMING_WINDOW_END = "window_end";
const std::vector<String> STREAMING_WINDOW_COLUMN_NAMES = {STREAMING_WINDOW_START, STREAMING_WINDOW_END};
const String STREAMING_TIMESTAMP_ALIAS = "__tp_ts";
const String STREAMING_SESSION_START = "__tp_session_start";
const String STREAMING_SESSION_END = "__tp_session_end";
const String PRIMARY_KEY_COLUMN_PREFIX = "__tp_pk_";
const String VERSION_COLUMN_PREFIX = "__tp_v_";

/// Internal function names
const String TUMBLE_FUNC_NAME = "__tumble";
const String HOP_FUNC_NAME = "__hop";
const String SESSION_FUNC_NAME = "__session";

const String RESERVED_EMIT_VERSION = "emit_version()";

/// Reserved column names / aliases for proton system
const String RESERVED_EVENT_TIME = "_tp_time";
const String RESERVED_EVENT_TIME_INDEX = RESERVED_EVENT_TIME + "_index";
const String RESERVED_PROCESS_TIME = "_tp_process_time";
const String RESERVED_APPEND_TIME = "_tp_append_time";
const String RESERVED_INGEST_TIME = "_tp_ingest_time";
const String RESERVED_EMIT_TIME = "_tp_emit_time";
const String RESERVED_INDEX_TIME = "_tp_index_time";
const String RESERVED_EVENT_SEQUENCE_ID = "_tp_sn";
const String RESERVED_EVENT_SEQUENCE_INDEX = RESERVED_EVENT_SEQUENCE_ID + "_index";
const String RESERVED_DELTA_FLAG = "_tp_delta";
const String RESERVED_SHARD = "_tp_shard";
const String RESERVED_SCHEMA_VERSION = "_tp_schema_version";
const String RESERVED_MESSAGE_KEY = "_tp_message_key";
const String RESERVED_MESSAGE_HEADERS = "_tp_message_headers";
const String RESERVED_ERROR = "_tp_error";
const String RESERVED_EVENT_TIME_API_NAME = "event_time_column";
const std::vector<String> RESERVED_COLUMN_NAMES = {RESERVED_EVENT_TIME, RESERVED_EVENT_SEQUENCE_ID, RESERVED_INDEX_TIME};
const String DEFAULT_EVENT_TIME = "now64(3, 'UTC')";
const String DEFAULT_STORAGE_TYPE = "hybrid";
const String STORAGE_TYPE_STREAMING = "streaming";
const String LOGSTORE_KAFKA = "kafka";
const String LOGSTORE_NATIVE_LOG = "nativelog";

/// Default settings for DDL
constexpr UInt64 DEFAULT_DDL_TIMEOUT_MS = 25000;

/// Default settings for session window
constexpr Int64 SESSION_SIZE_MULTIPLIER = 5; /// multiplier of session_size to timeout_interval of session window

/// Default periodic interval
const std::pair<Int64, IntervalKind> DEFAULT_PERIODIC_INTERVAL = {2, IntervalKind::Kind::Second};

/// Metastore namespace for UDF configuration
const String UDF_METASTORE_NAMESPACE = "udf";

/// JSON VALUES PREFIX for OptimizeJsonValueVisitor
const String JSON_VALUES_PREFIX = "__json_values_";

/// PREFIX of UDF config files
const String UDF_XML_PATTERN = "*_function.xml";
/// UDF VERSION used by this version of proton
constexpr uint32_t UDF_VERSION = 1;
/// Prefix for all Javascript UDF or UDA loggers
const String PROTON_JAVASCRIPT_UDF_LOGGER_PREFIX = "JavaScriptUDF";

const String DEFAULT_COLUMN_FAMILY_NAME = "__tp_cf_default";
const String FIXED_LENGTH_COLUMN_FAMILY_NAME = "__tp_cf_fixed_length";
const String VAR_LENGTH_COLUMN_FAMILY_NAME = "__tp_cf_var_length";

const String PRIMARY_KEY_INDEX_NAME = "__tp_primary_idx";
constexpr uint64_t PRIMARY_KEY_COLUMN_FAMILY_ID = 0;
constexpr uint64_t PRIMARY_KEY_INDEX_ID = 0;
constexpr uint64_t SECONDARY_INDEX_STARTING_ID = PRIMARY_KEY_INDEX_ID + 1;

/// Storage modes
const String APPEND_MODE = "append";
const String CHANGELOG_MODE = "changelog";

/// Stream engine subtypes
const String CHANGELOG_KV_MODE = "changelog_kv"; /// proton-historical alias for VERSIONED_COLLAPSING_MODE
const String VERSIONED_KV_MODE = "versioned_kv"; /// proton-historical alias for REPLACING_MODE

/// ClickHouse MergeTree merging modes
const String REPLACING_MODE = "Replacing"; /// upstream-aligned alias for VERSIONED_KV_MODE
const String VERSIONED_COLLAPSING_MODE = "VersionedCollapsing"; /// upstream-aligned alias for CHANGELOG_KV_MODE
const String SUMMING_MODE = "Summing";
const String COLLAPSING_MODE = "Collapsing";
const String AGGREGATING_MODE = "Aggregating";
const String GRAPHITE_MODE = "Graphite";

const std::map<String, String> LOG_STORE_SETTING_NAME_TO_KAFKA
    = {{"logstore_retention_bytes", "retention.bytes"},
       {"logstore_retention_ms", "retention.ms"},
       {"logstore_flush_messages", "flush.messages"},
       {"logstore_flush_ms", "flush_ms"}};

/// Flags of fields in Chunk/Block info
constexpr UInt64 WATERMARK_FLAG = 0x1 << 0;
constexpr UInt64 APPEND_TIME_FLAG = 0x1 << 1;
constexpr UInt64 HISTORICAL_DATA_START_FLAG = 0x1 << 2;
constexpr UInt64 HISTORICAL_DATA_END_FLAG = 0x1 << 3;
constexpr UInt64 CONSECUTIVE_DATA_FLAG = 0x1 << 4;
constexpr UInt64 SEQUENCE_NUMBER_FLAG = 0x1 << 5;
constexpr UInt64 MAX_TIMESTAMP_FLAG = 0x1 << 6;
constexpr UInt64 MIN_TIMESTAMP_FLAG = 0x1 << 7;
constexpr UInt64 RETRACT_FLAG = 0x1 << 8;
constexpr UInt64 AVOID_WATERMARK_FLAG = 0x8000'0000'0000'0000;
}
}
