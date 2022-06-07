#pragma once

#include <base/types.h>

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
const String STREAMING_SESSION_ID = "__tp_session_id";

/// Internal function names
const String TUMBLE_FUNC_NAME = "__tumble";
const String HOP_FUNC_NAME = "__hop";
const String SESSION_FUNC_NAME = "__session";
const String DEDUP_FUNC_NAME = "__dedup";

/// Reserved column names / aliases for streaming view
const String RESERVED_VIEW_VERSION = "__tp_version";

const String RESERVED_EMIT_VERSION = "emit_version()";

/// Reserved column names / aliases for proton system
const String RESERVED_EVENT_TIME = "_tp_time";
const String RESERVED_PROCESS_TIME = "_tp_process_time";
const String RESERVED_APPEND_TIME = "_tp_append_time";
const String RESERVED_INGEST_TIME = "_tp_ingest_time";
const String RESERVED_EMIT_TIME = "_tp_emit_time";
const String RESERVED_INDEX_TIME = "_tp_index_time";
const String RESERVED_EVENT_SEQUENCE_ID = "_tp_sn";
const String RESERVED_EVENT_TIME_API_NAME = "event_time_column";
const std::vector<String> RESERVED_COLUMN_NAMES = {RESERVED_EVENT_TIME, RESERVED_INDEX_TIME};
const String DEFAULT_EVENT_TIME = "now64(3, 'UTC')";
const String DEFAULT_STORAGE_TYPE = "hybrid";
const String STORAGE_TYPE_STREAMING = "streaming";
const String LOGSTORE_KAFKA = "kafka";
const String LOGSTORE_NATIVE_LOG = "nativelog";

/// Default settings for DDL
const UInt64 DEFAULT_DDL_TIMEOUT_MS = 10000;

/// Default settings for session window
const Int64 SESSION_SIZE_MULTIPLIER = 5; /// multiplier of session_size to timeout_interval of session window

/// Metastore namespace for UDF configuration
const String UDF_METASTORE_NAMESPACE = "udf";

/// JSON VALUES PREFIX for OptimizeJsonValueVisitor
const String JSON_VALUES_PREFIX = "__json_values_";

/// PREFIX of UDF config files
const String UDF_XML_PATTERN = "*_function.xml";
}
}
