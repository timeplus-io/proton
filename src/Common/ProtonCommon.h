#pragma once

#include <base/types.h>

#include <vector>

namespace DB
{
/// Reserved column names / aliases for streaming processing
const String STREAMING_WINDOW_START = "window_start";
const String STREAMING_WINDOW_END = "window_end";
const std::vector<String> STREAMING_WINDOW_COLUMN_NAMES = {STREAMING_WINDOW_START, STREAMING_WINDOW_END};
const String STREAMING_WINDOW_FUNC_ALIAS = "__tp_swin";
const String STREAMING_TIMESTAMP_ALIAS = "__tp_ts";

/// Reserved column names / aliases for proton system
const String RESERVED_EVENT_TIME = "_tp_time";
const String RESERVED_PROCESS_TIME = "_tp_process_time";
const String RESERVED_APPEND_TIME = "_tp_append_time";
const String RESERVED_INGEST_TIME = "_tp_ingest_time";
const String RESERVED_EMIT_TIME = "_tp_emit_time";
const String RESERVED_INDEX_TIME = "_tp_index_time";
const String RESERVED_EVENT_TIME_API_NAME = "event_time_column";
const std::vector<String> RESERVED_COLUMN_NAMES = {RESERVED_EVENT_TIME, RESERVED_INDEX_TIME};
}
