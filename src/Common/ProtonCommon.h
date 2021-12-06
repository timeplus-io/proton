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
const String RESERVED_EVENT_TIME_API_NAME = "event_time_column";
const String RESERVED_INDEX_TIME = "_tp_index_time";
const std::vector<String> RESERVED_COLUMN_NAMES = {RESERVED_EVENT_TIME, RESERVED_INDEX_TIME};
}
