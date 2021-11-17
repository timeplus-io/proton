#pragma once

#include <base/types.h>

namespace DB
{
const String STREAMING_WINDOW_START = "window_start";
const String STREAMING_WINDOW_END = "window_end";
const std::vector<String> STREAMING_WINDOW_COLUMN_NAMES = {STREAMING_WINDOW_START, STREAMING_WINDOW_END};
const String STREAMING_WINDOW_FUNC_ALIAS = "____SWIN";
}
