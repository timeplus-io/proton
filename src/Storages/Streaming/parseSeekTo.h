#pragma once

#include <base/types.h>

namespace DB
{
/// return the start timestamp in milli-seconds in UTC timezone or local timezone
int64_t parseSeekTo(const String & seek_to, bool utc);
}
