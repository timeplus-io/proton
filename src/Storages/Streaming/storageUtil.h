#pragma once

#include <Storages/IStorage_fwd.h>
#include <base/types.h>

namespace DB
{
/// return the start timestamp in milli-seconds in UTC timezone or local timezone
int64_t parseSeekTo(const String & seek_to, bool utc);

/// returns whether the storage supports streaming queries
bool supportStreamingQuery(const StoragePtr & storage);
}
