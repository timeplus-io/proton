#pragma once

#include <base/types.h>

namespace klog
{
using KConfParams = std::vector<std::pair<String, String>>;

/// Parses the `properties` setting for Kafka external streams.
/// properties example:
/// message.max.bytes=1024;max.in.flight=1000;group.id=my-group
KConfParams parseProperties(const String & properties);
}
