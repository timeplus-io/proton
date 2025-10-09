#pragma once

#include "config.h"

#if USE_PULSAR
#include <pulsar/Client.h>

#include <functional>

namespace Poco
{
class Logger;
}
using LoggerPtr = std::shared_ptr<Poco::Logger>;

namespace DB::ExternalStream
{
pulsar::Result executeWithRetry(std::function<pulsar::Result()> op, std::string_view op_name, size_t timeout_ms, LoggerPtr logger);
}

#endif
