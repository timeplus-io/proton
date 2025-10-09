#pragma once

#include <Interpreters/Context_fwd.h>

namespace cluster::klog
{
class KafkaWALPool;
}

namespace DB
{
cluster::klog::KafkaWALPool * bootstrapKafkaLogPool(const ContextPtr & global_context);
}
