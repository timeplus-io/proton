#pragma once

#include <Cluster/Common/SerdeTag.h>

namespace cluster
{
SERDE enum class FetchIsolation : uint8_t {
    Committed, /// Committed by the quorum or flushed on disk in scenario
    LogEnd, /// Read what just got appended
};
}
