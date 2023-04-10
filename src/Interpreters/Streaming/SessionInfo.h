#pragma once

#include <Core/Streaming/WatermarkInfo.h>

namespace DB
{
namespace Streaming
{
using SessionID = SubstreamID;
using SessionIDs = std::vector<SessionID>;

struct SessionInfo
{
    SessionID id = SessionID{};
    Int64 win_start = 0;
    Int64 win_end = 0;
    Int64 ignore_ts = 0; /// ignore event if event time < @nigore_ts
    Int64 timeout_ts = 0; /// close session if event time > @timeout_ts
    Int64 max_session_ts = 0; /// close session if global max event time > @max_session_ts
    UInt32 scale = 0;
    Int64 interval = 0;

    /// When some event meet the start condition, session window is active.
    /// And an event which meet the end condition will make session window inactive.
    /// Session window only accept event when it is active.
    bool active = false;

    String string() const { return fmt::format("({}, {})", win_start, win_end); }
};

using SessionInfoPtr = std::unique_ptr<SessionInfo>;
using SessionInfos = std::vector<SessionInfo>;
}
}
