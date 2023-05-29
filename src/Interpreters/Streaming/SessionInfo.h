#pragma once

#include <Core/Streaming/SubstreamID.h>

#include <deque>

namespace DB
{
namespace Streaming
{
using SessionID = Int64;

struct SessionInfo
{
    SessionID id = SessionID{};
    Int64 win_start = 0;
    Int64 win_end = 0;
    Int64 timeout_ts = 0; /// close session if event time > @timeout_ts
    Int64 max_session_ts = 0; /// close session if global max event time > @max_session_ts

    /// When some event meet the start condition, session window is active.
    /// And an event which meet the end condition will make session window inactive.
    /// Session window only accept event when it is active.
    bool active = false;

    String string() const { return fmt::format("({}, {})", win_start, win_end); }
};

using SessionInfoPtr = std::shared_ptr<SessionInfo>;
using SessionInfoQueue = std::deque<SessionInfoPtr>;
}
}
