#include "SessionMap.h"

#include <Interpreters/Streaming/StreamingWindowCommon.h>

namespace DB
{

SessionStatus handleSession(const DateTime64 & tp_time, SessionInfo & info, IntervalKind::Kind kind, Int64 session_size, Int64 window_interval)
{
    assert(info.win_start <= info.win_end);

    const DateLUTImpl & time_zone = DateLUT::instance("UTC");

    if (addTime(tp_time, kind, session_size, time_zone) < info.win_start)
    {
        /// late session, ignore this event
        return SessionStatus::IGNORE;
    }
    else if (addTime(tp_time, kind, window_interval, time_zone) < info.win_start)
    {
        /// with session_size, possible late event in current session
        /// TODO: append block into possible_session_end_list
        return SessionStatus::KEEP;
    }
    else if (addTime(tp_time, kind, window_interval, time_zone) >= info.win_start && tp_time < info.win_end)
    {
        //            session_data[offeset] = cur_session_id;
        return SessionStatus::KEEP;
    }
    else if (addTime(tp_time, kind, -1 * window_interval, time_zone) <= info.win_end)
    {
        /// belongs to current session
        //            session_data[offeset] = cur_session_id;
        info.win_end = tp_time;
        return SessionStatus::END_EXTENDED;
    }
    else
    {
        /// possible belongs to current session, cache it
        return SessionStatus::KEEP;
    }
}

void updateSessionInfo(DateTime64 /*timestamp*/, SessionBlockQueue & queue, size_t session_size, UInt64 window_interval)
{
    IntervalKind::Kind kind = IntervalKind::Kind::Second;
    SessionStatus result = SessionStatus::IGNORE;
    do
    {
        for (auto & it : queue.block_queue)
        {
            auto tp_time = it.first;
            result = handleSession(tp_time, queue.session_info, kind, session_size, window_interval);
            if (result == SessionStatus::END_EXTENDED || result == SessionStatus::START_EXTENDED)
                break;

            if (result == SessionStatus::IGNORE)
                std::cout << "LOGIC ERROR" << std::endl;

            if (result == SessionStatus::EMIT)
            {
                queue.removeCurrentAndLateSession(tp_time);
                auto chunks = queue.emitCurrentSession(tp_time);
                break;
            }
        }
    } while (result == SessionStatus::END_EXTENDED || result == SessionStatus::START_EXTENDED || result == SessionStatus::EMIT);
}
}
