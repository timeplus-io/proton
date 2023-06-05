#include <Interpreters/Streaming/SessionInfo.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{
namespace Streaming
{
void SessionInfo::serialize(WriteBuffer & wb) const
{
    writeIntBinary(id, wb);
    writeIntBinary(win_start, wb);
    writeIntBinary(win_end, wb);
    writeIntBinary(timeout_ts, wb);
    writeIntBinary(max_session_ts, wb);
    writeBoolText(active, wb);
}

void SessionInfo::deserialize(ReadBuffer & rb)
{
    readIntBinary(id, rb);
    readIntBinary(win_start, rb);
    readIntBinary(win_end, rb);
    readIntBinary(timeout_ts, rb);
    readIntBinary(max_session_ts, rb);
    readBoolText(active, rb);
}

void serialize(const SessionInfoQueue & sessions, WriteBuffer & wb)
{
    writeIntBinary(sessions.size(), wb);
    for (const auto & session_info : sessions)
        session_info->serialize(wb);
}

void deserialize(SessionInfoQueue & sessions, ReadBuffer & rb)
{
    size_t num_sessions;
    readIntBinary(num_sessions, rb);

    for (size_t i = 0; i < num_sessions; ++i)
    {
        auto & session_info = sessions.emplace_back(std::make_shared<SessionInfo>());
        session_info->deserialize(rb);
    }
}

}
}
