#pragma once

#include <Columns/IColumn.h>
#include <Interpreters/Streaming/WindowCommon.h>

namespace DB
{
class ColumnTuple;

namespace Streaming
{
namespace SessionWindowHelper
{
void assignWindow(
    SessionInfoQueue & sessions,
    SessionWindowParams & params,
    Columns & columns,
    ssize_t wstart_col_pos,
    ssize_t wend_col_pos,
    size_t time_col_pos,
    size_t session_start_col_pos,
    size_t session_end_col_pos);

/// @brief Get max window can be finalized
/// @return last finalized session info, return nullptr if no finalized session
SessionInfoPtr getLastFinalizedSession(const SessionInfoQueue & sessions);

/// @brief Remove expired sessions that not active
/// @return last removed expired session id
SessionID removeExpiredSessions(SessionInfoQueue & sessions);

/// @brief Get windows with buckets
WindowsWithBuckets getWindowsWithBuckets(const SessionInfoQueue & sessions);
}
}
}
