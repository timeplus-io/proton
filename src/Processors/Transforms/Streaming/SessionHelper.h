#pragma once

#include <Columns/IColumn.h>
#include <Interpreters/Streaming/WindowCommon.h>

namespace DB
{
class ColumnTuple;

namespace Streaming
{
namespace SessionHelper
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
}
}
}
