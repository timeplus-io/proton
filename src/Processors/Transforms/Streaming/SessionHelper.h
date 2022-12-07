#pragma once

#include <Columns/IColumn.h>
#include <Interpreters/Streaming/SessionInfo.h>

namespace DB
{
namespace Streaming
{
struct AggregatedDataVariants;
struct AggregatingTransformParams;
using AggregatingTransformParamsPtr = std::shared_ptr<AggregatingTransformParams>;

namespace SessionHelper
{
/// Return: <row filter per session> + <info per session>
std::pair<std::vector<IColumn::Filter>, SessionInfos> prepareSession(
    SessionInfo & info,
    ColumnPtr & time_column,
    ColumnPtr & session_start_column,
    ColumnPtr & session_end_column,
    size_t num_rows,
    AggregatingTransformParamsPtr params);

void finalizeSession(
    AggregatedDataVariants & variants, const SessionInfo & info, Block & final_block, AggregatingTransformParamsPtr params);
}
}
}
