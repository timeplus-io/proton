#include <Processors/Transforms/Streaming/SessionWindowAssignmentTransform.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/Transforms/Streaming/SessionHelper.h>

namespace DB
{
namespace Streaming
{
SessionWindowAssignmentTransform::SessionWindowAssignmentTransform(
    const Block & input_header, const Block & output_header, WindowParamsPtr window_params_)
    : WindowAssignmentTransform(input_header, output_header, std::move(window_params_), ProcessorID::SessionWindowAssignmentTransformID)
    , params(window_params->as<SessionWindowParams &>())
{
}

void SessionWindowAssignmentTransform::assignWindow(Columns & columns) const
{
    assert(params.pushdown_window_assignment);
    /// No calculate/cache for streaming session window aggregation optimization
    /// FIXME: Also support origin logic for historical session window here
    /// __session(timestamp_expr, timeout_interval, max_session_size, start_cond, start_with_inclusion, end_cond, end_with_inclusion)
    assert(columns.size() == 7);
    auto & time_col = columns[0];
    columns.emplace_back(time_col); /// No calculate window start here
    columns.emplace_back(time_col); /// No calculate window end here
}

}
}
