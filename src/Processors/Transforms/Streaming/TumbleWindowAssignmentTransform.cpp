#include <Processors/Transforms/Streaming/TumbleWindowAssignmentTransform.h>

#include <Columns/ColumnTuple.h>

namespace DB
{
namespace Streaming
{
TumbleWindowAssignmentTransform::TumbleWindowAssignmentTransform(
    const Block & input_header, const Block & output_header, WindowParamsPtr window_params_)
    : WindowAssignmentTransform(input_header, output_header, std::move(window_params_), ProcessorID::TumbleWindowAssignmentTransformID)
    , params(window_params->as<TumbleWindowParams &>())
{
}

void TumbleWindowAssignmentTransform::assignWindow(Columns & columns) const
{
    ::DB::Streaming::assignWindow(
        columns,
        WindowInterval{params.window_interval, params.interval_kind},
        /*time_col_pos*/ 0,
        params.time_col_is_datetime64,
        *params.time_zone);
}
}
}
