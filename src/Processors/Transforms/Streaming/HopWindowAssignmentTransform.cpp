#include <Processors/Transforms/Streaming/HopWindowAssignmentTransform.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Functions/FunctionHelpers.h>

namespace DB
{
namespace Streaming
{
HopWindowAssignmentTransform::HopWindowAssignmentTransform(
    const Block & input_header, const Block & output_header, WindowParamsPtr window_params_)
    : WindowAssignmentTransform(input_header, output_header, std::move(window_params_), ProcessorID::HopWindowAssignmentTransformID)
    , params(window_params->as<HopWindowParams &>())
{
}

void HopWindowAssignmentTransform::assignWindow(Columns & columns) const
{
    /// Use gcd_interval for streaming hop window aggregation optimization
    /// FIXME: Also support origin logic for historical hop window here
    ::DB::Streaming::assignWindow(
        columns,
        WindowInterval{params.gcd_interval, params.interval_kind},
        /*time_col_pos*/ 0,
        params.time_col_is_datetime64,
        *params.time_zone);
}

}
}
