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

void HopWindowAssignmentTransform::assignWindow(Chunk & chunk, Columns && columns, ColumnTuple && column_tuple) const
{
    Columns res;
    res.reserve(output_column_positions.size());
    for (auto pos : output_column_positions)
    {
        if (pos < 0)
            res.push_back(std::move(column_tuple.getColumnPtr(-1 - pos)));
        else
            res.push_back(std::move(columns[pos]));
    }
    auto num_rows = res.at(0)->size();
    chunk.setColumns(std::move(res), num_rows);
}

}
}
