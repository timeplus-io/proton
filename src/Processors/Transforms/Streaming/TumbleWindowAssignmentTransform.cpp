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

void TumbleWindowAssignmentTransform::assignWindow(Chunk & chunk, Columns && columns, ColumnTuple && column_tuple) const
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
