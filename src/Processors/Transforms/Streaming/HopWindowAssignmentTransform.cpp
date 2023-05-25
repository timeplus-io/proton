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
    assert(checkAndGetColumn<ColumnArray>(column_tuple.getColumnPtr(0).get()));
    const auto & offsets = checkAndGetColumn<ColumnArray>(column_tuple.getColumnPtr(0).get())->getOffsets();

    Columns res;
    res.reserve(output_column_positions.size());
    for (auto pos : output_column_positions)
    {
        if (pos < 0)
            res.push_back(std::move(assert_cast<ColumnArray &>(column_tuple.getColumn(-1 - pos)).getDataPtr()));
        else
            res.push_back(columns[pos]->replicate(offsets));
    }
    chunk.setColumns(std::move(res), offsets.back());
}

}
}
