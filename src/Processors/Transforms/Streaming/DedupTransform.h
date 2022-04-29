#pragma once

#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/Streaming/StreamingFunctionDescription.h>
#include <Processors/ISimpleTransform.h>

namespace DB
{
/**
 * DedupTransform dedup input column(s) according to the dedup keys
 */

class ColumnTuple;

class DedupTransform final : public ISimpleTransform
{
public:
    DedupTransform(const Block & input_header, const Block & output_header, StreamingFunctionDescriptionPtr dedup_func_desc_);

    ~DedupTransform() override = default;

    String getName() const override { return "DedupTransform"; }

    void transform(Chunk & chunk) override;

private:
    /// Calculate the positions of columns required by timestamp expr
    void calculateColumns(const Block & input_header, const Names & input_columns_);

private:
    ContextPtr context;

    StreamingFunctionDescriptionPtr dedup_func_desc;

    Chunk chunk_header;

    std::vector<size_t> expr_column_positions;
};
}
