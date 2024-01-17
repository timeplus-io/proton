#pragma once
#include <Processors/ISimpleTransform.h>
#include <QueryPipeline/SizeLimits.h>
#include <Core/ColumnNumbers.h>
#include <Interpreters/SetVariants.h>

/// proton: starts.
#include <Common/serde.h>
/// proton: ends.

namespace DB
{

class DistinctTransform final : public ISimpleTransform
{
public:
    DistinctTransform(
        const Block & header_,
        const SizeLimits & set_size_limits_,
        UInt64 limit_hint_,
        const Names & columns_);

    String getName() const override { return "DistinctTransform"; }

    /// proton: starts.
    void checkpoint(CheckpointContextPtr ckpt_ctx) override;
    void recover(CheckpointContextPtr ckpt_ctx) override;
    /// proton: ends.

protected:
    void transform(Chunk & chunk) override;

private:
    ColumnNumbers key_columns_pos;
    /// proton: starst.
    SERDE SetVariants data;
    /// proton: ends.
    Sizes key_sizes;
    UInt64 limit_hint;

    bool no_more_rows = false;

    /// Restrictions on the maximum size of the output data.
    SizeLimits set_size_limits;

    template <typename Method>
    void buildFilter(
        Method & method,
        const ColumnRawPtrs & key_columns,
        IColumn::Filter & filter,
        size_t rows,
        SetVariants & variants) const;
};

}
