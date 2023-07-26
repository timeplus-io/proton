#pragma once
#include <Processors/Transforms/ExceptionKeepingTransform.h>
#include <Processors/ISimpleTransform.h>

/// proton: starts.
#include <base/SerdeTag.h>
/// proton: ends.

namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

class ActionsDAG;

/// proton: starts.
class IExecutableFunction;
using ExecutableFunctionPtr = std::shared_ptr<IExecutableFunction>;
/// proton: ends.

/** Executes a certain expression over the block.
  * The expression consists of column identifiers from the block, constants, common functions.
  * For example: hits * 2 + 3, url LIKE '%proton%'
  * The expression processes each row independently of the others.
  */
class ExpressionTransform final : public ISimpleTransform
{
public:
    ExpressionTransform(
            const Block & header_,
            ExpressionActionsPtr expression_);

    String getName() const override { return "ExpressionTransform"; }

    static Block transformHeader(Block header, const ActionsDAG & expression);

    /// proton: starts.
    void checkpoint(CheckpointContextPtr ckpt_ctx) override;
    void recover(CheckpointContextPtr ckpt_ctx) override;
    /// proton: ends.

protected:
    void transform(Chunk & chunk) override;

private:
    ExpressionActionsPtr expression;
    /// proton: starts.
    SERDE std::vector<ExecutableFunctionPtr> stateful_functions;
    /// proton: ends.
};

class ConvertingTransform final : public ExceptionKeepingTransform
{
public:
    ConvertingTransform(
            const Block & header_,
            ExpressionActionsPtr expression_);

    String getName() const override { return "ConvertingTransform"; }

protected:
    void onConsume(Chunk chunk) override;
    GenerateResult onGenerate() override
    {
        GenerateResult res;
        res.chunk = std::move(cur_chunk);
        return res;
    }

private:
    ExpressionActionsPtr expression;
    Chunk cur_chunk;
};

}
