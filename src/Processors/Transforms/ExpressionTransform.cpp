#include <Processors/Transforms/ExpressionTransform.h>
#include <Interpreters/ExpressionActions.h>

/// proton; starts.
#include <Checkpoint/CheckpointContext.h>
#include <Checkpoint/CheckpointCoordinator.h>
#include <Functions/IFunction.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
/// proton: ends.

namespace DB
{

/// proton: starts.
namespace ErrorCodes
{
extern const int RECOVER_CHECKPOINT_FAILED;
}
/// proton: ends.

Block ExpressionTransform::transformHeader(Block header, const ActionsDAG & expression)
{
    return expression.updateHeader(std::move(header));
}


ExpressionTransform::ExpressionTransform(const Block & header_, ExpressionActionsPtr expression_)
    : ISimpleTransform(header_, transformHeader(header_, expression_->getActionsDAG()), false, ProcessorID::ExpressionTransformID)
    , expression(std::move(expression_))
    /// proton: starts.
    , stateful_functions(expression->getStatulFunctions())
    /// proton ends.
{
}

void ExpressionTransform::transform(Chunk & chunk)
{
    size_t num_rows = chunk.getNumRows();
    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());

    expression->execute(block, num_rows);

    chunk.setColumns(block.getColumns(), num_rows);
}

/// proton: starts.
void ExpressionTransform::checkpoint(CheckpointContextPtr ckpt_ctx)
{
    assert(isStreaming());
    if (stateful_functions.empty())
    {
        ckpt_ctx->coordinator->checkpointed(getVersion(), getLogicID(), ckpt_ctx);
        return;
    }

    ckpt_ctx->coordinator->checkpoint(getVersion(), getLogicID(), ckpt_ctx, [this](WriteBuffer & wb) {
        writeIntBinary(stateful_functions.size(), wb);
        for (auto & func : stateful_functions)
        {
            writeStringBinary(func->getName(), wb);
            func->serialize(wb);
        }
    });
}

void ExpressionTransform::recover(CheckpointContextPtr ckpt_ctx)
{
    if (!isStreaming() || stateful_functions.empty())
        return;

    ckpt_ctx->coordinator->recover(getLogicID(), ckpt_ctx, [this](VersionType /*version*/, ReadBuffer & rb) {
        size_t size = 0;
        readIntBinary(size, rb);
        if (size != stateful_functions.size())
            throw Exception(
                ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                "Failed to recover expression actions checkpoint. Number of stateful functions are not the same, checkpointed={}, "
                "current={}",
                size,
                stateful_functions.size());

        for (auto & func : stateful_functions)
        {
            String func_name;
            readStringBinary(func_name, rb);
            if (unlikely(func_name != func->getName()))
                throw Exception(
                    ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                    "Failed to recover expression actions checkpoint. Name of stateful function is not the same, checkpointed={}, "
                    "current={}",
                    func_name,
                    func->getName());

            func->deserialize(rb);
        }
    });
}
/// proton: ends.

ConvertingTransform::ConvertingTransform(const Block & header_, ExpressionActionsPtr expression_)
    : ExceptionKeepingTransform(header_, ExpressionTransform::transformHeader(header_, expression_->getActionsDAG()), true, ProcessorID::ConvertingTransformID)
    , expression(std::move(expression_))
{
}

void ConvertingTransform::onConsume(Chunk chunk)
{
    size_t num_rows = chunk.getNumRows();
    auto block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());

    expression->execute(block, num_rows);

    chunk.setColumns(block.getColumns(), num_rows);
    cur_chunk = std::move(chunk);
}

}
