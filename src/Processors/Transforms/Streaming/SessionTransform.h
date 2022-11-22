#pragma once

#include "Sessionizer.h"

#include <Interpreters/Streaming/FunctionDescription.h>
#include <Processors/ISimpleTransform.h>

namespace DB
{
namespace Streaming
{
class SessionTransform final : public ISimpleTransform
{
public:
    SessionTransform(const Block & input_header, const Block & output_header, FunctionDescriptionPtr desc);

    ~SessionTransform() override = default;

    String getName() const override { return "SessionTransform"; }

    void checkpoint(CheckpointContextPtr ckpt_ctx) override;
    void recover(CheckpointContextPtr ckpt_ctx) override;

private:
    void transform(Chunk & chunk) override;

private:
    Sessionizer sessionizer;
};
}
}
