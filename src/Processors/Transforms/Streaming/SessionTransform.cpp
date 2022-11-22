#include "SessionTransform.h"


namespace DB
{
namespace Streaming
{
SessionTransform::SessionTransform(const Block & input_header, const Block & output_header, FunctionDescriptionPtr desc)
    : ISimpleTransform(input_header, output_header, false, ProcessorID::SessionTransformID)
    , sessionizer(input_header, output_header, desc->session_start, desc->session_end)
{
}

void SessionTransform::transform(Chunk & chunk)
{
    assert(chunk);
    sessionizer.sessionize(chunk);
    assert(chunk);
}

void SessionTransform::checkpoint(CheckpointContextPtr ckpt_ctx)
{
    (void)ckpt_ctx;
}

void SessionTransform::recover(CheckpointContextPtr ckpt_ctx)
{
    (void)ckpt_ctx;
}

}
}
