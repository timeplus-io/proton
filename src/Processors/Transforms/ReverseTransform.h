#pragma once
#include <Processors/ISimpleTransform.h>

namespace DB
{

/// Reverse rows in chunk.
class ReverseTransform final : public ISimpleTransform
{
public:
    explicit ReverseTransform(const Block & header) : ISimpleTransform(header, header, false, ProcessorID::ReverseTransformID) {}
    String getName() const override { return "ReverseTransform"; }

protected:
    void transform(Chunk & chunk) override;
};

}
