#pragma once
#include <Processors/ISink.h>

namespace DB
{

/// Sink which reads everything and do nothing with it.
class EmptySink final : public ISink
{
public:
    explicit EmptySink(Block header) : ISink(std::move(header), ProcessorID::EmptySinkID) {}
    String getName() const override { return "EmptySink"; }

protected:
    void consume(Chunk) override {}
};

}
