#pragma once
#include <Processors/ISink.h>

namespace DB
{

/// Sink which closes input port and reads nothing.
class NullSink final : public ISink
{
public:
    explicit NullSink(Block header) : ISink(std::move(header), ProcessorID::NullSinkID) {}
    String getName() const override { return "NullSink"; }

    Status prepare() override
    {
        input.close();
        return Status::Finished;
    }
protected:
    void consume(Chunk) override {}
};

}
