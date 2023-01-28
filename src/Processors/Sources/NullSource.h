#pragma once
#include <Processors/ISource.h>


namespace DB
{

class NullSource final : public ISource
{
public:
    explicit NullSource(Block header) : ISource(std::move(header), true, ProcessorID::NullSourceID) {}
    String getName() const override { return "NullSource"; }

protected:
    Chunk generate() override { return Chunk(); }
};

}
