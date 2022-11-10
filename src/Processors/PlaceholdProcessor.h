#pragma once

#include "IProcessor.h"

namespace DB
{
class PlaceholdProcessor final : public IProcessor
{
public:
    using IProcessor::IProcessor;

    String getName() const override { return name; }

    void setName(String && name_) { name = std::move(name_); }

private:
    String name;
};
}
