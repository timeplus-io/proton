#pragma once

#include <Server/RestRouterHandlers/IngestRestRouterHandler.h>

namespace DB
{

class IngestV2RestRouterHandler final : public RestRouterHandler
{
public:
    explicit IngestV2RestRouterHandler(ContextMutablePtr context_) : RestRouterHandler(context_, "IngestV2"), v1_handler(context_) { }
    ~IngestV2RestRouterHandler() override = default;

    std::pair<String, Int32> execute(ReadBuffer & input) const override;

private:
    bool streamingInput() const override { return true; }
    mutable IngestRestRouterHandler v1_handler;
};

}
