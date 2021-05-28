#pragma once

#include "RestRouterHandler.h"

namespace DB
{
class IngestRawStoreHandler final : public RestRouterHandler
{
public:
    explicit IngestRawStoreHandler(ContextPtr query_context_) : RestRouterHandler(query_context_, "IngestRawStore") { }
    ~IngestRawStoreHandler() override = default;

    std::pair<String, Int32> execute(ReadBuffer & input) const override;

private:
    bool streamingInput() const override { return true; }

    bool handleEnrichment(ReadBuffer & buf, String & error) const;
};

}
