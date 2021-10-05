#pragma once

#include "JSONHelper.h"
#include "RestRouterHandler.h"

namespace DB
{
class IngestRestRouterHandler final : public RestRouterHandler
{
public:
    explicit IngestRestRouterHandler(ContextMutablePtr query_context_) : RestRouterHandler(query_context_, "Ingest") { }
    ~IngestRestRouterHandler() override { }

    std::pair<String, Int32> execute(ReadBuffer & input) const override;

private:
    bool streamingInput() const override { return true; }

    static inline bool parseColumns(JSONReadBuffers & buffers, String & cols, String & error) ;
};

}
