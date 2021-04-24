#pragma once

#include "JSONHelper.h"
#include "RestRouterHandler.h"

namespace DB
{
class IngestRestRouterHandler final : public RestRouterHandler
{
public:
    explicit IngestRestRouterHandler(ContextPtr query_context_) : RestRouterHandler(query_context_, "Ingest") { }
    ~IngestRestRouterHandler() override { }

    String execute(ReadBuffer & input, HTTPServerResponse & response, Int32 & http_status) const override;

private:
    bool streaming() const override { return true; }

    static inline bool parseColumns(JSONReadBuffers & buffers, String & cols, String & error) ;
};

}
