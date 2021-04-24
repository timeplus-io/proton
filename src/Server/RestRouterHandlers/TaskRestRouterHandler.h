#pragma once

#include "RestRouterHandler.h"

#include <DistributedMetadata/TaskStatusService.h>

namespace DB
{
class TaskRestRouterHandler final : public RestRouterHandler
{
public:
    explicit TaskRestRouterHandler(ContextPtr query_context_) : RestRouterHandler(query_context_, "Task") { }
    ~TaskRestRouterHandler() override { }

private:
    String executeGet(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const override;
};

}
