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
    std::pair<String, Int32> executeGet(const Poco::JSON::Object::Ptr & payload) const override;
};

}
