#pragma once

#include "RestRouterHandler.h"

namespace DB
{
class TaskRestRouterHandler final : public RestRouterHandler
{
public:
    explicit TaskRestRouterHandler(ContextMutablePtr query_context_) : RestRouterHandler(query_context_, "Task") { }
    ~TaskRestRouterHandler() override { }

private:
    std::pair<String, Int32> executeGet(const Poco::JSON::Object::Ptr & payload) const override;
    std::pair<String, Int32> getTasksLocally() const;
};

}
