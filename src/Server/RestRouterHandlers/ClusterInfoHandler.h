#pragma once

#include "RestRouterHandler.h"

namespace DB
{
class ClusterInfoHandler final : public RestRouterHandler
{
public:
    explicit ClusterInfoHandler(ContextPtr query_context_) : RestRouterHandler(query_context_, "ClusterInfo") { }
    ~ClusterInfoHandler() override = default;

private:
    std::pair<String, Int32> executeGet(const Poco::JSON::Object::Ptr & payload) const override;
    String getClusterInfoLocally() const;
};
}
