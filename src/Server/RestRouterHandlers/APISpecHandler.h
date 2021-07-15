#pragma once

#include "RestRouterHandler.h"

namespace DB
{
class APISpecHandler final : public RestRouterHandler
{
public:
   explicit APISpecHandler(ContextPtr query_context_) : RestRouterHandler(query_context_, "APISpec") { }
   ~APISpecHandler() override { }

private:
   std::pair<String, Int32> executeGet(const Poco::JSON::Object::Ptr & payload) const override;
   void buildResponse(const Block & block, String & resp) const;
};

}
