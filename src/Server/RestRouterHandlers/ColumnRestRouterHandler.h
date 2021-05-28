#pragma once

#include "RestRouterHandler.h"

namespace DB
{
class ColumnRestRouterHandler final : public RestRouterHandler
{
public:
    explicit ColumnRestRouterHandler(ContextPtr query_context_) : RestRouterHandler(query_context_, "Column") { }
    ~ColumnRestRouterHandler() override { }

private:
    bool validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const override;
    bool validatePatch(const Poco::JSON::Object::Ptr & payload, String & error_msg) const override;

    std::pair<String, Int32> executePost(const Poco::JSON::Object::Ptr & payload) const override;
    std::pair<String, Int32> executeDelete(const Poco::JSON::Object::Ptr & payload) const override;
    std::pair<String, Int32> executePatch(const Poco::JSON::Object::Ptr & payload) const override;

private:
    std::pair<bool, String> assertColumnExists(const String & table, const String & column) const;
    std::pair<bool, String> assertColumnNotExists(const String & table, const String & column) const;
};

}
