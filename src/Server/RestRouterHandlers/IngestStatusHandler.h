#pragma once

#include "RestRouterHandler.h"

namespace DB
{
using TablePollIdMap = std::
    unordered_map<std::pair<String, String>, std::pair<std::vector<String>, std::vector<UInt64>>, boost::hash<std::pair<String, String>>>;

class IngestStatusHandler final : public RestRouterHandler
{
public:
    explicit IngestStatusHandler(ContextMutablePtr query_context_) : RestRouterHandler(query_context_, "IngestStatus") { }
    ~IngestStatusHandler() override = default;

private:
    std::pair<String, Int32> executePost(const Poco::JSON::Object::Ptr & payload) const override;

    bool validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const override;
    bool streamingInput() const override { return false; }

    bool categorizePollIds(std::vector<String> poll_ids, TablePollIdMap & table_poll_ids, String & error) const;
    std::pair<String, Int32> getIngestStatusLocally(const Poco::JSON::Object::Ptr & payload) const;
};
}
