#include "IngestStatusHandler.h"
#include "SchemaValidator.h"

#include <Storages/Streaming/StorageStream.h>
#include <Common/sendRequest.h>

#include <Poco/Path.h>

#include <numeric>

namespace DB
{
namespace ErrorCodes
{
    extern const int OK;
    extern const int UNKNOWN_STREAM;
    extern const int TYPE_MISMATCH;
    extern const int INVALID_POLL_ID;
    extern const int INCORRECT_DATA;
    extern const int CHANNEL_ID_NOT_EXISTS;
    extern const int SEND_POLL_REQ_ERROR;
}

namespace
{
    const std::map<String, std::map<String, String>> POLL_SCHEMA = {{"required", {{"channel", "string"}, {"poll_ids", "array"}}}};

    StoragePtr
    getTableStorage(const String & database_name, const String & table_name, ContextPtr query_context, String & error, int & error_code)
    {
        StoragePtr storage;
        try
        {
            storage = DatabaseCatalog::instance().getTable(StorageID(database_name, table_name), query_context);
        }
        catch (Exception & e)
        {
            error = e.message();
            error_code = e.code();
            return nullptr;
        }

        if (!storage)
        {
            error = "Stream: " + database_name + "." + table_name + " does not exist";
            error_code = ErrorCodes::UNKNOWN_STREAM;
            return nullptr;
        }

        if (storage->getName() != "Stream")
        {
            error = database_name + "." + table_name + " is not a Stream";
            error_code = ErrorCodes::TYPE_MISMATCH;
            return nullptr;
        }
        return storage;
    }

    String makeBatchResponse(
        std::vector<std::pair<std::vector<String>, std::vector<IngestingBlocks::IngestStatus>>> statuses, const String & query_id)
    {
        Poco::JSON::Object resp;
        Poco::JSON::Array json_statuses;
        for (auto & poll_id_and_status : statuses)
        {
            assert(poll_id_and_status.first.size() == poll_id_and_status.second.size());

            for (size_t i = 0; auto & poll_id : poll_id_and_status.first)
            {
                Poco::JSON::Object::Ptr json(new Poco::JSON::Object());
                json->set("poll_id", poll_id);
                json->set("status", poll_id_and_status.second[i].status);
                json->set("progress", poll_id_and_status.second[i].progress);
                json_statuses.add(json);
                ++i;
            }
        }
        resp.set("status", json_statuses);
        resp.set("request_id", query_id);
        std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
        resp.stringify(resp_str_stream, 0);
        return resp_str_stream.str();
    }
}

bool IngestStatusHandler::validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const
{
    return validateSchema(POLL_SCHEMA, payload, error_msg);
}

bool IngestStatusHandler::categorizePollIds(std::vector<String> poll_ids, TablePollIdMap & table_poll_ids, String & error) const
{
    for (auto & poll_id : poll_ids)
    {
        try
        {
            /// components: 0: query_id, 1: database, 2: table, 3: user, 4: node_identity, 5: block_base_id, 6: timestamp
            auto components = query_context->parseQueryStatusPollId(poll_id);
            assert(components.size() == 7);
            auto & ids = table_poll_ids[std::make_pair(std::move(components[1]), std::move(components[2]))];
            ids.first.push_back(std::move(poll_id));
            ids.second.push_back(std::stoull(components[5]));
        }
        catch (const Exception & e)
        {
            error = fmt::format("Invalid query id: {}, error: {}", poll_id, e.code());
            return false;
        }
    }

    return true;
}

std::pair<String, Int32> IngestStatusHandler::executePost(const Poco::JSON::Object::Ptr & payload) const
{
    if (!payload)
        return {jsonErrorResponse("payload is empty", ErrorCodes::INCORRECT_DATA), HTTPResponse::HTTP_BAD_REQUEST};

    const auto & chan = payload->get("channel").toString();
    if (query_context->getChannel() == chan)
    {
        return getIngestStatusLocally(payload);
    }
    else
    {
        return {jsonErrorResponseFrom("not support to insert in cluster yet", ErrorCodes::SEND_POLL_REQ_ERROR), HTTPResponse::HTTP_BAD_REQUEST};
    }
}

std::pair<String, Int32> IngestStatusHandler::getIngestStatusLocally(const Poco::JSON::Object::Ptr & payload) const
{
    /// Assume clients group poll_ids by channel
    String error;
    /// The current node is the target ingesting node having the channel
    std::vector<String> poll_ids;
    for (const auto & poll_id : *payload->getArray("poll_ids"))
        poll_ids.emplace_back(poll_id.extract<String>());

    TablePollIdMap table_poll_ids;
    if (!categorizePollIds(std::move(poll_ids), table_poll_ids, error))
        return {jsonErrorResponse(error, ErrorCodes::INVALID_POLL_ID), HTTPResponse::HTTP_BAD_REQUEST};

    std::vector<std::pair<std::vector<String>, std::vector<IngestingBlocks::IngestStatus>>> statuses;
    statuses.reserve(table_poll_ids.size());

    for (auto & table_polls : table_poll_ids)
    {
        int error_code = ErrorCodes::OK;
        auto storage = getTableStorage(table_polls.first.first, table_polls.first.second, query_context, error, error_code);
        if (!storage)
        {
            LOG_ERROR(
                log,
                "{}, for poll_ids: {}",
                error,
                std::accumulate(table_polls.second.first.begin(), table_polls.second.first.end(), std::string{","}),
                error_code);
            continue;
        }

        statuses.emplace_back(std::move(table_polls.second.first), std::vector<IngestingBlocks::IngestStatus>());
        statuses.back().second.reserve(table_polls.second.second.size());

        StorageStream * dstorage = static_cast<StorageStream *>(storage.get());
        dstorage->getIngestionStatuses(table_polls.second.second, statuses.back().second);
    }

    if (statuses.empty())
        return {jsonErrorResponse("'poll_ids' are all invalid", ErrorCodes::INVALID_POLL_ID), HTTPResponse::HTTP_BAD_REQUEST};

    return {makeBatchResponse(std::move(statuses), query_context->getCurrentQueryId()), HTTPResponse::HTTP_OK};
}
}
