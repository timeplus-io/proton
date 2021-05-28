#include "IngestStatusHandler.h"
#include "SchemaValidator.h"

#include <DistributedMetadata/CatalogService.h>
#include <DistributedMetadata/sendRequest.h>
#include <Storages/StorageDistributedMergeTree.h>

#include <Poco/Path.h>

#include <numeric>

namespace DB
{
namespace ErrorCodes
{
    extern const int OK;
    extern const int UNKNOWN_TABLE;
    extern const int TYPE_MISMATCH;
    extern const int INVALID_POLL_ID;
    extern const int INCORRECT_DATA;
    extern const int CHANNEL_ID_NOT_EXISTS;
    extern const int SEND_POLL_REQ_ERROR;
}

namespace
{
const String BATCH_URL = "http://{}:{}/dae/v1/ingest/statuses";

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
        error = "table: " + database_name + "." + table_name + " does not exist";
        error_code = ErrorCodes::UNKNOWN_TABLE;
        return nullptr;
    }

    if (storage->getName() != "DistributedMergeTree")
    {
        error = "table: " + database_name + "." + table_name + " is not a DistributedMergeTreeTable";
        error_code = ErrorCodes::TYPE_MISMATCH;
        return nullptr;
    }
    return storage;
}

String makeBatchResponse(const std::vector<IngestingBlocks::IngestStatus> & statuses, const String & query_id)
{
    Poco::JSON::Object resp;
    Poco::JSON::Array json_statuses;
    for (const auto & status : statuses)
    {
        Poco::JSON::Object::Ptr json(new Poco::JSON::Object());
        json->set("poll_id", status.poll_id);
        json->set("status", status.status);
        json->set("progress", status.progress);
        json_statuses.add(json);
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

bool IngestStatusHandler::categorizePollIds(const std::vector<String> & poll_ids, TablePollIdMap & table_poll_ids, String & error) const
{
    error.clear();

    for (const auto & poll_id : poll_ids)
    {
        std::vector<String> components;
        try
        {
            /// components: 0: query_id, 1: database, 2: table, 3: user, 5: timestamp
            components = query_context->parseQueryStatusPollId(poll_id);
            auto db_table = std::make_pair(std::move(components[1]), std::move(components[2]));
            table_poll_ids[db_table].emplace_back(std::move(poll_id));
        }
        catch (Exception & e)
        {
            error = "Invalid query id: " + poll_id + " ErrorCode: " + std::to_string(e.code());
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
        auto target_node = CatalogService::instance(query_context).nodeByChannel(chan);
        if (target_node == nullptr)
        {
            /// Node not found, either node is gone or invalid channel
            return {jsonErrorResponse("Unknown channel", ErrorCodes::CHANNEL_ID_NOT_EXISTS), HTTPResponse::HTTP_NOT_FOUND};
        }

        std::stringstream req_body_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
        payload->stringify(req_body_stream, 0);
        const String & body = req_body_stream.str();

        /// Forward the request to target node
        /// FIXME, https
        Poco::URI uri{fmt::format(BATCH_URL, target_node->host, target_node->http_port)};
        auto [response, http_status] = sendRequest(
            uri,
            HTTPRequest::HTTP_POST,
            query_context->getCurrentQueryId(),
            query_context->getUserName(),
            query_context->getPasswordByUserName(query_context->getUserName()),
            body,
            log);
        if (http_status == HTTPResponse::HTTP_OK)
        {
            return {response, http_status};
        }
        return {jsonErrorResponseFrom(response, ErrorCodes::SEND_POLL_REQ_ERROR), http_status};
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
    if (!categorizePollIds(poll_ids, table_poll_ids, error))
    {
        return {jsonErrorResponse(error, ErrorCodes::INVALID_POLL_ID), HTTPResponse::HTTP_BAD_REQUEST};
    }

    std::vector<IngestingBlocks::IngestStatus> statuses;
    for (const auto & table_polls : table_poll_ids)
    {
        int error_code = ErrorCodes::OK;
        auto storage = getTableStorage(table_polls.first.first, table_polls.first.second, query_context, error, error_code);
        if (!storage)
        {
            LOG_ERROR(
                log,
                "{}, for poll_ids: {}",
                error,
                std::accumulate(table_polls.second.begin(), table_polls.second.end(), std::string{","}),
                error_code);
            continue;
        }

        StorageDistributedMergeTree * dstorage = static_cast<StorageDistributedMergeTree *>(storage.get());
        dstorage->getIngestionStatuses(table_polls.second, statuses);
    }

    if (statuses.empty())
    {
        return {jsonErrorResponse("'poll_ids' are all invalid", ErrorCodes::INVALID_POLL_ID), HTTPResponse::HTTP_BAD_REQUEST};
    }
    return {makeBatchResponse(statuses, query_context->getCurrentQueryId()), HTTPResponse::HTTP_OK};
}
}
