#include "IngestStatusHandler.h"
#include "SchemaValidator.h"

#include <DistributedMetadata/PlacementService.h>
#include <IO/HTTPCommon.h>
#include <Storages/StorageDistributedMergeTree.h>

#include <Poco/Net/HTTPRequest.h>
#include <Poco/Path.h>

#include <numeric>
#include <vector>


namespace DB
{
namespace ErrorCodes
{
    extern const int OK;
    extern const int UNKNOWN_TABLE;
    extern const int TYPE_MISMATCH;
    extern const int INVALID_POLL_ID;
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
    error.clear();
    error_code = ErrorCodes::OK;

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

String makeBatchResponse(const std::vector<IngestingBlocks::IngestStatus> & statuses)
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
    std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    resp.stringify(resp_str_stream, 0);
    return resp_str_stream.str();
}
}

bool IngestStatusHandler::validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const
{
    return !validateSchema(POLL_SCHEMA, payload, error_msg);
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

String IngestStatusHandler::executePost(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const
{
    String error;
    PlacementService & placement = PlacementService::instance(query_context);
    const String & target_node = placement.getNodeIdentityByChannel(payload->get("channel").toString());

    if (target_node.empty())
    {
        /// Invalid node
        http_status = Poco::Net::HTTPResponse::HTTP_NOT_FOUND;
        return jsonErrorResponse("Unknown channel", ErrorCodes::CHANNEL_ID_NOT_EXISTS);
    }

    if (target_node == query_context->getNodeIdentity())
    {
        const auto & arr = payload->getArray("poll_ids");
        std::vector<String> poll_ids;

        for (const auto & poll_id : *arr)
            poll_ids.emplace_back(poll_id.extract<String>());

        TablePollIdMap table_poll_ids;
        if (!categorizePollIds(poll_ids, table_poll_ids, error))
        {
            http_status = Poco::Net::HTTPResponse::HTTP_BAD_REQUEST;
            return jsonErrorResponse(error, ErrorCodes::INVALID_POLL_ID);
        }

        std::vector<IngestingBlocks::IngestStatus> statuses;
        int error_code = ErrorCodes::OK;

        for (const auto & table_polls : table_poll_ids)
        {
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
            http_status = Poco::Net::HTTPResponse::HTTP_BAD_REQUEST;
            return jsonErrorResponse("None of poll_id in 'poll_ids' is valid", ErrorCodes::INVALID_POLL_ID);
        }
        return makeBatchResponse(statuses);
    }
    else
    {
        Poco::URI uri{fmt::format(BATCH_URL, target_node, query_context->getConfigRef().getString("http_port"))};
        return forwardRequest(uri, payload, http_status);
    }
}

String IngestStatusHandler::forwardRequest(const Poco::URI & uri, const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const
{
    LOG_DEBUG(log, "Forward request to uri={}", uri.toString());

    /// One second for connect/send/receive
    ConnectionTimeouts timeouts({1, 0}, {1, 0}, {5, 0});

    String error;
    PooledHTTPSessionPtr session;
    try
    {
        if (!payload)
            return jsonErrorResponse("payload is empty", ErrorCodes::INCORRECT_DATA);

        session = makePooledHTTPSession(uri, timeouts, 1);
        Poco::Net::HTTPRequest request{Poco::Net::HTTPRequest::HTTP_GET, uri.getPathAndQuery(), Poco::Net::HTTPRequest::HTTP_1_1};
        request.setHost(uri.getHost());
        std::stringstream req_body_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
        payload->stringify(req_body_stream, 0);
        request.setMethod(Poco::Net::HTTPRequest::HTTP_POST);
        const String & body = req_body_stream.str();
        request.setContentType("application/json");
        request.setContentLength(body.length());
        request.add("X-ClickHouse-Query-Id", query_context->getCurrentQueryId());
        auto & ostr = session->sendRequest(request);
        ostr << req_body_stream.str();

        if (!ostr.good())
        {
            http_status = Poco::Net::HTTPResponse::HTTP_SERVICE_UNAVAILABLE;
            error = "Failed on uri=" + uri.toString();
            LOG_ERROR(log, error);
            return jsonErrorResponse(error, ErrorCodes::SEND_POLL_REQ_ERROR);
        }

        Poco::Net::HTTPResponse response;
        auto & istr = session->receiveResponse(response);
        http_status = response.getStatus();

        if (http_status != Poco::Net::HTTPResponse::HTTP_OK)
        {
            LOG_INFO(log, "Executed on uri={} failed", uri.toString());
        }
        return String(std::istreambuf_iterator<char>(istr), {});
    }
    catch (const Poco::Exception & e)
    {
        if (!session.isNull())
        {
            session->attachSessionData(e.message());
        }
        error = "Failed on uri=" + uri.toString() + " error=" + e.message() + " exception=" + getCurrentExceptionMessage(false, true);
        LOG_ERROR(log, error);
    }
    catch (...)
    {
        error = "Failed on uri=" + uri.toString() + " exception=" + getCurrentExceptionMessage(false, true);
        LOG_ERROR(log, error);
    }
    http_status = Poco::Net::HTTPResponse::HTTP_INTERNAL_SERVER_ERROR;
    return jsonErrorResponse(error, ErrorCodes::SEND_POLL_REQ_ERROR);
}

}
