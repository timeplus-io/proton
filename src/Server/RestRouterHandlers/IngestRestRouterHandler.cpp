#include "IngestRestRouterHandler.h"

#include <IO/ConcatReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <Interpreters/executeQuery.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_REQUEST_PARAMETER;
    extern const int INCORRECT_DATA;
}

String IngestRestRouterHandler::execute(ReadBuffer & input, HTTPServerResponse & /* response */, Int32 & http_status) const
{
    const auto & database_name = getPathParameter("database", "");
    const auto & table_name = getPathParameter("table", "");

    if (database_name.empty() || table_name.empty())
    {
        http_status = Poco::Net::HTTPResponse::HTTP_BAD_REQUEST;
        return jsonErrorResponse("Database or Table is empty", ErrorCodes::BAD_REQUEST_PARAMETER);
    }

    if (hasQueryParameter("mode"))
    {
        query_context->setIngestMode(getQueryParameter("mode"));
    }
    else
    {
        query_context->setIngestMode("async");
    }

    query_context->setSetting("output_format_parallel_formatting", false);

    /// Parse JSON into ReadBuffers
    PODArray<char> parse_buf;
    JSONReadBuffers buffers;
    String error;
    if (!readIntoBuffers(input, parse_buf, buffers, error))
    {
        http_status = Poco::Net::HTTPResponse::HTTP_BAD_REQUEST;
        LOG_ERROR(
            log,
            "Ingest to database {}, table {} failed with invalid JSON request, exception = {}",
            database_name,
            table_name,
            error,
            ErrorCodes::INCORRECT_DATA);
        return jsonErrorResponse(error, ErrorCodes::INCORRECT_DATA);
    }

    /// Get query
    String query, cols;
    if (!parseColumns(buffers, cols, error))
    {
        http_status = Poco::Net::HTTPResponse::HTTP_BAD_REQUEST;
        LOG_ERROR(
            log,
            "Ingest to database {}, table {} failed with invalid request, exception = {}",
            database_name,
            table_name,
            error,
            ErrorCodes::INCORRECT_DATA);
        return jsonErrorResponse(error, ErrorCodes::INCORRECT_DATA);
    }
    query = "INSERT into " + database_name + "." + table_name + " " + cols + " FORMAT JSONCompactEachRow ";

    auto it = buffers.find("data");
    std::unique_ptr<ReadBuffer> in;
    if (it != buffers.end())
    {
        ReadBufferFromString query_buf(query);
        in = std::make_unique<ConcatReadBuffer>(query_buf, *it->second);
    }
    else
    {
        http_status = Poco::Net::HTTPResponse::HTTP_BAD_REQUEST;
        LOG_ERROR(
            log,
            "Ingest to database {}, table {} failed with invalid request, exception = {}",
            database_name,
            table_name,
            "Invalid Request, missing 'data' field",
            ErrorCodes::INCORRECT_DATA);
        return jsonErrorResponse("Invalid Request, missing 'data' field", ErrorCodes::INCORRECT_DATA);
    }

    String dummy_string;
    WriteBufferFromString out(dummy_string);

    executeQuery(*in, out, /* allow_into_outfile = */ false, query_context, {});

    /// Send back ingest response
    Poco::JSON::Object resp;
    resp.set("query_id", query_context->getCurrentQueryId());
    const auto & poll_id = query_context->getQueryStatusPollId();
    if (!poll_id.empty())
    {
        resp.set("poll_id", poll_id);
        resp.set("channel", query_context->getChannel());
    }
    std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    resp.stringify(resp_str_stream, 0);

    return resp_str_stream.str();
}

inline bool IngestRestRouterHandler::parseColumns(JSONReadBuffers & buffers, String & cols, String & error)
{
    error.clear();
    auto it = buffers.find("columns");
    String query;
    if (it == buffers.end())
    {
        error = "Invalid Request, 'columns' field is missing";
        return false;
    }
    char * begin = it->second->internalBuffer().begin();
    char * end = it->second->internalBuffer().end();

    while (begin < end && *begin != '[')
        ++begin;
    if (*begin == '[')
        *begin = '(';
    else
    {
        error = "Invalid Request, 'columns' field is invalid";
        return false;
    }

    while (end > begin && *end != ']')
        --end;
    if (*end == ']')
        *end = ')';
    else
    {
        error = "Invalid Request, 'columns' field is invalid";
        return false;
    }

    cols.assign(begin, static_cast<size_t>(end - begin + 1));
    return true;
}
}
