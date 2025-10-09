#include <Server/RestRouterHandlers/IngestRestRouterHandler.h>

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

std::pair<String, Int32> IngestRestRouterHandler::execute(ReadBuffer & input) const
{
    const auto & table = getPathParameter("stream", "");

    if (table.empty())
        return {jsonErrorResponse("Stream is empty", ErrorCodes::BAD_REQUEST_PARAMETER), HTTPResponse::HTTP_BAD_REQUEST};

    if (hasQueryParameter("mode"))
        query_context->setIngestMode(ingestMode(getQueryParameter("mode"), IngestMode::Sync));

    query_context->setSetting("output_format_parallel_formatting", false);
    query_context->setSetting("date_time_input_format", String{"best_effort"});
    query_context->setSetting("input_format_json_read_numbers_as_strings", true);

    /// Request body can be compressed using algorithm specified in the Content-Encoding header.
    auto input_maybe_compressed
        = wrapReadBufferWithCompressionMethod(wrapReadBufferReference(input), chooseCompressionMethod({}, getContentEncoding()));

    /// Parse JSON into ReadBuffers
    PODArray<char> parse_buf;
    JSONReadBuffers buffers;
    String error;
    if (!readIntoBuffers(*input_maybe_compressed, parse_buf, buffers, error))
    {
        LOG_ERROR(
            log,
            "Ingest to database {}, table {} failed with invalid JSON request: {}, exception = {}",
            database,
            table,
            error,
            ErrorCodes::INCORRECT_DATA);

        return {jsonErrorResponse(error, ErrorCodes::INCORRECT_DATA), HTTPResponse::HTTP_BAD_REQUEST};
    }

    /// Get query
    String query, cols;
    if (!parseColumns(buffers, cols, error))
    {
        LOG_ERROR(
            log,
            "Ingest to database {}, table {} failed with invalid request: {}, exception = {}",
            database,
            table,
            error,
            ErrorCodes::INCORRECT_DATA);

        return {jsonErrorResponse(error, ErrorCodes::INCORRECT_DATA), HTTPResponse::HTTP_BAD_REQUEST};
    }
    query = fmt::format("INSERT into {}.`{}` {} FORMAT JSONCompactEachRow ", database, table, cols);

    auto it = buffers.find("data");
    if (it == buffers.end())
    {
        LOG_ERROR(
            log,
            "Ingest to database {}, table {} failed with invalid request: missing 'data' field, exception = {}",
            database,
            table,
            ErrorCodes::INCORRECT_DATA);

        return {jsonErrorResponse("Invalid Request, missing 'data' field", ErrorCodes::INCORRECT_DATA), HTTPResponse::HTTP_BAD_REQUEST};
    }

    /// Prepare ReadBuffer for executeQuery
    std::unique_ptr<ReadBuffer> in;
    ReadBufferFromString query_buf(query);
    in = std::make_unique<ConcatReadBuffer>(query_buf, *it->second);

    String dummy_string;
    WriteBufferFromString out(dummy_string);

    executeQuery(*in, out, /* allow_into_outfile = */ false, query_context, {});

    /// Send back ingest response
    Poco::JSON::Object resp;
    resp.set("request_id", query_context->getCurrentQueryId());
    std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    resp.stringify(resp_str_stream, 0);

    return {resp_str_stream.str(), HTTPResponse::HTTP_OK};
}

inline bool IngestRestRouterHandler::parseColumns(JSONReadBuffers & buffers, String & cols, String & error)
{
    error.clear();
    auto it = buffers.find("columns");
    if (it == buffers.end())
    {
        error = "Invalid Request, 'columns' field is missing";
        return false;
    }

    char * begin = it->second->internalBuffer().begin();
    char * end = it->second->internalBuffer().end();

    /// Converting `[[...], [...], ...]` in JSON payload to `(...), (...), ...` which is friendly to
    /// insert data parser like INSERT INTO ... VALUES (...), (...) , ...`
    while (begin < end && *begin != '[')
        ++begin;

    if (*begin == '[')
    {
        *begin = '(';
    }
    else
    {
        error = "Invalid Request, 'columns' field is invalid";
        return false;
    }

    while (end > begin && *end != ']')
        --end;

    if (*end == ']')
    {
        *end = ')';
    }
    else
    {
        error = "Invalid Request, 'columns' field is invalid";
        return false;
    }

    /// empty 'columns' field
    if (end - begin < 2)
    {
        error = "Invalid Request: 'columns' is empty";
        return false;
    }

    cols.assign(begin, static_cast<size_t>(end - begin + 1));
    return true;
}
}
