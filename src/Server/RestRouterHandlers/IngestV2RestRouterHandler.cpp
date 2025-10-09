#include <Server/RestRouterHandlers/IngestV2RestRouterHandler.h>

#include <IO/ConcatReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <Interpreters/executeQuery.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_REQUEST_PARAMETER;
}

std::pair<String, Int32> IngestV2RestRouterHandler::execute(ReadBuffer & input) const
{
    /// Additional V2 validation
    auto db = getPathParameter("database", "");
    if (db.empty())
        return {jsonErrorResponse("Database parameter is required", ErrorCodes::BAD_REQUEST_PARAMETER), HTTPResponse::HTTP_BAD_REQUEST};

    /// Proxy to V1 handler
    v1_handler.setCurrentDatabase(std::move(db));
    v1_handler.setQueryURI(std::move(query_uri));
    v1_handler.setAcceptedEncoding(std::move(accepted_encoding));
    v1_handler.setContentEncoding(std::move(content_encoding));
    v1_handler.setContentLength(std::move(content_length));
    v1_handler.setQueryParameters(std::move(query_parameters));
    v1_handler.setPathParameters(std::move(path_parameters));

    return v1_handler.execute(input);
}
}
