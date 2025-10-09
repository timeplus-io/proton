#include <Server/RestRouterHandlers/TabularTableV2RestRouterHandler.h>

namespace DB
{

void TabularTableV2RestRouterHandler::buildTablesJSON(Poco::JSON::Object & resp, const TablePtrs & tables) const
{
    /// Proxy to V1 handler
    v1_handler.setQueryURI(std::move(query_uri));
    v1_handler.setAcceptedEncoding(std::move(accepted_encoding));
    v1_handler.setContentEncoding(std::move(content_encoding));
    v1_handler.setContentLength(std::move(content_length));
    v1_handler.setQueryParameters(std::move(query_parameters));
    v1_handler.setPathParameters(std::move(path_parameters));

    v1_handler.buildTablesJSON(resp, tables);
}

bool TabularTableV2RestRouterHandler::validateDatabase(String & error_msg) const
{
    const auto & requested_database = getPathParameter("database", "");
    if (requested_database.empty())
    {
        error_msg = "Database is required in endpoint path";
        return false;
    }

    return true;
}

bool TabularTableV2RestRouterHandler::validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const
{
    if (auto ret = validateDatabase(error_msg); !ret)
        return ret;

    /// Delegate to V1
    return v1_handler.validatePost(payload, error_msg);
}

bool TabularTableV2RestRouterHandler::validatePatch(const Poco::JSON::Object::Ptr & /*payload*/, String & error_msg) const
{
    return validateDatabase(error_msg);
}

bool TabularTableV2RestRouterHandler::validateDelete(const Poco::JSON::Object::Ptr & /*payload*/, String & error_msg) const
{
    return validateDatabase(error_msg);
}

bool TabularTableV2RestRouterHandler::validateGet(const Poco::JSON::Object::Ptr & /*payload*/, String & error_msg) const
{
    return validateDatabase(error_msg);
}

}
