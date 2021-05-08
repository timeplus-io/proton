#include "RestStatusHandler.h"

#include <Core/Block.h>
#include <Interpreters/executeSelectQuery.h>
#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_TYPE_OF_QUERY;
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
}

namespace
{
    std::map<String, String> colname_bldkey_mapping = {{"VERSION_DESCRIBE", "version"}, {"BUILD_TIME", "time"}};
}

String RestStatusHandler::executeGet(const Poco::JSON::Object::Ptr & /*payload*/, Int32 & http_status) const
{
    const String & status = getPathParameter("status");

    if (status == "info")
    {
        String query = "SELECT name, value FROM system.build_options WHERE name IN ('VERSION_FULL','VERSION_DESCRIBE','BUILD_TIME');";

        String resp = "";
        executeSelectQuery(query, query_context, [this, &resp](Block && block) { return this->buildResponse(block, resp); });

        return resp;
    }
    else if (status == "ping")
    {
        /// FIXME : introduce more sophisticated health calculation in future.
        return "{\"status\":\"UP\"}";
    }
    else
    {
        http_status = Poco::Net::HTTPResponse::HTTP_NOT_FOUND;
        return jsonErrorResponse("Unknown URI", ErrorCodes::UNKNOWN_TYPE_OF_QUERY, query_context->getCurrentQueryId());
    }
}

void RestStatusHandler::buildResponse(const Block & block, String & resp) const
{
    const auto & name = block.findByName("name")->column;
    const auto & value = block.findByName("value")->column;

    Poco::JSON::Object build_info;
    for (size_t i = 0; i < name->size(); ++i)
    {
        const auto & it = colname_bldkey_mapping.find(name->getDataAt(i).toString());
        if (it != colname_bldkey_mapping.end())
        {
            build_info.set(it->second, value->getDataAt(i).toString());
        }
    }
    build_info.set("name", "Daisy");
    
    Poco::JSON::Object json_resp;
    json_resp.set("build", build_info);

    std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    json_resp.stringify(resp_str_stream, 0);
    resp = resp_str_stream.str();
}

}
