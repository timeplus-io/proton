#include "DatabaseRestRouterHandler.h"

#include "SchemaValidator.h"

#include <Core/Block.h>
#include <Interpreters/executeQuery.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>

#include <boost/functional/hash.hpp>

namespace DB
{

namespace
{
std::map<String, std::map<String, String>> CREATE_SCHEMA = {
    {"required",{
                    {"name", "string"}
                }
    }
};
}

bool DatabaseRestRouterHandler::validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const
{
    return validateSchema(CREATE_SCHEMA, payload, error_msg);
}

String DatabaseRestRouterHandler::executeGet(const Poco::JSON::Object::Ptr & /* payload */, Int32 & http_status) const
{
    String query = "SHOW DATABASES;";

    return processQuery(query, http_status);
}

String DatabaseRestRouterHandler::executePost(const Poco::JSON::Object::Ptr & payload, Int32 & http_status) const
{
    if (query_context->isDistributed() && getQueryParameter("distributed_ddl") != "false")
    {
        std::stringstream payload_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
        payload->stringify(payload_str_stream, 0);
        query_context->setQueryParameter("_payload", payload_str_stream.str());
        query_context->setDistributedDDLOperation(true);
    }

    const String & database_name = payload->get("name").toString();
    String query = "CREATE DATABASE " + database_name;

    return processQuery(query, http_status);
}

String DatabaseRestRouterHandler::executeDelete(const Poco::JSON::Object::Ptr & /* payload */, Int32 & http_status) const
{
    if (query_context->isDistributed() && getQueryParameter("distributed_ddl") != "false")
    {
        query_context->setDistributedDDLOperation(true);
        query_context->setQueryParameter("_payload", "{}");
    }

    const String & database_name = getPathParameter("database");
    String query = "DROP DATABASE " + database_name;

    return processQuery(query, http_status);
}

String DatabaseRestRouterHandler::processQuery(const String & query, Int32 & /* http_status */) const
{
    BlockIO io{executeQuery(query, query_context, false /* internal */)};

    Poco::JSON::Object resp;
    if (io.pipeline.initialized())
    {
        processQueryWithProcessors(resp, io.pipeline);
    }
    io.onFinish();

    resp.set("query_id", query_context->getCurrentQueryId());
    std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    resp.stringify(resp_str_stream, 0);

    return resp_str_stream.str();
}

void DatabaseRestRouterHandler::processQueryWithProcessors(Poco::JSON::Object & resp, QueryPipeline & pipeline) const
{
    PullingAsyncPipelineExecutor executor(pipeline);
    Poco::JSON::Array databases_mapping_json;
    Block block;

    while (executor.pull(block, 100))
    {
        if (block)
        {
            for (size_t index = 0; index < block.rows(); index++)
            {
                const auto & databases_info = block.getColumns().at(0)->getDataAt(index).data;
                databases_mapping_json.add(databases_info);
            }
        }
    }
    resp.set("databases", databases_mapping_json);
}

}
