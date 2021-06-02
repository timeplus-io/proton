#include "DatabaseRestRouterHandler.h"
#include "SchemaValidator.h"

#include <Core/Block.h>

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

    void buildDatabaseArray(const Block & block, Poco::JSON::Object & resp)
    {
        Poco::JSON::Array databases_mapping_json;

        for (size_t index = 0; index < block.rows(); index++)
        {
            const auto & databases_info = block.getColumns().at(0)->getDataAt(index).data;
            databases_mapping_json.add(databases_info);
        }

        resp.set("databases", databases_mapping_json);
    }
}

bool DatabaseRestRouterHandler::validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const
{
    return validateSchema(CREATE_SCHEMA, payload, error_msg);
}

std::pair<String, Int32> DatabaseRestRouterHandler::executeGet(const Poco::JSON::Object::Ptr & /* payload */) const
{
    String query = "SHOW DATABASES;";

    Poco::JSON::Object res_obj;
    const auto & res = processQuery(query, res_obj, ([&res_obj](Block && block) { buildDatabaseArray(block, res_obj); }));

    return {res, HTTPResponse::HTTP_OK};
}

std::pair<String, Int32> DatabaseRestRouterHandler::executePost(const Poco::JSON::Object::Ptr & payload) const
{
    if (isDistributedDDL())
    {
        setupDistributedQueryParameters({}, payload);
    }

    const String & database_name = payload->get("name").toString();
    String query = "CREATE DATABASE " + database_name;

    return {processQuery(query), HTTPResponse::HTTP_OK};
}

std::pair<String, Int32> DatabaseRestRouterHandler::executeDelete(const Poco::JSON::Object::Ptr & /* payload */) const
{
    if (isDistributedDDL())
    {
        setupDistributedQueryParameters({});
    }

    const String & database_name = getPathParameter("database");
    String query = "DROP DATABASE " + database_name;

    return {processQuery(query), HTTPResponse::HTTP_OK};
}

}
