#include "ExternalStreamRestRouterHandler.h"
#include "ColumnDefinition.h"
#include "SchemaValidator.h"

#include <Interpreters/executeQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/queryToString.h>
#include <Common/ProtonCommon.h>

#include <boost/algorithm/string/join.hpp>

#include <vector>

namespace DB
{
std::map<String, std::map<String, String>> ExternalStreamRestRouterHandler::create_schema
    = {{"required", {{"name", "string"}, {"columns", "array"}, {"settings", "array"}}},
       {"optional",
        {{ProtonConsts::RESERVED_EVENT_TIME_API_NAME, "string"}}}};

std::map<String, std::map<String, String>> ExternalStreamRestRouterHandler::column_schema
    = {{"required",
        {
            {"name", "string"},
            {"type", "string"},
        }},
       {"optional",
        {{"nullable", "bool"},
         {"default", "string"},
         {"alias", "string"}}}};

std::map<String, std::map<String, String>> ExternalStreamRestRouterHandler::settings_schema
    = {{"required",
        {
            {"key", "string"},
            {"value", "string"},
        }}};

bool ExternalStreamRestRouterHandler::validatePost(const Poco::JSON::Object::Ptr & payload, String & error_msg) const
{
    if (!validateSchema(create_schema, payload, error_msg))
        return false;

    Poco::JSON::Array::Ptr columns = payload->getArray("columns");
    for (const auto & col : *columns)
    {
        const auto col_ptr = col.extract<Poco::JSON::Object::Ptr>();
        if (!validateSchema(column_schema, col_ptr, error_msg))
            return false;
    }

    Poco::JSON::Array::Ptr settings = payload->getArray("settings");
    for (const auto & setting : *settings)
    {
        const auto setting_ptr = setting.extract<Poco::JSON::Object::Ptr>();
        if (!validateSchema(settings_schema, setting_ptr, error_msg))
            return false;
    }
    return true;
}

String ExternalStreamRestRouterHandler::getColumnsDefinition(const Poco::JSON::Object::Ptr & payload) const
{
    const auto & columns = payload->getArray("columns");

    std::vector<String> columns_definition;
    for (const auto & col : *columns)
    {
        auto col_json{col.extract<Poco::JSON::Object::Ptr>()};
        columns_definition.push_back(getCreateColumnDefinition(col_json));
    }

    return boost::algorithm::join(columns_definition, ", ");
}

String ExternalStreamRestRouterHandler::getSettings(const Poco::JSON::Object::Ptr & payload) const
{
    const auto & settings = payload->getArray("settings");
    std::vector<String> settings_definition;
    for (const auto & setting : *settings)
    {
        auto setting_json{setting.extract<Poco::JSON::Object::Ptr>()};
        settings_definition.push_back(fmt::format("{}='{}'",
            setting_json->get("key").toString(),
            setting_json->get("value").toString()));
    }
    return boost::algorithm::join(settings_definition, ",");
}
}
