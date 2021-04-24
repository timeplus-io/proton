#include "SchemaValidator.h"


namespace DB
{
bool validateSchema(const std::map<String, std::map<String, String>> & schema, const Poco::JSON::Object::Ptr & payload, String & error_msg)
{
    auto iter = schema.find("required");
    if (iter != schema.end())
    {
        for (const auto & required : iter->second)
        {
            if (!payload->has(required.first))
            {
                error_msg = "Required param '" + required.first + "' is missing.";
                return false;
            }

            if ((required.second == "int" && !payload->get(required.first).isInteger())
                || (required.second == "string" && !payload->get(required.first).isString())
                || (required.second == "bool" && !payload->get(required.first).isBoolean())
                || (required.second == "double" && !payload->get(required.first).isNumeric())
                || (required.second == "array" && !payload->get(required.first).isArray()))
            {
                error_msg = "Invalid type of param '" + required.first + "'";
                return false;
            }
        }
    }

    iter = schema.find("optional");
    if (iter != schema.end())
    {
        for (const auto & optional : iter->second)
        {
            if (payload->has(optional.first)
                && ((optional.second == "int" && !payload->get(optional.first).isInteger())
                    || (optional.second == "string" && !payload->get(optional.first).isString())
                    || (optional.second == "bool" && !payload->get(optional.first).isBoolean())
                    || (optional.second == "double" && !payload->get(optional.first).isNumeric())))
            {
                error_msg = "Invalid type of param '" + optional.first + "'";
                return false;
            }
        }
    }

    return true;
}
}
