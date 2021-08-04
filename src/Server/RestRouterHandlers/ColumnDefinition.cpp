#include "ColumnDefinition.h"

#include <boost/algorithm/string/join.hpp>

namespace DB
{

String getCreateColumnDefination(const Poco::JSON::Object::Ptr & column)
{
    std::vector<String> column_definition;

    column_definition.push_back(column->get("name").toString());
    if (column->has("nullable") && column->get("nullable"))
    {
        column_definition.push_back(" Nullable(" + column->get("type").toString() + ")");
    }
    else
    {
        column_definition.push_back(" " + column->get("type").toString());
    }

    if (column->has("default"))
    {
        column_definition.push_back(" DEFAULT " + column->get("default").toString());
    }
    else if (column->has("alias"))
    {
        column_definition.push_back(" ALIAS " + column->get("alias").toString());
    }

    if (column->has("compression_codec"))
    {
        column_definition.push_back(" CODEC(" + column->get("compression_codec").toString() + ")");
    }

    if (column->has("ttl_expression"))
    {
        column_definition.push_back(" TTL " + column->get("ttl_expression").toString());
    }

    if (column->has("skipping_index_expression"))
    {
        column_definition.push_back(", " + column->get("skipping_index_expression").toString());
    }

    return boost::algorithm::join(column_definition, " ");
}

String getUpdateColumnDefination(const Poco::JSON::Object::Ptr & payload, String & column_name)
{
    std::vector<String> update_segments;
    if (payload->has("name"))
    {
        update_segments.push_back(" RENAME COLUMN " + column_name + " TO " + payload->get("name").toString());
        column_name = payload->get("name").toString();
    }

    if (payload->has("comment"))
    {
        update_segments.push_back(" COMMENT COLUMN " + column_name + " COMMENT " + payload->get("comment").toString());
    }

    if (payload->has("type"))
    {
        update_segments.push_back(" MODIFY COLUMN " + column_name + " " + payload->get("type").toString());
    }

    if (payload->has("default"))
    {
        update_segments.push_back(" MODIFY COLUMN " + column_name + " DEFAULT " + payload->get("default").toString());
    }
    else if (payload->has("alias"))
    {
        update_segments.push_back(" MODIFY COLUMN " + column_name + " ALIAS " + payload->get("alias").toString());
    }

    if (payload->has("ttl_expression"))
    {
        update_segments.push_back(" MODIFY COLUMN " + column_name + " TTL " + payload->get("ttl_expression").toString());
    }

    if (payload->has("compression_codec"))
    {
        update_segments.push_back(" MODIFY COLUMN " + column_name + " CODEC(" + payload->get("compression_codec").toString() + ")");
    }

    return boost::algorithm::join(update_segments, ",");
}

}
