#include "ASTToJSONUtils.h"

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <Parsers/queryToString.h>


namespace DB
{
void ColumnDeclarationToJSON(Poco::JSON::Object & column_mapping_json, const ASTColumnDeclaration & col_decl)
{
    const auto & column_type = DataTypeFactory::instance().get(col_decl.type);

    column_mapping_json.set("name", col_decl.name);

    String type = column_type->getName();
    if (column_type->isNullable())
    {
        type = removeNullable(column_type)->getName();
        column_mapping_json.set("nullable", true);
    }
    else
    {
        column_mapping_json.set("nullable", false);
    }
    column_mapping_json.set("type", type);

    if (col_decl.default_expression)
    {
        if (col_decl.default_specifier == "DEFAULT")
        {
            String default_str = queryToString(col_decl.default_expression);
            if (type == "string")
            {
                default_str = default_str.substr(1, default_str.length() - 2);
            }

            column_mapping_json.set("default", default_str);
        }
        else if (col_decl.default_specifier == "ALIAS")
        {
            column_mapping_json.set("alias", queryToString(col_decl.default_expression));
        }
    }

    if (col_decl.comment)
    {
        const String & comment = queryToString(col_decl.comment);
        column_mapping_json.set("comment", comment.substr(1, comment.length() - 2));
    }

    if (col_decl.codec)
    {
        column_mapping_json.set("codec", queryToString(col_decl.codec));
    }

    if (col_decl.ttl)
    {
        column_mapping_json.set("ttl", queryToString(col_decl.ttl));
    }
}

String JSONToString(const Poco::JSON::Object & json)
{
    std::stringstream str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    json.stringify(str_stream, 0);
    return str_stream.str();
}
}
