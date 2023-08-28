#include "ColumnDefinition.h"

#include <Interpreters/DatabaseCatalog.h>
#include <Storages/IStorage.h>

#include <boost/algorithm/string/join.hpp>
#include <fmt/format.h>

namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
}

String getCreateColumnDefinition(const Poco::JSON::Object::Ptr & column)
{
    std::vector<String> column_definition;

    column_definition.push_back(fmt::format("`{}`", column->get("name").toString()));
    if (column->has("nullable") && column->get("nullable"))
        column_definition.push_back(fmt::format(" nullable({})", column->get("type").toString()));
    else
        column_definition.push_back(fmt::format(" {}", column->get("type").toString()));

    if (column->has("default"))
    {
        String default_str = column->get("default").toString();

        if (!default_str.empty())
        {
            if (column->get("type").toString() == "string")
                default_str = fmt::format("'{}'", default_str);

            column_definition.push_back(fmt::format(" DEFAULT {}", default_str));
        }
    }
    else if (column->has("alias"))
        column_definition.push_back(fmt::format(" ALIAS `{}`", column->get("alias").toString()));

    if (column->has("comment"))
        column_definition.push_back(fmt::format(" COMMENT '{}'", column->get("comment").toString()));

    if (column->has("compression_codec"))
        column_definition.push_back(fmt::format(" CODEC({})", column->get("compression_codec").toString()));

    if (column->has("ttl_expression"))
        column_definition.push_back(fmt::format(" TTL {}", column->get("ttl_expression").toString()));

    if (column->has("skipping_index_expression"))
        column_definition.push_back(fmt::format(", {}", column->get("skipping_index_expression").toString()));

    return boost::algorithm::join(column_definition, " ");
}

String getUpdateColumnDefinition(
    ContextPtr ctx, const Poco::JSON::Object::Ptr & payload, const String & database, const String & table, String & column)
{
    std::vector<String> update_segments;
    if (payload->has("name"))
    {
        auto new_name = payload->get("name").toString();
        if (column != new_name)
        {
            update_segments.push_back(fmt::format("RENAME COLUMN `{}` TO `{}`", column, new_name));
            /// Ignore other changes as rename can only be execute alone
            return boost::algorithm::join(update_segments, " ");
        }
    }
    update_segments.push_back(fmt::format("MODIFY COLUMN `{}`", column));

    if (payload->has("type"))
        update_segments.push_back(fmt::format(" {}", payload->get("type").toString()));

    if (payload->has("default"))
    {
        auto storage = DatabaseCatalog::instance().tryGetTable({database, table}, ctx);
        auto column_names_and_types{storage->getInMemoryMetadata().getColumns().getOrdinary()};
        if (!column_names_and_types.contains(column))
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "the column definitions of '{}.{}' stream does not contain the column '{}', it cannot be updated",
                database,
                table,
                column);

        String default_str = payload->get("default").toString(
            std::find_if(column_names_and_types.begin(), column_names_and_types.end(), [&](NameAndTypePair & col) -> bool {
                if (col.name == column && (col.type->getTypeId() == TypeIndex::String || col.type->getTypeId() == TypeIndex::FixedString))
                {
                    default_str = fmt::format("'{}'", default_str);
                    return true;
                }
                return false;
            }));

        update_segments.push_back(fmt::format("DEFAULT {}", default_str));
    }
    else if (payload->has("alias"))
        update_segments.push_back(fmt::format("ALIAS `{}`", payload->get("alias").toString()));

    if (payload->has("comment"))
        update_segments.push_back(fmt::format("COMMENT '{}'", payload->get("comment").toString()));

    if (payload->has("ttl_expression"))
        update_segments.push_back(fmt::format("TTL {}", payload->get("ttl_expression").toString()));

    if (payload->has("compression_codec"))
        update_segments.push_back(fmt::format("CODEC({})", payload->get("compression_codec").toString()));

    return boost::algorithm::join(update_segments, " ");
}

}
