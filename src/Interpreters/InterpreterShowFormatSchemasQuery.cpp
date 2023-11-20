#include <Access/Common/AccessRightsElement.h>
#include <Access/Common/AccessType.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Formats/FormatSchemaFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterShowFormatSchemasQuery.h>
#include <Parsers/ASTShowFormatSchemasQuery.h>
#include <Processors/Sources/SourceFromSingleChunk.h>

namespace DB
{

BlockIO InterpreterShowFormatSchemasQuery::execute()
{
    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::SHOW_FORMAT_SCHEMAS);
    getContext()->checkAccess(access_rights_elements);

    BlockIO res;
    res.pipeline = executeImpl();
    return res;
}


QueryPipeline InterpreterShowFormatSchemasQuery::executeImpl()
{
    auto & query = query_ptr->as<ASTShowFormatSchemasQuery &>();

    /// Build the result column.
    MutableColumnPtr name_col = ColumnString::create();
    MutableColumnPtr type_col = ColumnString::create();
    for (const auto & schema_entry : FormatSchemaFactory::instance().getSchemasList(getContext(), query.schema_type))
    {
        name_col->insert(schema_entry.name);
        type_col->insert(schema_entry.type);
    }

    return QueryPipeline(std::make_shared<SourceFromSingleChunk>(Block{
        {std::move(name_col), std::make_shared<DataTypeString>(), "name"},
        {std::move(type_col), std::make_shared<DataTypeString>(), "type"},
    }));
}

}
