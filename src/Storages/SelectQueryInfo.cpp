#include <Interpreters/TreeRewriter.h>
#include <Parsers/ASTSelectQuery.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{

bool SelectQueryInfo::isFinal() const
{
    /// if (table_expression_modifiers)
    ///    return table_expression_modifiers->hasFinal();

    const auto & select = query->as<ASTSelectQuery &>();
    return select.final();
}

bool SelectQueryInfo::isStreaming() const
{
    if (!syntax_analyzer_result)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Syntax is not analyzed, don't know the query type");

    return syntax_analyzer_result->streaming;
}

}
