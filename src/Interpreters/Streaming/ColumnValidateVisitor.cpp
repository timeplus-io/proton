#include "ColumnValidateVisitor.h"

#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTFunction.h>
#include <Common/ProtonCommon.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

namespace Streaming
{

void ColumnValidateMatcher::visit(ASTPtr & ast, ColumnValidateMatcher::Data & data)
{
    if (auto * column = ast->as<ASTColumnDeclaration>())
        visit(*column, data);
    else if (auto * node = ast->as<ASTCreateQuery>())
        visit(*node, data);
}

bool ColumnValidateMatcher::needChildVisit(
    const ASTPtr & node, [[maybe_unused]] const ASTPtr & child, const ColumnValidateMatcher::Data & data)
{
    return !data.found_time && !node->as<ASTColumnDeclaration>();
}

void ColumnValidateMatcher::visit(ASTCreateQuery & node, ColumnValidateMatcher::Data & data)
{
    if (node.storage && node.storage->engine && !node.storage->engine->name.compare("Stream"))
    {
        data.is_stream = true;
    }
}

void ColumnValidateMatcher::visit(ASTColumnDeclaration & column, ColumnValidateMatcher::Data & data)
{
    if (!column.name.compare(ProtonConsts::RESERVED_EVENT_TIME))
    {
        /// FIXME, ALIAS column check. column.default_specifier = "ALIAS", type of default_expression
        if (!column.type)
        {
            data.found_time = true;
            return;
        }

        auto * func = column.type->as<ASTFunction>();
        if (data.is_stream && (!func || func->name.compare("datetime64")))
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "The type of {} column must be datetime64, but got {}",
                ProtonConsts::RESERVED_EVENT_TIME,
                func->name);
        else
            data.found_time = true;
    }
}
}
}
