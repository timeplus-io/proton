#include "StreamColumnValidateVisitor.h"

#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTFunction.h>
#include <Common/ProtonCommon.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

void StreamColumnValidateMatcher::visit(ASTPtr & ast, StreamColumnValidateMatcher::Data & data)
{
    if (auto * column = ast->as<ASTColumnDeclaration>())
        visit(*column, data);
    else if (auto * node = ast->as<ASTCreateQuery>())
        visit(*node, data);
}

bool StreamColumnValidateMatcher::needChildVisit(
    const ASTPtr & node, [[maybe_unused]] const ASTPtr & child, const StreamColumnValidateMatcher::Data & data)
{
    return !data.found_time && !node->as<ASTColumnDeclaration>();
}

void StreamColumnValidateMatcher::visit(ASTCreateQuery & node, StreamColumnValidateMatcher::Data & data)
{
    if (node.storage && node.storage->engine && !node.storage->engine->name.compare("Stream"))
    {
        data.is_stream = true;
    }
}

void StreamColumnValidateMatcher::visit(ASTColumnDeclaration & column, StreamColumnValidateMatcher::Data & data)
{
    if (!column.name.compare(RESERVED_EVENT_TIME))
    {
        auto * func = column.type->as<ASTFunction>();
        if (data.is_stream && (!func || func->name.compare("DateTime64")))
        {
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN, "The type of {} column must be DateTime64, but got {}", RESERVED_EVENT_TIME, func->name);
        }
        else
        {
            data.found_time = true;
        }
    }
}


}
