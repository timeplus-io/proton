#include <Interpreters/DistributedMergeTreeColumnValidateVisitor.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTFunction.h>
#include <Common/ProtonCommon.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

void DistributedMergeTreeColumnValidateMatcher::visit(ASTPtr & ast, DistributedMergeTreeColumnValidateMatcher::Data & data)
{
    if (auto * column = ast->as<ASTColumnDeclaration>())
        visit(*column, data);
    else if (auto * node = ast->as<ASTCreateQuery>())
        visit(*node, data);
}

bool DistributedMergeTreeColumnValidateMatcher::needChildVisit(
    const ASTPtr & node, [[maybe_unused]] const ASTPtr & child, const DistributedMergeTreeColumnValidateMatcher::Data & data)
{
    return !data.found_time && !node->as<ASTColumnDeclaration>();
}

void DistributedMergeTreeColumnValidateMatcher::visit(ASTCreateQuery & node, DistributedMergeTreeColumnValidateMatcher::Data & data)
{
    if (node.storage && node.storage->engine && !node.storage->engine->name.compare("DistributedMergeTree"))
    {
        data.is_distributed_merge_tree = true;
    }
}

void DistributedMergeTreeColumnValidateMatcher::visit(ASTColumnDeclaration & column, DistributedMergeTreeColumnValidateMatcher::Data & data)
{
    if (!column.name.compare(RESERVED_EVENT_TIME))
    {
        auto * func = column.type->as<ASTFunction>();
        if (data.is_distributed_merge_tree && (!func || func->name.compare("DateTime64")))
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
