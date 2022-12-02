#pragma once

#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{
class PartitionByMatcher
{
public:
    using Visitor = InDepthNodeVisitor<PartitionByMatcher, true, true>;

    struct Data
    {
        ContextPtr context;
        ASTPtr win_define;
    };

    static void visit(ASTPtr & ast, Data & data);
    static bool needChildVisit(ASTPtr & node, ASTPtr & child, Data & data);
};

/// Visits AST nodes and collect their aliases in one map (with links to source nodes).
using PartitionByVisitor = PartitionByMatcher::Visitor;
}
