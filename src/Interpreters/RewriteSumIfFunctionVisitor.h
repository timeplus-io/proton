#pragma once

#include <unordered_set>

#include <Parsers/IAST.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

class ASTFunction;

/// Rewrite 'sum(if())' and 'sum_if' functions to coun_if.
/// sum_if(1, cond) -> count_if(1, cond)
/// sum(if(cond, 1, 0)) -> count_if(cond)
/// sum(if(cond, 0, 1)) -> count_if(not(cond))
class RewriteSumIfFunctionMatcher
{
public:
    struct Data
    {
    };

    static void visit(ASTPtr & ast, Data &);
    static void visit(const ASTFunction &, ASTPtr & ast, Data &);
    static bool needChildVisit(const ASTPtr &, const ASTPtr &) { return true; }
};

using RewriteSumIfFunctionVisitor = InDepthNodeVisitor<RewriteSumIfFunctionMatcher, false>;
}
