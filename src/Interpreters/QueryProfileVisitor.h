#pragma once

#include <Parsers/IAST_fwd.h>
#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{
class QueryProfileMatcher
{
public:
    using Visitor = InDepthNodeVisitor<QueryProfileMatcher, true>;

    struct Data
    {
        bool has_aggr = false;
        bool has_table_join = false;
        bool has_union = false;
        bool has_subquery = false;

        bool done() { return has_aggr && has_table_join && has_union && has_subquery; }
    };

    static void visit(ASTPtr & ast, Data & data);
    static bool needChildVisit(const ASTPtr & node, const ASTPtr & child);
};


using QueryProfileVisitor = QueryProfileMatcher::Visitor;
}
