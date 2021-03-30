#pragma once

#include <unordered_map>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/IAST.h>

namespace DB
{
class EliminateSubqueryVisitorData
{
public:
    using TypeToVisit = ASTSelectQuery;
    using Matcher = OneTypeMatcher<EliminateSubqueryVisitorData, NeedChild::none>;
    using Visitor = InDepthNodeVisitor<Matcher, false>;
    void visit(ASTSelectQuery & node, ASTPtr &);

private:
    void visit(ASTTableExpression & table, ASTSelectQuery & parent_select);
    void rewriteColumns(ASTPtr & ast, const std::unordered_map<String, ASTPtr> & child_select, bool drop_alias = false);
    bool mergeColumns(ASTSelectQuery & parent_query, ASTSelectQuery & child_query);
};

using EliminateSubqueryVisitor = EliminateSubqueryVisitorData::Visitor;

}
