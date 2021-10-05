#pragma once

#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/IAST.h>

#include <unordered_map>

namespace DB
{
class UnnestSubqueryVisitorData
{
public:
    using TypeToVisit = ASTSelectQuery;
    using Matcher = OneTypeMatcher<UnnestSubqueryVisitorData, NeedChild::none>;
    using Visitor = InDepthNodeVisitor<Matcher, false>;
    void visit(ASTSelectQuery & node, ASTPtr &);

private:
    void visit(ASTTableExpression & table, ASTSelectQuery & parent_select);
    void rewriteColumn(ASTPtr & ast, std::unordered_map<String, ASTPtr> & child_select, bool drop_alias = false);
    void setParentColumns(
        ASTs & new_parent_selects,
        bool asterisk_in_subquery,
        const ASTs & parent_selects,
        std::unordered_map<String, ASTPtr> & subquery_selects_map,
        const ASTs & subquery_selects);
    bool mergeable(
        const ASTSelectQuery & child_query,
        const ASTs & parent_selects,
        bool & asterisk_in_subquery,
        std::unordered_map<String, ASTPtr> & subquery_selects_map,
        ASTs & subquery_selects);
    bool isValid(const ASTPtr & ast, const std::unordered_map<String, ASTPtr> & subquery_selects_map) const;
};

using UnnestSubqueryVisitor = UnnestSubqueryVisitorData::Visitor;

}
