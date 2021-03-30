#pragma once

#include <Interpreters/Context.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Interpreters/TimeParam.h>
#include <Parsers/IAST_fwd.h>


/// Visit substitutions in a query, replace ASTQueryParameter with ASTLiteral.
/// Rebuild ASTIdentifiers if some parts are ASTQueryParameter.
namespace DB
{
class ASTSelectQuery;

class AddTimeVisitorMatcher
{
public:
    using Data = Context;

    static void visit(ASTPtr & ast, Context & context);
    static bool needChildVisit(ASTPtr &, ASTPtr &) { return false; }

private:
    static void visitSelectQuery(ASTPtr & ast, Context & context);
    static void visitSelectWithUnionQuery(ASTPtr & ast, Context & context);
    static void insertTimeParamTime(ASTSelectQuery * select, ASTPtr & table_name, Context & context);
    static bool containTimeField(ASTPtr & table_identifier_node, Context & context);
};

using AddTimeParamVisitor = InDepthNodeVisitor<AddTimeVisitorMatcher, false>;
}
