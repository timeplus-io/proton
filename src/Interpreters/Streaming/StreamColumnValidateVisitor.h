#pragma once

#include <Interpreters/InDepthNodeVisitor.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTColumnDeclaration.h>

namespace DB
{

class StreamColumnValidateMatcher
{
public:
    using Visitor = InDepthNodeVisitor<StreamColumnValidateMatcher, true, true>;

    struct Data
    {
        bool found_time = false;
        bool is_stream = false;
    };

    static void visit(ASTPtr & ast, Data & data);
    static bool needChildVisit(const ASTPtr & node, const ASTPtr & child, const Data & data);

private:
    static void visit(ASTCreateQuery & node, Data & data);
    static void visit(ASTColumnDeclaration & column, Data & data);
};

using StreamColumnValidateVisitor = StreamColumnValidateMatcher::Visitor;

}
