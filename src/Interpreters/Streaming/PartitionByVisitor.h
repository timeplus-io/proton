#pragma once

#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{
class ASTSelectQuery;
class ASTFunction;

class PartitionByMatcher
{
public:
    using Visitor = InDepthNodeVisitor<PartitionByMatcher, true, true>;

    struct Data
    {
        const bool streaming;
        ContextPtr context;

        std::vector<ASTFunction *> aggregate_functions {};
        std::vector<ASTFunction *> stateful_functions {};
        std::vector<ASTFunction *> aggr_over_functions {};
        std::vector<ASTFunction *> stateful_over_functions {};

        Data(bool streaming_, ContextPtr context_) : streaming(streaming_), context(context_) { }
        void reset()
        {
            aggregate_functions.clear();
            stateful_functions.clear();
            aggr_over_functions.clear();
            stateful_over_functions.clear();
        }
    };

    static void visit(ASTPtr & ast, Data & data);
    static bool needChildVisit(ASTPtr & node, ASTPtr & child, Data & data);

private:
    static void rewriteStatefulOverFunction(ASTSelectQuery & select, Data & data);
    static void rewriteByPartitionBy(ASTSelectQuery & select, Data & data);
};

/// Visits AST nodes and collect their aliases in one map (with links to source nodes).
using PartitionByVisitor = PartitionByMatcher::Visitor;
}
