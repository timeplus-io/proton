#pragma once

#include <Core/QueryProcessingStage.h>
#include <Interpreters/IInterpreter.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/IInterpreterUnionOrSelectQuery.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>


namespace DB
{

class Context;
class InterpreterSelectQuery;
class QueryPlan;

class InterpreterSelectIntersectExceptQuery : public IInterpreterUnionOrSelectQuery
{
using Operator = ASTSelectIntersectExceptQuery::Operator;

public:
    InterpreterSelectIntersectExceptQuery(
        const ASTPtr & query_ptr_,
        ContextPtr context_,
        const SelectQueryOptions & options_);

    BlockIO execute() override;

    Block getSampleBlock() { return result_header; }

    void ignoreWithTotals() override;

    /// proton: starts
    bool hasAggregation() const override;
    bool isStreaming() const override;
    bool hasGlobalAggregation() const override;
    bool hasStreamingWindowFunc() const override;

    ColumnsDescriptionPtr getExtendedObjects() const override;
    /// proton: ends

private:
    static String getName() { return "SelectIntersectExceptQuery"; }

    std::unique_ptr<IInterpreterUnionOrSelectQuery>
    buildCurrentChildInterpreter(const ASTPtr & ast_ptr_);

    void buildQueryPlan(QueryPlan & query_plan) override;

    std::vector<std::unique_ptr<IInterpreterUnionOrSelectQuery>> nested_interpreters;

    Operator final_operator;
};

}
