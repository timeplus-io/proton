#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

class InterpreterShowTasksQuery : public IInterpreter, WithContext
{
public:
    InterpreterShowTasksQuery(ASTPtr query_ptr_, ContextPtr context_) : WithContext(context_), query_ptr(std::move(query_ptr_)) { }

    BlockIO execute() override;

    [[nodiscard]] bool ignoreQuota() const override { return true; }
    [[nodiscard]] bool ignoreLimits() const override { return true; }

private:
    QueryPipeline executeImpl();

    ASTPtr query_ptr;
};

}
