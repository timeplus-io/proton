#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{
class InterpreterShowFunctionsQuery : public IInterpreter, WithContext
{
public:
    InterpreterShowFunctionsQuery(const ASTPtr & query_ptr_, ContextPtr context_) : WithContext(context_), query_ptr(query_ptr_) { }

    BlockIO execute() override;

    bool ignoreQuota() const override { return true; }
    bool ignoreLimits() const override { return true; }

private:
    QueryPipeline executeImpl();

    ASTPtr query_ptr;
};
}
