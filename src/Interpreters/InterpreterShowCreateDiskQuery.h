#pragma once
#include <Interpreters/IInterpreter.h>

namespace DB
{
class InterpreterShowCreateDiskQuery : public IInterpreter, WithContext
{
public:
    InterpreterShowCreateDiskQuery(const ASTPtr & query_ptr_, ContextPtr context_)
        : WithContext(context_), query_ptr(query_ptr_) {}

    BlockIO execute() override;
    Block getSampleBlock() const;

private:
    ASTPtr query_ptr;
    QueryPipeline executeImpl();
};
}
