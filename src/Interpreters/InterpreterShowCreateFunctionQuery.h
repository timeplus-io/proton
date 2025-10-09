#pragma once

#include <Interpreters/IInterpreter.h>

namespace DB
{

class InterpreterShowCreateFunctionQuery : public IInterpreter, WithContext
{
    ///  Return single row with single column "statement" of type String with text of query to SHOW CREATE specified function.

public:
    InterpreterShowCreateFunctionQuery(const ASTPtr & query_ptr_, ContextPtr context_) : WithContext(context_), query_ptr(query_ptr_) { }

    BlockIO execute() override;

    Block getSampleBlock() const;

private:
    ASTPtr query_ptr;

    QueryPipeline executeImpl();
    QueryPipeline showMultiVersions(const String & name) const;
};
}
