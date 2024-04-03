#pragma once

#include <Interpreters/IInterpreter.h>


namespace DB
{

class Context;

class InterpreterCreateFunctionQuery : public IInterpreter, WithMutableContext
{
public:
    InterpreterCreateFunctionQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_)
        : WithMutableContext(context_), query_ptr(query_ptr_)
    {
    }

    BlockIO execute() override;

private:
    ASTPtr query_ptr;

    /// proton: starts
    BlockIO handleJavaScriptUDF(bool throw_if_exists, bool replace_if_exists);
    BlockIO handlePythonUDF(bool throw_if_exists, bool replace_if_exists);
    /// proton: ends
};

}