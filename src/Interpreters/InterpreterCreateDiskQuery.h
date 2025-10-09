#pragma once

#include <Interpreters/IInterpreter.h>


namespace DB
{

class InterpreterCreateDiskQuery : public IInterpreter, WithContext
{
public:
    InterpreterCreateDiskQuery(const ASTPtr & query_ptr_, ContextPtr context_)
        : WithContext(context_), query_ptr(query_ptr_)
    {
    }

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};

}
