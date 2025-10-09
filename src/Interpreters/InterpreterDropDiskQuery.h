#pragma once

#include <Interpreters/IInterpreter.h>

namespace DB
{

class Context;

class InterpreterDropDiskQuery : public IInterpreter, WithContext
{
public:
    InterpreterDropDiskQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_) : WithContext(context_), query_ptr(query_ptr_) {}

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};

}
