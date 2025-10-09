#pragma once

#include <Interpreters/IInterpreter.h>


namespace DB
{

class Context;

class InterpreterDropTaskQuery : public IInterpreter, WithContext
{
public:
    InterpreterDropTaskQuery(ASTPtr query_ptr_, ContextMutablePtr context_) : WithContext(context_), query_ptr(std::move(query_ptr_)) { }

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};

}
