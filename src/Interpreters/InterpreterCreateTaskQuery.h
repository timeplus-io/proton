#pragma once

#include <Interpreters/IInterpreter.h>


namespace DB
{

class InterpreterCreateTaskQuery : public IInterpreter, WithContext
{
public:
    InterpreterCreateTaskQuery(ASTPtr query_ptr_, ContextPtr context_) : WithContext(context_), query_ptr(std::move(query_ptr_)) { }

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};

}
