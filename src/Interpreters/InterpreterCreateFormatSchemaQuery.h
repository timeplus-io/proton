#pragma once

#include <Interpreters/IInterpreter.h>


namespace DB
{

class InterpreterCreateFormatSchemaQuery : public IInterpreter, WithContext
{
public:
    InterpreterCreateFormatSchemaQuery(const ASTPtr & query_ptr_, ContextPtr context_)
        : WithContext(context_), query_ptr(query_ptr_)
    {
    }

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};

}
