#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

/** Return single row with single column "statement" of type String with text of query to CREATE specified format schema.
  */
class InterpreterShowCreateFormatSchemaQuery : public IInterpreter, WithContext
{
public:
    InterpreterShowCreateFormatSchemaQuery(const ASTPtr & query_ptr_, ContextPtr context_) : WithContext(context_), query_ptr(query_ptr_) {}

    BlockIO execute() override;

    static Block getSampleBlock();

private:
    ASTPtr query_ptr;

    QueryPipeline executeImpl();
};


}
