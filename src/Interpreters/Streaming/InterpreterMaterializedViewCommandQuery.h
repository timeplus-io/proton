#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{
namespace Streaming
{
class InterpreterMaterializedViewCommandQuery final : public IInterpreter, WithMutableContext
{
public:
    InterpreterMaterializedViewCommandQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_) : WithMutableContext(context_), query_ptr(query_ptr_) { }

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};

}
}
