#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{
namespace Streaming
{
/// Pause a running materialized view query
class InterpreterPauseMaterializedViewQuery final : public IInterpreter, WithMutableContext
{
public:
    InterpreterPauseMaterializedViewQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_) : WithMutableContext(context_), query_ptr(query_ptr_) { }

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};

}
}
