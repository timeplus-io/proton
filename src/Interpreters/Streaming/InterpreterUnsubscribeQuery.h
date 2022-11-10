#pragma once

#include <Interpreters/IInterpreter.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{
namespace Streaming
{
/// Unsubscribe a streaming query means
/// 1. If the streaming query is currently running / subscribed, kill it first
/// 2. Delete the corresponding checkpoints
class InterpreterUnsubscribeQuery final : public IInterpreter, WithMutableContext
{
public:
    InterpreterUnsubscribeQuery(const ASTPtr & query_ptr_, ContextMutablePtr context_) : WithMutableContext(context_), query_ptr(query_ptr_) { }

    BlockIO execute() override;

private:
    ASTPtr query_ptr;
};

}
}
