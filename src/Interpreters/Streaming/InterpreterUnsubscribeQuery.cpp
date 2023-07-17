#include "InterpreterUnsubscribeQuery.h"

#include <Parsers/Streaming/ASTUnsubscribeQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Checkpoint/CheckpointCoordinator.h>

namespace DB
{
namespace ErrorCodes
{
extern const int SYNTAX_ERROR;
}

namespace Streaming
{
BlockIO InterpreterUnsubscribeQuery::execute()
{
    auto * literal = query_ptr->as<Streaming::ASTUnsubscribeQuery>()->query_id->as<ASTLiteral>();
    if (!literal)
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Unsubscribe query requires a string query ID, for example, UNSUBSCRIBE TO 'my-query-id'");

    auto query_id = literal->value.get<String>();
    if (!query_id.empty())
        CheckpointCoordinator::instance(getContext()).removeCheckpoint(query_id);

    return {};
}
}
}
