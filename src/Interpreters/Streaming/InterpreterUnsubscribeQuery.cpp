#include <Interpreters/Streaming/InterpreterUnsubscribeQuery.h>

#include <Bootstrap/Globals.h>
#include <Checkpoint/CheckpointContext.h>
#include <Checkpoint/CheckpointCoordinator.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/Streaming/ASTUnsubscribeQuery.h>

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
        throw Exception(
            ErrorCodes::SYNTAX_ERROR, "Unsubscribe query requires a string query ID, for example, UNSUBSCRIBE TO 'my-query-id'");

    auto query_id = literal->value.get<String>();

    if (!query_id.empty())
    {
        /// Unsubscribe query only supports with local filesystem checkpoint storage
        auto & coordinator = Globals::getCheckpointCoordinator();
        auto ckpt_ctx = std::make_shared<CheckpointContext>(query_id, coordinator.getCheckpointStorage(CheckpointStorageType::LocalFileSystem), &coordinator);
        coordinator.removeCheckpoint(std::move(ckpt_ctx));
    }

    return {};
}
}
}
