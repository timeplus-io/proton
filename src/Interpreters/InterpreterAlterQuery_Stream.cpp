#include <Interpreters/InterpreterAlterQuery.h>
#include <Parsers/ASTAlterQuery.h>
#include <Storages/IStorage.h>
#include <Storages/Stream/storageUtil.h>

#include <unordered_set>

namespace DB
{
namespace
{

const std::unordered_set<ASTAlterCommand::Type> ENABLED_COMMANDS{
    ASTAlterCommand::Type::DROP_PARTITION,

    ASTAlterCommand::Type::MODIFY_TTL,
    ASTAlterCommand::Type::REMOVE_TTL,

    ASTAlterCommand::Type::MODIFY_SETTING,
    ASTAlterCommand::Type::RESET_SETTING,

    ASTAlterCommand::Type::MODIFY_DATABASE_SETTING,

    ASTAlterCommand::Type::MODIFY_QUERY,
    ASTAlterCommand::Type::MODIFY_QUERY_SETTING,
    ASTAlterCommand::Type::RESET_QUERY_SETTING,

    ASTAlterCommand::Type::ADD_COLUMN,
    ASTAlterCommand::Type::RENAME_COLUMN,

    ASTAlterCommand::Type::ADD_INDEX,
    ASTAlterCommand::Type::MATERIALIZE_INDEX,
    ASTAlterCommand::Type::DROP_INDEX,

    ASTAlterCommand::Type::UPDATE, /// lightweight delete required

    ASTAlterCommand::Type::MODIFY_COMMENT
};
}

void InterpreterAlterQuery::checkAlterCommandForStreaming(StoragePtr table, const ASTAlterCommand * command_ast) const
{
    if (isStreamingStorage(table, getContext()))
    {
        if (!ENABLED_COMMANDS.contains(command_ast->type))
            throw DB::Exception(
                ErrorCodes::NOT_IMPLEMENTED, "ALTER STREAM command '{}' is not allowed", ASTAlterCommand::typeToString(command_ast->type));

        if (command_ast->type == ASTAlterCommand::Type::ADD_INDEX)
        {
            if (table->getName() != "Stream")
                throw DB::Exception(ErrorCodes::NOT_IMPLEMENTED, "ADD_INDEX is not supported by storage {}", table->getName());
        }
    }
}

}
