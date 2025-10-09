#include <Parsers/ASTIdentifier.h>
#include <Parsers/ParserAlterQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <Storages/AlterCommands.h>

namespace DB
{

const char * AlterCommand::typeString() const noexcept
{
    switch (type)
    {
        case Type::UNKNOWN:
            return "UNKNOWN";
        case Type::ADD_COLUMN:
            return "ADD_COLUMN";
        case Type::DROP_COLUMN:
            return "DROP_COLUMN";
        case Type::MODIFY_COLUMN:
            return "MODIFY_COLUMN";
        case Type::COMMENT_COLUMN:
            return "COMMENT_COLUMN";
        case Type::MODIFY_ORDER_BY:
            return "MODIFY_ORDER_BY";
        case Type::MODIFY_SAMPLE_BY:
            return "MODIFY_SAMPLE_BY";
        case Type::ADD_INDEX:
            return "ADD_INDEX";
        case Type::DROP_INDEX:
            return "DROP_INDEX";
        case Type::ADD_CONSTRAINT:
            return "ADD_CONSTRAINT";
        case Type::DROP_CONSTRAINT:
            return "DROP_CONSTRAINT";
        case Type::ADD_PROJECTION:
            return "ADD_PROJECTION";
        case Type::DROP_PROJECTION:
            return "DROP_PROJECTION";
        case Type::MODIFY_TTL:
            return "MODIFY_TTL";
        case Type::MODIFY_SETTING:
            return "MODIFY_SETTING";
        case Type::RESET_SETTING:
            return "RESET_SETTING";
        case Type::MODIFY_QUERY:
            return "MODIFY_QUERY";
        case Type::RENAME_COLUMN:
            return "RENAME_COLUMN";
        case Type::REMOVE_TTL:
            return "REMOVE_TTL";
        case Type::MODIFY_DATABASE_SETTING:
            return "MODIFY_DATABASE_SETTING";
        case Type::COMMENT_TABLE:
            return "COMMENT_TABLE";
        case Type::REMOVE_SAMPLE_BY:
            return "REMOVE_SAMPLE_BY";
        case Type::MODIFY_QUERY_SETTING:
            return "MODIFY_QUERY_SETTING";
        case Type::RESET_QUERY_SETTING:
            return "RESET_QUERY_SETTING";
    }
}

std::optional<AlterCommand> AlterCommand::parse(std::string_view query_string)
{
    ParserAlterQuery parser;
    ASTPtr ast = parseQuery(parser, query_string.begin(), query_string.end(), 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);

    auto * alter_query = ast->as<ASTAlterQuery>();
    if (!alter_query || !alter_query->command_list)
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Failed to parse ALTER query or empty command list");

    for (const auto & command_ast : alter_query->command_list->children)
    {
        auto * alter_command = command_ast->as<ASTAlterCommand>();
        if (!alter_command)
            continue;

        auto alter_command_opt = AlterCommand::parse(alter_command);
        if (alter_command_opt)
            return alter_command_opt;
    }

    return std::nullopt;
}

std::string AlterCommand::toASTString(const String & table_name, const String & database_name) const
{
    auto alter_query = std::make_shared<ASTAlterQuery>();
    alter_query->alter_object = ASTAlterQuery::AlterObjectType::STREAM;

    alter_query->table = std::make_shared<ASTIdentifier>(table_name);
    alter_query->database = std::make_shared<ASTIdentifier>(database_name);

    auto expression_list = std::make_shared<ASTExpressionList>();
    alter_query->command_list = expression_list.get();
    alter_query->command_list->children.push_back(toAST());

    return queryToString(alter_query);
}

std::shared_ptr<ASTAlterCommand> AlterCommand::toAST() const
{
    chassert(ast);
    auto alter_cmd = ast->clone();
    return std::static_pointer_cast<ASTAlterCommand>(alter_cmd);
}

std::vector<String> AlterCommands::stringCommands() const
{
    std::vector<String> commands;
    commands.reserve(size());

    for (const auto & command : *this)
        commands.emplace_back(command.typeString());

    return commands;
}

std::vector<String> AlterCommands::astCommands(const StorageID & storage_id) const
{
    std::vector<String> alter_command_asts;
    alter_command_asts.reserve(size());

    for (const auto & alter_command : *this)
        alter_command_asts.push_back(alter_command.toASTString(storage_id.table_name, storage_id.database_name));

    return alter_command_asts;
}

}
