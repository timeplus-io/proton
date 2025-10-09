#include <Storages/MutationCommands.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ParserAlterQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>


namespace DB
{
namespace ErrorCodes
{
extern const int SYNTAX_ERROR;
}

const char * MutationCommand::typeString() const noexcept
{
    switch (type)
    {
        case Type::EMPTY:
            return "EMPTY";
        case Type::DELETE:
            return "DELETE";
        case Type::UPDATE:
            return "UPDATE";
        case Type::MATERIALIZE_INDEX:
            return "MATERIALIZE_INDEX";
        case Type::MATERIALIZE_PROJECTION:
            return "MATERIALIZE_PROJECTION";
        case Type::READ_COLUMN:
            return "READ_COLUMN";
        case Type::DROP_COLUMN:
            return "DROP_COLUMN";
        case Type::DROP_INDEX:
            return "DROP_INDEX";
        case Type::DROP_PROJECTION:
            return "DROP_PROJECTION";
        case Type::MATERIALIZE_TTL:
            return "MATERIALIZE_TTL";
        case Type::RENAME_COLUMN:
            return "RENAME_COLUMN";
        case Type::MATERIALIZE_COLUMN:
            return "MATERIALIZE_COLUMN";
        case Type::ALTER_WITHOUT_MUTATION:
            return "ALTER_WITHOUT_MUTATION";
    }
}

/// proton: starts
std::vector<std::string> MutationCommands::stringCommands() const
{
    std::vector<std::string> commands;
    commands.reserve(size());

    for (const auto & command : *this)
        commands.emplace_back(command.typeString());

    return commands;
}

std::optional<MutationCommands> MutationCommands::fromAlterCommandString(std::string_view query_string)
{
    ParserAlterQuery parser;
    MutationCommands commands;

    auto ast = parseQuery(
        parser, query_string.data(), query_string.data() + query_string.size(), "mutation commands list", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);

    auto * alter_query = ast->as<ASTAlterQuery>();
    if (!alter_query || !alter_query->command_list)
        throw Exception(ErrorCodes::SYNTAX_ERROR, "Failed to parse ALTER query or empty command list");

    for (const auto & command_ast : alter_query->command_list->children)
    {
        auto * alter_command = command_ast->as<ASTAlterCommand>();
        if (!alter_command)
            continue;

        auto command = MutationCommand::parse(alter_command, /*parse_alter_commands=*/true);
        if (command)
            commands.push_back(std::move(*command));
    }

    return commands;
}

String
MutationCommand::toAlterCommandString(const String & table_name, const String & database_name, ASTAlterQuery::AlterObjectType type_) const
{
    auto alter_query = std::make_shared<ASTAlterQuery>();
    alter_query->alter_object = type_;

    alter_query->table = std::make_shared<ASTIdentifier>(table_name);
    alter_query->database = std::make_shared<ASTIdentifier>(database_name);

    auto expression_list = std::make_shared<ASTExpressionList>();
    alter_query->command_list = expression_list.get();
    alter_query->command_list->children.push_back(ast->clone());

    return queryToString(alter_query, true);
}
/// proton: ends

}
