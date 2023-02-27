#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSetQuery.h>

#include <Parsers/CommonParsers.h>
#include <Parsers/ParserSetQuery.h>

#include <Core/Names.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Common/FieldVisitorToString.h>
#include <Common/SettingsChanges.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

static NameToNameMap::value_type convertToQueryParameter(SettingChange change)
{
    auto name = change.name.substr(strlen(QUERY_PARAMETER_NAME_PREFIX));
    if (name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter name cannot be empty");

    auto value = applyVisitor(FieldVisitorToString(), change.value);
    /// writeQuoted is not always quoted in line with SQL standard https://github.com/ClickHouse/ClickHouse/blob/master/src/IO/WriteHelpers.h
    if (value.starts_with('\''))
    {
        ReadBufferFromOwnString buf(value);
        readQuoted(value, buf);
    }
    return {name, value};
}

/// Parse `name = value`.
bool ParserSetQuery::parseNameValuePair(SettingChange & change, IParser::Pos & pos, Expected & expected)
{
    ParserCompoundIdentifier name_p;
    ParserLiteral value_p;
    ParserToken s_eq(TokenType::Equals);

    ASTPtr name;
    ASTPtr value;

    if (!name_p.parse(pos, name, expected))
        return false;

    if (!s_eq.ignore(pos, expected))
        return false;

    if (ParserKeyword("TRUE").ignore(pos, expected))
        value = std::make_shared<ASTLiteral>(Field(UInt64(1)));
    else if (ParserKeyword("FALSE").ignore(pos, expected))
        value = std::make_shared<ASTLiteral>(Field(UInt64(0)));
    else if (!value_p.parse(pos, value, expected))
        return false;

    tryGetIdentifierNameInto(name, change.name);
    change.value = value->as<ASTLiteral &>().value;

    return true;
}


bool ParserSetQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    ParserToken s_comma(TokenType::Comma);

    if (!parse_only_internals)
    {
        ParserKeyword s_set("SET");

        if (!s_set.ignore(pos, expected))
            return false;

        /// Parse SET TRANSACTION ... queries using ParserTransactionControl
        if (ParserKeyword{"TRANSACTION"}.check(pos, expected))
            return false;
    }

    SettingsChanges changes;
    NameToNameMap query_parameters;

    while (true)
    {
        if ((!changes.empty() || !query_parameters.empty()) && !s_comma.ignore(pos))
            break;

        /// Either a setting or a parameter for prepared statement (if name starts with QUERY_PARAMETER_NAME_PREFIX)
        SettingChange current;

        if (!parseNameValuePair(current, pos, expected))
            return false;

        if (current.name.starts_with(QUERY_PARAMETER_NAME_PREFIX))
            query_parameters.emplace(convertToQueryParameter(std::move(current)));
        else
            changes.push_back(std::move(current));
    }

    auto query = std::make_shared<ASTSetQuery>();
    node = query;

    query->is_standalone = !parse_only_internals;
    query->changes = std::move(changes);
    query->query_parameters = std::move(query_parameters);

    return true;
}


}
