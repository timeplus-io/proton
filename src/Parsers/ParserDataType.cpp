#include <Parsers/ParserDataType.h>

#include <boost/algorithm/string/case_conv.hpp>
#include <Parsers/ASTDataType.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTObjectTypeArgument.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserCreateQuery.h>

///proton: starts
#include <Common/ClickHouseCompatibleFlag.h>
///proton: ends


namespace DB
{

namespace
{

/// Parser of Dynamic type argument: Dynamic(max_types=N)
class DynamicArgumentParser : public IParserBase
{
private:
    const char * getName() const override { return "Dynamic data type optional argument"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint=true) override
    {
        ASTPtr identifier;
        ParserIdentifier identifier_parser;
        if (!identifier_parser.parse(pos, identifier, expected))
            return false;

        if (pos->type != TokenType::Equals)
        {
            expected.add(pos, "equals operator");
            return false;
        }

        ++pos;

        ASTPtr number;
        ParserNumber number_parser;
        if (!number_parser.parse(pos, number, expected))
            return false;

        node = makeASTFunction("equals", identifier, number);
        return true;
    }
};

/// Parser of Object type argument. For example: JSON(some_parameter=N, some.path SomeType, SKIP skip.path, ...)
class ObjectArgumentParser : public IParserBase
{
private:
    const char * getName() const override { return "JSON data type optional argument"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint=true) override
    {
        auto argument = std::make_shared<ASTObjectTypeArgument>();

        /// SKIP arguments
        if (ParserKeyword(Keyword::SKIP).ignore(pos))
        {
            /// SKIP REGEXP '<some_regexp>'
            if (ParserKeyword(Keyword::REGEXP).ignore(pos))
            {
                ParserStringLiteral literal_parser;
                ASTPtr literal;
                if (!literal_parser.parse(pos, literal, expected))
                    return false;
                argument->skip_path_regexp = literal;
                argument->children.push_back(argument->skip_path_regexp);
            }
            /// SKIP some.path
            else
            {
                ParserCompoundIdentifier compound_identifier_parser;
                ASTPtr compound_identifier;
                if (!compound_identifier_parser.parse(pos, compound_identifier, expected))
                    return false;

                argument->skip_path = compound_identifier;
                argument->children.push_back(argument->skip_path);
            }

            node = argument;
            return true;
        }

        ParserCompoundIdentifier compound_identifier_parser;
        ASTPtr identifier;
        if (!compound_identifier_parser.parse(pos, identifier, expected))
            return false;

        /// some_parameter=N
        if (pos->type == TokenType::Equals)
        {
            ++pos;
            ASTPtr number;
            ParserNumber number_parser;
            if (!number_parser.parse(pos, number, expected))
                return false;

            argument->parameter = makeASTFunction("equals", identifier, number);
            argument->children.push_back(argument->parameter);
            node = argument;
            return true;
        }

        ParserDataType type_parser;
        ASTPtr type;
        if (!type_parser.parse(pos, type, expected))
            return false;

        auto name_and_type = std::make_shared<ASTNameTypePair>();
        name_and_type->name = getIdentifierName(identifier);
        name_and_type->type = type;
        name_and_type->children.push_back(name_and_type->type);
        argument->path_with_type = name_and_type;
        argument->children.push_back(argument->path_with_type);
        node = argument;
        return true;
    }
};

}

bool ParserDataType::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    String type_name;

    ParserIdentifier name_parser;
    ASTPtr identifier;
    if (!name_parser.parse(pos, identifier, expected))
        return false;
    tryGetIdentifierNameInto(identifier, type_name);

    /// proton: starts
    if (isClickHouseCompatibleMode())
    {
        String type_name_lower = Poco::toLower(type_name);
        /// There are special type that cannot be directly changed to lowercase and need handle specially.
        if (type_name_lower == "lowcardinality")
            type_name_lower = "low_cardinality";
        else if (type_name_lower == "fixedstring")
            type_name_lower = "fixed_string";
        type_name = type_name_lower;
    }
    /// proton: ends

    String type_name_upper = Poco::toUpper(type_name);
    String type_name_suffix;

    /// Special cases for compatibility with SQL standard. We can parse several words as type name
    /// only for certain first words, otherwise we don't know how many words to parse
    if (type_name_upper == "NATIONAL")
    {
        if (ParserKeyword("CHARACTER LARGE OBJECT").ignore(pos))
            type_name_suffix = "CHARACTER LARGE OBJECT";
        else if (ParserKeyword("CHARACTER VARYING").ignore(pos))
            type_name_suffix = "CHARACTER VARYING";
        else if (ParserKeyword("CHAR VARYING").ignore(pos))
            type_name_suffix = "CHAR VARYING";
        else if (ParserKeyword("CHARACTER").ignore(pos))
            type_name_suffix = "CHARACTER";
        else if (ParserKeyword("CHAR").ignore(pos))
            type_name_suffix = "CHAR";
    }
    else if (type_name_upper == "BINARY" ||
             type_name_upper == "CHARACTER" ||
             type_name_upper == "CHAR" ||
             type_name_upper == "NCHAR")
    {
        if (ParserKeyword("LARGE OBJECT").ignore(pos))
            type_name_suffix = "LARGE OBJECT";
        else if (ParserKeyword("VARYING").ignore(pos))
            type_name_suffix = "VARYING";
    }
    else if (type_name_upper == "DOUBLE")
    {
        if (ParserKeyword("PRECISION").ignore(pos))
            type_name_suffix = "PRECISION";
    }
    else if (type_name_upper.find("INT") != std::string::npos)
    {
        /// Support SIGNED and UNSIGNED integer type modifiers for compatibility with MySQL
        if (ParserKeyword("SIGNED").ignore(pos))
            type_name_suffix = "SIGNED";
        else if (ParserKeyword("UNSIGNED").ignore(pos))
            type_name_suffix = "UNSIGNED";
    }

    if (!type_name_suffix.empty())
        type_name = type_name_upper + " " + type_name_suffix;

    /// skip trailing comma in types, e.g. Tuple(Int, String,)
    if (pos->type == TokenType::Comma)
    {
        Expected test_expected;
        auto test_pos = pos;
        ++test_pos;
        if (ParserToken(TokenType::ClosingRoundBracket).ignore(test_pos, test_expected))
        { // the end of the type definition was reached and there was a trailing comma
            ++pos;
        }
    }

    auto data_type_node = std::make_shared<ASTDataType>();
    data_type_node->name = type_name;

    if (pos->type != TokenType::OpeningRoundBracket)
    {
        node = data_type_node;
        return true;
    }
    ++pos;

    /// Parse optional parameters
    ASTPtr expr_list_args = std::make_shared<ASTExpressionList>();

    /// Allow mixed lists of nested and normal types.
    /// Parameters are either:
    /// - Nested table element;
    /// - Tuple element
    /// - Enum element in form of 'a' = 1;
    /// - literal;
    /// - Dynamic type argument;
    /// - JSON type argument;
    /// - another data type (or identifier);

    size_t arg_num = 0;
    bool have_version_of_aggregate_function = false;
    while (true)
    {
        if (arg_num > 0)
        {
            if (pos->type == TokenType::Comma)
                ++pos;
            else
                break;
        }

        ASTPtr arg;
        if (type_name == "dynamic")
        {
            DynamicArgumentParser parser;
            parser.parse(pos, arg, expected);
        }
        else if (type_name == "json")
        {
            ObjectArgumentParser parser;
            parser.parse(pos, arg, expected);
        }
        else if (type_name == "nested")
        {
            ParserNameTypePair name_and_type_parser;
            name_and_type_parser.parse(pos, arg, expected);
        }
        else if (type_name == "tuple")
        {
            ParserNameTypePair name_and_type_parser;
            ParserDataType only_type_parser;
            name_and_type_parser.parse(pos, arg, expected) || only_type_parser.parse(pos, arg, expected);
        }
        else if (type_name == "aggregate_function" || type_name == "simple_aggregate_function")
        {
            /// This is less trivial.
            /// The first optional argument for AggregateFunction is a numeric literal, defining the version.
            /// The next argument is the function name, optionally with parameters.
            /// Subsequent arguments are data types.

            if (arg_num == 0 && type_name == "aggregate_function")
            {
                ParserUnsignedInteger version_parser;
                if (version_parser.parse(pos, arg, expected))
                {
                    have_version_of_aggregate_function = true;
                    expr_list_args->children.emplace_back(std::move(arg));
                    ++arg_num;
                    continue;
                }
            }

            if (arg_num == (have_version_of_aggregate_function ? 1 : 0))
            {
                ParserFunction function_parser;
                ParserIdentifier identifier_parser;
                function_parser.parse(pos, arg, expected)
                    || identifier_parser.parse(pos, arg, expected);
            }
            else
            {
                ParserDataType data_type_parser;
                data_type_parser.parse(pos, arg, expected);
            }
        }
        else
        {
            ParserDataType data_type_parser;
            ParserAllCollectionsOfLiterals literal_parser(false);

            const char * operators[] = {"=", "equals", nullptr};
            ParserLeftAssociativeBinaryOperatorList enum_parser(operators, std::make_unique<ParserLiteral>());

            enum_parser.parse(pos, arg, expected)
               || literal_parser.parse(pos, arg, expected)
               || data_type_parser.parse(pos, arg, expected);
        }

        if (!arg)
            break;

        expr_list_args->children.emplace_back(std::move(arg));
        ++arg_num;
    }

    if (pos->type != TokenType::ClosingRoundBracket)
        return false;
    ++pos;

    data_type_node->arguments = expr_list_args;
    data_type_node->children.push_back(data_type_node->arguments);

    node = data_type_node;
    return true;
}

}
