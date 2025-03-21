#include <cerrno>
#include <cstdlib>

#include <Poco/String.h>

#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <Parsers/DumpASTNode.h>
#include <Common/typeid_cast.h>

#include <Parsers/ASTAssignment.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTColumnsMatcher.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTColumnsTransformers.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTFunctionWithKeyValueArguments.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTOrderByElement.h>
#include <Parsers/ASTQualifiedAsterisk.h>
#include <Parsers/ASTQueryParameter.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTTLElement.h>
#include <Parsers/ASTWindowDefinition.h>

#include <Parsers/parseIdentifierOrStringLiteral.h>
#include <Parsers/parseIntervalKind.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserSelectWithUnionQuery.h>
#include <Parsers/ParserCase.h>

#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserExplainQuery.h>

#include <Parsers/queryToString.h>

#include <Interpreters/StorageID.h>

/// proton: starts.
#include <Parsers/Streaming/ParserIntervalAliasExpression.h>
#include <Parsers/Streaming/ParserSessionRangeComparisonExpressionIfPossible.h>
/// proton: ends.

/// proton: starts
#include <Common/thread_local_is_clickhouse_compatible.h>
/// proton: ends

namespace DB
{
/// proton: starts
class Context;
/// proton: ends

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int SYNTAX_ERROR;
    extern const int LOGICAL_ERROR;
}

/*
 * Build an AST with the following structure:
 *
 * ```
 * SelectWithUnionQuery (children 1)
 *  ExpressionList (children 1)
 *   SelectQuery (children 2)
 *    ExpressionList (children 1)
 *     Asterisk
 *    TablesInSelectQuery (children 1)
 *     TablesInSelectQueryElement (children 1)
 *      TableExpression (children 1)
 *       Function <...>
 * ```
 */
static ASTPtr buildSelectFromTableFunction(const std::shared_ptr<ASTFunction> & ast_function)
{
    auto result_select_query = std::make_shared<ASTSelectWithUnionQuery>();

    {
        auto select_ast = std::make_shared<ASTSelectQuery>();
        select_ast->setExpression(ASTSelectQuery::Expression::SELECT, std::make_shared<ASTExpressionList>());
        select_ast->select()->children.push_back(std::make_shared<ASTAsterisk>());

        auto list_of_selects = std::make_shared<ASTExpressionList>();
        list_of_selects->children.push_back(select_ast);

        result_select_query->children.push_back(std::move(list_of_selects));
        result_select_query->list_of_selects = result_select_query->children.back();

        {
            auto tables = std::make_shared<ASTTablesInSelectQuery>();
            select_ast->setExpression(ASTSelectQuery::Expression::TABLES, tables);
            auto tables_elem = std::make_shared<ASTTablesInSelectQueryElement>();
            auto table_expr = std::make_shared<ASTTableExpression>();
            tables->children.push_back(tables_elem);
            tables_elem->table_expression = table_expr;
            tables_elem->children.push_back(table_expr);

            table_expr->table_function = ast_function;
            table_expr->children.push_back(table_expr->table_function);
        }
    }

    return result_select_query;
}

bool ParserArray::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    ASTPtr contents_node;
    ParserExpressionList contents(false);

    if (pos->type != TokenType::OpeningSquareBracket)
        return false;
    ++pos;

    if (!contents.parse(pos, contents_node, expected))
        return false;

    if (pos->type != TokenType::ClosingSquareBracket)
        return false;
    ++pos;

    auto function_node = std::make_shared<ASTFunction>();
    function_node->name = "array_cast";
    function_node->arguments = contents_node;
    function_node->children.push_back(contents_node);
    node = function_node;

    return true;
}


bool ParserParenthesisExpression::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    ASTPtr contents_node;
    ParserExpressionList contents(false);

    if (pos->type != TokenType::OpeningRoundBracket)
        return false;
    ++pos;

    if (!contents.parse(pos, contents_node, expected))
        return false;

    bool is_elem = true;
    if (pos->type == TokenType::Comma)
    {
        is_elem = false;
        ++pos;
    }

    if (pos->type != TokenType::ClosingRoundBracket)
        return false;
    ++pos;

    const auto & expr_list = contents_node->as<ASTExpressionList &>();

    /// Empty expression in parentheses is not allowed.
    if (expr_list.children.empty())
    {
        if (hint)
            expected.add(pos, "non-empty parenthesized list of expressions");
        return false;
    }

    /// Special case for one-element tuple.
    if (expr_list.children.size() == 1 && is_elem)
    {
        auto * ast_literal = expr_list.children.front()->as<ASTLiteral>();
        /// But only if its argument is not tuple,
        /// since otherwise it will do incorrect transformation:
        ///
        ///     (foo,bar) IN (('foo','bar')) -> (foo,bar) IN ('foo','bar')
        if (!(ast_literal && ast_literal->value.getType() == Field::Types::Tuple))
        {
            node = expr_list.children.front();
            return true;
        }
    }

    auto function_node = std::make_shared<ASTFunction>();
    function_node->name = "tuple_cast";
    function_node->arguments = contents_node;
    function_node->children.push_back(contents_node);
    node = function_node;

    return true;
}


bool ParserSubquery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    ParserSelectWithUnionQuery select;
    ParserExplainQuery explain;

    if (pos->type != TokenType::OpeningRoundBracket)
        return false;
    ++pos;

    ASTPtr result_node = nullptr;

    if (ASTPtr select_node; select.parse(pos, select_node, expected))
    {
        result_node = std::move(select_node);
    }
    else if (ASTPtr explain_node; explain.parse(pos, explain_node, expected))
    {
        /// Replace SELECT * FROM (EXPLAIN SELECT ...) with SELECT * FROM viewExplain(EXPLAIN SELECT ...)
        result_node = buildSelectFromTableFunction(makeASTFunction("viewExplain", explain_node));
    }
    else
    {
        return false;
    }

    if (pos->type != TokenType::ClosingRoundBracket)
        return false;
    ++pos;

    node = std::make_shared<ASTSubquery>();
    node->children.push_back(result_node);
    return true;
}


bool ParserIdentifier::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    /// Identifier in backquotes or in double quotes
    if (pos->type == TokenType::QuotedIdentifier)
    {
        ReadBufferFromMemory buf(pos->begin, pos->size());
        String s;

        if (*pos->begin == '`')
            readBackQuotedStringWithSQLStyle(s, buf);
        else
            readDoubleQuotedStringWithSQLStyle(s, buf);

        if (s.empty())    /// Identifiers "empty string" are not allowed.
            return false;

        node = std::make_shared<ASTIdentifier>(s);
        ++pos;
        return true;
    }
    else if (pos->type == TokenType::BareWord)
    {
        node = std::make_shared<ASTIdentifier>(String(pos->begin, pos->end));
        ++pos;
        return true;
    }
    else if (allow_query_parameter && pos->type == TokenType::OpeningCurlyBrace)
    {
        ++pos;
        if (pos->type != TokenType::BareWord)
        {
            if (hint)
                expected.add(pos, "substitution name (identifier)");
            return false;
        }

        String name(pos->begin, pos->end);
        ++pos;

        if (pos->type != TokenType::Colon)
        {
            if (hint)
                expected.add(pos, "colon between name and type");
            return false;
        }

        ++pos;

        if (pos->type != TokenType::BareWord)
        {
            if (hint)
                expected.add(pos, "substitution type (identifier)");
            return false;
        }

        String type(pos->begin, pos->end);
        ++pos;

        if (type != "Identifier")
        {
            if (hint)
                expected.add(pos, "substitution type (identifier)");
            return false;
        }

        if (pos->type != TokenType::ClosingCurlyBrace)
        {
            if (hint)
                expected.add(pos, "closing curly brace");
            return false;
        }
        ++pos;

        node = std::make_shared<ASTIdentifier>("", std::make_shared<ASTQueryParameter>(name, type));
        return true;
    }
    return false;
}


bool ParserCompoundIdentifier::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    ASTPtr id_list;
    if (!ParserList(std::make_unique<ParserIdentifier>(allow_query_parameter), std::make_unique<ParserToken>(TokenType::Dot), false)
             .parse(pos, id_list, expected))
        return false;

    std::vector<String> parts;
    std::vector<ASTPtr> params;
    const auto & list = id_list->as<ASTExpressionList &>();
    for (const auto & child : list.children)
    {
        parts.emplace_back(getIdentifierName(child));
        if (parts.back().empty())
            params.push_back(child->as<ASTIdentifier>()->getParam());
    }

    ParserKeyword s_uuid("UUID");
    UUID uuid = UUIDHelpers::Nil;

    if (table_name_with_optional_uuid)
    {
        if (parts.size() > 2)
            return false;

        if (s_uuid.ignore(pos, expected))
        {
            ParserStringLiteral uuid_p;
            ASTPtr ast_uuid;
            if (!uuid_p.parse(pos, ast_uuid, expected))
                return false;
            uuid = parseFromString<UUID>(ast_uuid->as<ASTLiteral>()->value.get<String>());
        }

        if (parts.size() == 1) node = std::make_shared<ASTTableIdentifier>(parts[0], std::move(params));
        else node = std::make_shared<ASTTableIdentifier>(parts[0], parts[1], std::move(params));
        node->as<ASTTableIdentifier>()->uuid = uuid;
    }
    else
        node = std::make_shared<ASTIdentifier>(std::move(parts), false, std::move(params));

    return true;
}


ASTPtr createFunctionCast(const ASTPtr & expr_ast, const ASTPtr & type_ast)
{
    /// Convert to canonical representation in functional form: CAST(expr, 'type')
    auto type_literal = std::make_shared<ASTLiteral>(queryToString(type_ast));

    auto expr_list_args = std::make_shared<ASTExpressionList>();
    expr_list_args->children.push_back(expr_ast);
    expr_list_args->children.push_back(std::move(type_literal));

    auto func_node = std::make_shared<ASTFunction>();
    func_node->name = "cast";
    func_node->arguments = std::move(expr_list_args);
    func_node->children.push_back(func_node->arguments);

    return func_node;
}


namespace
{
    bool parseCastAs(IParser::Pos & pos, ASTPtr & node, Expected & expected)
    {
        /// expr AS type

        ASTPtr expr_node;
        ASTPtr type_node;

        if (ParserExpression().parse(pos, expr_node, expected))
        {
            if (ParserKeyword("AS").ignore(pos, expected))
            {
                if (ParserDataType().parse(pos, type_node, expected))
                {
                    node = createFunctionCast(expr_node, type_node);
                    return true;
                }
            }
            else if (ParserToken(TokenType::Comma).ignore(pos, expected, false))
            {
                if (ParserExpression().parse(pos, type_node, expected))
                {
                    node = makeASTFunction("cast", expr_node, type_node);
                    return true;
                }
            }
        }

        return false;
    }

    bool parseSubstring(IParser::Pos & pos, ASTPtr & node, Expected & expected)
    {
        /// Either SUBSTRING(expr FROM start) or SUBSTRING(expr FROM start FOR length) or SUBSTRING(expr, start, length)
        /// The latter will be parsed normally as a function later.

        ASTPtr expr_node;
        ASTPtr start_node;
        ASTPtr length_node;

        if (!ParserExpression().parse(pos, expr_node, expected))
            return false;

        if (pos->type != TokenType::Comma)
        {
            if (!ParserKeyword("FROM").ignore(pos, expected))
                return false;
        }
        else
        {
            ++pos;
        }

        if (!ParserExpression().parse(pos, start_node, expected))
            return false;

        if (pos->type != TokenType::ClosingRoundBracket)
        {
            if (pos->type != TokenType::Comma)
            {
                if (!ParserKeyword("FOR").ignore(pos, expected))
                    return false;
            }
            else
            {
                ++pos;
            }

            if (!ParserExpression().parse(pos, length_node, expected))
                return false;
        }

        /// Convert to canonical representation in functional form: SUBSTRING(expr, start, length)
        if (length_node)
            node = makeASTFunction("substring", expr_node, start_node, length_node);
        else
            node = makeASTFunction("substring", expr_node, start_node);

        return true;
    }

    bool parseTrim(bool trim_left, bool trim_right, IParser::Pos & pos, ASTPtr & node, Expected & expected)
    {
        /// Handles all possible TRIM/LTRIM/RTRIM call variants

        std::string func_name;
        bool char_override = false;
        ASTPtr expr_node;
        ASTPtr pattern_node;
        ASTPtr to_remove;

        if (!trim_left && !trim_right)
        {
            if (ParserKeyword("BOTH").ignore(pos, expected))
            {
                trim_left = true;
                trim_right = true;
                char_override = true;
            }
            else if (ParserKeyword("LEADING").ignore(pos, expected))
            {
                trim_left = true;
                char_override = true;
            }
            else if (ParserKeyword("TRAILING").ignore(pos, expected))
            {
                trim_right = true;
                char_override = true;
            }
            else
            {
                trim_left = true;
                trim_right = true;
            }

            if (char_override)
            {
                if (!ParserExpression().parse(pos, to_remove, expected))
                    return false;
                if (!ParserKeyword("FROM").ignore(pos, expected))
                    return false;

                auto quote_meta_func_node = std::make_shared<ASTFunction>();
                auto quote_meta_list_args = std::make_shared<ASTExpressionList>();
                quote_meta_list_args->children = {to_remove};

                quote_meta_func_node->name = "regexp_quote_meta";
                quote_meta_func_node->arguments = std::move(quote_meta_list_args);
                quote_meta_func_node->children.push_back(quote_meta_func_node->arguments);

                to_remove = std::move(quote_meta_func_node);
            }
        }

        if (!ParserExpression().parse(pos, expr_node, expected))
            return false;

        /// Convert to regexp replace function call

        if (char_override)
        {
            auto pattern_func_node = std::make_shared<ASTFunction>();
            auto pattern_list_args = std::make_shared<ASTExpressionList>();
            if (trim_left && trim_right)
            {
                pattern_list_args->children = {
                    std::make_shared<ASTLiteral>("^["),
                    to_remove,
                    std::make_shared<ASTLiteral>("]*|["),
                    to_remove,
                    std::make_shared<ASTLiteral>("]*$")
                };
                func_name = "replace_regex";
            }
            else
            {
                if (trim_left)
                {
                    pattern_list_args->children = {
                        std::make_shared<ASTLiteral>("^["),
                        to_remove,
                        std::make_shared<ASTLiteral>("]*")
                    };
                }
                else
                {
                    /// trim_right == false not possible
                    pattern_list_args->children = {
                        std::make_shared<ASTLiteral>("["),
                        to_remove,
                        std::make_shared<ASTLiteral>("]*$")
                    };
                }
                func_name = "replace_regexp_one";
            }

            pattern_func_node->name = "concat";
            pattern_func_node->arguments = std::move(pattern_list_args);
            pattern_func_node->children.push_back(pattern_func_node->arguments);

            pattern_node = std::move(pattern_func_node);
        }
        else
        {
            if (trim_left && trim_right)
            {
                func_name = "trim_both";
            }
            else
            {
                if (trim_left)
                {
                    func_name = "trim_left";
                }
                else
                {
                    /// trim_right == false not possible
                    func_name = "trim_right";
                }
            }
        }

        if (char_override)
            node = makeASTFunction(func_name, expr_node, pattern_node, std::make_shared<ASTLiteral>(""));
        else
            node = makeASTFunction(func_name, expr_node);
        return true;
    }

    bool parseExtract(IParser::Pos & pos, ASTPtr & node, Expected & expected)
    {
        ASTPtr expr;

        IntervalKind interval_kind;
        if (!parseIntervalKind(pos, expected, interval_kind))
        {
            ASTPtr expr_list;
            if (!ParserExpressionList(false, false).parse(pos, expr_list, expected))
                return false;

            auto res = std::make_shared<ASTFunction>();
            res->name = "extract";
            res->arguments = expr_list;
            res->children.push_back(res->arguments);
            node = std::move(res);
            return true;
        }

        ParserKeyword s_from("FROM");
        if (!s_from.ignore(pos, expected))
            return false;

        ParserExpression elem_parser;
        if (!elem_parser.parse(pos, expr, expected))
            return false;

        node = makeASTFunction(interval_kind.toNameOfFunctionExtractTimePart(), expr);
        return true;
    }

    bool parsePosition(IParser::Pos & pos, ASTPtr & node, Expected & expected)
    {
        ASTPtr expr_list_node;
        if (!ParserExpressionList(false, false).parse(pos, expr_list_node, expected))
            return false;

        ASTExpressionList * expr_list = typeid_cast<ASTExpressionList *>(expr_list_node.get());
        if (expr_list && expr_list->children.size() == 1)
        {
            ASTFunction * func_in = typeid_cast<ASTFunction *>(expr_list->children[0].get());
            if (func_in && func_in->name == "in")
            {
                ASTExpressionList * in_args = typeid_cast<ASTExpressionList *>(func_in->arguments.get());
                if (in_args && in_args->children.size() == 2)
                {
                    node = makeASTFunction("position", in_args->children[1], in_args->children[0]);
                    return true;
                }
            }
        }

        auto res = std::make_shared<ASTFunction>();
        res->name = "position";
        res->arguments = expr_list_node;
        res->children.push_back(res->arguments);
        node = std::move(res);
        return true;
    }

    bool parseDateAdd(const char * function_name, IParser::Pos & pos, ASTPtr & node, Expected & expected)
    {
        ASTPtr timestamp_node;
        ASTPtr offset_node;

        IntervalKind interval_kind;
        ASTPtr interval_func_node;
        if (parseIntervalKind(pos, expected, interval_kind))
        {
            /// function(unit, offset, timestamp)
            if (pos->type != TokenType::Comma)
                return false;
            ++pos;

            if (!ParserExpression().parse(pos, offset_node, expected))
                return false;

            if (pos->type != TokenType::Comma)
                return false;
            ++pos;

            if (!ParserExpression().parse(pos, timestamp_node, expected))
                return false;
            auto interval_expr_list_args = std::make_shared<ASTExpressionList>();
            interval_expr_list_args->children = {offset_node};

            interval_func_node = std::make_shared<ASTFunction>();
            interval_func_node->as<ASTFunction &>().name = interval_kind.toNameOfFunctionToIntervalDataType();
            interval_func_node->as<ASTFunction &>().arguments = std::move(interval_expr_list_args);
            interval_func_node->as<ASTFunction &>().children.push_back(interval_func_node->as<ASTFunction &>().arguments);
        }
        else
        {
            ASTPtr expr_list;
            if (!ParserExpressionList(false, false).parse(pos, expr_list, expected))
                return false;

            auto res = std::make_shared<ASTFunction>();
            res->name = function_name;
            res->arguments = expr_list;
            res->children.push_back(res->arguments);
            node = std::move(res);
            return true;
        }

        node = makeASTFunction(function_name, timestamp_node, interval_func_node);
        return true;
    }

    bool parseDateDiff(IParser::Pos & pos, ASTPtr & node, Expected & expected)
    {
        ASTPtr left_node;
        ASTPtr right_node;

        IntervalKind interval_kind;
        if (!parseIntervalKind(pos, expected, interval_kind))
        {
            ASTPtr expr_list;
            if (!ParserExpressionList(false, false).parse(pos, expr_list, expected))
                return false;

            auto res = std::make_shared<ASTFunction>();
            res->name = "date_diff";
            res->arguments = expr_list;
            res->children.push_back(res->arguments);
            node = std::move(res);
            return true;
        }

        if (pos->type != TokenType::Comma)
            return false;
        ++pos;

        if (!ParserExpression().parse(pos, left_node, expected))
            return false;

        if (pos->type != TokenType::Comma)
            return false;
        ++pos;

        if (!ParserExpression().parse(pos, right_node, expected))
            return false;

        node = makeASTFunction("date_diff", std::make_shared<ASTLiteral>(interval_kind.toDateDiffUnit()), left_node, right_node);
        return true;
    }

    bool parseExists(IParser::Pos & pos, ASTPtr & node, Expected & expected)
    {
        if (!ParserSelectWithUnionQuery().parse(pos, node, expected))
            return false;

        auto subquery = std::make_shared<ASTSubquery>();
        subquery->children.push_back(node);
        node = makeASTFunction("exists", subquery);
        return true;
    }

    bool parseGrouping(IParser::Pos & pos, ASTPtr & node, Expected & expected)
    {
        ASTPtr expr_list;
        if (!ParserExpressionList(false, false).parse(pos, expr_list, expected))
            return false;

        auto res = std::make_shared<ASTFunction>();
        res->name = "grouping";
        res->arguments = expr_list;
        res->children.push_back(res->arguments);
        node = std::move(res);
        return true;
    }
}

/// proton: starts
/// When timeplus registers a new function, the new function needs to be added to one of the following two maps.
static std::unordered_map<std::string, std::string> function_map
    = {{"cast", "cast"},
       {"encodeURLComponent", "encode_url_component"},
       {"decodeURLComponent", "decode_url_component"},
       {"URLHierarchy", "url_hierarchy"},
       {"URLPathHierarchy", "url_path_hierarchy"},
       {"extractURLParameterNames", "extract_url_parameter_names"},
       {"extractURLParameters", "extract_url_parameters"},
       {"generateUUIDv4", "generate_uuidv4"},
       {"toYYYYMMDDhhmmss", "to_YYYYMMDDhhmmss"},
       {"toYYYYMM", "to_YYYYMM"},
       {"to_YYYYMM", "to_YYYYMM"},
       {"BLAKE3", "blake3"},
       {"connectionId", "connection_id"},
       {"countMatches", "count_matches"},
       {"countMatchesCaseInsensitive", "count_matches_case_insensitive"},
       {"dateName", "date_name"},
       {"displayName", "display_name"},
       {"formatReadableDecimalSize", "format_readable_decimal_size"},
       {"FQDN", "fqdn"},
       {"hasToken", "has_token"},
       {"leftUTF8", "left_utf8"},
       {"position", "position"},
       {"rightUTF8", "right_utf8"},
       {"roundBankers", "round_bankers"},
       {"starts", "starts"},
       {"structureToCapnProtoSchema", "structure_to_capn_proto_schema"},
       {"structureToProtobufSchema", "structure_to_protobuf_schema"},
       {"xxh3", "xxh3"}};

static std::unordered_map<std::string, std::string> case_insensitive_function_map
    /// case_insensitive_functions
    = {{"bithammingdistance", "bit_hamming_distance"},
       {"polygonsdistancespherical", "polygons_distance_spherical"},
       {"polygonsdistancecartesian", "polygons_distance_cartesian"},
       {"fuzzbits", "fuzz_bits"},
       {"initializeaggregation", "initialize_aggregation"},
       {"extractgroups", "extract_groups"},
       {"tostartofsecond", "to_start_of_second"},
       {"randomintype", "random_in_type"},
       {"jsonvalues", "json_values"},
       {"jsonexists", "json_exists"},
       {"rand", "rand"},
       {"tointervalmonth", "to_interval_month"},
       {"tointervalminute", "to_interval_minute"},
       {"tointervalsecond", "to_interval_second"},
       {"tointervalmillisecond", "to_interval_millisecond"},
       {"tointervalmicrosecond", "to_interval_microsecond"},
       {"parsedatetime64besteffortusornull", "parse_datetime64_best_effort_us_or_null"},
       {"parsedatetime64besteffortorzero", "parse_datetime64_best_effort_or_zero"},
       {"parsedatetime64besteffort", "parse_datetime64_best_effort"},
       {"parsedatetime32besteffortornull", "parse_datetime32_best_effort_or_null"},
       {"parsedatetimebesteffortusorzero", "parse_datetime_best_effort_us_or_zero"},
       {"parsedatetimebesteffortus", "parse_datetime_best_effort_us"},
       {"toipv6ornull", "to_ipv6_or_null"},
       {"toipv4ornull", "to_ipv4_or_null"},
       {"touuidornull", "to_uuid_or_null"},
       {"todecimal32ornull", "to_decimal32_or_null"},
       {"todate32ornull", "to_date32_or_null"},
       {"tofloat64ornull", "to_float64_or_null"},
       {"tofloat32ornull", "to_float32_or_null"},
       {"toint256ornull", "to_int256_or_null"},
       {"toint128ornull", "to_int128_or_null"},
       {"toint64ornull", "to_int64_or_null"},
       {"toint16ornull", "to_int16_or_null"},
       {"touint256ornull", "to_uint256_or_null"},
       {"touint32ornull", "to_uint32_or_null"},
       {"touint16ornull", "to_uint16_or_null"},
       {"touint8ornull", "to_uint8_or_null"},
       {"toipv6orzero", "to_ipv6_or_zero"},
       {"toipv4orzero", "to_ipv4_or_zero"},
       {"touuidorzero", "to_uuid_or_zero"},
       {"todecimal128orzero", "to_decimal128_or_zero"},
       {"todecimal64orzero", "to_decimal64_or_zero"},
       {"todecimal32orzero", "to_decimal32_or_zero"},
       {"todatetimeorzero", "to_datetime_or_zero"},
       {"todate32orzero", "to_date32_or_zero"},
       {"tofloat64orzero", "to_float64_or_zero"},
       {"toint64orzero", "to_int64_or_zero"},
       {"toint8orzero", "to_int8_or_zero"},
       {"parsedatetimebesteffortorzero", "parse_datetime_best_effort_or_zero"},
       {"touint128orzero", "to_uint128_or_zero"},
       {"touint64orzero", "to_uint64_or_zero"},
       {"touint32orzero", "to_uint32_or_zero"},
       {"touint8orzero", "to_uint8_or_zero"},
       {"toipv4", "to_ipv4"},
       {"todatetime64", "to_datetime64"},
       {"todate", "to_date"},
       {"todate16", "to_date16"},
       {"todecimal128", "to_decimal128"},
       {"todecimal64", "to_decimal64"},
       {"todecimal32", "to_decimal32"},
       {"toint256", "to_int256"},
       {"toint128", "to_int128"},
       {"toint64", "to_int64"},
       {"toint32", "to_int32"},
       {"touint256", "to_uint256"},
       {"touint128", "to_uint128"},
       {"touint64", "to_uint64"},
       {"touint32", "to_uint32"},
       {"touint16", "to_uint16"},
       {"touint8", "to_uint8"},
       {"blocksize", "block_size"},
       {"isnotnull", "is_not_null"},
       {"rounddown", "round_down"},
       {"trunc", "trunc"},
       {"floor", "floor"},
       {"round", "round"},
       {"weakhash32", "weak_hash32"},
       {"wyhash64", "wy_hash64"},
       {"xxhash64", "xx_hash64"},
       {"xxhash32", "xx_hash32"},
       {"gccmurmurhash", "gcc_murmur_hash"},
       {"murmurhash3128", "murmur_hash3_128"},
       {"murmurhash364", "murmur_hash3_64"},
       {"todecimal256orzero", "to_decimal256_or_zero"},
       {"murmurhash332", "murmur_hash3_32"},
       {"hivehash", "hive_hash"},
       {"javahashutf16le", "java_hash_utf16_le"},
       {"javahash", "java_hash"},
       {"urlhash", "url_hash"},
       {"inthash64", "int_hash64"},
       {"inthash32", "int_hash32"},
       {"metrohash64", "metro_hash64"},
       {"farmfingerprint64", "farm_fingerprint64"},
       {"siphash128", "sip_hash128"},
       {"sha512", "sha512"},
       {"sha384", "sha384"},
       {"uuidstringtonum", "uuid_string_to_num"},
       {"sha224", "sha224"},
       {"sha1", "sha1"},
       {"todecimal", "to_decimal"},
       {"md5", "md5"},
       {"halfmd5", "half_md5"},
       {"lowcardinalityindices", "low_cardinality_indices"},
       {"logtrace", "log_trace"},
       {"edition", "edition"},
       {"stringtoh3", "string_to_h3"},
       {"toint16", "to_int16"},
       {"jsonextractkeysandvaluesraw", "json_extract_keys_and_values_raw"},
       {"jsonextractarrayraw", "json_extract_array_raw"},
       {"jsonextractraw", "json_extract_raw"},
       {"jsonextractkeysandvalues", "json_extract_keys_and_values"},
       {"jsonextract", "json_extract"},
       {"jsonextractstring", "json_extract_string"},
       {"jsonextractbool", "json_extract_bool"},
       {"jsonextractint", "json_extract_int"},
       {"jsontype", "json_type"},
       {"jsonlength", "json_length"},
       {"isvalidjson", "is_valid_json"},
       {"char", "char"},
       {"cbrt", "cbrt"},
       {"s2togeo", "s2_to_geo"},
       {"sleepeachrow", "sleep_each_row"},
       {"regiontoname", "region_to_name"},
       {"regionin", "region_in"},
       {"regiontocountry", "region_to_country"},
       {"regiontodistrict", "region_to_district"},
       {"adddays", "add_days"},
       {"countsubstringscaseinsensitiveutf8", "count_substrings_case_insensitive_utf8"},
       {"makedatetime64", "make_datetime64"},
       {"date", "date"},
       {"makedate", "make_date"},
       {"subtractnanoseconds", "subtract_nanoseconds"},
       {"ipv6stringtonumornull", "ipv6_string_to_num_or_null"},
       {"ipv6stringtonumordefault", "ipv6_string_to_num_or_default"},
       {"ipv6stringtonum", "ipv6_string_to_num"},
       {"ipv6numtostring", "ipv6_num_to_string"},
       {"ipv4stringtonumordefault", "ipv4_string_to_num_or_default"},
       {"ipv4stringtonum", "ipv4_string_to_num"},
       {"ipv4numtostringclassc", "ipv4_num_to_string_class_c"},
       {"isipv4string", "is_ipv4_string"},
       {"ipv4cidrtorange", "ipv4_cidr_to_range"},
       {"ipv6cidrtorange", "ipv6_cidr_to_range"},
       {"macstringtooui", "mac_string_to_oui"},
       {"macnumtostring", "mac_num_to_string"},
       {"ipv4toipv6", "ipv4_to_ipv6"},
       {"cutipv6", "cut_ipv6"},
       {"snowflaketodatetime64", "snowflake_to_datetime64"},
       {"subtractmilliseconds", "subtract_milliseconds"},
       {"getserverport", "get_server_port"},
       {"regiontocity", "region_to_city"},
       {"multisearchany", "multi_search_any"},
       {"bitmaphasany", "bitmap_has_any"},
       {"bitmapandnot", "bitmap_andnot"},
       {"parsedatetime64besteffortus", "parse_datetime64_best_effort_us"},
       {"bitmapand", "bitmap_and"},
       {"detectcharset", "detect_charset"},
       {"bitmapxorcardinality", "bitmap_xor_cardinality"},
       {"h3getpentagonindexes", "h3_get_pentagon_indexes"},
       {"arraylastindex", "array_last_index"},
       {"bitmaporcardinality", "bitmap_or_cardinality"},
       {"subbitmap", "sub_bitmap"},
       {"runningconcurrency", "running_concurrency"},
       {"bitmapsubsetlimit", "bitmap_subset_limit"},
       {"bitmapsubsetinrange", "bitmap_subset_in_range"},
       {"bitmapbuild", "bitmap_build"},
       {"extracttextfromhtml", "extract_text_from_html"},
       {"monthname", "month_name"},
       {"splitbyregexp", "split_by_regexp"},
       {"isnan", "is_nan"},
       {"geohashencode", "geohash_encode"},
       {"torelativequarternum", "to_relative_quarter_num"},
       {"or", "or"},
       {"dictgetchildren", "dict_get_children"},
       {"dictisin", "dict_is_in"},
       {"todecimal128ornull", "to_decimal128_or_null"},
       {"dictgetstringordefault", "dict_get_string_or_default"},
       {"dictgetipv6ordefault", "dict_get_ipv6_or_default"},
       {"dictgetdatetimeordefault", "dict_get_datetime_or_default"},
       {"datetimetosnowflake", "datetime_to_snowflake"},
       {"todateordefault", "to_date_or_default"},
       {"dictgetdateordefault", "dict_get_date_or_default"},
       {"dictgetfloat64ordefault", "dict_get_float64_or_default"},
       {"h3getresolution", "h3_get_resolution"},
       {"exp", "exp"},
       {"dictgetfloat32ordefault", "dict_get_float32_or_default"},
       {"addseconds", "add_seconds"},
       {"dictgetint64ordefault", "dict_get_int64_or_default"},
       {"toint", "to_int"},
       {"dictgetint16ordefault", "dict_get_int16_or_default"},
       {"dictgetuint64ordefault", "dict_get_uint64_or_default"},
       {"emptyarraystring", "empty_array_string"},
       {"dictgetuint32ordefault", "dict_get_uint32_or_default"},
       {"h3cellaream2", "h3_cell_area_m2"},
       {"date_diff", "date_diff"},
       {"dictgetstring", "dict_get_string"},
       {"dictgetuuid", "dict_get_uuid"},
       {"dictgetfloat64", "dict_get_float64"},
       {"tuplemultiplybynumber", "tuple_multiply_by_number"},
       {"tomonth", "to_month"},
       {"dictgetint16", "dict_get_int16"},
       {"dictgetint8", "dict_get_int8"},
       {"dictgetuint32", "dict_get_uint32"},
       {"multisearchanycaseinsensitiveutf8", "multi_search_any_case_insensitive_utf8"},
       {"cosh", "cosh"},
       {"dictgetuint16", "dict_get_uint16"},
       {"sleep", "sleep"},
       {"dictget", "dict_get"},
       {"replicate", "replicate"},
       {"tomodifiedjulianday", "to_modified_julian_day"},
       {"h3getunidirectionaledge", "h3_get_unidirectional_edge"},
       {"regiontocontinent", "region_to_continent"},
       {"hascolumnintable", "has_column_in_table"},
       {"detectlanguageunknown", "detect_language_unknown"},
       {"arrayslice", "array_slice"},
       {"todecimal128ordefault", "to_decimal128_or_default"},
       {"partitionid", "partition_id"},
       {"formatdatetimeinjodasyntax", "format_datetime_in_joda_syntax"},
       {"dictgetornull", "dict_get_or_null"},
       {"tostartofquarter", "to_start_of_quarter"},
       {"multisearchfirstindex", "multi_search_first_index"},
       {"bytesize", "byte_size"},
       {"geohashdecode", "geohash_decode"},
       {"polygonsequalscartesian", "polygons_equals_cartesian"},
       {"jsonkey", "json_key"},
       {"bitmapmax", "bitmap_max"},
       {"tid", "tid"},
       {"bitcount", "bit_count"},
       {"tointervalyear", "to_interval_year"},
       {"not", "not"},
       {"formatrow", "format_row"},
       {"arrayreduceinranges", "array_reduce_in_ranges"},
       {"addmicroseconds", "add_microseconds"},
       {"geotoh3", "geo_to_h3"},
       {"bitmapmin", "bitmap_min"},
       {"normalizequery", "normalize_query"},
       {"if", "if"},
       {"defaultprofiles", "default_profiles"},
       {"countsubstrings", "count_substrings"},
       {"ngramdistancecaseinsensitiveutf8", "ngram_distance_case_insensitive_utf8"},
       {"acosh", "acosh"},
       {"accuratecastornull", "accurate_cast_or_null"},
       {"h3indexesareneighbors", "h3_indexes_are_neighbors"},
       {"accuratecast", "accurate_cast"},
       {"cast", "cast"},
       {"notlike", "not_like"},
       {"aesdecryptmysql", "aes_decrypt_mysql"},
       {"toint8", "to_int8"},
       {"dictgetfloat32", "dict_get_float32"},
       {"arraycompact", "array_compact"},
       {"dictgetint32", "dict_get_int32"},
       {"bitslice", "bit_slice"},
       {"bitrotateright", "bit_rotate_right"},
       {"toweek", "to_week"},
       {"hashid", "hashid"},
       {"h3edgelengthm", "h3_edge_length_m"},
       {"bitshiftright", "bit_shift_right"},
       {"tan", "tan"},
       {"substring", "substring"},
       {"tostartofday", "to_start_of_day"},
       {"addhours", "add_hours"},
       {"ipv4numtostring", "ipv4_num_to_string"},
       {"randcanonical", "rand_canonical"},
       {"h3togeoboundary", "h3_to_geo_boundary"},
       {"blocknumber", "block_number"},
       {"l2normalize", "l2_normalize"},
       {"reverse", "reverse"},
       {"multisearchfirstindexutf8", "multi_search_first_index_utf8"},
       {"torelativesecondnum", "to_relative_second_num"},
       {"greatcircleangle", "great_circle_angle"},
       {"subtractyears", "subtract_years"},
       {"isvalidutf8", "is_valid_utf8"},
       {"arrayfold", "array_fold"},
       {"casewithexpression", "case_with_expression"},
       {"addyears", "add_years"},
       {"tointervalquarter", "to_interval_quarter"},
       {"tonullable", "to_nullable"},
       {"intexp2", "int_exp2"},
       {"wordshingleminhashargutf8", "word_shingle_min_hash_arg_utf8"},
       {"fromunixtimestamp", "from_unix_timestamp"},
       {"dictgetint32ordefault", "dict_get_int32_or_default"},
       {"wordshingleminhashcaseinsensitive", "word_shingle_min_hash_case_insensitive"},
       {"rownumberinblock", "row_number_in_block"},
       {"splitbywhitespace", "split_by_whitespace"},
       {"wordshingleminhasharg", "word_shingle_min_hash_arg"},
       {"s2capcontains", "s2_cap_contains"},
       {"ngramminhashargcaseinsensitiveutf8", "ngram_min_hash_arg_case_insensitive_utf8"},
       {"extractallgroupsvertical", "extract_all_groups_vertical"},
       {"globalnotnullin", "global_not_null_in"},
       {"ngramminhashargutf8", "ngram_min_hash_arg_utf8"},
       {"ngramminhashargcaseinsensitive", "ngram_min_hash_arg_case_insensitive"},
       {"ngramminhashutf8", "ngram_min_hash_utf8"},
       {"ngramminhash", "ngram_min_hash"},
       {"wordshinglesimhashutf8", "word_shingle_sim_hash_utf8"},
       {"jsonvalue", "json_value"},
       {"wordshinglesimhashcaseinsensitive", "word_shingle_sim_hash_case_insensitive"},
       {"getmacro", "get_macro"},
       {"wordshinglesimhash", "word_shingle_sim_hash"},
       {"ngramsimhashcaseinsensitiveutf8", "ngram_sim_hash_case_insensitive_utf8"},
       {"ngramsimhashutf8", "ngram_sim_hash_utf8"},
       {"wordshingleminhashargcaseinsensitiveutf8", "word_shingle_min_hash_arg_case_insensitive_utf8"},
       {"h3pointdistrads", "h3_point_dist_rads"},
       {"runningdifferencestartingwithfirstvalue", "running_difference_starting_with_first_value"},
       {"ngramsimhashcaseinsensitive", "ngram_sim_hash_case_insensitive"},
       {"base58decode", "base58_decode"},
       {"moduloorzero", "modulo_or_zero"},
       {"normalizeutf8nfd", "normalize_utf8_nfd"},
       {"currentdatabase", "current_database"},
       {"streamingnow64", "__streaming_now64"},
       {"identity", "identity"},
       {"ascii", "ascii"},
       {"currentprofiles", "current_profiles"},
       {"lowerutf8", "lower_utf8"},
       {"polygonperimetercartesian", "polygon_perimeter_cartesian"},
       {"positivemodulo", "positive_modulo"},
       {"bitpositionstoarray", "bit_positions_to_array"},
       {"dictgetipv4ordefault", "dict_get_ipv4_or_default"},
       {"jsonhas", "json_has"},
       {"currentuser", "current_user"},
       {"tofloat64", "to_float64"},
       {"age", "age"},
       {"regionhierarchy", "region_hierarchy"},
       {"reinterpret", "reinterpret"},
       {"todayofweek", "to_day_of_week"},
       {"unbin", "unbin"},
       {"h3edgeangle", "h3_edge_angle"},
       {"modulo", "modulo"},
       {"toipv6ordefault", "to_ipv6_or_default"},
       {"cos", "cos"},
       {"unhex", "unhex"},
       {"hex", "hex"},
       {"gcd", "gcd"},
       {"jumpconsistenthash", "jump_consistent_hash"},
       {"defaultvalueofargumenttype", "default_value_of_argument_type"},
       {"subtractminutes", "subtract_minutes"},
       {"frommodifiedjulianday", "from_modified_julian_day"},
       {"concat", "concat"},
       {"demangle", "demangle"},
       {"log", "log"},
       {"alphatokens", "alpha_tokens"},
       {"l1norm", "l1_norm"},
       {"dividedecimal", "divide_decimal"},
       {"h3tochildren", "h3_to_children"},
       {"fullhostname", "full_host_name"},
       {"getsetting", "get_setting"},
       {"bitboolmaskand", "__bit_bool_mask_and"},
       {"tupleplus", "tuple_plus"},
       {"minus", "minus"},
       {"sqrt", "sqrt"},
       {"casewithoutexpr", "case_without_expr"},
       {"bitrotateleft", "bit_rotate_left"},
       {"getsizeofenumtype", "get_size_of_enum_type"},
       {"endswith", "ends_with"},
       {"wordshinglesimhashcaseinsensitiveutf8", "word_shingle_sim_hash_case_insensitive_utf8"},
       {"streamingrownumber", "__streaming_row_number"},
       {"formatreadablesize", "format_readable_size"},
       {"countdigits", "count_digits"},
       {"subtractquarters", "subtract_quarters"},
       {"polygonareacartesian", "polygon_area_cartesian"},
       {"trybase58decode", "try_base58_decode"},
       {"exp10", "exp10"},
       {"bitswaplasttwo", "__bit_swap_last_two"},
       {"lowcardinalitykeys", "low_cardinality_keys"},
       {"subtractinterval", "subtract_interval"},
       {"finalizeaggregation", "finalize_aggregation"},
       {"trybase64decode", "try_base64_decode"},
       {"tokens", "tokens"},
       {"ngrams", "ngrams"},
       {"geohashesinbox", "geohashes_in_box"},
       {"runningdifference", "running_difference"},
       {"arrayenumeratedenseranked", "array_enumerate_dense_ranked"},
       {"flattentuple", "flatten_tuple"},
       {"bitmapor", "bitmap_or"},
       {"concatassumeinjective", "concat_assume_injective"},
       {"lcm", "lcm"},
       {"mapextractkeylike", "map_extract_key_like"},
       {"formatreadablequantity", "format_readable_quantity"},
       {"formatrownonewline", "format_row_no_newline"},
       {"casewithoutexpression", "case_without_expression"},
       {"multiif", "multi_if"},
       {"fromunixtimestamp64milli", "from_unix_timestamp64_milli"},
       {"hastokencaseinsensitive", "has_token_case_insensitive"},
       {"geodistance", "geo_distance"},
       {"h3getres0indexes", "h3_get_res0_indexes"},
       {"toyear", "to_year"},
       {"decodexmlcomponent", "decode_xml_component"},
       {"now64", "now64"},
       {"multimatchany", "multi_match_any"},
       {"h3exactedgelengthkm", "h3_exact_edge_length_km"},
       {"arraysum", "array_sum"},
       {"toisoyear", "to_iso_year"},
       {"enabledroles", "enabled_roles"},
       {"parsedatetimebesteffortusornull", "parse_datetime_best_effort_us_or_null"},
       {"tostartoftenminutes", "to_start_of_ten_minutes"},
       {"lower", "lower"},
       {"h3exactedgelengthm", "h3_exact_edge_length_m"},
       {"bitmapandcardinality", "bitmap_and_cardinality"},
       {"dictgetdatetime", "dict_get_datetime"},
       {"totime", "__to_time"},
       {"h3getdestinationindexfromunidirectionaledge", "h3_get_destination_index_from_unidirectional_edge"},
       {"roundduration", "round_duration"},
       {"h3getoriginindexfromunidirectionaledge", "h3_get_origin_index_from_unidirectional_edge"},
       {"crc32", "crc32"},
       {"trimright", "trim_right"},
       {"trimleft", "trim_left"},
       {"dumpcolumnstructure", "dump_column_structure"},
       {"touuid", "to_uuid"},
       {"extractallgroupshorizontal", "extract_all_groups_horizontal"},
       {"uniqthetaunion", "uniq_theta_union"},
       {"s2cellsintersect", "s2_cells_intersect"},
       {"coalesce", "coalesce"},
       {"todateorzero", "to_date_or_zero"},
       {"initialqueryid", "initial_query_id"},
       {"toyearweek", "to_year_week"},
       {"detectlanguagemixed", "detect_language_mixed"},
       {"h3line", "h3_line"},
       {"atanh", "atanh"},
       {"wordshingleminhashutf8", "word_shingle_min_hash_utf8"},
       {"toint32ornull", "to_int32_or_null"},
       {"simplejsonextractuint", "simple_json_extract_uint"},
       {"h3hexring", "h3_hex_ring"},
       {"todecimal256", "to_decimal256"},
       {"ngramsearch", "ngram_search"},
       {"arraydifference", "array_difference"},
       {"arraypushback", "array_push_back"},
       {"h3unidirectionaledgeisvalid", "h3_unidirectional_edge_is_valid"},
       {"arraystringconcat", "array_string_concat"},
       {"repeat", "repeat"},
       {"stem", "stem"},
       {"totypename", "to_type_name"},
       {"todatetime64ornull", "to_datetime64_or_null"},
       {"reversednsquery", "reverse_dns_query"},
       {"splitbynonalpha", "split_by_non_alpha"},
       {"ignore", "ignore"},
       {"tostartofnanosecond", "to_start_of_nanosecond"},
       {"bittest", "bit_test"},
       {"tointervalhour", "to_interval_hour"},
       {"h3toparent", "h3_to_parent"},
       {"buildid", "build_id"},
       {"multisearchfirstposition", "multi_search_first_position"},
       {"dictgetuint64", "dict_get_uint64"},
       {"h3hexareakm2", "h3_hex_area_km2"},
       {"h3kring", "h3k_ring"},
       {"toint8ornull", "to_int8_or_null"},
       {"hypot", "hypot"},
       {"kostikconsistenthash", "kostik_consistent_hash"},
       {"globalnotnullinignoreset", "global_not_null_in_ignore_set"},
       {"detectlanguage", "detect_language"},
       {"tupleelement", "tuple_element"},
       {"notnullinignoreset", "not_null_in_ignore_set"},
       {"h3exactedgelengthrads", "h3_exact_edge_length_rads"},
       {"globalnotinignoreset", "global_not_in_ignore_set"},
       {"globalinignoreset", "global_in_ignore_set"},
       {"subtractmonths", "subtract_months"},
       {"inignoreset", "in_ignore_set"},
       {"bitshiftleft", "bit_shift_left"},
       {"notnullin", "not_null_in"},
       {"globalnullin", "global_null_in"},
       {"globalnotin", "global_not_in"},
       {"bitmaptransform", "bitmap_transform"},
       {"parsetimedelta", "parse_time_delta"},
       {"extractkeyvaluepairs", "extract_key_value_pairs"},
       {"in", "in"},
       {"h3getbasecell", "h3_get_base_cell"},
       {"errorcodetoname", "error_code_to_name"},
       {"randomfixedstring", "random_fixed_string"},
       {"tofloat32orzero", "to_float32_or_zero"},
       {"acos", "acos"},
       {"isconstant", "is_constant"},
       {"countsubstringscaseinsensitive", "count_substrings_case_insensitive"},
       {"emptyarraytosingle", "empty_array_to_single"},
       {"isipv6string", "is_ipv6_string"},
       {"addnanoseconds", "add_nanoseconds"},
       {"monotonic", "monotonic"},
       {"randexponential", "rand_exponential"},
       {"extractkeyvaluepairswithescaping", "extract_key_value_pairs_with_escaping"},
       {"polygonsunionspherical", "polygons_union_spherical"},
       {"polygonsunioncartesian", "polygons_union_cartesian"},
       {"isnullable", "is_nullable"},
       {"iszeroornull", "is_zero_or_null"},
       {"macstringtonum", "mac_string_to_num"},
       {"leftpadutf8", "left_pad_utf8"},
       {"lagbehind", "lag_behind"},
       {"multifuzzymatchanyindex", "multi_fuzzy_match_any_index"},
       {"lemmatize", "lemmatize"},
       {"isdecimaloverflow", "is_decimal_overflow"},
       {"multisearchallpositionscaseinsensitiveutf8", "multi_search_all_positions_case_insensitive_utf8"},
       {"lengthutf8", "length_utf8"},
       {"h3hexaream2", "h3_hex_area_m2"},
       {"tobool", "to_bool"},
       {"lessorequals", "less_or_equals"},
       {"multiplydecimal", "multiply_decimal"},
       {"wkt", "wkt"},
       {"filesystemunreserved", "filesystem_unreserved"},
       {"randnormal", "rand_normal"},
       {"abs", "abs"},
       {"and", "and"},
       {"arraymap", "array_map"},
       {"log1p", "log1p"},
       {"fromunixtimestamp64nano", "from_unix_timestamp64_nano"},
       {"todecimal32ordefault", "to_decimal32_or_default"},
       {"todatetime64ordefault", "to_datetime64_or_default"},
       {"greatcircledistance", "great_circle_distance"},
       {"reinterpretasfixedstring", "reinterpret_as_fixed_string"},
       {"dotproduct", "dot_product"},
       {"touint256orzero", "to_uint256_or_zero"},
       {"jsonextractfloat", "json_extract_float"},
       {"tofloat64ordefault", "to_float64_or_default"},
       {"toint256ordefault", "to_int256_or_default"},
       {"tuplenegate", "tuple_negate"},
       {"arrayflatten", "array_flatten"},
       {"materialize", "materialize"},
       {"toint8ordefault", "to_int8_or_default"},
       {"tostartofminute", "to_start_of_minute"},
       {"extract", "extract"},
       {"touint64ordefault", "to_uint64_or_default"},
       {"arrayenumerateuniq", "array_enumerate_uniq"},
       {"svg", "svg"},
       {"bitmaphasall", "bitmap_has_all"},
       {"h3ispentagon", "h3_is_pentagon"},
       {"mapupdate", "map_update"},
       {"proportionsztest", "proportions_ztest"},
       {"base58encode", "base58_encode"},
       {"h3isvalid", "h3_is_valid"},
       {"bitmasktolist", "bitmask_to_list"},
       {"mapvalues", "map_values"},
       {"positioncaseinsensitive", "position_case_insensitive"},
       {"mapcontains", "map_contains"},
       {"meilimatch", "meili_match"},
       {"snowflaketodatetime", "snowflake_to_datetime"},
       {"multisearchfirstpositioncaseinsensitive", "multi_search_first_position_case_insensitive"},
       {"minsamplesizeconversion", "min_sample_size_conversion"},
       {"divide", "divide"},
       {"modulolegacy", "modulo_legacy"},
       {"tojsonstring", "to_json_string"},
       {"isinfinite", "is_infinite"},
       {"mortondecode", "morton_decode"},
       {"enabledprofiles", "enabled_profiles"},
       {"grok", "grok"},
       {"mortonencode", "morton_encode"},
       {"randomprintableascii", "random_printable_ascii"},
       {"todatetimeordefault", "to_datetime_or_default"},
       {"timeslot", "time_slot"},
       {"formatreadabletimedelta", "format_readable_time_delta"},
       {"s2rectadd", "s2_rect_add"},
       {"runningaccumulate", "running_accumulate"},
       {"hasthreadfuzzer", "has_thread_fuzzer"},
       {"appendtrailingcharifabsent", "append_trailing_char_if_absent"},
       {"bitboolmaskor", "__bit_bool_mask_or"},
       {"multifuzzymatchany", "multi_fuzzy_match_any"},
       {"torelativeweeknum", "to_relative_week_num"},
       {"dictgethierarchy", "dict_get_hierarchy"},
       {"aesencryptmysql", "aes_encrypt_mysql"},
       {"intdivorzero", "int_div_or_zero"},
       {"rightpadutf8", "right_pad_utf8"},
       {"reverseutf8", "reverse_utf8"},
       {"leftpad", "left_pad"},
       {"translateutf8", "translate_utf8"},
       {"multimatchanyindex", "multi_match_any_index"},
       {"todecimal64ornull", "to_decimal64_or_null"},
       {"arrayfirstornull", "array_first_or_null"},
       {"addmonths", "add_months"},
       {"timeslots", "time_slots"},
       {"toipv4ordefault", "to_ipv4_or_default"},
       {"joinget", "join_get"},
       {"base64decode", "base64_decode"},
       {"atan2", "atan2"},
       {"toint16ordefault", "to_int16_or_default"},
       {"lpdistance", "lp_distance"},
       {"normalizeutf8nfkd", "normalize_utf8_nfkd"},
       {"multisearchallpositions", "multi_search_all_positions"},
       {"dictgetuint8", "dict_get_uint8"},
       {"accuratecastordefault", "accurate_cast_or_default"},
       {"file", "file"},
       {"normalizeutf8nfkc", "normalize_utf8_nfkc"},
       {"arraypushfront", "array_push_front"},
       {"normalizeutf8nfc", "normalize_utf8_nfc"},
       {"mapkeys", "map_keys"},
       {"todatetimeornull", "to_datetime_or_null"},
       {"encrypt", "encrypt"},
       {"arrayauc", "array_auc"},
       {"tostartofmonth", "to_start_of_month"},
       {"siphash64", "sip_hash64"},
       {"arrayelement", "array_element"},
       {"bitor", "bit_or"},
       {"formatdatetime", "format_datetime"},
       {"simplejsonextractraw", "simple_json_extract_raw"},
       {"subtractseconds", "subtract_seconds"},
       {"addresstosymbol", "address_to_symbol"},
       {"emitversion", "emit_version"},
       {"tostring", "to_string"},
       {"makedate32", "make_date32"},
       {"simplejsonextractbool", "simple_json_extract_bool"},
       {"bittestall", "bit_test_all"},
       {"reinterpretasdate", "reinterpret_as_date"},
       {"getoskernelversion", "get_os_kernel_version"},
       {"arraycount", "array_count"},
       {"evalmlmethod", "eval_ml_method"},
       {"addminutes", "add_minutes"},
       {"sin", "sin"},
       {"positioncaseinsensitiveutf8", "position_case_insensitive_utf8"},
       {"multisearchfirstindexcaseinsensitiveutf8", "multi_search_first_index_case_insensitive_utf8"},
       {"mapcast", "map_cast"},
       {"todatetime64orzero", "to_datetime64_or_zero"},
       {"timediff", "time_diff"},
       {"has", "has"},
       {"base64encode", "base64_encode"},
       {"s2rectcontains", "s2_rect_contains"},
       {"negate", "negate"},
       {"intdiv", "int_div"},
       {"lags", "lags"},
       {"replaceall", "replace_all"},
       {"farmhash64", "farm_hash64"},
       {"rightpad", "right_pad"},
       {"subtractweeks", "subtract_weeks"},
       {"totimezone", "to_timezone"},
       {"currentroles", "current_roles"},
       {"bin", "bin"},
       {"h3edgelengthkm", "h3_edge_length_km"},
       {"today", "today"},
       {"match", "match"},
       {"torelativemonthnum", "to_relative_month_num"},
       {"ngramminhashcaseinsensitiveutf8", "ngram_min_hash_case_insensitive_utf8"},
       {"arraycumsum", "array_cum_sum"},
       {"arrayfirst", "array_first"},
       {"emptyarraydatetime", "empty_array_datetime"},
       {"regiontotopcontinent", "region_to_top_continent"},
       {"globalnullinignoreset", "global_null_in_ignore_set"},
       {"emptyarraydate", "empty_array_date"},
       {"emptyarrayfloat64", "empty_array_float64"},
       {"h3getunidirectionaledgesfromhexagon", "h3_get_unidirectional_edges_from_hexagon"},
       {"emptyarrayfloat32", "empty_array_float32"},
       {"atan", "atan"},
       {"md4", "md4"},
       {"queryid", "query_id"},
       {"emptyarrayuint64", "empty_array_uint64"},
       {"now", "now"},
       {"subtractdays", "subtract_days"},
       {"json_query", "json_query"},
       {"todatetime32", "to_datetime32"},
       {"nullif", "null_if"},
       {"bitand", "bit_and"},
       {"ngramminhasharg", "ngram_min_hash_arg"},
       {"boottime", "boot_time"},
       {"torelativedaynum", "to_relative_day_num"},
       {"left", "left"},
       {"asin", "asin"},
       {"tosecond", "to_second"},
       {"roundtoexp2", "round_to_exp2"},
       {"parsedatetime", "parse_datetime"},
       {"readwktmultipolygon", "read_wkt_multi_polygon"},
       {"arrayenumerate", "array_enumerate"},
       {"plus", "plus"},
       {"dictgetint8ordefault", "dict_get_int8_or_default"},
       {"reinterpretasint128", "reinterpret_as_int128"},
       {"arrayconcat", "array_concat"},
       {"h3pointdistm", "h3_point_dist_m"},
       {"parsedatetime32besteffort", "parse_datetime32_best_effort"},
       {"randconstant", "rand_constant"},
       {"cityhash64", "city_hash64"},
       {"pointinellipses", "point_in_ellipses"},
       {"toconcretetype", "to_concrete_type"},
       {"murmurhash264", "murmur_hash2_64"},
       {"nullin", "null_in"},
       {"toint16orzero", "to_int16_or_zero"},
       {"multifuzzymatchallindices", "multi_fuzzy_match_all_indices"},
       {"polygonareaspherical", "polygon_area_spherical"},
       {"dictgetdate", "dict_get_date"},
       {"dictgetint64", "dict_get_int64"},
       {"e", "e"},
       {"bitwrapperfunc", "__bit_wrapper_func"},
       {"roundage", "round_age"},
       {"polygonperimeterspherical", "polygon_perimeter_spherical"},
       {"lgamma", "lgamma"},
       {"throwif", "throw_if"},
       {"h3pointdistkm", "h3_point_dist_km"},
       {"h3isresclassiii", "h3_is_res_class_iii"},
       {"randstudentt", "rand_student_t"},
       {"polygonsintersectionspherical", "polygons_intersection_spherical"},
       {"arraymax", "array_max"},
       {"serveruuid", "server_uuid"},
       {"polygonssymdifferencespherical", "polygons_sym_difference_spherical"},
       {"polygonssymdifferencecartesian", "polygons_sym_difference_cartesian"},
       {"polygonswithinspherical", "polygons_within_spherical"},
       {"tostartoffiveminutes", "to_start_of_five_minutes"},
       {"reinterpretasint32", "reinterpret_as_int32"},
       {"todecimal64ordefault", "to_decimal64_or_default"},
       {"arrayresize", "array_resize"},
       {"joingetornull", "join_get_or_null"},
       {"encodexmlcomponent", "encode_xml_component"},
       {"polygonsintersectioncartesian", "polygons_intersection_cartesian"},
       {"pow", "pow"},
       {"decrypt", "decrypt"},
       {"emptyarrayint64", "empty_array_int64"},
       {"addweeks", "add_weeks"},
       {"streamingneighbor", "__streaming_neighbor"},
       {"addtupleofintervals", "add_tuple_of_intervals"},
       {"toquarter", "to_quarter"},
       {"yesterday", "yesterday"},
       {"todateornull", "to_date_or_null"},
       {"positionutf8", "position_utf8"},
       {"getscalar", "__get_scalar"},
       {"ilike", "ilike"},
       {"touint256ordefault", "to_uint256_or_default"},
       {"ngramsearchcaseinsensitiveutf8", "ngram_search_case_insensitive_utf8"},
       {"subtracthours", "subtract_hours"},
       {"wordshingleminhashcaseinsensitiveutf8", "word_shingle_min_hash_case_insensitive_utf8"},
       {"pointinpolygon", "point_in_polygon"},
       {"dictgetuint8ordefault", "dict_get_uint8_or_default"},
       {"ngramsearchutf8", "ngram_search_utf8"},
       {"nullinignoreset", "null_in_ignore_set"},
       {"todayofmonth", "to_day_of_month"},
       {"mapadd", "map_add"},
       {"randpoisson", "rand_poisson"},
       {"randnegativebinomial", "rand_negative_binomial"},
       {"uuidnumtostring", "uuid_num_to_string"},
       {"convertcharset", "convert_charset"},
       {"randbinomial", "rand_binomial"},
       {"todecimal256ordefault", "to_decimal256_or_default"},
       {"randbernoulli", "rand_bernoulli"},
       {"randfisherf", "rand_fisher_f"},
       {"h3tostring", "h3_to_string"},
       {"arrayall", "array_all"},
       {"blockserializedsize", "block_serialized_size"},
       {"randchisquared", "rand_chi_squared"},
       {"arrayenumeratedense", "array_enumerate_dense"},
       {"multisearchallpositionscaseinsensitive", "multi_search_all_positions_case_insensitive"},
       {"rand64", "rand64"},
       {"tofloat32ordefault", "to_float32_or_default"},
       {"radians", "radians"},
       {"multisearchanycaseinsensitive", "multi_search_any_case_insensitive"},
       {"emptyarrayint32", "empty_array_int32"},
       {"randuniform", "rand_uniform"},
       {"tominute", "to_minute"},
       {"log2", "log2"},
       {"mapapply", "map_apply"},
       {"tuplecast", "tuple_cast"},
       {"ipv4stringtonumornull", "ipv4_string_to_num_or_null"},
       {"randomstringutf8", "random_string_utf8"},
       {"parsedatetime32besteffortorzero", "parse_datetime32_best_effort_or_zero"},
       {"mapcontainskeylike", "map_contains_key_like"},
       {"arrayreversefill", "array_reverse_fill"},
       {"reinterpretasstring", "reinterpret_as_string"},
       {"reinterpretasuuid", "reinterpret_as_uuid"},
       {"uuid", "uuid"},
       {"bitmasktoarray", "bitmask_to_array"},
       {"tostartofmicrosecond", "to_start_of_microsecond"},
       {"tostartofhour", "to_start_of_hour"},
       {"normalizedqueryhash", "normalized_query_hash"},
       {"reinterpretasdatetime", "reinterpret_as_datetime"},
       {"tuplehammingdistance", "tuple_hamming_distance"},
       {"touint8ordefault", "to_uint8_or_default"},
       {"reinterpretasfloat64", "reinterpret_as_float64"},
       {"tocolumntypename", "to_column_type_name"},
       {"reinterpretasfloat32", "reinterpret_as_float32"},
       {"nowinblock", "now_in_block"},
       {"sigmoid", "sigmoid"},
       {"format", "format"},
       {"emptyarrayint16", "empty_array_int16"},
       {"replaceregexpone", "replace_regexp_one"},
       {"reinterpretasint256", "reinterpret_as_int256"},
       {"fromunixtimestamp64micro", "from_unix_timestamp64_micro"},
       {"simplejsonextractstring", "simple_json_extract_string"},
       {"reinterpretasint64", "reinterpret_as_int64"},
       {"bitmaptoarray", "bitmap_to_array"},
       {"normalizedqueryhashkeepnames", "normalized_query_hash_keep_names"},
       {"reinterpretasint16", "reinterpret_as_int16"},
       {"xor", "xor"},
       {"addinterval", "add_interval"},
       {"reinterpretasuint128", "reinterpret_as_uint128"},
       {"intexp10", "int_exp10"},
       {"touint16ordefault", "to_uint16_or_default"},
       {"notin", "not_in"},
       {"randomstring", "random_string"},
       {"dicthas", "dict_has"},
       {"mapsubtract", "map_subtract"},
       {"linfnormalize", "linf_normalize"},
       {"hasany", "has_any"},
       {"tohour", "to_hour"},
       {"readwktpolygon", "read_wkt_polygon"},
       {"reinterpretasuint32", "reinterpret_as_uint32"},
       {"wordshingleminhashargcaseinsensitive", "word_shingle_min_hash_arg_case_insensitive"},
       {"simplejsonextractfloat", "simple_json_extract_float"},
       {"filesystemavailable", "filesystem_available"},
       {"readwktring", "read_wkt_ring"},
       {"addmilliseconds", "add_milliseconds"},
       {"cosinedistance", "cosine_distance"},
       {"minsamplesizecontinous", "min_sample_size_continous"},
       {"regexpquotemeta", "regexp_quote_meta"},
       {"emptyarrayuint32", "empty_array_uint32"},
       {"parsedatetimebesteffort", "parse_datetime_best_effort"},
       {"uptime", "uptime"},
       {"arraysort", "array_sort"},
       {"l1normalize", "l1_normalize"},
       {"arraydistinct", "array_distinct"},
       {"addquarters", "add_quarters"},
       {"ifnotfinite", "if_not_finite"},
       {"equals", "equals"},
       {"ifnull", "if_null"},
       {"regiontoarea", "region_to_area"},
       {"arrayjaccardindex", "array_jaccard_index"},
       {"right", "right"},
       {"dictgetall", "dict_get_all"},
       {"notequals", "not_equals"},
       {"s2capunion", "s2_cap_union"},
       {"factorial", "factorial"},
       {"tofloat32", "to_float32"},
       {"arraymin", "array_min"},
       {"h3getunidirectionaledgeboundary", "h3_get_unidirectional_edge_boundary"},
       {"multisearchfirstindexcaseinsensitive", "multi_search_first_index_case_insensitive"},
       {"revision", "revision"},
       {"visiblewidth", "visible_width"},
       {"synonyms", "synonyms"},
       {"randlognormal", "rand_log_normal"},
       {"multisearchfirstpositionutf8", "multi_search_first_position_utf8"},
       {"globalin", "global_in"},
       {"erf", "erf"},
       {"arraypopback", "array_pop_back"},
       {"s2rectunion", "s2_rect_union"},
       {"timezone", "timezone"},
       {"countequal", "count_equal"},
       {"isfinite", "is_finite"},
       {"emptyarrayint8", "empty_array_int8"},
       {"normalizequerykeepnames", "normalize_query_keep_names"},
       {"arrayexists", "array_exists"},
       {"shardnum", "shard_num"},
       {"hassubstr", "has_substr"},
       {"streamingnow", "__streaming_now"},
       {"touint16orzero", "to_uint16_or_zero"},
       {"multisearchfirstpositioncaseinsensitiveutf8", "multi_search_first_position_case_insensitive_utf8"},
       {"dictgetipv4", "dict_get_ipv4"},
       {"replaceregexpall", "replace_regexp_all"},
       {"like", "like"},
       {"tostartofmillisecond", "to_start_of_millisecond"},
       {"fromunixtimestampinjodasyntax", "from_unix_timestamp_in_joda_syntax"},
       {"tostartoffifteenminutes", "to_start_of_fifteen_minutes"},
       {"ngramdistanceutf8", "ngram_distance_utf8"},
       {"min2", "min2"},
       {"tostartofweek", "to_start_of_week"},
       {"toint32orzero", "to_int32_or_zero"},
       {"length", "length"},
       {"lpnorm", "lp_norm"},
       {"linfnorm", "linf_norm"},
       {"datetime64tosnowflake", "datetime64_to_snowflake"},
       {"touint128ornull", "to_uint128_or_null"},
       {"tupleminus", "tuple_minus"},
       {"validatenestedarraysizes", "validate_nested_array_sizes"},
       {"reinterpretasint8", "reinterpret_as_int8"},
       {"tolastdayofmonth", "to_last_day_of_month"},
       {"sign", "sign"},
       {"upper", "upper"},
       {"arrayintersect", "array_intersect"},
       {"notempty", "not_empty"},
       {"arraysplit", "array_split"},
       {"dictgetordefault", "dict_get_or_default"},
       {"tointervalday", "to_interval_day"},
       {"substringutf8", "substring_utf8"},
       {"parsedatetimebesteffortornull", "parse_datetime_best_effort_or_null"},
       {"toint256orzero", "to_int256_or_zero"},
       {"casewithexpr", "case_with_expr"},
       {"tanh", "tanh"},
       {"tostringcuttozero", "to_string_cut_to_zero"},
       {"arraywithconstant", "array_with_constant"},
       {"murmurhash232", "murmur_hash2_32"},
       {"erfc", "erfc"},
       {"frommodifiedjuliandayornull", "from_modified_julian_day_or_null"},
       {"replaceone", "replace_one"},
       {"multisearchallpositionsutf8", "multi_search_all_positions_utf8"},
       {"asinh", "asinh"},
       {"detecttonality", "detect_tonality"},
       {"bitxor", "bit_xor"},
       {"timezoneof", "timezone_of"},
       {"greaterorequals", "greater_or_equals"},
       {"jsonextractuint", "json_extract_uint"},
       {"h3getfaces", "h3_get_faces"},
       {"notinignoreset", "not_in_ignore_set"},
       {"jsonextractarray", "json_extract_array"},
       {"ngramminhashcaseinsensitive", "ngram_min_hash_case_insensitive"},
       {"arrayreduce", "array_reduce"},
       {"todayofyear", "to_day_of_year"},
       {"l2squarednorm", "l2_squared_norm"},
       {"h3tocenterchild", "h3_to_center_child"},
       {"tounixtimestamp", "to_unix_timestamp"},
       {"toint128ordefault", "to_int128_or_default"},
       {"degrees", "degrees"},
       {"tostartofyear", "to_start_of_year"},
       {"toisoweek", "to_iso_week"},
       {"regiontopopulation", "region_to_population"},
       {"geotos2", "geo_to_s2"},
       {"max2", "max2"},
       {"upperutf8", "upper_utf8"},
       {"torelativeminutenum", "to_relative_minute_num"},
       {"tolowcardinality", "to_low_cardinality"},
       {"parsedatetime64besteffortornull", "parse_datetime64_best_effort_or_null"},
       {"tounixtimestamp64milli", "to_unix_timestamp64_milli"},
       {"hostname", "hostname"},
       {"tomodifiedjuliandayornull", "to_modified_julian_day_or_null"},
       {"multiply", "multiply"},
       {"dictgetuint16ordefault", "dict_get_uint16_or_default"},
       {"splitbystring", "split_by_string"},
       {"pi", "pi"},
       {"version", "version"},
       {"linfdistance", "linf_distance"},
       {"torelativehournum", "to_relative_hour_num"},
       {"datediffwithin", "date_diff_within"},
       {"datediff", "date_diff"},
       {"toint64ordefault", "to_int64_or_default"},
       {"tounixtimestamp64nano", "to_unix_timestamp64_nano"},
       {"emptyarrayuint8", "empty_array_uint8"},
       {"multimatchallindices", "multi_match_all_indices"},
       {"toyyyymmddhhmmss", "to_yyyymmddhhmmss"},
       {"to_yyyymmddhhmmss", "to_yyyymmddhhmmss"},
       {"toint128orzero", "to_int128_or_zero"},
       {"tovalidutf8", "to_valid_utf8"},
       {"polygonswithincartesian", "polygons_within_cartesian"},
       {"arrayzip", "array_zip"},
       {"torelativeyearnum", "to_relative_year_num"},
       {"ngramsimhash", "ngram_sim_hash"},
       {"isipaddressinrange", "is_ip_address_in_range"},
       {"exp2", "exp2"},
       {"tcpport", "tcp_port"},
       {"arraylastornull", "array_last_or_null"},
       {"transform", "transform"},
       {"bitmapcontains", "bitmap_contains"},
       {"reinterpretasuint256", "reinterpret_as_uint256"},
       {"toipv6", "to_ipv6"},
       {"subtractmicroseconds", "subtract_microseconds"},
       {"defaultroles", "default_roles"},
       {"lag", "lag"},
       {"mapfilter", "map_filter"},
       {"bitmapcardinality", "bitmap_cardinality"},
       {"toyyyymmdd", "to_yyyymmdd"},
       {"to_yyyymmdd", "to_yyyymmdd"},
       {"lpnormalize", "lp_normalize"},
       {"l2squareddistance", "l2_squared_distance"},
       {"readwktpoint", "read_wkt_point"},
       {"trimboth", "trim_both"},
       {"s2rectintersection", "s2_rect_intersection"},
       {"greatest", "greatest"},
       {"l1distance", "l1_distance"},
       {"neighbor", "neighbor"},
       {"arrayreversesort", "array_reverse_sort"},
       {"trydecrypt", "try_decrypt"},
       {"tupledividebynumber", "tuple_divide_by_number"},
       {"touint64ornull", "to_uint64_or_null"},
       {"defaultvalueoftypename", "default_value_of_type_name"},
       {"h3distance", "h3_distance"},
       {"tgamma", "tgamma"},
       {"s2getneighbors", "s2_get_neighbors"},
       {"tupledivide", "tuple_divide"},
       {"arrayfirstindex", "array_first_index"},
       {"toint32ordefault", "to_int32_or_default"},
       {"addresstoline", "address_to_line"},
       {"assumenotnull", "assume_not_null"},
       {"simplejsonhas", "simple_json_has"},
       {"arrayjoin", "array_join"},
       {"bitmapandnotcardinality", "bitmap_andnot_cardinality"},
       {"detectprogramminglanguage", "detect_programming_language"},
       {"bitmapxor", "bitmap_xor"},
       {"gettypeserializationstreams", "get_type_serialization_streams"},
       {"reinterpretasuint8", "reinterpret_as_uint8"},
       {"timezoneoffset", "timezone_offset"},
       {"filesystemcapacity", "filesystem_capacity"},
       {"arraycast", "array_cast"},
       {"touint32ordefault", "to_uint32_or_default"},
       {"startswith", "starts_with"},
       {"tofloat", "to_float"},
       {"notilike", "not_ilike"},
       {"ngramdistance", "ngram_distance"},
       {"totime", "to_time"},
       {"multisearchanyutf8", "multi_search_any_utf8"},
       {"ngramdistancecaseinsensitive", "ngram_distance_case_insensitive"},
       {"ngramsearchcaseinsensitive", "ngram_search_case_insensitive"},
       {"arraycumsumnonnegative", "array_cum_sum_non_negative"},
       {"globalvariable", "global_variable"},
       {"tointervalweek", "to_interval_week"},
       {"indexof", "index_of"},
       {"translate", "translate"},
       {"h3numhexagons", "h3_num_hexagons"},
       {"sinh", "sinh"},
       {"parsedatetimeinjodasyntax", "parse_datetime_in_joda_syntax"},
       {"tointervalnanosecond", "to_interval_nanosecond"},
       {"extractall", "extract_all"},
       {"empty", "empty"},
       {"simplejsonextractint", "simple_json_extract_int"},
       {"todatetime", "to_datetime"},
       {"bar", "bar"},
       {"h3togeo", "h3_to_geo"},
       {"indexhint", "index_hint"},
       {"date_trunc", "date_trunc"},
       {"date_add", "date_add"},
       {"date_sub", "date_sub"},
       {"uniqthetanot", "uniq_theta_not"},
       {"ceil", "ceil"},
       {"dictgetuuidordefault", "dict_get_uuid_or_default"},
       {"utctimestamp", "utc_timestamp"},
       {"arraylast", "array_last"},
       {"l2distance", "l2_distance"},
       {"wordshingleminhash", "word_shingle_min_hash"},
       {"log10", "log10"},
       {"mappopulateseries", "map_populate_series"},
       {"todecimal256ornull", "to_decimal256_or_null"},
       {"arrayenumerateuniqranked", "array_enumerate_uniq_ranked"},
       {"makedatetime", "make_datetime"},
       {"shardcount", "shard_count"},
       {"h3cellarearads2", "h3_cell_area_rads2"},
       {"arrayfilter", "array_filter"},
       {"subtracttupleofintervals", "subtract_tuple_of_intervals"},
       {"bitnot", "bit_not"},
       {"jsonextractkeys", "json_extract_keys"},
       {"addresstolinewithinlines", "address_to_line_with_inlines"},
       {"l2norm", "l2_norm"},
       {"uniqthetaintersect", "uniq_theta_intersect"},
       {"hasall", "has_all"},
       {"reinterpretasuint64", "reinterpret_as_uint64"},
       {"tofixedstring", "to_fixed_string"},
       {"emptyarrayuint16", "empty_array_uint16"},
       {"tostartofinterval", "to_start_of_interval"},
       {"tostartofisoyear", "to_start_of_iso_year"},
       {"h3getindexesfromunidirectionaledge", "h3_get_indexes_from_unidirectional_edge"},
       {"arraypopfront", "array_pop_front"},
       {"reinterpretasuint16", "reinterpret_as_uint16"},
       {"dictgetipv6", "dict_get_ipv6"},
       {"arrayreversesplit", "array_reverse_split"},
       {"dictgetdescendants", "dict_get_descendants"},
       {"crc64", "crc64"},
       {"least", "least"},
       {"crc32ieee", "crc32_ieee"},
       {"todate32ordefault", "to_date32_or_default"},
       {"arrayavg", "array_avg"},
       {"isnull", "is_null"},
       {"tomonday", "to_monday"},
       {"range", "range"},
       {"toyyyymm", "to_yyyymm"},
       {"to_yyyymm", "to_yyyymm"},
       {"splitbychar", "split_by_char"},
       {"polygonconvexhullcartesian", "polygon_convex_hull_cartesian"},
       {"tuplemultiply", "tuple_multiply"},
       {"tupletonamevaluepairs", "tuple_to_name_value_pairs"},
       {"touuidordefault", "to_uuid_or_default"},
       {"greater", "greater"},
       {"arrayuniq", "array_uniq"},
       {"less", "less"},
       {"sha256", "sha256"},
       {"bittestany", "bit_test_any"},
       {"arrayreverse", "array_reverse"},
       {"earliesttimestamp", "earliest_timestamp"},
       {"tounixtimestamp64micro", "to_unix_timestamp64_micro"},
       {"arrayfill", "array_fill"},
       {"arrayproduct", "array_product"},
       /// case_insensitive_aggregate_functions
       {"uniqueretract", "__unique_retract"},
       {"groupuniqarrayretract", "group_uniq_array_retract"},
       {"minkretract", "__min_k_retract"},
       {"maxkretract", "__max_k_retract"},
       {"minretract", "__min_retract"},
       {"maxretract", "__max_retract"},
       {"sumretract", "__sum_retract"},
       {"exponentialtimedecayedcount", "exponential_time_decayed_count"},
       {"denserank", "dense_rank"},
       {"rank", "rank"},
       {"exponentialmovingaverage", "exponential_moving_average"},
       {"intervallengthsum", "interval_length_sum"},
       {"singlevalueornull", "single_value_or_null"},
       {"nothing", "nothing"},
       {"meanztest", "mean_ztest"},
       {"rankcorr", "rank_corr"},
       {"categoricalinformationvalue", "categorical_information_value"},
       {"sumkahanretract", "__sum_kahan_retract"},
       {"grouparraymovingavg", "group_array_moving_avg"},
       {"grouparraymovingsum", "group_array_moving_sum"},
       {"simplelinearregression", "simple_linear_regression"},
       {"entropy", "entropy"},
       {"exponentialtimedecayedsum", "exponential_time_decayed_sum"},
       {"retention", "retention"},
       {"histogram", "histogram"},
       {"uniqueexactretract", "__unique_exact_retract"},
       {"stochasticlogisticregression", "stochastic_logistic_regression"},
       {"groupbitmapxor", "group_bitmap_xor"},
       {"groupbitmapor", "group_bitmap_or"},
       {"groupbitmap", "group_bitmap"},
       {"cramersvbiascorrected", "cramers_v_bias_corrected"},
       {"cramersv", "cramers_v"},
       {"grouparray", "group_array"},
       {"p99", "p99"},
       {"anyheavy", "any_heavy"},
       {"groupbitand", "group_bit_and"},
       {"sparkbar", "sparkbar"},
       {"groupbitor", "group_bit_or"},
       {"leadinframe", "lead_in_frame"},
       {"deltasumtimestamp", "delta_sum_timestamp"},
       {"covarpop", "covar_pop"},
       {"topkexact", "top_k_exact"},
       {"p95", "p95"},
       {"approxtopksum", "approx_top_k_sum"},
       {"anylast", "any_last"},
       {"uniquecombined64", "unique_combined64"},
       {"quantilesexactexclusive", "quantiles_exact_exclusive"},
       {"uniquetheta", "unique_theta"},
       {"unique", "unique"},
       {"laginframe", "lag_in_frame"},
       {"summapfilteredwithoverflow", "sum_map_filtered_with_overflow"},
       {"quantilestdigest", "quantiles_t_digest"},
       {"uniquecombined", "unique_combined"},
       {"exponentialtimedecayedmax", "exponential_time_decayed_max"},
       {"minmappedarrays", "min_mapped_arrays"},
       {"maxk", "max_k"},
       {"summappedarrays", "sum_mapped_arrays"},
       {"corr", "corr"},
       {"mannwhitneyutest", "mann_whitney_utest"},
       {"stddevpop", "stddev_pop"},
       {"stddevsamp", "stddev_samp"},
       {"corrstable", "corr_stable"},
       {"avgretract", "__avg_retract"},
       {"covarsampstable", "covar_samp_stable"},
       {"stddevpopstable", "stddev_pop_stable"},
       {"maxintersections", "max_intersections"},
       {"kurtpop", "kurt_pop"},
       {"stddevsampstable", "stddev_samp_stable"},
       {"aggthrow", "agg_throw"},
       {"varsampstable", "var_samp_stable"},
       {"lastvalue", "last_value"},
       {"xirr", "xirr"},
       {"latest", "latest"},
       {"summapfiltered", "sum_map_filtered"},
       {"any", "any"},
       {"groupuniqarray", "group_uniq_array"},
       {"skewpop", "skew_pop"},
       {"max", "max"},
       {"analysisofvariance", "analysis_of_variance"},
       {"quantilebfloat16weighted", "quantile_b_float16_weighted"},
       {"countretract", "__count_retract"},
       {"groupbitxor", "group_bit_xor"},
       {"quantilesbfloat16", "quantiles_b_float16"},
       {"boundingratio", "bounding_ratio"},
       {"skewsamp", "skew_samp"},
       {"p90", "p90"},
       {"windowfunnel", "window_funnel"},
       {"quantilestdigestweighted", "quantiles_t_digest_weighted"},
       {"quantilebfloat16", "quantile_b_float16"},
       {"topkexactweighted", "top_k_exact_weighted"},
       {"quantiletdigestweighted", "quantile_t_digest_weighted"},
       {"welchttest", "welch_ttest"},
       {"quantiletdigest", "quantile_t_digest"},
       {"quantilestimingweighted", "quantiles_timing_weighted"},
       {"contingency", "contingency"},
       {"deltasum", "delta_sum"},
       {"topkweighted", "top_k_weighted"},
       {"quantiletimingweighted", "quantile_timing_weighted"},
       {"uniqueexact", "unique_exact"},
       {"quantiletiming", "quantile_timing"},
       {"quantilesexactinclusive", "quantiles_exact_inclusive"},
       {"studentttest", "student_ttest"},
       {"groupbitmapand", "group_bitmap_and"},
       {"sequencecount", "sequence_count"},
       {"avg", "avg"},
       {"rownumber", "row_number"},
       {"theilsu", "theils_u"},
       {"quantilesexacthigh", "quantiles_exact_high"},
       {"varpopstable", "var_pop_stable"},
       {"sumwithoverflow", "sum_with_overflow"},
       {"quantileexactweighted", "quantile_exact_weighted"},
       {"sequencenextnode", "sequence_next_node"},
       {"quantileexacthigh", "quantile_exact_high"},
       {"quantilesexactlow", "quantiles_exact_low"},
       {"quantilesexactweighted", "quantiles_exact_weighted"},
       {"quantilestiming", "quantiles_timing"},
       {"grouparraysample", "group_array_sample"},
       {"uniquehll12", "unique_hll12"},
       {"summapwithoverflow", "sum_map_with_overflow"},
       {"sumcount", "sum_count"},
       {"earliest", "earliest"},
       {"quantilesdeterministic", "quantiles_deterministic"},
       {"sumwithoverflowretract", "__sum_with_overflow_retract"},
       {"min", "min"},
       {"varpop", "var_pop"},
       {"quantileexactlow", "quantile_exact_low"},
       {"quantiles", "quantiles"},
       {"sum", "sum"},
       {"kurtsamp", "kurt_samp"},
       {"grouparrayinsertat", "group_array_insert_at"},
       {"quantilesexact", "quantiles_exact"},
       {"approxtopk", "approx_top_k"},
       {"exponentialtimedecayedavg", "exponential_time_decayed_avg"},
       {"firstvalue", "first_value"},
       {"quantileexact", "quantile_exact"},
       {"covarpopstable", "covar_pop_stable"},
       {"sequencematch", "sequence_match"},
       {"quantilesbfloat16weighted", "quantiles_b_float16_weighted"},
       {"uniqueupto", "unique_up_to"},
       {"largesttrianglethreebuckets", "largest_triangle_three_buckets"},
       {"quantile", "quantile"},
       {"covarsamp", "covar_samp"},
       {"varsamp", "var_samp"},
       {"quantiledeterministic", "quantile_deterministic"},
       {"quantileexactinclusive", "quantile_exact_inclusive"},
       {"maxintersectionsposition", "max_intersections_position"},
       {"maxmappedarrays", "max_mapped_arrays"},
       {"mink", "min_k"},
       {"quantileexactexclusive", "quantile_exact_exclusive"},
       {"sumkahan", "sum_kahan"},
       {"stochasticlinearregression", "stochastic_linear_regression"},
       {"count", "count"},
       {"topk", "top_k"},
       {"avgweighted", "avg_weighted"}};

String ParserFunction::functionNameFromCamelToSnake(const String & name) const
{
    auto it = function_map.find(name);
    if (it != function_map.end())
        return it->second;

    String lower_name = Poco::toLower(name);
    it = case_insensitive_function_map.find(lower_name);
    if (it != case_insensitive_function_map.end())
        return it->second;

    return name;
}
/// proton: ends

bool ParserFunction::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    ParserIdentifier id_parser;

    bool has_all = false;
    bool has_distinct = false;

    ASTPtr identifier;
    ASTPtr query;
    ASTPtr expr_list_args;
    /// proton: starts
    ASTPtr expr_list_params;
    /// proton: ends

    if (is_table_function)
    {
        if (ParserTableFunctionView().parse(pos, node, expected))
            return true;
    }

    if (!id_parser.parse(pos, identifier, expected))
        return false;

    if (pos->type != TokenType::OpeningRoundBracket)
        return false;
    ++pos;

    /// Avoid excessive backtracking.
    //pos.putBarrier();

    /// Special cases for expressions that look like functions but contain some syntax sugar:

    /// CAST, EXTRACT, POSITION, EXISTS
    /// DATE_ADD, DATEADD, TIMESTAMPADD, DATE_SUB, DATESUB, TIMESTAMPSUB,
    /// DATE_DIFF, DATEDIFF, TIMESTAMPDIFF, TIMESTAMP_DIFF,
    /// SUBSTRING, TRIM, LTRIM, RTRIM, POSITION

    /// Can be parsed as a composition of functions, but the contents must be unwrapped:
    /// POSITION(x IN y) -> POSITION(in(x, y)) -> POSITION(y, x)

    /// Can be parsed as a function, but not always:
    /// CAST(x AS type) - alias has to be unwrapped
    /// CAST(x AS type(params))

    /// Can be parsed as a function, but some identifier arguments have special meanings.
    /// DATE_ADD(MINUTE, x, y) -> addMinutes(x, y)
    /// DATE_DIFF(MINUTE, x, y)

    /// Have keywords that have to processed explicitly:
    /// EXTRACT(x FROM y)
    /// TRIM(BOTH|LEADING|TRAILING x FROM y)
    /// SUBSTRING(x FROM a)
    /// SUBSTRING(x FROM a FOR b)

    String function_name = getIdentifierName(identifier);
    String function_name_lowercase = Poco::toLower(function_name);

    /// proton: starts
    if (thread_local_is_clickhouse_compatible)
    {
        String tmp_name = functionNameFromCamelToSnake(function_name);
        function_name = tmp_name;
        auto * tmpnode = dynamic_cast<ASTIdentifier *>(identifier.get());
        tmpnode->setShortName(function_name);
    }
    /// proton: ends

    std::optional<bool> parsed_special_function;

    if (function_name_lowercase == "cast")
        parsed_special_function = parseCastAs(pos, node, expected);
    else if (function_name_lowercase == "extract")
        parsed_special_function = parseExtract(pos, node, expected);
    else if (function_name_lowercase == "substring")
        parsed_special_function = parseSubstring(pos, node, expected);
    else if (function_name_lowercase == "position")
        parsed_special_function = parsePosition(pos, node, expected);
    else if (function_name_lowercase == "exists")
        parsed_special_function = parseExists(pos, node, expected);
    else if (function_name_lowercase == "trim")
        parsed_special_function = parseTrim(false, false, pos, node, expected);
    else if (function_name_lowercase == "ltrim")
        parsed_special_function = parseTrim(true, false, pos, node, expected);
    else if (function_name_lowercase == "rtrim")
        parsed_special_function = parseTrim(false, true, pos, node, expected);
    else if (function_name_lowercase == "date_add" || function_name_lowercase == "timestamp_add")
        parsed_special_function = parseDateAdd("plus", pos, node, expected);
    else if (function_name_lowercase == "date_sub" || function_name_lowercase == "timestamp_sub")
        parsed_special_function = parseDateAdd("minus", pos, node, expected);
    else if (function_name_lowercase == "date_diff" || function_name_lowercase == "timestamp_diff")
        parsed_special_function = parseDateDiff(pos, node, expected);
    else if (function_name_lowercase == "grouping")
        parsed_special_function = parseGrouping(pos, node, expected);

    /// proton: starts. we shall show original name.
    /// e.g. for 'date_add(now(), 1s)', we don't show 'now() + 1s'.
    /// TODO: we can do a better impl after porting latest community code, for now reverted
    /// proton: ends.
    if (parsed_special_function.has_value())
        return parsed_special_function.value() && ParserToken(TokenType::ClosingRoundBracket).ignore(pos);

    auto pos_after_bracket = pos;
    auto old_expected = expected;

    ParserKeyword all("ALL");
    ParserKeyword distinct("DISTINCT");

    if (all.ignore(pos, expected))
        has_all = true;

    if (distinct.ignore(pos, expected))
        has_distinct = true;

    if (!has_all && all.ignore(pos, expected))
        has_all = true;

    if (has_all && has_distinct)
        return false;

    if (has_all || has_distinct)
    {
        /// case f(ALL), f(ALL, x), f(DISTINCT), f(DISTINCT, x), ALL and DISTINCT should be treat as identifier
        if (pos->type == TokenType::Comma || pos->type == TokenType::ClosingRoundBracket)
        {
            pos = pos_after_bracket;
            expected = old_expected;
            has_all = false;
            has_distinct = false;
        }
    }

    ParserExpressionList contents(false, is_table_function);

    const char * contents_begin = pos->begin;
    /// proton: starts. support session range comparision expression for table function `session`
    if (is_table_function && function_name == "session")
    {
        if (!ParserList(
                 std::make_unique<ParserSessionRangeComparisonExpressionIfPossible>(
                     std::make_unique<ParserExpressionWithOptionalAlias>(false, true)),
                 std::make_unique<ParserToken>(TokenType::Comma))
                 .parse(pos, expr_list_args, expected))
            return false;
    }
    else
    {
        if (!contents.parse(pos, expr_list_args, expected))
            return false;
    }
    /// proton: ends.
    const char * contents_end = pos->begin;

    if (pos->type != TokenType::ClosingRoundBracket)
        return false;
    ++pos;

    /** Check for a common error case - often due to the complexity of quoting command-line arguments,
      *  an expression of the form toDate(2014-01-01) appears in the query instead of toDate('2014-01-01').
      * If you do not report that the first option is an error, then the argument will be interpreted as 2014 - 01 - 01 - some number,
      *  and the query silently returns an unexpected result.
      */
    if (function_name == "to_date"
        && contents_end - contents_begin == strlen("2014-01-01")
        && contents_begin[0] >= '2' && contents_begin[0] <= '3'
        && contents_begin[1] >= '0' && contents_begin[1] <= '9'
        && contents_begin[2] >= '0' && contents_begin[2] <= '9'
        && contents_begin[3] >= '0' && contents_begin[3] <= '9'
        && contents_begin[4] == '-'
        && contents_begin[5] >= '0' && contents_begin[5] <= '9'
        && contents_begin[6] >= '0' && contents_begin[6] <= '9'
        && contents_begin[7] == '-'
        && contents_begin[8] >= '0' && contents_begin[8] <= '9'
        && contents_begin[9] >= '0' && contents_begin[9] <= '9')
    {
        std::string contents_str(contents_begin, contents_end - contents_begin);
        throw Exception("Argument of function to_date is unquoted: to_date(" + contents_str + "), must be: to_date('" + contents_str + "')"
            , ErrorCodes::SYNTAX_ERROR);
    }

    /// proton: starts
    /// First determine whether need to be compatible with ClickHouse. Timeplus does not allow the use of syntax such as quantile(0.9)(x).
    /// The parametric aggregate function has two lists (parameters and arguments) in parentheses. Example: quantile(0.9)(x).
    if (thread_local_is_clickhouse_compatible && allow_function_parameters && pos->type == TokenType::OpeningRoundBracket)
    {
        ++pos;

        /// Parametric aggregate functions cannot have DISTINCT in parameters list.
        if (has_distinct)
            return false;

        expr_list_params = expr_list_args;
        expr_list_args = nullptr;

        pos_after_bracket = pos;
        old_expected = expected;

        if (all.ignore(pos, expected))
            has_all = true;

        if (distinct.ignore(pos, expected))
            has_distinct = true;

        if (!has_all && all.ignore(pos, expected))
            has_all = true;

        if (has_all && has_distinct)
            return false;

        if (has_all || has_distinct)
        {
            /// case f(ALL), f(ALL, x), f(DISTINCT), f(DISTINCT, x), ALL and DISTINCT should be treat as identifier
            if (pos->type == TokenType::Comma || pos->type == TokenType::ClosingRoundBracket)
            {
                pos = pos_after_bracket;
                expected = old_expected;
                has_distinct = false;
            }
        }

        if (!contents.parse(pos, expr_list_args, expected))
            return false;

        if (pos->type != TokenType::ClosingRoundBracket)
            return false;
        ++pos;
    }
    /// proton: ends

    /// proton: starts.
    /// proton: don't support parametric aggregation function any more.
    /// proton: ends.

    auto function_node = std::make_shared<ASTFunction>();
    tryGetIdentifierNameInto(identifier, function_node->name);

    /// func(DISTINCT ...) is equivalent to func_distinct(...)
    if (has_distinct)
        function_node->name += "_distinct";

    function_node->arguments = expr_list_args;
    function_node->children.push_back(function_node->arguments);

    /// proton: starts
    if (expr_list_params)
    {
        function_node->parameters = expr_list_params;
        function_node->children.push_back(function_node->parameters);
    }
    /// proton: ends

    ParserKeyword filter("FILTER");
    ParserKeyword over("OVER");

    if (filter.ignore(pos, expected))
    {
        // We are slightly breaking the parser interface by parsing the window
        // definition into an existing ASTFunction. Normally it would take a
        // reference to ASTPtr and assign it the new node. We only have a pointer
        // of a different type, hence this workaround with a temporary pointer.
        ASTPtr function_node_as_iast = function_node;

        ParserFilterClause filter_parser;
        if (!filter_parser.parse(pos, function_node_as_iast, expected))
            return false;
    }

    if (over.ignore(pos, expected))
    {
        function_node->is_window_function = true;

        ASTPtr function_node_as_iast = function_node;

        ParserWindowReference window_reference;
        if (!window_reference.parse(pos, function_node_as_iast, expected))
            return false;
    }

    node = function_node;
    return true;
}

bool ParserTableFunctionView::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    ParserIdentifier id_parser;
    ParserKeyword view("VIEW");
    ParserSelectWithUnionQuery select;

    ASTPtr identifier;
    ASTPtr query;

    if (!view.ignore(pos, expected))
        return false;

    if (pos->type != TokenType::OpeningRoundBracket)
        return false;

    ++pos;

    bool maybe_an_subquery = pos->type == TokenType::OpeningRoundBracket;

    if (!select.parse(pos, query, expected))
        return false;

    auto & select_ast = query->as<ASTSelectWithUnionQuery &>();
    if (select_ast.list_of_selects->children.size() == 1 && maybe_an_subquery)
    {
        // It's an subquery. Bail out.
        return false;
    }

    if (pos->type != TokenType::ClosingRoundBracket)
        return false;
    ++pos;
    auto function_node = std::make_shared<ASTFunction>();
    tryGetIdentifierNameInto(identifier, function_node->name);
    auto expr_list_with_single_query = std::make_shared<ASTExpressionList>();
    expr_list_with_single_query->children.push_back(query);
    function_node->name = "view";
    function_node->arguments = expr_list_with_single_query;
    function_node->children.push_back(function_node->arguments);
    node = function_node;
    return true;
}

bool ParserFilterClause::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    assert(node);
    ASTFunction & function = dynamic_cast<ASTFunction &>(*node);

    ParserToken parser_opening_bracket(TokenType::OpeningRoundBracket);
    if (!parser_opening_bracket.ignore(pos, expected))
    {
        return false;
    }

    ParserKeyword parser_where("WHERE");
    if (!parser_where.ignore(pos, expected))
    {
        return false;
    }
    ParserExpressionList parser_condition(false);
    ASTPtr condition;
    if (!parser_condition.parse(pos, condition, expected) || condition->children.size() != 1)
    {
        return false;
    }

    ParserToken parser_closing_bracket(TokenType::ClosingRoundBracket);
    if (!parser_closing_bracket.ignore(pos, expected))
    {
        return false;
    }
    /// proton: starts.
    function.name += "_if";
    /// proton: end.
    function.arguments->children.push_back(condition->children[0]);
    return true;
}

bool ParserWindowReference::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    assert(node);
    ASTFunction & function = dynamic_cast<ASTFunction &>(*node);

    // Variant 1:
    // function_name ( * ) OVER window_name
    if (pos->type != TokenType::OpeningRoundBracket)
    {
        ASTPtr window_name_ast;
        ParserIdentifier window_name_parser;
        if (window_name_parser.parse(pos, window_name_ast, expected))
        {
            function.window_name = getIdentifierName(window_name_ast);
            return true;
        }
        else
        {
            return false;
        }
    }

    // Variant 2:
    // function_name ( * ) OVER ( window_definition )
    ParserWindowDefinition parser_definition;
    return parser_definition.parse(pos, function.window_definition, expected);
}

static bool tryParseFrameDefinition(ASTWindowDefinition * node, IParser::Pos & pos,
    Expected & expected)
{
    ParserKeyword keyword_rows("ROWS");
    ParserKeyword keyword_groups("GROUPS");
    ParserKeyword keyword_range("RANGE");

    node->frame_is_default = false;
    if (keyword_rows.ignore(pos, expected))
    {
        node->frame_type = WindowFrame::FrameType::ROWS;
    }
    else if (keyword_groups.ignore(pos, expected))
    {
        node->frame_type = WindowFrame::FrameType::GROUPS;
    }
    else if (keyword_range.ignore(pos, expected))
    {
        node->frame_type = WindowFrame::FrameType::RANGE;
    }
    else
    {
        /* No frame clause. */
        node->frame_is_default = true;
        return true;
    }

    ParserKeyword keyword_between("BETWEEN");
    ParserKeyword keyword_unbounded("UNBOUNDED");
    ParserKeyword keyword_preceding("PRECEDING");
    ParserKeyword keyword_following("FOLLOWING");
    ParserKeyword keyword_and("AND");
    ParserKeyword keyword_current_row("CURRENT ROW");

    // There are two variants of grammar for the frame:
    // 1) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    // 2) ROWS UNBOUNDED PRECEDING
    // When the frame end is not specified (2), it defaults to CURRENT ROW.
    const bool has_frame_end = keyword_between.ignore(pos, expected);

    if (keyword_current_row.ignore(pos, expected))
    {
        node->frame_begin_type = WindowFrame::BoundaryType::Current;
    }
    else
    {
        ParserExpression parser_expression;
        if (keyword_unbounded.ignore(pos, expected))
        {
            node->frame_begin_type = WindowFrame::BoundaryType::Unbounded;
        }
        else if (parser_expression.parse(pos, node->frame_begin_offset, expected))
        {
            // We will evaluate the expression for offset expression later.
            node->frame_begin_type = WindowFrame::BoundaryType::Offset;
        }
        else
        {
            return false;
        }

        if (keyword_preceding.ignore(pos, expected))
        {
            node->frame_begin_preceding = true;
        }
        else if (keyword_following.ignore(pos, expected))
        {
            node->frame_begin_preceding = false;
            if (node->frame_begin_type == WindowFrame::BoundaryType::Unbounded)
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Frame start cannot be UNBOUNDED FOLLOWING");
            }
        }
        else
        {
            return false;
        }
    }

    if (has_frame_end)
    {
        if (!keyword_and.ignore(pos, expected))
        {
            return false;
        }

        if (keyword_current_row.ignore(pos, expected))
        {
            node->frame_end_type = WindowFrame::BoundaryType::Current;
        }
        else
        {
            ParserExpression parser_expression;
            if (keyword_unbounded.ignore(pos, expected))
            {
                node->frame_end_type = WindowFrame::BoundaryType::Unbounded;
            }
            else if (parser_expression.parse(pos, node->frame_end_offset, expected))
            {
                // We will evaluate the expression for offset expression later.
                node->frame_end_type = WindowFrame::BoundaryType::Offset;
            }
            else
            {
                return false;
            }

            if (keyword_preceding.ignore(pos, expected))
            {
                node->frame_end_preceding = true;
                if (node->frame_end_type == WindowFrame::BoundaryType::Unbounded)
                {
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Frame end cannot be UNBOUNDED PRECEDING");
                }
            }
            else if (keyword_following.ignore(pos, expected))
            {
                // Positive offset or UNBOUNDED FOLLOWING.
                node->frame_end_preceding = false;
            }
            else
            {
                return false;
            }
        }
    }

    return true;
}

// All except parent window name.
static bool parseWindowDefinitionParts(IParser::Pos & pos,
    ASTWindowDefinition & node, Expected & expected)
{
    ParserKeyword keyword_partition_by("PARTITION BY");
    ParserNotEmptyExpressionList columns_partition_by(
        false /* we don't allow declaring aliases here*/);
    ParserKeyword keyword_order_by("ORDER BY");
    ParserOrderByExpressionList columns_order_by;

    if (keyword_partition_by.ignore(pos, expected))
    {
        ASTPtr partition_by_ast;
        if (columns_partition_by.parse(pos, partition_by_ast, expected))
        {
            node.children.push_back(partition_by_ast);
            node.partition_by = partition_by_ast;
        }
        else
        {
            return false;
        }
    }

    if (keyword_order_by.ignore(pos, expected))
    {
        ASTPtr order_by_ast;
        if (columns_order_by.parse(pos, order_by_ast, expected))
        {
            node.children.push_back(order_by_ast);
            node.order_by = order_by_ast;
        }
        else
        {
            return false;
        }
    }

    return tryParseFrameDefinition(&node, pos, expected);
}

bool ParserWindowDefinition::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    auto result = std::make_shared<ASTWindowDefinition>();

    ParserToken parser_openging_bracket(TokenType::OpeningRoundBracket);
    if (!parser_openging_bracket.ignore(pos, expected))
    {
        return false;
    }

    // We can have a parent window name specified before all other things. No
    // easy way to distinguish identifier from keywords, so just try to parse it
    // both ways.
    if (parseWindowDefinitionParts(pos, *result, expected))
    {
        // Successfully parsed without parent window specifier. It can be empty,
        // so check that it is followed by the closing bracket.
        ParserToken parser_closing_bracket(TokenType::ClosingRoundBracket);
        if (parser_closing_bracket.ignore(pos, expected))
        {
            node = result;
            return true;
        }
    }

    // Try to parse with parent window specifier.
    ParserIdentifier parser_parent_window;
    ASTPtr window_name_identifier;
    if (!parser_parent_window.parse(pos, window_name_identifier, expected))
    {
        return false;
    }
    result->parent_window_name = window_name_identifier->as<const ASTIdentifier &>().name();

    if (!parseWindowDefinitionParts(pos, *result, expected))
    {
        return false;
    }

    ParserToken parser_closing_bracket(TokenType::ClosingRoundBracket);
    if (!parser_closing_bracket.ignore(pos, expected))
    {
        return false;
    }

    node = result;
    return true;
}

bool ParserWindowList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    auto result = std::make_shared<ASTExpressionList>();

    for (;;)
    {
        auto elem = std::make_shared<ASTWindowListElement>();

        ParserIdentifier parser_window_name;
        ASTPtr window_name_identifier;
        if (!parser_window_name.parse(pos, window_name_identifier, expected))
        {
            return false;
        }
        elem->name = getIdentifierName(window_name_identifier);

        ParserKeyword keyword_as("AS");
        if (!keyword_as.ignore(pos, expected))
        {
            return false;
        }

        ParserWindowDefinition parser_window_definition;
        if (!parser_window_definition.parse(pos, elem->definition, expected))
        {
            return false;
        }

        result->children.push_back(elem);

        // If the list countinues, there should be a comma.
        ParserToken parser_comma(TokenType::Comma);

        if (!parser_comma.ignore(pos, false))
        {
            break;
        }
    }

    node = result;
    return true;
}

bool ParserCodecDeclarationList::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    return ParserList(std::make_unique<ParserIdentifierWithOptionalParameters>(),
        std::make_unique<ParserToken>(TokenType::Comma), false).parse(pos, node, expected);
}

bool ParserCodec::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    ParserCodecDeclarationList codecs;
    ASTPtr expr_list_args;

    if (pos->type != TokenType::OpeningRoundBracket)
        return false;

    ++pos;
    if (!codecs.parse(pos, expr_list_args, expected))
        return false;

    if (pos->type != TokenType::ClosingRoundBracket)
        return false;
    ++pos;

    auto function_node = std::make_shared<ASTFunction>();
    function_node->name = "CODEC";
    function_node->arguments = expr_list_args;
    function_node->children.push_back(function_node->arguments);

    node = function_node;
    return true;
}


template <TokenType ...tokens>
static bool isOneOf(TokenType token)
{
    return ((token == tokens) || ...);
}

bool ParserCastOperator::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    /// Parse numbers (including decimals), strings, arrays and tuples of them.

    const char * data_begin = pos->begin;
    const char * data_end = pos->end;
    bool is_string_literal = pos->type == TokenType::StringLiteral;

    if (pos->type == TokenType::Minus)
    {
        ++pos;
        if (pos->type != TokenType::Number)
            return false;

        data_end = pos->end;
        ++pos;
    }
    else if (pos->type == TokenType::Number || is_string_literal)
    {
        ++pos;
    }
    else if (isOneOf<TokenType::OpeningSquareBracket, TokenType::OpeningRoundBracket>(pos->type))
    {
        TokenType last_token = TokenType::OpeningSquareBracket;
        std::vector<TokenType> stack;
        while (pos.isValid())
        {
            if (isOneOf<TokenType::OpeningSquareBracket, TokenType::OpeningRoundBracket>(pos->type))
            {
                stack.push_back(pos->type);
                if (!isOneOf<TokenType::OpeningSquareBracket, TokenType::OpeningRoundBracket, TokenType::Comma>(last_token))
                    return false;
            }
            else if (pos->type == TokenType::ClosingSquareBracket)
            {
                if (isOneOf<TokenType::Comma, TokenType::OpeningRoundBracket, TokenType::Minus>(last_token))
                    return false;
                if (stack.empty() || stack.back() != TokenType::OpeningSquareBracket)
                    return false;
                stack.pop_back();
            }
            else if (pos->type == TokenType::ClosingRoundBracket)
            {
                if (isOneOf<TokenType::Comma, TokenType::OpeningSquareBracket, TokenType::Minus>(last_token))
                    return false;
                if (stack.empty() || stack.back() != TokenType::OpeningRoundBracket)
                    return false;
                stack.pop_back();
            }
            else if (pos->type == TokenType::Comma)
            {
                if (isOneOf<TokenType::OpeningSquareBracket, TokenType::OpeningRoundBracket, TokenType::Comma, TokenType::Minus>(last_token))
                    return false;
            }
            else if (pos->type == TokenType::Number)
            {
                if (!isOneOf<TokenType::OpeningSquareBracket, TokenType::OpeningRoundBracket, TokenType::Comma, TokenType::Minus>(last_token))
                    return false;
            }
            else if (isOneOf<TokenType::StringLiteral, TokenType::Minus>(pos->type))
            {
                if (!isOneOf<TokenType::OpeningSquareBracket, TokenType::OpeningRoundBracket, TokenType::Comma>(last_token))
                    return false;
            }
            else
            {
                break;
            }

            /// Update data_end on every iteration to avoid appearances of extra trailing
            /// whitespaces into data. Whitespaces are skipped at operator '++' of Pos.
            data_end = pos->end;
            last_token = pos->type;
            ++pos;
        }

        if (!stack.empty())
            return false;
    }
    else
        return false;

    ASTPtr type_ast;

    if (ParserToken(TokenType::DoubleColon).ignore(pos, expected, false)
        && ParserDataType().parse(pos, type_ast, expected))
    {
        String s;
        size_t data_size = data_end - data_begin;
        if (is_string_literal)
        {
            ReadBufferFromMemory buf(data_begin, data_size);
            readQuotedStringWithSQLStyle(s, buf);
            assert(buf.count() == data_size);
        }
        else
            s = String(data_begin, data_size);

        auto literal = std::make_shared<ASTLiteral>(std::move(s));
        node = createFunctionCast(literal, type_ast);
        return true;
    }

    return false;
}


bool ParserNull::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    ParserKeyword nested_parser("NULL");
    if (nested_parser.parse(pos, node, expected))
    {
        node = std::make_shared<ASTLiteral>(Null());
        return true;
    }
    else
        return false;
}


bool ParserBool::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    if (ParserKeyword("true").parse(pos, node, expected))
    {
        node = std::make_shared<ASTLiteral>(true);
        return true;
    }
    else if (ParserKeyword("false").parse(pos, node, expected))
    {
        node = std::make_shared<ASTLiteral>(false);
        return true;
    }
    else
        return false;
}

static bool parseNumber(char * buffer, size_t size, bool negative, int base, Field & res)
{
    errno = 0;    /// Functions strto* don't clear errno.

    char * pos_integer = buffer;
    UInt64 uint_value = std::strtoull(buffer, &pos_integer, base);

    if (pos_integer == buffer + size && errno != ERANGE && (!negative || uint_value <= (1ULL << 63)))
    {
        if (negative)
            res = static_cast<Int64>(-uint_value);
        else
            res = uint_value;

        return true;
    }

    return false;
}

bool ParserNumber::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    Pos literal_begin = pos;
    bool negative = false;

    if (pos->type == TokenType::Minus)
    {
        ++pos;
        negative = true;
    }
    else if (pos->type == TokenType::Plus)  /// Leading plus is simply ignored.
        ++pos;

    Field res;

    if (!pos.isValid())
        return false;

    auto try_read_float = [&](const char * it, const char * end)
    {
        char * str_end;
        errno = 0;    /// Functions strto* don't clear errno.
        Float64 float_value = std::strtod(it, &str_end);
        if (str_end == end && errno != ERANGE)
        {
            if (float_value < 0)
                throw Exception("Logical error: token number cannot begin with minus, but parsed float number is less than zero.", ErrorCodes::LOGICAL_ERROR);

            if (negative)
                float_value = -float_value;

            res = float_value;

            auto literal = std::make_shared<ASTLiteral>(res);
            literal->begin = literal_begin;
            literal->end = ++pos;
            node = literal;

            return true;
        }

        expected.add(pos, "number");
        return false;
    };

    /// NaN and Inf
    if (pos->type == TokenType::BareWord)
    {
        return try_read_float(pos->begin, pos->end);
    }

    if (pos->type != TokenType::Number)
    {
        expected.add(pos, "number");
        return false;
    }

    /** Maximum length of number. 319 symbols is enough to write maximum double in decimal form.
      * Copy is needed to use strto* functions, which require 0-terminated string.
      */
    static constexpr size_t MAX_LENGTH_OF_NUMBER = 319;

    char buf[MAX_LENGTH_OF_NUMBER + 1];

    size_t buf_size = 0;
    for (const auto * it = pos->begin; it != pos->end; ++it)
    {
        if (*it != '_')
            buf[buf_size++] = *it;
        if (unlikely(buf_size > MAX_LENGTH_OF_NUMBER))
        {
            expected.add(pos, "number");
            return false;
        }
    }

    size_t size = buf_size;
    buf[size] = 0;
    char * start_pos = buf;

    if (*start_pos == '0')
    {
        ++start_pos;
        --size;

        /// binary
        if (*start_pos == 'b')
        {
            ++start_pos;
            --size;
            if (parseNumber(start_pos, size, negative, 2, res))
            {
                auto literal = std::make_shared<ASTLiteral>(res);
                literal->begin = literal_begin;
                literal->end = ++pos;
                node = literal;

                return true;
            }
            else
                return false;
        }

        /// hexadecimal
        if (*start_pos == 'x' || *start_pos == 'X')
        {
            ++start_pos;
            --size;
            if (parseNumber(start_pos, size, negative, 16, res))
            {
                auto literal = std::make_shared<ASTLiteral>(res);
                literal->begin = literal_begin;
                literal->end = ++pos;
                node = literal;

                return true;
            }
        }
        else
        {
            /// possible leading zeroes in integer
            while (*start_pos == '0')
            {
                ++start_pos;
                --size;
            }
            if (parseNumber(start_pos, size, negative, 10, res))
            {
                auto literal = std::make_shared<ASTLiteral>(res);
                literal->begin = literal_begin;
                literal->end = ++pos;
                node = literal;

                return true;
            }
        }
    }
    else if (parseNumber(start_pos, size, negative, 10, res))
    {
        auto literal = std::make_shared<ASTLiteral>(res);
        literal->begin = literal_begin;
        literal->end = ++pos;
        node = literal;

        return true;
    }

    return try_read_float(buf, buf + buf_size);
}


bool ParserUnsignedInteger::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    Field res;

    if (!pos.isValid())
        return false;

    UInt64 x = 0;
    ReadBufferFromMemory in(pos->begin, pos->size());
    if (!tryReadIntText(x, in) || in.count() != pos->size())
    {
        if (hint)
            expected.add(pos, "unsigned integer");
        return false;
    }

    res = x;
    auto literal = std::make_shared<ASTLiteral>(res);
    literal->begin = pos;
    literal->end = ++pos;
    node = literal;
    return true;
}


bool ParserStringLiteral::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    if (pos->type != TokenType::StringLiteral && pos->type != TokenType::HereDoc)
        return false;

    String s;

    if (pos->type == TokenType::StringLiteral)
    {
        ReadBufferFromMemory in(pos->begin, pos->size());

        try
        {
            readQuotedStringWithSQLStyle(s, in);
        }
        catch (const Exception &)
        {
            if (hint)
                expected.add(pos, "string literal");
            return false;
        }

        if (in.count() != pos->size())
        {
            if (hint)
                expected.add(pos, "string literal");
            return false;
        }
    }
    else if (pos->type == TokenType::HereDoc)
    {
        std::string_view here_doc(pos->begin, pos->size());
        size_t heredoc_size = here_doc.find('$', 1) + 1;
        assert(heredoc_size != std::string_view::npos);
        s = String(pos->begin + heredoc_size, pos->size() - heredoc_size * 2);
    }

    auto literal = std::make_shared<ASTLiteral>(s);
    literal->begin = pos;
    literal->end = ++pos;
    node = literal;
    return true;
}

template <typename Collection>
bool ParserCollectionOfLiterals<Collection>::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    if (pos->type != opening_bracket)
        return false;

    Pos literal_begin = pos;

    Collection arr;
    ParserLiteral literal_p;
    ParserCollectionOfLiterals<Collection> collection_p(opening_bracket, closing_bracket);

    ++pos;
    while (pos.isValid())
    {
        if (!arr.empty())
        {
            if (pos->type == closing_bracket)
            {
                std::shared_ptr<ASTLiteral> literal;

                /// Parse one-element tuples (e.g. (1)) later as single values for backward compatibility.
                if (std::is_same_v<Collection, Tuple> && arr.size() == 1)
                    return false;

                literal = std::make_shared<ASTLiteral>(std::move(arr));
                literal->begin = literal_begin;
                literal->end = ++pos;
                node = literal;
                return true;
            }
            else if (pos->type == TokenType::Comma)
            {
                ++pos;
            }
            else if (pos->type == TokenType::Colon && std::is_same_v<Collection, Map> && arr.size() % 2 == 1)
            {
                ++pos;
            }
            else
            {
                if (hint)
                    expected.add(pos, "comma or closing bracket");
                return false;
            }
        }

        ASTPtr literal_node;
        if (!literal_p.parse(pos, literal_node, expected) && !collection_p.parse(pos, literal_node, expected))
            return false;

        arr.push_back(literal_node->as<ASTLiteral &>().value);
    }

    if (hint)
        expected.add(pos, getTokenName(closing_bracket));
    return false;
}

template bool ParserCollectionOfLiterals<Array>::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint);
template bool ParserCollectionOfLiterals<Tuple>::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint);

namespace
{

class ICollection;
using Collections = std::vector<std::unique_ptr<ICollection>>;

class ICollection
{
public:
    virtual ~ICollection() = default;
    virtual bool parse(IParser::Pos & pos, Collections & collections, ASTPtr & node, Expected & expected) = 0;
};

template <class Container, TokenType end_token>
class CommonCollection : public ICollection
{
public:
    bool parse(IParser::Pos & pos, Collections & collections, ASTPtr & node, Expected & expected) override;

private:
    Container container;
};

class MapCollection : public ICollection
{
public:
    bool parse(IParser::Pos & pos, Collections & collections, ASTPtr & node, Expected & expected) override;

private:
    Map container;
};

bool parseAllCollectionsStart(IParser::Pos & pos, Collections & collections, Expected & /*expected*/)
{
    if (pos->type == TokenType::OpeningCurlyBrace)
        collections.push_back(std::make_unique<MapCollection>());
    else if (pos->type == TokenType::OpeningRoundBracket)
        collections.push_back(std::make_unique<CommonCollection<Tuple, TokenType::ClosingRoundBracket>>());
    else if (pos->type == TokenType::OpeningSquareBracket)
        collections.push_back(std::make_unique<CommonCollection<Array, TokenType::ClosingSquareBracket>>());
    else
        return false;

    ++pos;
    return true;
}

template <class Container, TokenType end_token>
bool CommonCollection<Container, end_token>::parse(IParser::Pos & pos, Collections & collections, ASTPtr & node, Expected & expected)
{
    if (node)
    {
        container.push_back(std::move(node->as<ASTLiteral &>().value));
        node.reset();
    }

    ASTPtr literal;
    ParserLiteral literal_p;
    ParserToken comma_p(TokenType::Comma);
    ParserToken end_p(end_token);

    while (true)
    {
        if (end_p.ignore(pos, expected))
        {
            node = std::make_shared<ASTLiteral>(std::move(container));
            break;
        }

        if (!container.empty() && !comma_p.ignore(pos, expected))
                return false;

        if (literal_p.parse(pos, literal, expected))
            container.push_back(std::move(literal->as<ASTLiteral &>().value));
        else
            return parseAllCollectionsStart(pos, collections, expected);
    }

    return true;
}

bool MapCollection::parse(IParser::Pos & pos, Collections & collections, ASTPtr & node, Expected & expected)
{
    if (node)
    {
        container.push_back(std::move(node->as<ASTLiteral &>().value));
        node.reset();
    }

    ASTPtr literal;
    ParserLiteral literal_p;
    ParserToken comma_p(TokenType::Comma);
    ParserToken colon_p(TokenType::Colon);
    ParserToken end_p(TokenType::ClosingCurlyBrace);

    while (true)
    {
        if (end_p.ignore(pos, expected))
        {
            node = std::make_shared<ASTLiteral>(std::move(container));
            break;
        }

        if (!container.empty() && !comma_p.ignore(pos, expected))
            return false;

        if (!literal_p.parse(pos, literal, expected))
            return false;

        if (!colon_p.parse(pos, literal, expected))
            return false;

        container.push_back(std::move(literal->as<ASTLiteral &>().value));

        if (literal_p.parse(pos, literal, expected))
            container.push_back(std::move(literal->as<ASTLiteral &>().value));
        else
            return parseAllCollectionsStart(pos, collections, expected);
    }

    return true;
}

}


bool ParserAllCollectionsOfLiterals::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    Collections collections;

    if (!parseAllCollectionsStart(pos, collections, expected))
        return false;

    while (!collections.empty())
    {
        if (!collections.back()->parse(pos, collections, node, expected))
            return false;

        if (node)
            collections.pop_back();
    }

    return true;
}

bool ParserLiteral::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    ParserNull null_p;
    ParserNumber num_p;
    ParserBool bool_p;
    ParserStringLiteral str_p;

    if (null_p.parse(pos, node, expected))
        return true;

    if (num_p.parse(pos, node, expected))
        return true;

    if (bool_p.parse(pos, node, expected))
        return true;

    if (str_p.parse(pos, node, expected))
        return true;

    return false;
}


const char * ParserAlias::restricted_keywords[] =
{
    "ALL",
    "ANTI",
    "ANY",
    "ARRAY",
    "ASOF",
    "BETWEEN",
    "CROSS",
    "FINAL",
    "FORMAT",
    "FROM",
    "FULL",
    "GLOBAL",
    "GROUP",
    "HAVING",
    "ILIKE",
    "INNER",
    "INTO",
    "JOIN",
    "LEFT",
    "LIKE",
    "LIMIT",
    "NOT",
    "OFFSET",
    "ON",
    "ONLY", /// YQL's synonym for ANTI. Note: YQL is the name of one of proprietary languages, completely unrelated to ClickHouse.
    "ORDER",
    "PREWHERE",
    "RIGHT",
    "SAMPLE",
    "SEMI",
    "SETTINGS",
    "UNION",
    "USING",
    "WHERE",
    "WINDOW",
    "WITH",
    "INTERSECT",
    "EXCEPT",
    "EMIT",
    /// proton: starts.
    "PARTITION",
    "LATEST",
    /// proton: ends.
    nullptr
};

bool ParserAlias::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    ParserKeyword s_as("AS");
    ParserIdentifier id_p;

    bool has_as_word = s_as.ignore(pos, expected);
    if (!allow_alias_without_as_keyword && !has_as_word)
        return false;

    if (!id_p.parse(pos, node, expected))
        return false;

    if (!has_as_word)
    {
        /** In this case, the alias can not match the keyword -
          *  so that in the query "SELECT x FROM t", the word FROM was not considered an alias,
          *  and in the query "SELECT x FR FROM t", the word FR was considered an alias.
          */

        const String name = getIdentifierName(node);

        for (const char ** keyword = restricted_keywords; *keyword != nullptr; ++keyword)
            if (0 == strcasecmp(name.data(), *keyword))
                return false;
    }

    return true;
}


bool ParserColumnsMatcher::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    ParserKeyword columns("COLUMNS");
    ParserList columns_p(std::make_unique<ParserCompoundIdentifier>(false, true), std::make_unique<ParserToken>(TokenType::Comma), false);
    ParserStringLiteral regex;

    if (!columns.ignore(pos, expected))
        return false;

    if (pos->type != TokenType::OpeningRoundBracket)
        return false;
    ++pos;

    ASTPtr column_list;
    ASTPtr regex_node;
    if (!columns_p.parse(pos, column_list, expected) && !regex.parse(pos, regex_node, expected))
        return false;

    if (pos->type != TokenType::ClosingRoundBracket)
        return false;
    ++pos;

    ASTPtr res;
    if (column_list)
    {
        auto list_matcher = std::make_shared<ASTColumnsListMatcher>();
        list_matcher->column_list = column_list;
        res = list_matcher;
    }
    else
    {
        auto regexp_matcher = std::make_shared<ASTColumnsRegexpMatcher>();
        regexp_matcher->setPattern(regex_node->as<ASTLiteral &>().value.get<String>());
        res = regexp_matcher;
    }

    ParserColumnsTransformers transformers_p(allowed_transformers);
    ASTPtr transformer;
    while (transformers_p.parse(pos, transformer, expected))
    {
        res->children.push_back(transformer);
    }
    node = std::move(res);
    return true;
}


bool ParserColumnsTransformers::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    ParserKeyword apply("APPLY");
    ParserKeyword except("EXCEPT");
    ParserKeyword replace("REPLACE");
    ParserKeyword as("AS");
    ParserKeyword strict("STRICT");

    if (allowed_transformers.isSet(ColumnTransformer::APPLY) && apply.ignore(pos, expected))
    {
        bool with_open_round_bracket = false;

        if (pos->type == TokenType::OpeningRoundBracket)
        {
            ++pos;
            with_open_round_bracket = true;
        }

        ASTPtr lambda;
        String lambda_arg;
        ASTPtr func_name;
        ASTPtr expr_list_args;
        auto opos = pos;
        if (ParserLambdaExpression().parse(pos, lambda, expected))
        {
            if (const auto * func = lambda->as<ASTFunction>(); func && func->name == "lambda")
            {
                if (func->arguments->children.size() != 2)
                    throw Exception(ErrorCodes::SYNTAX_ERROR, "lambda requires two arguments");

                const auto * lambda_args_tuple = func->arguments->children.at(0)->as<ASTFunction>();
                if (!lambda_args_tuple || lambda_args_tuple->name != "tuple_cast")
                    throw Exception(ErrorCodes::SYNTAX_ERROR, "First argument of lambda must be a tuple");

                const ASTs & lambda_arg_asts = lambda_args_tuple->arguments->children;
                if (lambda_arg_asts.size() != 1)
                    throw Exception(ErrorCodes::SYNTAX_ERROR, "APPLY column transformer can only accept lambda with one argument");

                if (auto opt_arg_name = tryGetIdentifierName(lambda_arg_asts[0]); opt_arg_name)
                    lambda_arg = *opt_arg_name;
                else
                    throw Exception(ErrorCodes::SYNTAX_ERROR, "lambda argument declarations must be identifiers");
            }
            else
            {
                lambda = nullptr;
                pos = opos;
            }
        }

        if (!lambda)
        {
            if (!ParserIdentifier().parse(pos, func_name, expected))
                return false;

            if (pos->type == TokenType::OpeningRoundBracket)
            {
                ++pos;
                if (!ParserExpressionList(false).parse(pos, expr_list_args, expected))
                    return false;

                if (pos->type != TokenType::ClosingRoundBracket)
                    return false;
                ++pos;
            }
        }

        String column_name_prefix;
        if (with_open_round_bracket && pos->type == TokenType::Comma)
        {
            ++pos;

            ParserStringLiteral parser_string_literal;
            ASTPtr ast_prefix_name;
            if (!parser_string_literal.parse(pos, ast_prefix_name, expected))
                return false;

            column_name_prefix = ast_prefix_name->as<ASTLiteral &>().value.get<const String &>();
        }

        if (with_open_round_bracket)
        {
            if (pos->type != TokenType::ClosingRoundBracket)
                return false;
            ++pos;
        }

        auto res = std::make_shared<ASTColumnsApplyTransformer>();
        if (lambda)
        {
            res->lambda = lambda;
            res->lambda_arg = lambda_arg;
        }
        else
        {
            res->func_name = getIdentifierName(func_name);
            res->parameters = expr_list_args;
        }
        res->column_name_prefix = column_name_prefix;
        node = std::move(res);
        return true;
    }
    else if (allowed_transformers.isSet(ColumnTransformer::EXCEPT) && except.ignore(pos, expected))
    {
        if (strict.ignore(pos, expected))
            is_strict = true;

        ASTs identifiers;
        ASTPtr regex_node;
        ParserStringLiteral regex;
        auto parse_id = [&identifiers, &pos, &expected]
        {
            ASTPtr identifier;
            if (!ParserIdentifier(true).parse(pos, identifier, expected))
                return false;

            identifiers.emplace_back(std::move(identifier));
            return true;
        };

        if (pos->type == TokenType::OpeningRoundBracket)
        {
            // support one or more parameter
            ++pos;
            if (!ParserList::parseUtil(pos, expected, parse_id, false) && !regex.parse(pos, regex_node, expected))
                return false;

            if (pos->type != TokenType::ClosingRoundBracket)
                return false;
            ++pos;
        }
        else
        {
            // only one parameter
            if (!parse_id() && !regex.parse(pos, regex_node, expected))
                return false;
        }

        auto res = std::make_shared<ASTColumnsExceptTransformer>();
        if (regex_node)
            res->setPattern(regex_node->as<ASTLiteral &>().value.get<String>());
        else
            res->children = std::move(identifiers);
        res->is_strict = is_strict;
        node = std::move(res);
        return true;
    }
    else if (allowed_transformers.isSet(ColumnTransformer::REPLACE) && replace.ignore(pos, expected))
    {
        if (strict.ignore(pos, expected))
            is_strict = true;

        ASTs replacements;
        ParserExpression element_p;
        ParserIdentifier ident_p;
        auto parse_id = [&]
        {
            ASTPtr expr;

            if (!element_p.parse(pos, expr, expected))
                return false;
            if (!as.ignore(pos, expected))
                return false;

            ASTPtr ident;
            if (!ident_p.parse(pos, ident, expected))
                return false;

            auto replacement = std::make_shared<ASTColumnsReplaceTransformer::Replacement>();
            replacement->name = getIdentifierName(ident);
            replacement->expr = std::move(expr);
            replacements.emplace_back(std::move(replacement));
            return true;
        };

        if (pos->type == TokenType::OpeningRoundBracket)
        {
            ++pos;

            if (!ParserList::parseUtil(pos, expected, parse_id, false))
                return false;

            if (pos->type != TokenType::ClosingRoundBracket)
                return false;
            ++pos;
        }
        else
        {
            // only one parameter
            if (!parse_id())
                return false;
        }

        auto res = std::make_shared<ASTColumnsReplaceTransformer>();
        res->children = std::move(replacements);
        res->is_strict = is_strict;
        node = std::move(res);
        return true;
    }

    return false;
}


bool ParserAsterisk::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    if (pos->type == TokenType::Asterisk)
    {
        ++pos;
        auto asterisk = std::make_shared<ASTAsterisk>();
        ParserColumnsTransformers transformers_p(allowed_transformers);
        ASTPtr transformer;
        while (transformers_p.parse(pos, transformer, expected))
        {
            asterisk->children.push_back(transformer);
        }
        node = asterisk;
        return true;
    }
    return false;
}


bool ParserQualifiedAsterisk::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    if (!ParserCompoundIdentifier(true, true).parse(pos, node, expected))
        return false;

    if (pos->type != TokenType::Dot)
        return false;
    ++pos;

    if (pos->type != TokenType::Asterisk)
        return false;
    ++pos;

    auto res = std::make_shared<ASTQualifiedAsterisk>();
    res->children.push_back(node);
    ParserColumnsTransformers transformers_p;
    ASTPtr transformer;
    while (transformers_p.parse(pos, transformer, expected))
    {
        res->children.push_back(transformer);
    }
    node = std::move(res);
    return true;
}


bool ParserSubstitution::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    if (pos->type != TokenType::OpeningCurlyBrace)
        return false;

    ++pos;

    if (pos->type != TokenType::BareWord)
    {
        if (hint)
            expected.add(pos, "substitution name (identifier)");
        return false;
    }

    String name(pos->begin, pos->end);
    ++pos;

    if (pos->type != TokenType::Colon)
    {
        if (hint)
            expected.add(pos, "colon between name and type");
        return false;
    }

    ++pos;

    auto old_pos = pos;
    ParserDataType type_parser;
    if (!type_parser.ignore(pos, expected))
    {
        if (hint)
            expected.add(pos, "substitution type");
        return false;
    }

    String type(old_pos->begin, pos->begin);

    if (pos->type != TokenType::ClosingCurlyBrace)
    {
        if (hint)
            expected.add(pos, "closing curly brace");
        return false;
    }

    ++pos;
    node = std::make_shared<ASTQueryParameter>(name, type);
    return true;
}


bool ParserMySQLGlobalVariable::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    if (pos->type != TokenType::DoubleAt)
        return false;

    ++pos;

    if (pos->type != TokenType::BareWord)
    {
        if (hint)
            expected.add(pos, "variable name");
        return false;
    }

    String name(pos->begin, pos->end);
    ++pos;

    /// SELECT @@session|global.variable style
    if (pos->type == TokenType::Dot)
    {
        ++pos;

        if (pos->type != TokenType::BareWord)
        {
            if (hint)
                expected.add(pos, "variable name");
            return false;
        }
        name = String(pos->begin, pos->end);
        ++pos;
    }

    auto name_literal = std::make_shared<ASTLiteral>(name);

    auto expr_list_args = std::make_shared<ASTExpressionList>();
    expr_list_args->children.push_back(std::move(name_literal));

    auto function_node = std::make_shared<ASTFunction>();
    function_node->name = "global_variable";
    function_node->arguments = expr_list_args;
    function_node->children.push_back(expr_list_args);

    node = function_node;
    node->setAlias("@@" + name);
    return true;
}


bool ParserExpressionElement::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    return ParserSubquery().parse(pos, node, expected)
        || ParserCastOperator().parse(pos, node, expected)
        || ParserTupleOfLiterals().parse(pos, node, expected)
        || ParserParenthesisExpression().parse(pos, node, expected)
        || ParserArrayOfLiterals().parse(pos, node, expected)
        || ParserArray().parse(pos, node, expected)
        || ParserLiteral().parse(pos, node, expected)
        || ParserCase().parse(pos, node, expected)
        || ParserColumnsMatcher().parse(pos, node, expected) /// before ParserFunction because it can be also parsed as a function.
        || ParserFunction().parse(pos, node, expected)
        || ParserQualifiedAsterisk().parse(pos, node, expected)
        || ParserAsterisk().parse(pos, node, expected)
        || ParserCompoundIdentifier(false, true).parse(pos, node, expected)
        || ParserSubstitution().parse(pos, node, expected)
        || ParserMySQLGlobalVariable().parse(pos, node, expected);
}


bool ParserWithOptionalAlias::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    if (!elem_parser->parse(pos, node, expected))
        return false;

    /** Little hack.
      *
      * In the SELECT section, we allow parsing aliases without specifying the AS keyword.
      * These aliases can not be the same as the query keywords.
      * And the expression itself can be an identifier that matches the keyword.
      * For example, a column may be called where. And in the query it can be written `SELECT where AS x FROM table` or even `SELECT where x FROM table`.
      * Even can be written `SELECT where AS from FROM table`, but it can not be written `SELECT where from FROM table`.
      * See the ParserAlias implementation for details.
      *
      * But there is a small problem - an inconvenient error message if there is an extra comma in the SELECT section at the end.
      * Although this error is very common. Example: `SELECT x, y, z, FROM tbl`
      * If you do nothing, it's parsed as a column with the name FROM and alias tbl.
      * To avoid this situation, we do not allow the parsing of the alias without the AS keyword for the identifier with the name FROM.
      *
      * Note: this also filters the case when the identifier is quoted.
      * Example: SELECT x, y, z, `FROM` tbl. But such a case could be solved.
      *
      * In the future it would be easier to disallow unquoted identifiers that match the keywords.
      */
    bool allow_alias_without_as_keyword_now = allow_alias_without_as_keyword;
    if (allow_alias_without_as_keyword)
        if (auto opt_id = tryGetIdentifierName(node))
            if (0 == strcasecmp(opt_id->data(), "FROM"))
                allow_alias_without_as_keyword_now = false;

    ASTPtr alias_node;
    if (ParserAlias(allow_alias_without_as_keyword_now).parse(pos, alias_node, expected, false))
    {
        /// FIXME: try to prettify this cast using `as<>()`
        if (auto * ast_with_alias = dynamic_cast<ASTWithAlias *>(node.get()))
        {
            tryGetIdentifierNameInto(alias_node, ast_with_alias->alias);
        }
        else
        {
            if (hint)
                expected.add(pos, "alias cannot be here");
            return false;
        }
    }

    return true;
}


bool ParserOrderByElement::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    ParserExpressionWithOptionalAlias elem_p(false);
    ParserKeyword ascending("ASCENDING");
    ParserKeyword descending("DESCENDING");
    ParserKeyword asc("ASC");
    ParserKeyword desc("DESC");
    ParserKeyword nulls("NULLS");
    ParserKeyword first("FIRST");
    ParserKeyword last("LAST");
    ParserKeyword collate("COLLATE");
    ParserKeyword with_fill("WITH FILL");
    ParserKeyword from("FROM");
    ParserKeyword to("TO");
    ParserKeyword step("STEP");
    ParserStringLiteral collate_locale_parser;
    ParserExpressionWithOptionalAlias exp_parser(false);

    ASTPtr expr_elem;
    if (!elem_p.parse(pos, expr_elem, expected))
        return false;

    int direction = 1;

    if (descending.ignore(pos) || desc.ignore(pos))
        direction = -1;
    else
        ascending.ignore(pos) || asc.ignore(pos);

    int nulls_direction = direction;
    bool nulls_direction_was_explicitly_specified = false;

    if (nulls.ignore(pos))
    {
        nulls_direction_was_explicitly_specified = true;

        if (first.ignore(pos))
            nulls_direction = -direction;
        else if (last.ignore(pos))
            ;
        else
            return false;
    }

    ASTPtr locale_node;
    if (collate.ignore(pos))
    {
        if (!collate_locale_parser.parse(pos, locale_node, expected))
            return false;
    }

    /// WITH FILL [FROM x] [TO y] [STEP z]
    bool has_with_fill = false;
    ASTPtr fill_from;
    ASTPtr fill_to;
    ASTPtr fill_step;
    if (with_fill.ignore(pos))
    {
        has_with_fill = true;
        if (from.ignore(pos) && !exp_parser.parse(pos, fill_from, expected))
            return false;

        if (to.ignore(pos) && !exp_parser.parse(pos, fill_to, expected))
            return false;

        if (step.ignore(pos) && !exp_parser.parse(pos, fill_step, expected))
            return false;
    }

    auto elem = std::make_shared<ASTOrderByElement>();

    elem->direction = direction;
    elem->nulls_direction = nulls_direction;
    elem->nulls_direction_was_explicitly_specified = nulls_direction_was_explicitly_specified;
    elem->collation = locale_node;
    elem->with_fill = has_with_fill;
    elem->fill_from = fill_from;
    elem->fill_to = fill_to;
    elem->fill_step = fill_step;
    elem->children.push_back(expr_elem);
    if (locale_node)
        elem->children.push_back(locale_node);

    node = elem;

    return true;
}

bool ParserFunctionWithKeyValueArguments::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    ParserIdentifier id_parser;
    ParserKeyValuePairsList pairs_list_parser;

    ASTPtr identifier;
    ASTPtr expr_list_args;
    if (!id_parser.parse(pos, identifier, expected))
        return false;


    bool left_bracket_found = false;
    if (pos.get().type != TokenType::OpeningRoundBracket)
    {
        if (!brackets_can_be_omitted)
             return false;
    }
    else
    {
        ++pos;
        left_bracket_found = true;
    }

    if (!pairs_list_parser.parse(pos, expr_list_args, expected))
        return false;

    if (left_bracket_found)
    {
        if (pos.get().type != TokenType::ClosingRoundBracket)
            return false;
        ++pos;
    }

    auto function = std::make_shared<ASTFunctionWithKeyValueArguments>(left_bracket_found);
    function->name = Poco::toLower(identifier->as<ASTIdentifier>()->name());
    function->elements = expr_list_args;
    function->children.push_back(function->elements);
    node = function;

    return true;
}

bool ParserTTLElement::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    ParserKeyword s_to_disk("TO DISK");
    ParserKeyword s_to_volume("TO VOLUME");
    ParserKeyword s_if_exists("IF EXISTS");
    ParserKeyword s_delete("DELETE");
    ParserKeyword s_where("WHERE");
    ParserKeyword s_group_by("GROUP BY");
    ParserKeyword s_set("SET");
    ParserKeyword s_recompress("RECOMPRESS");
    ParserKeyword s_codec("CODEC");
    ParserToken s_comma(TokenType::Comma);
    ParserToken s_eq(TokenType::Equals);

    ParserIdentifier parser_identifier;
    ParserStringLiteral parser_string_literal;
    ParserExpression parser_exp;
    ParserExpressionList parser_keys_list(false);
    ParserCodec parser_codec;

    ParserList parser_assignment_list(
        std::make_unique<ParserAssignment>(), std::make_unique<ParserToken>(TokenType::Comma));

    ASTPtr ttl_expr;
    if (!parser_exp.parse(pos, ttl_expr, expected))
        return false;

    TTLMode mode;
    DataDestinationType destination_type = DataDestinationType::DELETE;
    String destination_name;

    if (s_to_disk.ignore(pos))
    {
        mode = TTLMode::MOVE;
        destination_type = DataDestinationType::DISK;
    }
    else if (s_to_volume.ignore(pos))
    {
        mode = TTLMode::MOVE;
        destination_type = DataDestinationType::VOLUME;
    }
    else if (s_group_by.ignore(pos))
    {
        mode = TTLMode::GROUP_BY;
    }
    else if (s_recompress.ignore(pos))
    {
        mode = TTLMode::RECOMPRESS;
    }
    else
    {
        s_delete.ignore(pos);
        mode = TTLMode::DELETE;
    }

    ASTPtr where_expr;
    ASTPtr group_by_key;
    ASTPtr recompression_codec;
    ASTPtr group_by_assignments;
    bool if_exists = false;

    if (mode == TTLMode::MOVE)
    {
        if (s_if_exists.ignore(pos))
            if_exists = true;

        ASTPtr ast_space_name;
        if (!parser_string_literal.parse(pos, ast_space_name, expected))
            return false;

        destination_name = ast_space_name->as<ASTLiteral &>().value.get<const String &>();
    }
    else if (mode == TTLMode::GROUP_BY)
    {
        if (!parser_keys_list.parse(pos, group_by_key, expected))
            return false;

        if (s_set.ignore(pos))
        {
            if (!parser_assignment_list.parse(pos, group_by_assignments, expected))
                return false;
        }
    }
    else if (mode == TTLMode::DELETE && s_where.ignore(pos))
    {
        if (!parser_exp.parse(pos, where_expr, expected))
            return false;
    }
    else if (mode == TTLMode::RECOMPRESS)
    {
        if (!s_codec.ignore(pos))
            return false;

        if (!parser_codec.parse(pos, recompression_codec, expected))
            return false;
    }

    auto ttl_element = std::make_shared<ASTTTLElement>(mode, destination_type, destination_name, if_exists);
    ttl_element->setTTL(std::move(ttl_expr));
    if (where_expr)
        ttl_element->setWhere(std::move(where_expr));

    if (mode == TTLMode::GROUP_BY)
    {
        ttl_element->group_by_key = std::move(group_by_key->children);
        if (group_by_assignments)
            ttl_element->group_by_assignments = std::move(group_by_assignments->children);
    }

    if (mode == TTLMode::RECOMPRESS)
        ttl_element->recompression_codec = recompression_codec;

    node = ttl_element;
    return true;
}

bool ParserIdentifierWithOptionalParameters::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    ParserIdentifier non_parametric;
    ParserIdentifierWithParameters parametric;

    if (parametric.parse(pos, node, expected))
    {
        auto * func = node->as<ASTFunction>();
        func->no_empty_args = true;
        return true;
    }

    ASTPtr ident;
    if (non_parametric.parse(pos, ident, expected))
    {
        auto func = std::make_shared<ASTFunction>();
        tryGetIdentifierNameInto(ident, func->name);
        func->no_empty_args = true;
        node = func;
        return true;
    }

    return false;
}

bool ParserAssignment::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    auto assignment = std::make_shared<ASTAssignment>();
    node = assignment;

    ParserIdentifier p_identifier;
    ParserToken s_equals(TokenType::Equals);
    ParserExpression p_expression;

    ASTPtr column;
    if (!p_identifier.parse(pos, column, expected))
        return false;

    if (!s_equals.ignore(pos, expected))
        return false;

    ASTPtr expression;
    if (!p_expression.parse(pos, expression, expected))
        return false;

    tryGetIdentifierNameInto(column, assignment->column_name);
    if (expression)
        assignment->children.push_back(expression);

    return true;
}

}
