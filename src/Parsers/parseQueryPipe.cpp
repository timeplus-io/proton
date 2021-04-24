#include "parseQueryPipe.h"

#include <Parsers/parseQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/Lexer.h>
#include <Parsers/ParserInsertQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/TokenIterator.h>
#include <Common/StringUtils/StringUtils.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

namespace
{

enum class PipeType
{
    select = 0,
    where,
    insert,
    create
};

ASTPtr tryParseQueryBase(
    IParser & parser,
    const char * pos,
    const char * end,
    const String & desc,
    bool hilite,
    Expected & expected,
    String & error_message,
    Tokens & tokens,
    IParser::Pos & token_iterator)
{
    if (token_iterator->isEnd() || token_iterator->type == TokenType::Semicolon)
    {
        error_message = "Empty query";
        return nullptr;
    }

    ASTPtr res;
    bool parse_res = parser.parse(token_iterator, res, expected);
    Token last_token = token_iterator.max();

    ASTInsertQuery * insert = nullptr;
    if (parse_res)
        insert = res->as<ASTInsertQuery>();

    if (!(insert && insert->data))
    {
        /// Lexical error
        if (last_token.isError())
        {
            error_message = getLexicalErrorMessage(pos, end, last_token, hilite, desc);
            /// Return whateve we parsed and we will validate lexical error in caller function
            return res;
        }

        /// Unmatched parentheses
        UnmatchedParentheses unmatched_parens = checkUnmatchedParentheses(TokenIterator(tokens));
        if (!unmatched_parens.empty())
        {
            error_message = getUnmatchedParenthesesErrorMessage(pos, end, unmatched_parens, hilite, desc);
            return nullptr;
        }
    }

    if (!parse_res)
    {
        /// Parse error.
        error_message = getSyntaxErrorMessage(pos, end, last_token, expected, hilite, desc);
        return nullptr;
    }

    /// Excessive input after query. Parsed query must end with end of data or semicolon or data for INSERT.
    if (!token_iterator->isEnd()
        && token_iterator->type != TokenType::Semicolon
        && !(insert && insert->data))
    {
        expected.add(pos, "end of query");
        error_message = getSyntaxErrorMessage(pos, end, last_token, expected, hilite, desc);
        return nullptr;
    }

    return res;
}

int handleErrors(
    const Token & last_token,
    const char *& last_pos,
    const char * end,
    Expected & expected,
    const ASTPtr & res,
    String & error_message,
    std::vector<std::tuple<String, PipeType, ASTPtr>> & queries,
    PipeType type)
{
    const char * desc = "querypipe";

    /// Lexical error
    if (last_token.isError())
    {
        /// Only SELECT or WHERE query can end with pipe
        if ((type == PipeType::select || type == PipeType::where) && last_token.type == TokenType::ErrorSinglePipeMark)
        {
            queries.push_back(std::make_tuple(String(last_pos, last_token.begin), type, res));
            last_pos = last_token.end;
            error_message.clear();
            return 1;
        }
        else
        {
            error_message = getLexicalErrorMessage(last_pos, end, last_token, false, desc);
            return -1;
        }
    }

    /// Syntax error
    error_message = getSyntaxErrorMessage(last_pos, end, last_token, expected, false, desc);
    return -1;
}

inline bool posEqualIgnoringSpace(const char *& last_pos, const char * next_token_begin)
{
    if (next_token_begin < last_pos)
        return false;

    if (next_token_begin == last_pos)
        return true;

    for (;last_pos != next_token_begin; ++last_pos)
        if (!isWhitespaceASCII(*last_pos))
            return false;

    return true;
}

/// Inject subquery table to SELECT clause in the pipe if necessary
void injectSubQuery(const std::tuple<String, PipeType, ASTPtr> & query_tuple, String & all_query)
{
    /// For example: SELECT avg(c) GROUP BY d ->
    /// SELECT avgc <FROM subquery-table> GROUP BY d
    /// SELECT avg(c) -> SELECT avg(c), there is no change

    auto & ast = std::get<2>(query_tuple);
    auto select_query = ast->as<ASTSelectQuery>();
    select_query->setSubQueryPipe(all_query);

    WriteBufferFromOwnString buf;
    ast->format(IAST::FormatSettings(buf, false));
    all_query = buf.str();
}

/// Return 1 for success, 0 unhandled, -1 for failure
int parseSelect(
    const char *& last_pos,
    const char * end,
    IParser::Pos & pos,
    Expected & expected,
    String & error_message,
    std::vector<std::tuple<String, PipeType, ASTPtr>> & queries)
{
    /// SELECT query but without `FROM table | subquery`. E.g.
    /// SELECT a ORDER BY b
    /// SELECT avg(c) GROUP BY d ORDER BY e LIMIT 2
    ParserSelectQuery selectQuery;
    selectQuery.setPipeMode(true);
    ASTPtr res;
    auto parse_res = selectQuery.parse(pos, res, expected);
    Token last_token = pos.max();

    if (last_token.isError())
        return handleErrors(last_token, last_pos, end, expected, res, error_message, queries, PipeType::select);

    if (parse_res)
    {
        queries.push_back(std::make_tuple(String(last_pos, last_token.end), PipeType::select, res));
        last_pos = last_token.end;
        return 1;
    }

    /// Not a select query
    if (posEqualIgnoringSpace(last_pos, last_token.begin))
        return 0;

    /// Shall never reach here
    error_message = "Invalid syntax";
    return -1;
}

/// Return 1 for success, 0 unhandled, -1 for failure
int parseWhere(
    const char *& last_pos,
    const char * end,
    IParser::Pos & pos,
    Expected & expected,
    String & error_message,
    std::vector<std::tuple<String, PipeType, ASTPtr>> & queries)
{
    ParserKeyword s_where("WHERE");
    ParserExpressionWithOptionalAlias exp_elem(false);
    ASTPtr res;

    if (!s_where.ignore(pos, expected))
        return 0;

    if (!exp_elem.parse(pos, res, expected))
        return handleErrors(pos.max(), last_pos, end, expected, res, error_message, queries, PipeType::where);

    const auto & last_token = pos.max();
    if (last_token.isError())
        return handleErrors(last_token, last_pos, end, expected, res, error_message, queries, PipeType::where);

    /// Everything is good
    queries.push_back(std::make_tuple(String(last_pos, last_token.end), PipeType::where, res));
    last_pos = last_token.end;
    return 1;
}

int drainLastPipe(
    IParser::Pos & pos,
    const char *& last_pos,
    const char * end,
    const char * emsg,
    String & error_message,
    std::vector<std::tuple<String, PipeType, ASTPtr>> & queries,
    PipeType type)
{
    const auto & last_token = pos.max();
    auto endpos = last_token.end;
    if (!posEqualIgnoringSpace(endpos, end))
    {
        error_message = emsg;
        return -1;
    }

    /// We don't really care about the AST
    queries.push_back(std::make_tuple(String(last_pos, last_token.end), type, nullptr));

    /// Last pipe
    last_pos = end;
    return 1;
}

/// Return 1 for success, 0 unhandled, -1 for failure
int parseInsert(
    const char *& last_pos,
    const char * end,
    IParser::Pos & pos,
    Expected & expected,
    String & error_message,
    std::vector<std::tuple<String, PipeType, ASTPtr>> & queries)
{
    /// INSERT INTO [TABLE] database.table_name (c1, c2, ...)
    ParserKeyword s_insert_into("INSERT INTO");
    ParserKeyword s_table("TABLE");
    ParserToken s_dot(TokenType::Dot);
    ParserToken s_lparen(TokenType::OpeningRoundBracket);
    ParserToken s_rparen(TokenType::ClosingRoundBracket);
    ParserIdentifier name_p;
    ParserList columns_p(std::make_unique<ParserInsertElement>(), std::make_unique<ParserToken>(TokenType::Comma), false);

    ASTPtr database;
    ASTPtr table;
    ASTPtr columns;

    if (!s_insert_into.ignore(pos, expected))
        return 0;

    s_table.ignore(pos, expected);

    if (!name_p.parse(pos, table, expected))
        return handleErrors(pos.max(), last_pos, end, expected, nullptr, error_message, queries, PipeType::insert);

    if (s_dot.ignore(pos, expected))
    {
        database = table;
        if (!name_p.parse(pos, table, expected))
            return handleErrors(pos.max(), last_pos, end, expected, nullptr, error_message, queries, PipeType::insert);
    }

    /// Is there a list of columns
    if (s_lparen.ignore(pos, expected))
    {
        if (!columns_p.parse(pos, columns, expected))
            return handleErrors(pos.max(), last_pos, end, expected, nullptr, error_message, queries, PipeType::insert);

        if (!s_rparen.ignore(pos, expected))
            return handleErrors(pos.max(), last_pos, end, expected, nullptr, error_message, queries, PipeType::insert);
    }

    auto emsg = "Invalid syntax, only `INSERT INTO TABLE <database>.<table> (col1, col2, ...)` is allowed in the last pipe";
    return drainLastPipe(pos, last_pos, end, emsg, error_message, queries, PipeType::insert);
}

/// Return 1 for success, 0 unhandled, -1 for failure
int parseCreate(
    const char *& last_pos,
    const char * end,
    IParser::Pos & pos,
    Expected & expected,
    String & error_message,
    std::vector<std::tuple<String, PipeType, ASTPtr>> & queries)
{
    /// CREATE TABLE IF NOT EXISTS database.table ON CLUSTER cluster ENGINE=MergeTree
    ParserKeyword s_create("CREATE");
    ParserKeyword s_temporary("TEMPORARY");
    ParserKeyword s_table("TABLE");
    ParserKeyword s_if_not_exists("IF NOT EXISTS");
    ParserKeyword s_on("ON");
    ParserCompoundIdentifier table_name_p(true);

    ASTPtr table;
    ASTPtr as_database;
    ASTPtr as_table;

    if (!s_create.ignore(pos, expected))
        return 0;

    s_temporary.ignore(pos, expected);

    if (!s_table.ignore(pos, expected))
        return handleErrors(pos.max(), last_pos, end, expected, nullptr, error_message, queries, PipeType::create);

    s_if_not_exists.ignore(pos, expected);

    if (!table_name_p.parse(pos, table, expected))
        return handleErrors(pos.max(), last_pos, end, expected, nullptr, error_message, queries, PipeType::create);

    String cluster_str;
    if (s_on.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return handleErrors(pos.max(), last_pos, end, expected, nullptr, error_message, queries, PipeType::create);
    }

    auto emsg = "Invalid syntax, only `CREATE TABLE IF NOT EXISTS <database>.<table> ON CLUSTER <cluster>` is allowed in the last pipe";

    int r = drainLastPipe(pos, last_pos, end, emsg, error_message, queries, PipeType::create);
    if (r != 1)
        return r;

    /// FIXME, DistributedMergeTree
    auto & query = queries.back();
    std::get<0>(query) += " ENGINE=MergeTree() ORDER BY tuple() ";
    return r;
}

std::pair<String, ASTPtr> doRewriteQueryPipe(
    const std::vector<std::tuple<String, PipeType, ASTPtr>> & queries,
    String & error_message,
    bool hilite,
    size_t max_query_size,
    size_t max_parser_depth)
{
    /// We have pipe queries, rewrite them
    String all_query = "";
    for (const auto & query : queries)
    {
        switch (std::get<1>(query))
        {
        case PipeType::select:
            if (all_query.empty())
                all_query = std::get<0>(query);
            else
                injectSubQuery(query, all_query);
            break;

        case PipeType::where:
            all_query = "SELECT * FROM (" + all_query + ") " + std::get<0>(query);
            break;

        case PipeType::insert:
            /// INSERT INTO [db.]table [(c1, c2, c3)] SELECT ...
            all_query = std::get<0>(query) + " " + all_query;
            break;

        case PipeType::create:
            /// CREATE TABLE IF NOT EXISTS <database>.<table> ON CLUSTER <cluster> AS SELECT ...
            all_query = std::get<0>(query) + " AS " + all_query;
            break;
        }
    }

    error_message.clear();

    auto pos = all_query.c_str();
    auto end = pos + all_query.size();
    auto parser = ParserQuery(end);
    auto res = tryParseQuery(parser, pos, end, error_message, hilite, "", false, max_query_size, max_parser_depth);
    return std::make_pair(all_query, res);
}

std::pair<String, ASTPtr> doRewriteQueryPipeAndParse(
    const char * pos,
    const char * end,
    String & error_message,
    bool hilite,
    size_t max_query_size,
    size_t max_parser_depth,
    std::vector<std::tuple<String, PipeType, ASTPtr>> & queries)
{
    /// The rest of the queries in the pipe are can be an abitrary combinations
    /// of | WHERE ... | SELECT ...
    /// INSERT INTO or CREATE TABLE can only appear at the very end of the pipe

    for (; pos != end;)
    {
        Expected expected;
        Tokens tokens(pos, end, max_query_size);
        IParser::Pos token_iterator(tokens, max_parser_depth);

        if (token_iterator->isEnd() || token_iterator->type == TokenType::Semicolon)
        {
            pos = end;
            break;
        }

        /// SELECT
        int res = parseSelect(pos, end, token_iterator, expected, error_message, queries);
        if (res == -1)
            return std::make_pair("", nullptr);

        if (res == 1)
            continue;

        /// WHERE
        res = parseWhere(pos, end, token_iterator, expected, error_message, queries);
        if (res == -1)
            return std::make_pair("", nullptr);

        if (res == 1)
            continue;

        /// INSERT
        res = parseInsert(pos, end, token_iterator, expected, error_message, queries);
        if (res == -1)
            return std::make_pair("", nullptr);

        if (res == 1)
            break;

        /// CREATE
        res = parseCreate(pos, end, token_iterator, expected, error_message, queries);
        if (res == -1)
            return std::make_pair("", nullptr);

        if (res == 1)
            break;
    }

    return doRewriteQueryPipe(queries, error_message, hilite, max_query_size, max_parser_depth);
}

}

std::pair<String, ASTPtr> rewriteQueryPipeAndParse(
    IParser & parser,
    const char * pos,
    const char * end,
    String & error_message,
    bool hilite,
    size_t max_query_size,
    size_t max_parser_depth)
{
    const auto desc = "querypipe";
    Tokens tokens(pos, end, max_query_size);
    IParser::Pos token_iterator(tokens, max_parser_depth);
    Expected expected;

    /// Parse base query. Base query can be any query. If base query can go through
    /// just return what it parsed
    auto res = tryParseQueryBase(parser, pos, end, desc, hilite, expected, error_message, tokens, token_iterator);
    if (error_message.empty() || !res)
        return std::make_pair(String(pos, end), res);

    /// If there are errors and the query is not select query, return directly
    if ((res->as<ASTSelectWithUnionQuery>() == nullptr) && (res->as<ASTSelectQuery>() == nullptr))
        return std::make_pair("", nullptr);

    /// SELECT ... -> WHERE ... -> SELECT ... -> INSERT INTO table (col1, col2, ...)
    /// SELECT ... -> WHERE ... -> SELECT ... -> CREATE TABLE table

    std::vector<std::tuple<String, PipeType, ASTPtr>> queries;
    auto r = handleErrors(token_iterator.max(), pos, end, expected, res, error_message, queries, PipeType::select);
    if (r != 1)
        return std::make_pair("", nullptr);

    return doRewriteQueryPipeAndParse(pos, end, error_message, hilite, max_query_size, max_parser_depth, queries);
}

ASTPtr tryParseQueryPipe(
    IParser & parser,
    const char * pos,
    const char * end,
    String & error_message,
    bool hilite,
    size_t max_query_size,
    size_t max_parser_depth)
{
    auto res = rewriteQueryPipeAndParse(parser, pos, end, error_message, hilite, max_query_size, max_parser_depth);
    if (std::get<1>(res) != nullptr)
        return std::get<1>(res);

    return nullptr;
}

ASTPtr parseQueryPipe(IParser & parser, const char * pos, const char * end, size_t max_query_size, size_t max_parser_depth)
{
    String error_message;
    auto res = tryParseQueryPipe(parser, pos, end, error_message, false, max_query_size, max_parser_depth);
    if (res != nullptr)
        return res;

    throw Exception(error_message, ErrorCodes::SYNTAX_ERROR);
}
}
