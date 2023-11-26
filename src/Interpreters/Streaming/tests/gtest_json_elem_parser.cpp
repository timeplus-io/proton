#include <Parsers/ExpressionListParsers.h>
#include <Parsers/IAST.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/Streaming/ParserJsonElementExpression.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <gtest/gtest.h>

namespace DB::ErrorCodes
{
extern const int SYNTAX_ERROR;
}

using namespace DB;
static bool checkASTTree(const String & origin_query, const String & check_query)
{
    ParserQuery origin_parser(origin_query.end().base());
    ASTPtr origin_ast = parseQuery(origin_parser, origin_query.begin().base(), origin_query.end().base(), "", 0, 0);

    ParserQuery check_parser(check_query.end().base());
    ASTPtr check_ast = parseQuery(check_parser, check_query.begin().base(), check_query.end().base(), "", 0, 0);

    bool is_equal = origin_ast->getTreeHash() == check_ast->getTreeHash();
    if (!is_equal)
    {
        std::cerr << "[  DETIAL  ] origin_query: " << serializeAST(*origin_ast) << std::endl;
        std::cerr << "[  DETIAL  ] check_query : " << serializeAST(*check_ast) << std::endl;
    }
    return is_equal;
}

TEST(JsonElementTest, JsonElement)
{
    EXPECT_TRUE(checkASTTree(
        /* origin query */
        "SELECT raw:obj.data FROM stream",
        /* check query */
        "SELECT json_value(raw, '$.`obj`.`data`') FROM stream"));

    EXPECT_TRUE(checkASTTree(
        /* origin query */
        "SELECT raw:obj.data::int FROM stream",
        /* check query */
        "SELECT cast(json_value(raw, '$.`obj`.`data`'), 'int') FROM stream"));

    EXPECT_TRUE(checkASTTree(
        /* origin query */
        "SELECT raw:`a.b`.c::string FROM stream",
        /* check query */
        "SELECT cast(json_value(raw, '$.`a.b`.`c`'), 'string') FROM stream"));

    EXPECT_TRUE(checkASTTree(
        /* origin query */
        "SELECT raw:`a.b`.`1`::string FROM stream",
        /* check query */
        "SELECT cast(json_value(raw, '$.`a.b`.`1`'), 'string') FROM stream"));
}

TEST(JsonElementTest, JsonElementException)
{
    EXPECT_THROW(
        checkASTTree(
            /* origin query */
            "SELECT raw:1 FROM stream",
            /* check query */
            ""),
        DB::Exception);

    EXPECT_THROW(
        checkASTTree(
            /* origin query */
            "SELECT raw::obj.data FROM stream",
            /* check query */
            ""),
        DB::Exception);

    EXPECT_THROW(
        checkASTTree(
            /* origin query */
            "SELECT raw:x.y.z:a.b.c FROM stream",
            /* check query */
            ""),
        DB::Exception);
}
