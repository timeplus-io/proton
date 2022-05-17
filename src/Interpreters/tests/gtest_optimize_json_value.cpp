#include <Interpreters/Streaming/OptimizeJsonValueVisitor.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/IAST.h>
#include <Parsers/ParserQuery.h>
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
    OptimizeJsonValueVisitor::Data data;
    OptimizeJsonValueVisitor(data).visit(origin_ast->as<ASTSelectWithUnionQuery>()->children[0]);

    ParserQuery check_parser(check_query.end().base());
    ASTPtr check_ast = parseQuery(check_parser, check_query.begin().base(), check_query.end().base(), "", 0, 0);
    bool is_equal = origin_ast->getTreeHash() == check_ast->getTreeHash();
    if (!is_equal)
    {
        std::cerr << "[  DETIAL  ] origin_query: " << serializeAST(*origin_ast) << std::endl;
        std::cerr << "[  DETIAL  ] check_query : " << serializeAST(*check_ast) << std::endl;

        // {
        //     WriteBufferFromOwnString buf;
        //     dumpAST(*origin_ast, buf);
        //     std::cerr << "[  DETIAL  ] origin_ast: " << buf.str() << std::endl;
        // }
        //
        // {
        //     WriteBufferFromOwnString buf;
        //     dumpAST(*check_ast, buf);
        //     std::cerr << "[  DETIAL  ] check_ast : " << buf.str() << std::endl;
        // }
    }
    return is_equal;
}

TEST(OptimizeJsonValue, OptimizeJsonValue)
{
    EXPECT_TRUE(checkASTTree(
        /* origin query */
        "SELECT raw:obj.data FROM stream",
        /* check query */
        "SELECT json_value(raw, '$.`obj`.`data`') FROM stream"));

    EXPECT_TRUE(checkASTTree(
        /* origin query */
        "SELECT raw:obj.data, raw:obj.data::string as data_str FROM stream",
        /* check query */
        "SELECT json_value(raw, '$.`obj`.`data`') as `raw:obj.data`, cast(`raw:obj.data`, 'string') as `data_str` FROM stream"));

    EXPECT_TRUE(checkASTTree(
        /* origin query */
        "WITH sub as (select 1) SELECT raw:obj.data, raw:obj.val as raw_ov FROM stream",
        /* check query */
        "WITH sub as (select 1), json_values(raw, '$.`obj`.`data`', '$.`obj`.`val`') as `__json_values_raw` SELECT __json_values_raw[1] as "
        "`raw:obj.data`, __json_values_raw[2] as `raw_ov` FROM stream"));

    EXPECT_TRUE(checkASTTree(
        /* origin query */
        "WITH now() as time SELECT raw:obj.data, raw:obj.val::int as raw_ov FROM stream",
        /* check query */
        "WITH now() as time, json_values(raw, '$.`obj`.`data`', '$.`obj`.`val`') as __json_values_raw SELECT __json_values_raw[1] as "
        "`raw:obj.data`, cast(__json_values_raw[2], 'int') as `raw_ov` FROM stream"));
}
