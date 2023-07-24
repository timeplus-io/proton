#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>

#include <Interpreters/Streaming/SyntaxAnalyzeUtils.h>
#include <Common/tests/gtest_global_register.h>

#include <gtest/gtest.h>

namespace
{
auto analyzeAggregatesOrJoin(const String & query)
{
    const char * start = query.data();
    const char * end = start + query.size();
    DB::ParserQuery parser(end);
    DB::ASTPtr ast = DB::parseQuery(parser, start, end, "", 0, 0);
    const auto * select_with_union_query = ast->as<DB::ASTSelectWithUnionQuery>();

    return DB::Streaming::analyzeSelectQueryForJoinOrAggregates(select_with_union_query->list_of_selects->children.front());
}
}

TEST(AnalyzeAggregatesOrJoin, Aggregates)
{
    tryRegisterAggregateFunctions();

    EXPECT_EQ(analyzeAggregatesOrJoin("SELECT * FROM test"), std::pair(false, false));
    EXPECT_EQ(analyzeAggregatesOrJoin("SELECT count() FROM test GROUP BY k"), std::pair(false, true));
    EXPECT_EQ(analyzeAggregatesOrJoin("SELECT count() FROM test"), std::pair(false, true));
    EXPECT_EQ(analyzeAggregatesOrJoin("SELECT latest(k) FROM test"), std::pair(false, true));
    EXPECT_EQ(analyzeAggregatesOrJoin("SELECT * FROM (SELECT latest(k) FROM test)"), std::pair(false, false));
}

TEST(AnalyzeAggregatesOrJoin, Join)
{
    EXPECT_EQ(analyzeAggregatesOrJoin("SELECT * FROM lhs INNER JOIN rhs ON lhs.k = rhs.k"), std::pair(true, false));
    EXPECT_EQ(analyzeAggregatesOrJoin("SELECT * FROM (SELECT * FROM lhs INNER JOIN rhs ON lhs.k = rhs.k)"), std::pair(false, false));
    EXPECT_EQ(analyzeAggregatesOrJoin("SELECT * FROM lhs INNER JOIN rhs ON lhs.k = rhs.k INNER JOIN mhs ON lhs.k = mhs.k"), std::pair(true, false));
}

TEST(AnalyzeAggregatesOrJoin, JoinAndAggregates)
{
    EXPECT_EQ(analyzeAggregatesOrJoin("SELECT count() FROM lhs INNER JOIN rhs ON lhs.k = rhs.k"), std::pair(true, true));
    EXPECT_EQ(analyzeAggregatesOrJoin("SELECT count() FROM lhs INNER JOIN rhs ON lhs.k = rhs.k INNER JOIN mhs ON lhs.k = mhs.k"), std::pair(true, true));
}
