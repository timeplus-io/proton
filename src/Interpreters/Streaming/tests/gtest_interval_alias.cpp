#include <Parsers/ExpressionListParsers.h>
#include <Parsers/IAST.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/Streaming/ParserIntervalAliasExpression.h>
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
    return origin_ast->getTreeHash() == check_ast->getTreeHash();
}

static bool checkIntervalParser(const String & alias_str, const String & interval_str)
{
    ParserIntervalAliasExpression alias_parser;
    ParserIntervalOperatorExpression interval_parser;
    ASTPtr alias_ast, interval_ast;
    Tokens alias_tokens(alias_str.data(), alias_str.data() + alias_str.size());
    Tokens interval_tokens(interval_str.data(), interval_str.data() + interval_str.size());
    IParser::Pos alias_pos(alias_tokens, 0), interval_pos(interval_tokens, 0);
    Expected alias_expected, interval_expected;

    if (!alias_parser.parse(alias_pos, alias_ast, alias_expected))
        throw DB::Exception("Failed to parse interval alias string.", ErrorCodes::SYNTAX_ERROR);
    if (!interval_parser.parse(interval_pos, interval_ast, interval_expected))
        throw DB::Exception("Failed to parse interval string.", ErrorCodes::SYNTAX_ERROR);

    return alias_ast->getTreeHash() == interval_ast->getTreeHash();
}

TEST(IntervalAliasTest, IntervalAlias)
{
    EXPECT_TRUE(checkIntervalParser("+1s", "interval +1 second"));
    EXPECT_TRUE(checkIntervalParser("-1s", "interval -1 second"));
    EXPECT_TRUE(checkIntervalParser("1s", "interval 1 second"));
    EXPECT_TRUE(checkIntervalParser("1m", "interval 1 minute"));
    EXPECT_TRUE(checkIntervalParser("1h", "interval 1 hour"));
    EXPECT_TRUE(checkIntervalParser("1d", "interval 1 day"));
    EXPECT_TRUE(checkIntervalParser("1w", "interval 1 week"));
    EXPECT_TRUE(checkIntervalParser("1M", "interval 1 month"));
    EXPECT_TRUE(checkIntervalParser("1q", "interval 1 quarter"));
    EXPECT_TRUE(checkIntervalParser("1y", "interval 1 year"));
}

TEST(IntervalAliasTest, IntervalAliasError)
{
    /// Error double +-
    EXPECT_THROW(checkIntervalParser("++1s", ""), DB::Exception);
    EXPECT_THROW(checkIntervalParser("--1s", ""), DB::Exception);
    EXPECT_THROW(checkIntervalParser("+-1s", ""), DB::Exception);
    EXPECT_THROW(checkIntervalParser("-+1s", ""), DB::Exception);

    /// Error float number
    EXPECT_THROW(checkIntervalParser("1.1s", ""), DB::Exception);

    /// Error kind case
    EXPECT_THROW(checkIntervalParser("1S", ""), DB::Exception);
    EXPECT_THROW(checkIntervalParser("1M", ""), DB::Exception);
    EXPECT_THROW(checkIntervalParser("1H", ""), DB::Exception);
    EXPECT_THROW(checkIntervalParser("1W", ""), DB::Exception);
    EXPECT_THROW(checkIntervalParser("1Q", ""), DB::Exception);
    EXPECT_THROW(checkIntervalParser("1Y", ""), DB::Exception);

    /// Error empty number
    EXPECT_THROW(checkIntervalParser("s", ""), DB::Exception);
    EXPECT_THROW(checkIntervalParser("m", ""), DB::Exception);
    EXPECT_THROW(checkIntervalParser("h", ""), DB::Exception);
    EXPECT_THROW(checkIntervalParser("d", ""), DB::Exception);
    EXPECT_THROW(checkIntervalParser("w", ""), DB::Exception);
    EXPECT_THROW(checkIntervalParser("M", ""), DB::Exception);
    EXPECT_THROW(checkIntervalParser("q", ""), DB::Exception);
    EXPECT_THROW(checkIntervalParser("y", ""), DB::Exception);
}

TEST(IntervalAliasTest, IntervalAliasInEmit)
{
    EXPECT_TRUE(checkASTTree(
        /* origin query */
        "SELECT device, avg(temperature) FROM default.devices group by device emit after watermark with delay +1s and last +1s and periodic +1s",
        /* check query */
        "SELECT device, avg(temperature) FROM default.devices group by device emit after watermark with delay interval +1 second and periodic interval +1 second and last interval +1 second"));

    EXPECT_TRUE(checkASTTree(
        /* origin query */
        "SELECT device, avg(temperature) FROM default.devices group by device emit after watermark with delay -1s and last -1s and periodic +1s",
        /* check query */
        "SELECT device, avg(temperature) FROM default.devices group by device emit after watermark with delay interval -1 second and periodic interval 1 second and last interval -1 second"));

    EXPECT_TRUE(checkASTTree(
        /* origin query */
        "SELECT device, avg(temperature) FROM default.devices group by device emit after watermark with delay +1s and last +1s and periodic +1s",
        /* check query */
        "SELECT device, avg(temperature) FROM default.devices group by device emit after watermark with delay interval +1 second and periodic interval 1 second and last interval 1 second"));
}

TEST(IntervalAliasTest, IntervalAliasInTumbleFunc)
{
    EXPECT_TRUE(checkASTTree(
        /* origin query */
        "SELECT device, avg(temperature) FROM tumble(default.devices, +5s) group by device, window_end",
        /* check query */
        "SELECT device, avg(temperature) FROM tumble(default.devices, interval +5 second) group by device, window_end"));

    EXPECT_TRUE(checkASTTree(
        /* origin query */
        "SELECT device, avg(temperature) FROM tumble(default.devices, -5s) group by device, window_end",
        /* check query */
        "SELECT device, avg(temperature) FROM tumble(default.devices, interval -5 second) group by device, window_end"));

    EXPECT_TRUE(checkASTTree(
        /* origin query */
        "SELECT device, avg(temperature) FROM tumble(default.devices, 5s) group by device, window_end",
        /* check query */
        "SELECT device, avg(temperature) FROM tumble(default.devices, interval 5 second) group by device, window_end"));
}

TEST(IntervalAliasTest, IntervalAliasInHopFunc)
{
    EXPECT_TRUE(checkASTTree(
        /* origin query */
        "SELECT device, avg(temperature) FROM hop(default.devices, +1s, +5m) group by device, window_end",
        /* check query */
        "SELECT device, avg(temperature) FROM hop(default.devices, interval +1 second, interval +5 minute) group by device, window_end"));

    EXPECT_TRUE(checkASTTree(
        /* origin query */
        "SELECT device, avg(temperature) FROM hop(default.devices, -1s, -5m) group by device, window_end",
        /* check query */
        "SELECT device, avg(temperature) FROM hop(default.devices, interval -1 second, interval -5 minute) group by device, window_end"));

    EXPECT_TRUE(checkASTTree(
        /* origin query */
        "SELECT device, avg(temperature) FROM hop(default.devices, 1s, 5m) group by device, window_end",
        /* check query */
        "SELECT device, avg(temperature) FROM hop(default.devices, interval 1 second, interval 5 minute) group by device, window_end"));
}

TEST(IntervalAliasTest, IntervalAliasInAnyError)
{
    /// Error in Projection
    EXPECT_THROW(
        checkASTTree(
            /* origin query */
            "SELECT 1s, device, avg(temperature) FROM default.devices group by device",
            /* check query */
            ""),
        DB::Exception);

    /// Error in Having
    EXPECT_THROW(
        checkASTTree(
            /* origin query */
            "SELECT device, avg(temperature) FROM default.devices group by device having _tp_time - inteval 1 second = _tp_time - 1s",
            /* check query */
            ""),
        DB::Exception);

    /// Error in Where
    EXPECT_THROW(
        checkASTTree(
            /* origin query */
            "SELECT device, temperature FROM default.devices where _tp_time - interval 1 minute = _tp_time - 1m",
            /* check query */
            ""),
        DB::Exception);

    /// Error in Group By
    EXPECT_THROW(
        checkASTTree(
            /* origin query */
            "SELECT 1 FROM default.devices group by 1m",
            /* check query */
            ""),
        DB::Exception);

    /// Error in Order By
    EXPECT_THROW(
        checkASTTree(
            /* origin query */
            "SELECT 1 FROM default.devices order by 1h",
            /* check query */
            ""),
        DB::Exception);
}
