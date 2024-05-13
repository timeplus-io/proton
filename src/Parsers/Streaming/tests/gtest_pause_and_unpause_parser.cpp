#include <Parsers/Streaming/ASTPauseMaterializedViewQuery.h>
#include <Parsers/Streaming/ASTUnpauseMaterializedViewQuery.h>
#include <Parsers/Streaming/ParserPauseMaterializedViewQuery.h>
#include <Parsers/ParserQueryWithOutput.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>

#include <string_view>

#include <gtest/gtest.h>

namespace
{
using namespace DB;
using namespace std::literals;
}

struct TPParserTestCase
{
    const std::string_view input_text;
    const char * expected_ast = nullptr;
};

std::ostream & operator<<(std::ostream & ostr, const TPParserTestCase & test_case)
{
    return ostr << "TPParserTestCase input: " << test_case.input_text;
}

class TPParserTest : public ::testing::TestWithParam<std::tuple<std::shared_ptr<IParser>, TPParserTestCase>>
{};

TEST_P(TPParserTest, parseQuery)
{
    const auto & parser = std::get<0>(GetParam());
    const auto & [input_text, expected_ast] = std::get<1>(GetParam());

    ASSERT_NE(nullptr, parser);

    if (expected_ast)
    {
        ASTPtr ast;
        ASSERT_NO_THROW(ast = parseQuery(*parser, input_text.begin(), input_text.end(), 0, 0));
        EXPECT_EQ(expected_ast, serializeAST(*ast->clone(), false));
    }
    else
    {
        ASSERT_THROW(parseQuery(*parser, input_text.begin(), input_text.end(), 0, 0), DB::Exception);
    }
}

INSTANTIATE_TEST_SUITE_P(ParserPauseMaterializedViewQuery, TPParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<Streaming::ParserPauseMaterializedViewQuery>()),
        ::testing::ValuesIn(std::initializer_list<TPParserTestCase>
        {
            {
                "PAUSE MATERIALIZED VIEW mv_name",
                "PAUSE MATERIALIZED VIEW mv_name ASYNC"
            },
            {
                "PAUSE MATERIALIZED VIEW default.mv_name",
                "PAUSE MATERIALIZED VIEW default.mv_name ASYNC"
            },
            {
                "PAUSE MATERIALIZED VIEW default.mv_name, default.mv_name2",
                "PAUSE MATERIALIZED VIEW default.mv_name, default.mv_name2 ASYNC"
            },
            {
                "PAUSE MATERIALIZED VIEW default.mv_name, mv_name2, default.mv_name3",
                "PAUSE MATERIALIZED VIEW default.mv_name, mv_name2, default.mv_name3 ASYNC"
            },
            {
                "PAUSE MATERIALIZED VIEW default.mv_name, mv_name2, default.mv_name3 SYNC",
                "PAUSE MATERIALIZED VIEW default.mv_name, mv_name2, default.mv_name3 SYNC"
            },
            {
                "PAUSE MATERIALIZED VIEW default.mv_name, mv_name2, default.mv_name3 ASYNC",
                "PAUSE MATERIALIZED VIEW default.mv_name, mv_name2, default.mv_name3 ASYNC"
            }
        }
)));

INSTANTIATE_TEST_SUITE_P(ParserPauseMaterializedViewQuery_FAIL, TPParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<Streaming::ParserPauseMaterializedViewQuery>()),
        ::testing::ValuesIn(std::initializer_list<TPParserTestCase>
        {
            {
                "PAUSE",
            },
            {
                "PAUSE default.mv_name",
            },
            {
                "PAUSE MATERIALIZED default.mv_name",
            },
            {
                "PAUSE MATERIALIZED VIEW default.mv_name.col_name",
            },
            {
                "PAUSE MATERIALIZED VIEW default.mv_name and default.mv_name2",
            }
        }
)));

INSTANTIATE_TEST_SUITE_P(ParserUnpauseMaterializedViewQuery, TPParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<Streaming::ParserUnpauseMaterializedViewQuery>()),
        ::testing::ValuesIn(std::initializer_list<TPParserTestCase>
        {
            {
                "UNPAUSE MATERIALIZED VIEW mv_name",
                "UNPAUSE MATERIALIZED VIEW mv_name ASYNC"
            },
            {
                "UNPAUSE MATERIALIZED VIEW default.mv_name",
                "UNPAUSE MATERIALIZED VIEW default.mv_name ASYNC"
            },
            {
                "UNPAUSE MATERIALIZED VIEW default.mv_name, default.mv_name2",
                "UNPAUSE MATERIALIZED VIEW default.mv_name, default.mv_name2 ASYNC"
            },
            {
                "UNPAUSE MATERIALIZED VIEW default.mv_name, mv_name2, default.mv_name3",
                "UNPAUSE MATERIALIZED VIEW default.mv_name, mv_name2, default.mv_name3 ASYNC"
            },
            {
                "UNPAUSE MATERIALIZED VIEW default.mv_name, mv_name2, default.mv_name3 SYNC",
                "UNPAUSE MATERIALIZED VIEW default.mv_name, mv_name2, default.mv_name3 SYNC"
            },
            {
                "UNPAUSE MATERIALIZED VIEW default.mv_name, mv_name2, default.mv_name3 ASYNC",
                "UNPAUSE MATERIALIZED VIEW default.mv_name, mv_name2, default.mv_name3 ASYNC"
            }
        }
)));

INSTANTIATE_TEST_SUITE_P(ParserUnpauseMaterializedViewQuery_FAIL, TPParserTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<Streaming::ParserUnpauseMaterializedViewQuery>()),
        ::testing::ValuesIn(std::initializer_list<TPParserTestCase>
        {
            {
                "UNPAUSE",
            },
            {
                "UNPAUSE default.mv_name",
            },
            {
                "UNPAUSE MATERIALIZED default.mv_name",
            },
            {
                "UNPAUSE MATERIALIZED VIEW default.mv_name.col_name",
            },
            {
                "UNPAUSE MATERIALIZED VIEW default.mv_name and default.mv_name2",
            }
        }
)));
