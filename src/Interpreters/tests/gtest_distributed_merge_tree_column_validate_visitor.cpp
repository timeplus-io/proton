#include <Interpreters/Streaming/StreamColumnValidateVisitor.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>

#include <gtest/gtest.h>

using namespace DB;

static void validateCreate(const String & query)
{
    const char * start = query.data();
    const char * end = start + query.size();
    ParserQuery parser(end);
    ASTPtr ast = parseQuery(parser, start, end, "", 0, 0);
    StreamColumnValidateMatcher::Data column_validate_data;
    StreamColumnValidateVisitor column_validate_visitor(column_validate_data);
    column_validate_visitor.visit(ast);
}

TEST(StreamColumnValidateVisitor, validCreate)
{
    EXPECT_NO_THROW(validateCreate("CREATE STREAM example_table(d DateTime64(3)) ENGINE = Stream PARTITION BY to_YYYYMM(d) ORDER BY d"));
    EXPECT_NO_THROW(validateCreate(
        "CREATE STREAM example_table(d DateTime64(3), _time DateTime64) ENGINE = Stream PARTITION BY to_YYYYMM(d) ORDER BY d"));
    EXPECT_NO_THROW(validateCreate(
        "CREATE STREAM example_table(d DateTime64(3), _time DateTime64(3)) ENGINE = Stream PARTITION BY to_YYYYMM(d) ORDER BY d"));
    EXPECT_NO_THROW(validateCreate("CREATE STREAM example_table(d DateTime64(3), _time DateTime64(3) DEFAULT d) ENGINE = Stream "
                                   "PARTITION BY to_YYYYMM(d) ORDER BY d"));
    EXPECT_NO_THROW(
        validateCreate(
            "CREATE STREAM example_table(d DateTime64(3), _time DateTime DEFAULT d) ENGINE = MergeTree PARTITION BY to_YYYYMM(d) ORDER BY d"));
}

TEST(StreamColumnValidateVisitor, invalidCreate)
{
    EXPECT_THROW(
        validateCreate(
            "CREATE STREAM example_table(d DateTime64(3), _tp_time String DEFAULT d) ENGINE = Stream PARTITION BY to_YYYYMM(d) ORDER BY d"),
        DB::Exception);
}
