#include <Interpreters/Streaming/ColumnValidateVisitor.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>

#include <gtest/gtest.h>

static void validateCreate(const String & query)
{
    const char * start = query.data();
    const char * end = start + query.size();
    DB::ParserQuery parser(end);
    DB::ASTPtr ast = DB::parseQuery(parser, start, end, "", 0, 0);
    DB::Streaming::ColumnValidateMatcher::Data column_validate_data;
    DB::Streaming::ColumnValidateVisitor column_validate_visitor(column_validate_data);
    column_validate_visitor.visit(ast);
}

TEST(StreamColumnValidateVisitor, validCreate)
{
    EXPECT_NO_THROW(validateCreate("CREATE STREAM example_table(d datetime64(3)) ENGINE = Stream PARTITION BY to_YYYYMM(d) ORDER BY d"));
    EXPECT_NO_THROW(validateCreate(
        "CREATE STREAM example_table(d datetime64(3), _time datetime64) ENGINE = Stream PARTITION BY to_YYYYMM(d) ORDER BY d"));
    EXPECT_NO_THROW(validateCreate(
        "CREATE STREAM example_table(d datetime64(3), _time datetime64(3)) ENGINE = Stream PARTITION BY to_YYYYMM(d) ORDER BY d"));
    EXPECT_NO_THROW(validateCreate("CREATE STREAM example_table(d datetime64(3), _time datetime64(3) DEFAULT d) ENGINE = Stream "
                                   "PARTITION BY to_YYYYMM(d) ORDER BY d"));
    EXPECT_NO_THROW(
        validateCreate(
            "CREATE STREAM example_table(d datetime64(3), _time dateTime DEFAULT d) ENGINE = MergeTree PARTITION BY to_YYYYMM(d) ORDER BY d"));
}

TEST(StreamColumnValidateVisitor, invalidCreate)
{
    EXPECT_THROW(
        validateCreate(
            "CREATE STREAM example_table(d datetime64(3), _tp_time string DEFAULT d) ENGINE = Stream PARTITION BY to_YYYYMM(d) ORDER BY d"),
        DB::Exception);
}
