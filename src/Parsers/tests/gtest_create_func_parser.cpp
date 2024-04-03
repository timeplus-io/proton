#include <IO/WriteBufferFromString.h>
#include <Parsers/ASTCreateFunctionQuery.h>
#include <Parsers/ASTFunctionWithKeyValueArguments.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/DumpASTNode.h>
#include <Parsers/ParserCreateFunctionQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserDropQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/TablePropertiesQueriesASTs.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <base/types.h>

#include <gtest/gtest.h>
#include <Poco/JSON/Parser.h>

using namespace DB;

TEST(ParserCreateFunctionQuery, UDFFunction)
{
    String input = " CREATE FUNCTION add_five(value float32)"
                   " RETURNS float32"
                   " LANGUAGE JAVASCRIPT "
                   " AS $$"
                   " function add_five(value){for(let i=0;i<value.length;i++){value[i]=value[i]+5}return value}"
                   "$$  "
                   ";";

    ParserCreateFunctionQuery parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTCreateFunctionQuery * create = ast->as<ASTCreateFunctionQuery>();
    EXPECT_EQ(create->getFunctionName(), "add_five");
    EXPECT_EQ(create->lang, ASTCreateFunctionQuery::Language::JavaScript);
    EXPECT_NE(create->function_core, nullptr);
    EXPECT_NE(create->arguments, nullptr);

    /// Check arguments
    String args = queryToString(*create->arguments.get(), true);
    EXPECT_EQ(args, "(value float32)");

    /// Check return type
    String ret = queryToString(*create->return_type.get(), true);
    EXPECT_EQ(ret, "float32");

    ASTLiteral * js_src = create->function_core->as<ASTLiteral>();
    EXPECT_EQ(js_src->value.safeGet<String>(), " function add_five(value){for(let i=0;i<value.length;i++){value[i]=value[i]+5}return value}");

    String func_str = queryToString(*create, true);
    EXPECT_EQ(
        func_str,
        "CREATE FUNCTION add_five(value float32) RETURNS float32 AS $$\n function add_five(value){for(let "
        "i=0;i<value.length;i++){value[i]=value[i]+5}return value}\n$$");

    auto json = create->toJSON();
    std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    json->stringify(resp_str_stream, 0);
    String json_str = resp_str_stream.str();
    EXPECT_EQ(
        json_str,
        R"###({"function":{"name":"add_five","arguments":[{"name":"value","type":"float32"}],"type":"javascript","is_aggregation":false,"return_type":"float32","source":" function add_five(value){for(let i=0;i<value.length;i++){value[i]=value[i]+5}return value}"}})###");
}

TEST(ParserCreateFunctionQuery, UDAFunction)
{
    String input = " CREATE AGGREGATE FUNCTION add_five(value float32)"
                   " RETURNS float32"
                   " LANGUAGE JAVASCRIPT "
                   " AS $$"
                   " function add_five(value){for(let i=0;i<value.length;i++){value[i]=value[i]+5}return value}"
                   "$$  "
                   ";";

    ParserCreateFunctionQuery parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTCreateFunctionQuery * create = ast->as<ASTCreateFunctionQuery>();
    EXPECT_EQ(create->is_aggregation, true);
    EXPECT_NE(create->function_core, nullptr);
    EXPECT_NE(create->arguments, nullptr);
}

TEST(ParserCreateFunctionQuery, ArgTypes)
{
    String input = " CREATE AGGREGATE FUNCTION add_five(value float32, complex array(datetime64(3)), t tuple(i int64, f float32))"
                   " RETURNS array(float64)"
                   " LANGUAGE JAVASCRIPT "
                   " AS $$"
                   " return false;"
                   "$$  "
                   ";";

    ParserCreateFunctionQuery parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTCreateFunctionQuery * create = ast->as<ASTCreateFunctionQuery>();
    EXPECT_EQ(create->is_aggregation, true);
    EXPECT_NE(create->function_core, nullptr);
    EXPECT_NE(create->arguments, nullptr);

    /// Check arguments
    String args = queryToString(*create->arguments.get(), true);
    EXPECT_EQ(args, "(value float32, complex array(datetime64(3)), t tuple(i int64, f float32))");

    /// Check return type
    String ret = queryToString(*create->return_type.get(), true);
    EXPECT_EQ(ret, "array(float64)");

    auto json = create->toJSON();
    std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    json->stringify(resp_str_stream, 0);
    String json_str = resp_str_stream.str();
    EXPECT_EQ(
        json_str,
        R"###({"function":{"name":"add_five","arguments":[{"name":"value","type":"float32"},{"name":"complex","type":"array(datetime64(3))"},{"name":"t","type":"tuple(i int64, f float32)"}],"type":"javascript","is_aggregation":true,"return_type":"array(float64)","source":" return false;"}})###");
}

TEST(ParserCreateFunctionQuery, ReturnType)
{
    String input = " CREATE AGGREGATE FUNCTION add_five(value float32, dt datetime64(3))"
                   " RETURNS tuple(v float64, dt datetime64(1), id string)"
                   " LANGUAGE JAVASCRIPT "
                   " AS $$"
                   " return false;"
                   "$$  "
                   ";";

    ParserCreateFunctionQuery parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTCreateFunctionQuery * create = ast->as<ASTCreateFunctionQuery>();
    EXPECT_EQ(create->is_aggregation, true);
    EXPECT_NE(create->function_core, nullptr);
    EXPECT_NE(create->arguments, nullptr);

    /// Check arguments
    String args = queryToString(*create->arguments.get(), true);
    EXPECT_EQ(args, "(value float32, dt datetime64(3))");

    /// Check return type
    String ret = queryToString(*create->return_type.get(), true);
    EXPECT_EQ(ret, "tuple(v float64, dt datetime64(1), id string)");

    auto json = create->toJSON();
    std::stringstream resp_str_stream; /// STYLE_CHECK_ALLOW_STD_STRING_STREAM
    json->stringify(resp_str_stream, 0);
    String json_str = resp_str_stream.str();
    EXPECT_EQ(
        json_str,
        R"###({"function":{"name":"add_five","arguments":[{"name":"value","type":"float32"},{"name":"dt","type":"datetime64(3)"}],"type":"javascript","is_aggregation":true,"return_type":"tuple(v float64, dt datetime64(1), id string)","source":" return false;"}})###");
}
