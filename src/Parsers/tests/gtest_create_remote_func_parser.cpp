#include <IO/WriteBufferFromString.h>
#include <Parsers/ASTCreateFunctionQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunctionWithKeyValueArguments.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/DumpASTNode.h>
#include <Parsers/ParserCreateFunctionQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserDropQuery.h>
#include <Parsers/TablePropertiesQueriesASTs.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <base/types.h>

#include <gtest/gtest.h>
#include <Poco/JSON/Parser.h>
using namespace DB;

TEST(ParserCreateRemoteFunctionQuery, UDFNoHeaderMethod)
{
    String input = "CREATE REMOTE FUNCTION ip_lookup(ip string) RETURNS string "
                   "URL 'https://hn6wip76uexaeusz5s7bh3e4u40lrrrz.lambda-url.us-west-2.on.aws/';";
    ParserCreateFunctionQuery parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTCreateFunctionQuery * create = ast->as<ASTCreateFunctionQuery>();
    EXPECT_EQ(create->getFunctionName(), "ip_lookup");
    EXPECT_EQ(create->lang, "Remote");
    EXPECT_NE(create->function_core, nullptr);
    EXPECT_NE(create->arguments, nullptr);

    /// Check arguments
    String args = queryToString(*create->arguments.get(), true);
    EXPECT_EQ(args, "(ip string)");

    /// Check return type
    String ret = queryToString(*create->return_type.get(), true);
    EXPECT_EQ(ret, "string");

    auto remote_func_settings = create->function_core;
    EXPECT_NE(remote_func_settings->as<ASTExpressionList>(), nullptr);
    EXPECT_EQ(remote_func_settings->children.size(), 1);
    EXPECT_EQ(
        remote_func_settings->children.front()->as<ASTPair>()->second->as<ASTLiteral>()->value.safeGet<String>(),
        "https://hn6wip76uexaeusz5s7bh3e4u40lrrrz.lambda-url.us-west-2.on.aws/");
}

TEST(ParserCreateRemoteFunctionQuery, UDFHeaderMethodIsNone)
{
    String input = "CREATE REMOTE  FUNCTION ip_lookup(ip string) RETURNS string "
                   "AUTH_METHOD 'none'"
                   "URL 'https://hn6wip76uexaeusz5s7bh3e4u40lrrrz.lambda-url.us-west-2.on.aws/'";
    ParserCreateFunctionQuery parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTCreateFunctionQuery * create = ast->as<ASTCreateFunctionQuery>();
    EXPECT_EQ(create->getFunctionName(), "ip_lookup");
    EXPECT_EQ(create->lang, "Remote");
    EXPECT_NE(create->function_core, nullptr);
    EXPECT_NE(create->arguments, nullptr);

    /// Check arguments
    String args = queryToString(*create->arguments.get(), true);
    EXPECT_EQ(args, "(ip string)");

    /// Check return type
    String ret = queryToString(*create->return_type.get(), true);
    EXPECT_EQ(ret, "string");
    auto remote_func_settings = create->function_core;
    EXPECT_EQ(
        remote_func_settings->children.back()->as<ASTPair>()->second->as<ASTLiteral>()->value.safeGet<String>(),
        "https://hn6wip76uexaeusz5s7bh3e4u40lrrrz.lambda-url.us-west-2.on.aws/");
    EXPECT_EQ(remote_func_settings->children.size(), 2);
    EXPECT_EQ(remote_func_settings->children.front()->as<ASTPair>()->second->as<ASTLiteral>()->value.safeGet<String>(), "none"); // Auth method
}

TEST(ParserCreateRemoteFunctionQuery, UDFHeaderMethodIsAuthHeader)
{
    String input = "CREATE REMOTE  FUNCTION ip_lookup(ip string) RETURNS string "
                   "URL 'https://hn6wip76uexaeusz5s7bh3e4u40lrrrz.lambda-url.us-west-2.on.aws/'"
                   "AUTH_HEADER 'auth'"
                   "AUTH_KEY 'proton'"
                   "EXECUTION_TIMEOUT 30000"
                   " AUTH_METHOD 'auth_header'";
    ParserCreateFunctionQuery parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTCreateFunctionQuery * create = ast->as<ASTCreateFunctionQuery>();
    EXPECT_EQ(create->getFunctionName(), "ip_lookup");
    EXPECT_EQ(create->lang, "Remote");
    EXPECT_NE(create->function_core, nullptr);
    EXPECT_NE(create->arguments, nullptr);

    /// Check arguments
    String args = queryToString(*create->arguments.get(), true);
    EXPECT_EQ(args, "(ip string)");

    /// Check return type
    String ret = queryToString(*create->return_type.get(), true);
    EXPECT_EQ(ret, "string");

    auto remote_func_settings = create->function_core;
    EXPECT_EQ(remote_func_settings->children.size(), 5);
    EXPECT_EQ(remote_func_settings->children.front()->as<ASTPair>()->first, "url");
    EXPECT_EQ(
        remote_func_settings->children.front()->as<ASTPair>()->second->as<ASTLiteral>()->value.safeGet<String>(),
        "https://hn6wip76uexaeusz5s7bh3e4u40lrrrz.lambda-url.us-west-2.on.aws/");
    EXPECT_EQ(remote_func_settings->children[4]->as<ASTPair>()->second->as<ASTLiteral>()->value.safeGet<String>(), "auth_header"); /// Auth method
    EXPECT_EQ(remote_func_settings->children[1]->as<ASTPair>()->second->as<ASTLiteral>()->value.safeGet<String>(), "auth"); /// auth_header
    EXPECT_EQ(remote_func_settings->children[2]->as<ASTPair>()->second->as<ASTLiteral>()->value.safeGet<String>(), "proton"); /// auth key
    EXPECT_EQ(remote_func_settings->children[3]->as<ASTPair>()->second->as<ASTLiteral>()->value.safeGet<UInt64>(), 30000u);
}


TEST(ParserCreateRemoteFunctionQuery, UDFHeaderMethodIsOther)
{
    String input = "CREATE REMOTE  FUNCTION ip_lookup(ip string) RETURNS string "
                   "URL 'https://hn6wip76uexaeusz5s7bh3e4u40lrrrz.lambda-url.us-west-2.on.aws/'"
                   "AUTH_METHOD 'token'";
    ParserCreateFunctionQuery parser;
    EXPECT_NO_THROW(parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0));
}

TEST(ParserCreateRemoteFunctionQuery, UDFMultipleKey)
{
    String input = "CREATE REMOTE  FUNCTION ip_lookup(ip string) RETURNS string "
                   "URL 'https://hn6wip76uexaeusz5s7bh3e4u40lrrrz.lambda-url.us-west-2.on.aws/'"
                   "AUTH_METHOD 'token'"
                   "AUTH_HEADER 'auth'"
                   "AUTH_KEY 'proton'"
                   "EXECUTION_TIMEOUT 30000"
                   " AUTH_METHOD 'auth_header'";
    ParserCreateFunctionQuery parser;
    EXPECT_THROW(parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0), Exception);
}

