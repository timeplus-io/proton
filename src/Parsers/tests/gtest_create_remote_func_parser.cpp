#include <IO/WriteBufferFromString.h>
#include <Parsers/ASTCreateFunctionQuery.h>
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
#include "Common/Exception.h"
#include "Parsers/IParser.h"

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

    auto remote_func_settings = create->remote_func_settings;
    EXPECT_EQ(remote_func_settings->get("URL").toString(), "https://hn6wip76uexaeusz5s7bh3e4u40lrrrz.lambda-url.us-west-2.on.aws/");
    EXPECT_FALSE(remote_func_settings->has("AUTH_METHOD"));
    EXPECT_FALSE(remote_func_settings->has("AUTH_HEADER"));
    EXPECT_FALSE(remote_func_settings->has("AUTH_KEY"));
}

TEST(ParserCreateRemoteFunctionQuery, UDFHeaderMethodIsNone)
{
    String input = "CREATE REMOTE  FUNCTION ip_lookup(ip string) RETURNS string "
                   "URL 'https://hn6wip76uexaeusz5s7bh3e4u40lrrrz.lambda-url.us-west-2.on.aws/'"
                   "AUTH_METHOD 'none'";
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

    auto remote_func_settings = create->remote_func_settings;
    EXPECT_EQ(remote_func_settings->get("URL").toString(), "https://hn6wip76uexaeusz5s7bh3e4u40lrrrz.lambda-url.us-west-2.on.aws/");
    EXPECT_EQ(remote_func_settings->get("AUTH_METHOD").toString(), "none");
    EXPECT_FALSE(remote_func_settings->has("AUTH_HEADER"));
    EXPECT_FALSE(remote_func_settings->has("AUTH_KEY"));
}

TEST(ParserCreateRemoteFunctionQuery, UDFHeaderMethodIsAuthHeader)
{
    String input = "CREATE REMOTE  FUNCTION ip_lookup(ip string) RETURNS string "
                   "URL 'https://hn6wip76uexaeusz5s7bh3e4u40lrrrz.lambda-url.us-west-2.on.aws/'"
                   "AUTH_METHOD 'auth_header'"
                    "AUTH_HEADER 'auth'"
                    "AUTH_KEY 'proton'";
    ParserCreateFunctionQuery parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTCreateFunctionQuery * create = ast->as<ASTCreateFunctionQuery>();
    EXPECT_EQ(create->getFunctionName(), "ip_lookup");
    EXPECT_EQ(std::string(magic_enum::enum_name(create->lang));, "Remote");
    EXPECT_NE(create->function_core, nullptr);
    EXPECT_NE(create->arguments, nullptr);

    /// Check arguments
    String args = queryToString(*create->arguments.get(), true);
    EXPECT_EQ(args, "(ip string)");

    /// Check return type
    String ret = queryToString(*create->return_type.get(), true);
    EXPECT_EQ(ret, "string");

    auto remote_func_settings = create->remote_func_settings;
    EXPECT_EQ(remote_func_settings->get("URL").toString(), "https://hn6wip76uexaeusz5s7bh3e4u40lrrrz.lambda-url.us-west-2.on.aws/");
    EXPECT_EQ(remote_func_settings->get("AUTH_METHOD").toString(), "auth_header");
    EXPECT_TRUE(remote_func_settings->has("AUTH_HEADER"));
    EXPECT_TRUE(remote_func_settings->has("AUTH_KEY"));
    EXPECT_EQ(remote_func_settings->get("AUTH_HEADER"), "auth");
    EXPECT_EQ(remote_func_settings->get("AUTH_KEY"), "proton");
}


TEST(ParserCreateRemoteFunctionQuery, UDFHeaderMethodIsOther)
{
    String input = "CREATE REMOTE  FUNCTION ip_lookup(ip string) RETURNS string "
                   "URL 'https://hn6wip76uexaeusz5s7bh3e4u40lrrrz.lambda-url.us-west-2.on.aws/'"
                   "AUTH_METHOD 'token'";
    ParserCreateFunctionQuery parser;
    EXPECT_THROW(parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0), Exception);

}