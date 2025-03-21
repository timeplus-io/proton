#include <Parsers/parseQuery.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserDataType.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>

#include <string_view>
#include <Common/thread_local_is_clickhouse_compatible.h>

#include <gtest/gtest.h>

using namespace DB;

/// clickhouse datatype
TEST(ParserDataTypeNameTest, TestDatatypeInt8)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS Int8);";
    
    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "int8");
    thread_local_is_clickhouse_compatible = false;
}
TEST(ParserDataTypeNameTest, TestDatatypeUInt8)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS UInt8);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "uint8");
    thread_local_is_clickhouse_compatible = false;
}
TEST(ParserDataTypeNameTest, TestDatatypeInt16)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS Int16);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "int16");
    thread_local_is_clickhouse_compatible = false;
}
TEST(ParserDataTypeNameTest, TestDatatypeUInt16)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS UInt16);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "uint16");
    thread_local_is_clickhouse_compatible = false;
}
TEST(ParserDataTypeNameTest, TestDatatypeInt32)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS Int32);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "int32");
    thread_local_is_clickhouse_compatible = false;
}
TEST(ParserDataTypeNameTest, TestDatatypeUInt32)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS UInt32);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "uint32");
    thread_local_is_clickhouse_compatible = false;
}
TEST(ParserDataTypeNameTest, TestDatatypeInt64)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS Int64);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "int64");
    thread_local_is_clickhouse_compatible = false;
}
TEST(ParserDataTypeNameTest, TestDatatypeUInt64)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS UInt64);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "uint64");
    thread_local_is_clickhouse_compatible = false;
}
TEST(ParserDataTypeNameTest, TestDatatypeInt128)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS Int128);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "int128");
    thread_local_is_clickhouse_compatible = false;
}
TEST(ParserDataTypeNameTest, TestDatatypeUInt128)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS UInt128);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "uint128");
    thread_local_is_clickhouse_compatible = false;
}
TEST(ParserDataTypeNameTest, TestDatatypeInt256)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS Int256);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "int256");
    thread_local_is_clickhouse_compatible = false;
}
TEST(ParserDataTypeNameTest, TestDatatypeUInt256)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS UInt256);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "uint256");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatypeFloat32)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS Float32);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "float32");
    thread_local_is_clickhouse_compatible = false;
}
TEST(ParserDataTypeNameTest, TestDatatypeFloat64)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS Float64);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "float64");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatypeString)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS String);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "string");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatypeDate)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS Date);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "date");
    thread_local_is_clickhouse_compatible = false;
}
TEST(ParserDataTypeNameTest, TestDatatypeDate32)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS Date32);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "date32");
    thread_local_is_clickhouse_compatible = false;
}
TEST(ParserDataTypeNameTest, TestDatatypeDateTime)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS DateTime);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "datetime");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatypeBool)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS Bool);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "bool");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatypeUUID)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS UUID);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "uuid");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatypeIPv4)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS IPv4);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "ipv4");
    thread_local_is_clickhouse_compatible = false;
}
TEST(ParserDataTypeNameTest, TestDatatypeIPv6)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS IPv6);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "ipv6");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatypeJSON)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS JSON);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "json");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatypeNullable)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS Nullable(String));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "nullable(string)");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatypeDecimal)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS Decimal(10, 3));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "decimal(10, 3)");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatypeDecimal32)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS Decimal32(3));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "decimal32(3)");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatypeDecimal64)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS Decimal64(8));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "decimal64(8)");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatypeDecimal128)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS Decimal128(8));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "decimal128(8)");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatypeDecimal256)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS Decimal256(8));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "decimal256(8)");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatypeFixedString)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS FixedString(8));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "fixed_string(8)");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatypeDateTime_TimeZone)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS DateTime('Asia/Istanbul'));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "datetime('Asia/Istanbul')");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatypeDateTime64_Precision)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS DateTime64(3));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "datetime64(3)");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatypeDateTime64_Precision_TimeZone)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS DateTime64(3, 'Asia/Istanbul'));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "datetime64(3, 'Asia/Istanbul')");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatypeEnum)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS Enum('One' = 1, 'Two' = 2, 'Three' = 3));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "enum('One' = 1, 'Two' = 2, 'Three' = 3)");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatypeArray)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS Array(Int32));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "array(int32)");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatypeMap)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS Map(String, Int32));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "map(string, int32)");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatypeTuple)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS Tuple(String, Int32, String));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "tuple(string, int32, string)");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatypeLowCardinality)
{

    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS LowCardinality(String));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "low_cardinality(string)");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatypeNested1)
{

    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS Nullable(Map(String, Tuple(Array(Nullable(String), Nullable(Int32), Nullable(FixedString(10)), Nullable(DateTime64(3, 'UTC')), LowCardinality(Nullable(Float32)))))));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "nullable(map(string, tuple(array(nullable(string), nullable(int32), nullable(fixed_string(10)), nullable(datetime64(3, 'UTC')), low_cardinality(nullable(float32))))))");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatypeNested2)
{

    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS Nullable(Array(Tuple(Nullable(Int32), Nullable(String)))));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "nullable(array(tuple(nullable(int32), nullable(string))))");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatypeNested3)
{

    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS Tuple(Nullable(String), Nullable(Int32), Nullable(DateTime64(3, 'UTC'))));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "tuple(nullable(string), nullable(int32), nullable(datetime64(3, 'UTC')))");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatypeNested4)
{

    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS Map(String, Tuple(Nullable(Int32), Nullable(String))))";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "map(string, tuple(nullable(int32), nullable(string)))");
    thread_local_is_clickhouse_compatible = false;
}

/// timeplus datatype
TEST(ParserDataTypeNameTest, TestDatatype_int8)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS int8);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "int8");
    thread_local_is_clickhouse_compatible = false;
}
TEST(ParserDataTypeNameTest, TestDatatype_uint8)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS uint8);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "uint8");
    thread_local_is_clickhouse_compatible = false;
}
TEST(ParserDataTypeNameTest, TestDatatype_int16)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS int16);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "int16");
    thread_local_is_clickhouse_compatible = false;
}
TEST(ParserDataTypeNameTest, TestDatatype_uint16)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS uint16);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "uint16");
    thread_local_is_clickhouse_compatible = false;
}
TEST(ParserDataTypeNameTest, TestDatatype_int32)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS Int32);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "int32");
    thread_local_is_clickhouse_compatible = false;
}
TEST(ParserDataTypeNameTest, TestDatatype_uint32)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS uint32);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "uint32");
    thread_local_is_clickhouse_compatible = false;
}
TEST(ParserDataTypeNameTest, TestDatatype_int64)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS int64);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "int64");
    thread_local_is_clickhouse_compatible = false;
}
TEST(ParserDataTypeNameTest, TestDatatype_uint64)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS uint64);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "uint64");
    thread_local_is_clickhouse_compatible = false;
}
TEST(ParserDataTypeNameTest, TestDatatype_int128)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS int128);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "int128");
    thread_local_is_clickhouse_compatible = false;
}
TEST(ParserDataTypeNameTest, TestDatatype_uint128)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS uint128);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "uint128");
    thread_local_is_clickhouse_compatible = false;
}
TEST(ParserDataTypeNameTest, TestDatatype_int256)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS int256);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "int256");
    thread_local_is_clickhouse_compatible = false;
}
TEST(ParserDataTypeNameTest, TestDatatype_uint256)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS uint256);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "uint256");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatype_float32)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS float32);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "float32");
    thread_local_is_clickhouse_compatible = false;
}
TEST(ParserDataTypeNameTest, TestDatatype_float64)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS float64);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "float64");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatype_string)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS string);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "string");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatype_date)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS date);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "date");
    thread_local_is_clickhouse_compatible = false;
}
TEST(ParserDataTypeNameTest, TestDatatype_date32)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS date32);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "date32");
    thread_local_is_clickhouse_compatible = false;
}
TEST(ParserDataTypeNameTest, TestDatatype_dateTime)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS datetime);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "datetime");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatype_bool)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS bool);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "bool");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatype_uuid)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS uuid);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "uuid");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatype_ipv4)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS ipv4);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "ipv4");
    thread_local_is_clickhouse_compatible = false;
}
TEST(ParserDataTypeNameTest, TestDatatype_ipv6)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS ipv6);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "ipv6");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatype_json)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS json);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "json");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatype_nullable)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS nullable(string));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "nullable(string)");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatype_decimal)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS decimal(10, 3));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "decimal(10, 3)");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatype_decimal32)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS decimal32(3));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "decimal32(3)");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatype_decimal64)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS decimal64(8));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "decimal64(8)");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatype_decimal128)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS decimal128(8));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "decimal128(8)");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatype_decimal256)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS decimal256(8));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "decimal256(8)");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatype_fixed_string)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS fixed_string(8));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "fixed_string(8)");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatype_datetime_timezone)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS datetime('Asia/Istanbul'));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "datetime('Asia/Istanbul')");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatype_datetime64_precision)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS datetime64(3));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "datetime64(3)");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatype_datetime64_precision_timezone)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS datetime64(3, 'Asia/Istanbul'));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "datetime64(3, 'Asia/Istanbul')");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatype_enum)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS enum('One' = 1, 'Two' = 2, 'Three' = 3));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "enum('One' = 1, 'Two' = 2, 'Three' = 3)");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatype_array)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS array(Int32));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "array(int32)");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatype_map)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS map(string, int32));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "map(string, int32)");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatype_tuple)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS tuple(string, int32, string));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "tuple(string, int32, string)");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatype_low_cardinality)
{

    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS low_cardinality(string));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "low_cardinality(string)");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatype_nested1)
{

    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS nullable(map(string, tuple(array(nullable(string), nullable(int32), nullable(fixed_string(10)), nullable(datetime64(3, 'UTC')), low_cardinality(nullable(float32)))))));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "nullable(map(string, tuple(array(nullable(string), nullable(int32), nullable(fixed_string(10)), nullable(datetime64(3, 'UTC')), low_cardinality(nullable(float32))))))");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatype_nested2)
{

    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS nullable(array(tuple(nullable(int32), nullable(string)))));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "nullable(array(tuple(nullable(int32), nullable(string))))");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatype_nested3)
{

    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS tuple(nullable(string), nullable(int32), nullable(datetime64(3, 'UTC'))));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "tuple(nullable(string), nullable(int32), nullable(datetime64(3, 'UTC')))");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserDataTypeNameTest, TestDatatype_nested4)
{

    thread_local_is_clickhouse_compatible = true;
    String input = "CAST(id AS map(string, tuple(nullable(int32), nullable(string))))";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "map(string, tuple(nullable(int32), nullable(string)))");
    thread_local_is_clickhouse_compatible = false;
}


/// clickhouse function name
TEST(ParserFunctionNameTest, TestFunction_toUInt16)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "toUInt16('123')";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction * function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "to_uint16");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserFunctionNameTest, TestFunction_encodeURLComponent)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "encodeURLComponent('Hello world! How are you?')";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction * function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "encode_url_component");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserFunctionNameTest, TestFunction_todAte)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "todAte(now())";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction * function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "to_date");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserFunctionNameTest, TestFunction_generateUUIDv4)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "generateUUIDv4(1)";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction * function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "generate_uuidv4");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserFunctionNameTest, TestFunction_stddevSamp)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "stddevSamp(v)";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction * function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "stddev_samp");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserFunctionNameTest, TestFunction_ifNull)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "ifNull(value, 0)";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction * function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "if_null");
    thread_local_is_clickhouse_compatible = false;
}

/// timeplus function name
TEST(ParserFunctionNameTest, TestFunction_to_uint16)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "to_uint16('123')";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction * function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "to_uint16");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserFunctionNameTest, TestFunction_encode_url_component)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "encode_url_component('Hello world! How are you?')";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction * function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "encode_url_component");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserFunctionNameTest, TestFunction_to_date)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "to_date(now())";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction * function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "to_date");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserFunctionNameTest, TestFunction_generate_uuidv4)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "generate_uuidv4(1)";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction * function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "generate_uuidv4");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserFunctionNameTest, TestFunction_stddev_samp)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "stddev_samp(v)";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction * function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "stddev_samp");
    thread_local_is_clickhouse_compatible = false;
}

TEST(ParserFunctionNameTest, TestFunction_if_null)
{
    thread_local_is_clickhouse_compatible = true;
    String input = "if_null(value, 0)";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction * function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "if_null");
    thread_local_is_clickhouse_compatible = false;
}
