#include <Parsers/parseQuery.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserDataType.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>

#include <string_view>
#include <Common/ClickHouseCompatibleFlag.h>

#include <gtest/gtest.h>

using namespace DB;

/// clickhouse datatype
TEST(ParserDataTypeNameTest, TestDatatypeInt8)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS Int8);";
    
    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "int8");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatypeUInt8)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS UInt8);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "uint8");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatypeInt16)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS Int16);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "int16");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatypeUInt16)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS UInt16);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "uint16");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatypeInt32)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS Int32);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "int32");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatypeUInt32)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS UInt32);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "uint32");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatypeInt64)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS Int64);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "int64");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatypeUInt64)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS UInt64);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "uint64");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatypeInt128)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS Int128);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "int128");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatypeUInt128)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS UInt128);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "uint128");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatypeInt256)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS Int256);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "int256");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatypeUInt256)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS UInt256);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "uint256");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatypeFloat32)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS Float32);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "float32");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatypeFloat64)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS Float64);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "float64");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatypeString)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS String);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "string");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatypeDate)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS Date);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "date");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatypeDate32)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS Date32);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "date32");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatypeDateTime)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS DateTime);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "datetime");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatypeBool)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS Bool);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "bool");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatypeUUID)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS UUID);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "uuid");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatypeIPv4)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS IPv4);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "ipv4");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatypeIPv6)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS IPv6);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "ipv6");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatypeJSON)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS JSON);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "json");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatypeNullable)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS Nullable(String));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "nullable(string)");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatypeDecimal)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS Decimal(10, 3));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "decimal(10, 3)");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatypeDecimal32)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS Decimal32(3));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "decimal32(3)");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatypeDecimal64)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS Decimal64(8));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "decimal64(8)");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatypeDecimal128)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS Decimal128(8));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "decimal128(8)");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatypeDecimal256)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS Decimal256(8));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "decimal256(8)");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatypeFixedString)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS FixedString(8));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "fixed_string(8)");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatypeDateTime_TimeZone)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS DateTime('Asia/Istanbul'));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "datetime('Asia/Istanbul')");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatypeDateTime64_Precision)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS DateTime64(3));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "datetime64(3)");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatypeDateTime64_Precision_TimeZone)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS DateTime64(3, 'Asia/Istanbul'));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "datetime64(3, 'Asia/Istanbul')");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatypeEnum)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS Enum('One' = 1, 'Two' = 2, 'Three' = 3));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "enum('One' = 1, 'Two' = 2, 'Three' = 3)");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatypeArray)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS Array(Int32));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "array(int32)");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatypeMap)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS Map(String, Int32));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "map(string, int32)");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatypeTuple)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS Tuple(String, Int32, String));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "tuple(string, int32, string)");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatypeLowCardinality)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS LowCardinality(String));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "low_cardinality(string)");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatypeNested1)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS Nullable(Map(String, Tuple(Array(Nullable(String), Nullable(Int32), Nullable(FixedString(10)), Nullable(DateTime64(3, 'UTC')), LowCardinality(Nullable(Float32)))))));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "nullable(map(string, tuple(array(nullable(string), nullable(int32), nullable(fixed_string(10)), nullable(datetime64(3, 'UTC')), low_cardinality(nullable(float32))))))");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatypeNested2)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS Nullable(Array(Tuple(Nullable(Int32), Nullable(String)))));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "nullable(array(tuple(nullable(int32), nullable(string))))");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatypeNested3)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS Tuple(Nullable(String), Nullable(Int32), Nullable(DateTime64(3, 'UTC'))));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "tuple(nullable(string), nullable(int32), nullable(datetime64(3, 'UTC')))");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatypeNested4)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS Map(String, Tuple(Nullable(Int32), Nullable(String))))";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "map(string, tuple(nullable(int32), nullable(string)))");
    setClickHouseCompatibleMode(false);
}

/// timeplus datatype
TEST(ParserDataTypeNameTest, TestDatatype_int8)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS int8);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "int8");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatype_uint8)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS uint8);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "uint8");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatype_int16)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS int16);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "int16");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatype_uint16)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS uint16);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "uint16");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatype_int32)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS Int32);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "int32");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatype_uint32)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS uint32);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "uint32");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatype_int64)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS int64);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "int64");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatype_uint64)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS uint64);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "uint64");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatype_int128)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS int128);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "int128");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatype_uint128)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS uint128);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "uint128");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatype_int256)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS int256);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "int256");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatype_uint256)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS uint256);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "uint256");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatype_float32)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS float32);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "float32");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatype_float64)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS float64);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "float64");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatype_string)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS string);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "string");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatype_date)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS date);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "date");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatype_date32)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS date32);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "date32");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatype_dateTime)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS datetime);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "datetime");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatype_bool)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS bool);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "bool");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatype_uuid)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS uuid);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "uuid");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatype_ipv4)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS ipv4);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "ipv4");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatype_ipv6)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS ipv6);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "ipv6");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatype_json)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS json);";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "json");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatype_nullable)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS nullable(string));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "nullable(string)");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatype_decimal)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS decimal(10, 3));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "decimal(10, 3)");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatype_decimal32)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS decimal32(3));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "decimal32(3)");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatype_decimal64)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS decimal64(8));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "decimal64(8)");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatype_decimal128)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS decimal128(8));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "decimal128(8)");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatype_decimal256)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS decimal256(8));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "decimal256(8)");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatype_fixed_string)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS fixed_string(8));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "fixed_string(8)");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatype_datetime_timezone)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS datetime('Asia/Istanbul'));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "datetime('Asia/Istanbul')");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatype_datetime64_precision)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS datetime64(3));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "datetime64(3)");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatype_datetime64_precision_timezone)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS datetime64(3, 'Asia/Istanbul'));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "datetime64(3, 'Asia/Istanbul')");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatype_enum)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS enum('One' = 1, 'Two' = 2, 'Three' = 3));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "enum('One' = 1, 'Two' = 2, 'Three' = 3)");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatype_array)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS array(Int32));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "array(int32)");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatype_map)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS map(string, int32));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "map(string, int32)");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatype_tuple)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS tuple(string, int32, string));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "tuple(string, int32, string)");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatype_low_cardinality)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS low_cardinality(string));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "low_cardinality(string)");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatype_nested1)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS nullable(map(string, tuple(array(nullable(string), nullable(int32), nullable(fixed_string(10)), nullable(datetime64(3, 'UTC')), low_cardinality(nullable(float32)))))));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "nullable(map(string, tuple(array(nullable(string), nullable(int32), nullable(fixed_string(10)), nullable(datetime64(3, 'UTC')), low_cardinality(nullable(float32))))))");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatype_nested2)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS nullable(array(tuple(nullable(int32), nullable(string)))));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "nullable(array(tuple(nullable(int32), nullable(string))))");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatype_nested3)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS tuple(nullable(string), nullable(int32), nullable(datetime64(3, 'UTC'))));";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "tuple(nullable(string), nullable(int32), nullable(datetime64(3, 'UTC')))");
    setClickHouseCompatibleMode(false);
}

TEST(ParserDataTypeNameTest, TestDatatype_nested4)
{
    setClickHouseCompatibleMode(true);
    String input = "CAST(id AS map(string, tuple(nullable(int32), nullable(string))))";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction *function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "cast");

    ASTIdentifier * arg1 = function->arguments->children[0]->as<ASTIdentifier>();
    EXPECT_EQ(arg1->name(), "id");
    ASTLiteral * arg2 = function->arguments->children[1]->as<ASTLiteral>();
    EXPECT_EQ(arg2->value, "map(string, tuple(nullable(int32), nullable(string)))");
    setClickHouseCompatibleMode(false);
}


/// clickhouse function name
TEST(ParserFunctionNameTest, TestFunction_toUInt16)
{
    setClickHouseCompatibleMode(true);
    String input = "toUInt16('123')";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction * function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "to_uint16");
    setClickHouseCompatibleMode(false);
}

TEST(ParserFunctionNameTest, TestFunction_encodeURLComponent)
{
    setClickHouseCompatibleMode(true);
    String input = "encodeURLComponent('Hello world! How are you?')";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction * function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "encode_url_component");
    setClickHouseCompatibleMode(false);
}

TEST(ParserFunctionNameTest, TestFunction_todAte)
{
    setClickHouseCompatibleMode(true);
    String input = "todAte(now())";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction * function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "to_date");
    setClickHouseCompatibleMode(false);
}

TEST(ParserFunctionNameTest, TestFunction_generateUUIDv4)
{
    setClickHouseCompatibleMode(true);
    String input = "generateUUIDv4(1)";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction * function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "generate_uuidv4");
    setClickHouseCompatibleMode(false);
}

TEST(ParserFunctionNameTest, TestFunction_stddevSamp)
{
    setClickHouseCompatibleMode(true);
    String input = "stddevSamp(v)";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction * function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "stddev_samp");
    setClickHouseCompatibleMode(false);
}

TEST(ParserFunctionNameTest, TestFunction_ifNull)
{
    setClickHouseCompatibleMode(true);
    String input = "ifNull(value, 0)";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction * function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "if_null");
    setClickHouseCompatibleMode(false);
}

/// timeplus function name
TEST(ParserFunctionNameTest, TestFunction_to_uint16)
{
    setClickHouseCompatibleMode(true);
    String input = "to_uint16('123')";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction * function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "to_uint16");
    setClickHouseCompatibleMode(false);
}

TEST(ParserFunctionNameTest, TestFunction_encode_url_component)
{
    setClickHouseCompatibleMode(true);
    String input = "encode_url_component('Hello world! How are you?')";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction * function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "encode_url_component");
    setClickHouseCompatibleMode(false);
}

TEST(ParserFunctionNameTest, TestFunction_to_date)
{
    setClickHouseCompatibleMode(true);
    String input = "to_date(now())";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction * function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "to_date");
    setClickHouseCompatibleMode(false);
}

TEST(ParserFunctionNameTest, TestFunction_generate_uuidv4)
{
    setClickHouseCompatibleMode(true);
    String input = "generate_uuidv4(1)";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction * function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "generate_uuidv4");
    setClickHouseCompatibleMode(false);
}

TEST(ParserFunctionNameTest, TestFunction_stddev_samp)
{
    setClickHouseCompatibleMode(true);
    String input = "stddev_samp(v)";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction * function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "stddev_samp");
    setClickHouseCompatibleMode(false);
}

TEST(ParserFunctionNameTest, TestFunction_if_null)
{
    setClickHouseCompatibleMode(true);
    String input = "if_null(value, 0)";

    ParserFunction parser;
    ASTPtr ast = parseQuery(parser, input.data(), input.data() + input.size(), "", 0, 0);
    ASTFunction * function = ast->as<ASTFunction>();

    EXPECT_EQ(function->name, "if_null");
    setClickHouseCompatibleMode(false);
}
