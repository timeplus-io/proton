#include <DataTypes/convertToClickHouseType.h>

#include <gtest/gtest.h>

#include <string_view>

using namespace DB;

/// test function: convertToClickHouseType
TEST(convertToClickHouseTypeTest, TestInputIs_int8)
{
    std::string input = "int8";
    std::string output = convertToClickHouseType(input);
    EXPECT_EQ(output, "Int8");
}

TEST(convertToClickHouseTypeTest, TestInputIs_uint8)
{
    std::string input = "uint8";
    std::string output = convertToClickHouseType(input);
    EXPECT_EQ(output, "UInt8");
}

TEST(convertToClickHouseTypeTest, TestInputIs_int16)
{
    std::string input = "int16";
    std::string output = convertToClickHouseType(input);
    EXPECT_EQ(output, "Int16");
}

TEST(convertToClickHouseTypeTest, TestInputIs_uint16)
{
    std::string input = "uint16";
    std::string output = convertToClickHouseType(input);
    EXPECT_EQ(output, "UInt16");
}

TEST(convertToClickHouseTypeTest, TestInputIs_int32)
{
    std::string input = "int32";
    std::string output = convertToClickHouseType(input);
    EXPECT_EQ(output, "Int32");
}

TEST(convertToClickHouseTypeTest, TestInputIs_uint32)
{
    std::string input = "uint32";
    std::string output = convertToClickHouseType(input);
    EXPECT_EQ(output, "UInt32");
}

TEST(convertToClickHouseTypeTest, TestInputIs_int64)
{
    std::string input = "int64";
    std::string output = convertToClickHouseType(input);
    EXPECT_EQ(output, "Int64");
}

TEST(convertToClickHouseTypeTest, TestInputIs_uint64)
{
    std::string input = "uint64";
    std::string output = convertToClickHouseType(input);
    EXPECT_EQ(output, "UInt64");
}

TEST(convertToClickHouseTypeTest, TestInputIs_int128)
{
    std::string input = "int128";
    std::string output = convertToClickHouseType(input);
    EXPECT_EQ(output, "Int128");
}

TEST(convertToClickHouseTypeTest, TestInputIs_uint128)
{
    std::string input = "uint128";
    std::string output = convertToClickHouseType(input);
    EXPECT_EQ(output, "UInt128");
}

TEST(convertToClickHouseTypeTest, TestInputIs_int256)
{
    std::string input = "int256";
    std::string output = convertToClickHouseType(input);
    EXPECT_EQ(output, "Int256");
}

TEST(convertToClickHouseTypeTest, TestInputIs_uint256)
{
    std::string input = "uint256";
    std::string output = convertToClickHouseType(input);
    EXPECT_EQ(output, "UInt256");
}

TEST(convertToClickHouseTypeTest, TestInputIs_float32)
{
    std::string input = "float32";
    std::string output = convertToClickHouseType(input);
    EXPECT_EQ(output, "Float32");
}

TEST(convertToClickHouseTypeTest, TestInputIsfloat64)
{
    std::string input = "float64";
    std::string output = convertToClickHouseType(input);
    EXPECT_EQ(output, "Float64");
}

TEST(convertToClickHouseTypeTest, TestInputIs_string)
{
    std::string input = "string";
    std::string output = convertToClickHouseType(input);
    EXPECT_EQ(output, "String");
}

TEST(convertToClickHouseTypeTest, TestInputIs_date)
{
    std::string input = "date";
    std::string output = convertToClickHouseType(input);
    EXPECT_EQ(output, "Date");
}

TEST(convertToClickHouseTypeTest, TestInputIs_date32)
{
    std::string input = "date32";
    std::string output = convertToClickHouseType(input);
    EXPECT_EQ(output, "Date32");
}

TEST(convertToClickHouseTypeTest, TestInputIs_datetime)
{
    std::string input = "datetime";
    std::string output = convertToClickHouseType(input);
    EXPECT_EQ(output, "DateTime");
}

TEST(convertToClickHouseTypeTest, TestInputIs_bool)
{
    std::string input = "bool";
    std::string output = convertToClickHouseType(input);
    EXPECT_EQ(output, "Bool");
}

TEST(convertToClickHouseTypeTest, TestInputIs_uuid)
{
    std::string input = "uuid";
    std::string output = convertToClickHouseType(input);
    EXPECT_EQ(output, "UUID");
}

TEST(convertToClickHouseTypeTest, TestInputIs_ipv4)
{
    std::string input = "ipv4";
    std::string output = convertToClickHouseType(input);
    EXPECT_EQ(output, "IPv4");
}

TEST(convertToClickHouseTypeTest, TestInputIs_ipv6)
{
    std::string input = "ipv6";
    std::string output = convertToClickHouseType(input);
    EXPECT_EQ(output, "IPv6");
}

TEST(convertToClickHouseTypeTest, TestInputIs_json)
{
    std::string input = "json";
    std::string output = convertToClickHouseType(input);
    EXPECT_EQ(output, "JSON");
}

TEST(convertToClickHouseTypeTest, TestInputIs_nullable)
{
    std::string input = "nullable(string)";
    std::string output = convertToClickHouseType(input);
    EXPECT_EQ(output, "Nullable(String)");
}

TEST(convertToClickHouseTypeTest, TestInputIs_decimal)
{
    std::string input = "decimal(10, 3)";
    std::string output = convertToClickHouseType(input);
    EXPECT_EQ(output, "Decimal(10, 3)");
}

TEST(convertToClickHouseTypeTest, TestInputIs_decimal32)
{
    std::string input = "decimal32(3)";
    std::string output = convertToClickHouseType(input);
    EXPECT_EQ(output, "Decimal32(3)");
}

TEST(convertToClickHouseTypeTest, TestInputIs_decimal64)
{
    std::string input = "decimal64(3)";
    std::string output = convertToClickHouseType(input);
    EXPECT_EQ(output, "Decimal64(3)");
}

TEST(convertToClickHouseTypeTest, TestInputIs_decimal128)
{
    std::string input = "decimal128(3)";
    std::string output = convertToClickHouseType(input);
    EXPECT_EQ(output, "Decimal128(3)");
}

TEST(convertToClickHouseTypeTest, TestInputIs_decimal256)
{
    std::string input = "decimal256(3)";
    std::string output = convertToClickHouseType(input);
    EXPECT_EQ(output, "Decimal256(3)");
}

TEST(convertToClickHouseTypeTest, TestInputIs_fixed_string)
{
    std::string input = "fixed_string(10)";
    std::string output = convertToClickHouseType(input);
    EXPECT_EQ(output, "FixedString(10)");
}

TEST(convertToClickHouseTypeTest, TestInputIs_datetime64)
{
    std::string input = "datetime64(3)";
    std::string output = convertToClickHouseType(input);
    EXPECT_EQ(output, "DateTime64(3)");
}

TEST(convertToClickHouseTypeTest, TestInputIs_enum)
{
    std::string input = "enum('One' = 1, 'Two' = 2, 'Three' = 3)";
    std::string output = convertToClickHouseType(input);
    EXPECT_EQ(output, "Enum('One' = 1, 'Two' = 2, 'Three' = 3)");
}

TEST(convertToClickHouseTypeTest, TestInputIs_enum8)
{
    std::string input = "enum8('One' = 1, 'Two' = 2, 'Three' = 3)";
    std::string output = convertToClickHouseType(input);
    EXPECT_EQ(output, "Enum8('One' = 1, 'Two' = 2, 'Three' = 3)");
}

TEST(convertToClickHouseTypeTest, TestInputIs_enum16)
{
    std::string input = "enum16('One' = 1, 'Two' = 2, 'Three' = 3)";
    std::string output = convertToClickHouseType(input);
    EXPECT_EQ(output, "Enum16('One' = 1, 'Two' = 2, 'Three' = 3)");
}

TEST(convertToClickHouseTypeTest, TestInputIs_array)
{
    std::string input = "array(int32)";
    std::string output = convertToClickHouseType(input);
    EXPECT_EQ(output, "Array(Int32)");
}

TEST(convertToClickHouseTypeTest, TestInputIs_map)
{
    std::string input = "map(string, int32)";
    std::string output = convertToClickHouseType(input);
    EXPECT_EQ(output, "Map(String, Int32)");
}

TEST(convertToClickHouseTypeTest, TestInputIs_tuple)
{
    std::string input = "tuple(string, int32, string)";
    std::string output = convertToClickHouseType(input);
    EXPECT_EQ(output, "Tuple(String, Int32, String)");
}

TEST(convertToClickHouseTypeTest, TestInputIs_low_cardinality)
{
    std::string input = "low_cardinality(string)";
    std::string output = convertToClickHouseType(input);
    EXPECT_EQ(output, "LowCardinality(String)");
}

TEST(convertToClickHouseTypeTest, TestInputIs_datetime_timezone)
{
    std::string input = "datetime('UTC')";
    std::string output = convertToClickHouseType(input);
    EXPECT_EQ(output, "DateTime('UTC')");
}

TEST(convertToClickHouseTypeTest, TestInputIs_datetime64_precision_timezone)
{
    std::string input = "datetime64(3, 'UTC')";
    std::string output = convertToClickHouseType(input);
    EXPECT_EQ(output, "DateTime64(3, 'UTC')");
}

TEST(convertToClickHouseTypeTest, TestInputIs_nested1)
{
    std::string input = "nullable(map(string, tuple(array(nullable(string), nullable(int32), nullable(fixed_string(10)), "
                        "nullable(datetime64(3, 'UTC')), low_cardinality(nullable(float32))))))";
    std::string output = convertToClickHouseType(input);
    EXPECT_EQ(
        output,
        "Nullable(Map(String, Tuple(Array(Nullable(String), Nullable(Int32), Nullable(FixedString(10)), Nullable(DateTime64(3, 'UTC')), "
        "LowCardinality(Nullable(Float32))))))");
}

TEST(convertToClickHouseTypeTest, TestInputIs_nested2)
{
    std::string input = "nullable(array(tuple(nullable(int32), nullable(string))))";
    std::string output = convertToClickHouseType(input);
    EXPECT_EQ(output, "Nullable(Array(Tuple(Nullable(Int32), Nullable(String))))");
}

TEST(convertToClickHouseTypeTest, TestInputIs_nested3)
{
    std::string input = "tuple(nullable(string), nullable(int32), nullable(datetime64(3, 'UTC')))";
    std::string output = convertToClickHouseType(input);
    EXPECT_EQ(output, "Tuple(Nullable(String), Nullable(Int32), Nullable(DateTime64(3, 'UTC')))");
}

TEST(convertToClickHouseTypeTest, TestInputIs_nested4)
{
    std::string input = "map(string, tuple(nullable(int32), nullable(string)))";
    std::string output = convertToClickHouseType(input);
    EXPECT_EQ(output, "Map(String, Tuple(Nullable(Int32), Nullable(String)))");
}
