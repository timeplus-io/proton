#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/getLeastSupertype.h>
#include <DataTypes/getMostSubtype.h>

#include <sstream>
#include <gtest/gtest.h>

namespace DB
{

static bool operator==(const IDataType & left, const IDataType & right)
{
    return left.equals(right);
}

}

using namespace DB;

static auto typeFromString(const std::string & str)
{
    auto & data_type_factory = DataTypeFactory::instance();
    return data_type_factory.get(str);
}

static auto typesFromString(const std::string & str)
{
    std::istringstream data_types_stream(str);      // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    DataTypes data_types;
    std::string data_type;
    while (data_types_stream >> data_type)
        data_types.push_back(typeFromString(data_type));

    return data_types;
}

struct TypesTestCase
{
    const char * from_types = nullptr;
    const char * expected_type = nullptr;
};

std::ostream & operator<<(std::ostream & ostr, const TypesTestCase & test_case)
{
    ostr << "TypesTestCase{\"" << test_case.from_types << "\", ";
    if (test_case.expected_type)
        ostr << "\"" << test_case.expected_type << "\"";
    else
        ostr << "nullptr";

    return ostr << "}";
}

class TypeTest : public ::testing::TestWithParam<TypesTestCase>
{
public:
    void SetUp() override
    {
        const auto & p = GetParam();
        from_types = typesFromString(p.from_types);

        if (p.expected_type)
            expected_type = typeFromString(p.expected_type);
        else
            expected_type.reset();
    }

    DataTypes from_types;
    DataTypePtr expected_type;
};

class LeastSuperTypeTest : public TypeTest {};

TEST_P(LeastSuperTypeTest, getLeastSupertype)
{
    if (this->expected_type)
    {
        ASSERT_EQ(*this->expected_type, *getLeastSupertype(this->from_types));
    }
    else
    {
        EXPECT_ANY_THROW(getLeastSupertype(this->from_types));
    }
}

class MostSubtypeTest : public TypeTest {};

TEST_P(MostSubtypeTest, getMostSubtype)
{
    if (this->expected_type)
    {
        ASSERT_EQ(*this->expected_type, *getMostSubtype(this->from_types));
    }
    else
    {
        EXPECT_ANY_THROW(getMostSubtype(this->from_types, true));
    }
}

INSTANTIATE_TEST_SUITE_P(data_type,
    LeastSuperTypeTest,
    ::testing::ValuesIn(
        std::initializer_list<TypesTestCase>{
            {"", "nothing"},
            {"nothing", "nothing"},

            {"uint8", "uint8"},
            {"uint8 uint8", "uint8"},
            {"int8 int8", "int8"},
            {"uint8 int8", "int16"},
            {"uint8 int16", "int16"},
            {"uint8 uint32 uint64", "uint64"},
            {"int8 int32 int64", "int64"},
            {"uint8 uint32 int64", "int64"},

            {"float32 float64", "float64"},
            {"float32 uint16 int16", "float32"},
            {"float32 uint16 int32", "float64"},
            {"float32 int16 uint32", "float64"},

            {"date date", "date"},
            {"date datetime", "dateTime"},
            {"date datetime64(3)", "datetime64(3)"},
            {"datetime datetime64(3)", "datetime64(3)"},
            {"datetime datetime64(0)", "datetime64(0)"},
            {"datetime64(9) datetime64(3)", "datetime64(9)"},

            {"string fixed_string(32) fixed_string(8)", "string"},

            {"array(uint8) array(uint8)", "array(uint8)"},
            {"array(uint8) array(int8)", "array(int16)"},
            {"array(float32) array(int16) array(uint32)", "array(float64)"},
            {"array(array(uint8)) array(array(uint8))", "array(array(uint8))"},
            {"array(array(uint8)) array(array(int8))", "array(array(int16))"},
            {"array(date) array(datetime)", "array(datetime)"},
            {"array(string) array(fixed_string(32))", "array(string)"},

            {"nullable(nothing) nothing", "nullable(nothing)"},
            {"nullable(uint8) int8", "nullable(int16)"},
            {"nullable(nothing) uint8 int8", "nullable(int16)"},

            {"tuple(int8,uint8) tuple(uint8,int8)", "tuple(int16,int16)"},
            {"tuple(nullable(nothing)) tuple(nullable(uint8))", "tuple(nullable(uint8))"},

            {"int8 string", nullptr},
            {"int64 uint64", nullptr},
            {"float32 uint64", nullptr},
            {"float64 int64", nullptr},
            {"tuple(int64) tuple(uint64)", nullptr},
            {"tuple(int64,int8) tuple(uint64)", nullptr},
            {"array(int64) array(string)", nullptr},
        }
    )
);

INSTANTIATE_TEST_SUITE_P(data_type,
    MostSubtypeTest,
    ::testing::ValuesIn(
        std::initializer_list<TypesTestCase>{
            {"", "nothing"},
            {"nothing", "nothing"},

            {"uint8", "uint8"},
            {"uint8 uint8", "uint8"},
            {"int8 int8", "int8"},
            {"uint8 int8", "uint8"},
            {"int8 uint16", "int8"},
            {"uint8 uint32 uint64", "uint8"},
            {"int8 int32 int64", "int8"},
            {"uint8 int64 uint64", "uint8"},

            {"float32 float64", "float32"},
            {"float32 uint16 int16", "uint16"},
            {"float32 uint16 int32", "uint16"},
            {"float32 int16 uint32", "int16"},

            {"datetime datetime", "datetime"},
            {"date datetime", "date"},

            {"string fixed_string(8)", "fixed_string(8)"},
            {"fixed_string(16) fixed_string(8)", "nothing"},

            {"array(uint8) array(uint8)", "array(uint8)"},
            {"array(uint8) array(int8)", "array(uint8)"},
            {"array(float32) array(int16) array(uint32)", "array(int16)"},
            {"array(array(uint8)) array(array(uint8))", "array(array(uint8))"},
            {"array(array(uint8)) array(array(int8))", "array(array(uint8))"},
            {"array(Date) array(DateTime)", "array(Date)"},
            {"array(string) array(fixed_string(32))", "array(fixed_string(32))"},
            {"array(string) array(fixed_string(32))", "array(fixed_string(32))"},

            {"nullable(nothing) nothing", "nothing"},
            {"nullable(uint8) int8", "uint8"},
            {"nullable(nothing) uint8 int8", "nothing"},
            {"nullable(uint8) nullable(int8)", "nullable(uint8)"},
            {"nullable(nothing) nullable(int8)", "nullable(nothing)"},

            {"tuple(int8,uint8) tuple(uint8,int8)", "tuple(uint8,uint8)"},
            {"tuple(nullable(nothing)) tuple(nullable(uint8))", "tuple(nullable(nothing))"},

            {"int8 string", nullptr},
            {"nothing", nullptr},
            {"fixed_string(16) fixed_string(8) string", nullptr},
        }
    )
);
