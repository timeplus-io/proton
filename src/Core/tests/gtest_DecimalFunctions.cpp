#include <gtest/gtest.h>

#include <Core/DecimalFunctions.h>

namespace
{
using namespace DB;

struct DecimalUtilsSplitAndCombineTestParam
{
    const char * description;

    Decimal64 decimal_value;
    uint8_t scale;

    DecimalUtils::DecimalComponents<Decimal64> components;
};


class DecimalUtilsSplitAndCombineTest : public ::testing::TestWithParam<DecimalUtilsSplitAndCombineTestParam>
{};

template <typename DecimalType>
void testSplit(const DecimalUtilsSplitAndCombineTestParam & param)
{
    const DecimalType decimal_value(static_cast<typename DecimalType::NativeType>(param.decimal_value.value));
    const auto & actual_components = DecimalUtils::split(decimal_value, param.scale);

    EXPECT_EQ(param.components.whole, actual_components.whole);
    EXPECT_EQ(param.components.fractional, actual_components.fractional);
}

template <typename DecimalType>
void testDecimalFromComponents(const DecimalUtilsSplitAndCombineTestParam & param)
{
    EXPECT_EQ(param.decimal_value,
              DecimalUtils::decimalFromComponents<DecimalType>(
                  static_cast<typename DecimalType::NativeType>(param.components.whole),
                  static_cast<typename DecimalType::NativeType>(param.components.fractional),
                  param.scale));
}

template <typename DecimalType>
void testGetWhole(const DecimalUtilsSplitAndCombineTestParam & param)
{
    EXPECT_EQ(param.components.whole,
              DecimalUtils::getWholePart(
                  DecimalType{static_cast<typename DecimalType::NativeType>(param.decimal_value.value)},
                  param.scale));
}

template <typename DecimalType>
void testGetFractional(const DecimalUtilsSplitAndCombineTestParam & param)
{
    EXPECT_EQ(param.components.fractional,
              DecimalUtils::getFractionalPart(
                  DecimalType{static_cast<typename DecimalType::NativeType>(param.decimal_value.value)},
                  param.scale));
}

// Unfortunately typed parametrized tests () are not supported in this version of gtest, so I have to emulate by hand.
TEST_P(DecimalUtilsSplitAndCombineTest, splitDecimal32)
{
    testSplit<Decimal32>(GetParam());
}

TEST_P(DecimalUtilsSplitAndCombineTest, splitDecimal64)
{
    testSplit<Decimal64>(GetParam());
}

TEST_P(DecimalUtilsSplitAndCombineTest, splitDecimal128)
{
    testSplit<Decimal128>(GetParam());
}

TEST_P(DecimalUtilsSplitAndCombineTest, combineDecimal32)
{
    testDecimalFromComponents<Decimal32>(GetParam());
}

TEST_P(DecimalUtilsSplitAndCombineTest, combineDecimal64)
{
    testDecimalFromComponents<Decimal64>(GetParam());
}

TEST_P(DecimalUtilsSplitAndCombineTest, combineDecimal128)
{
    testDecimalFromComponents<Decimal64>(GetParam());
}

TEST_P(DecimalUtilsSplitAndCombineTest, getWholePartDecimal32)
{
    testGetWhole<Decimal32>(GetParam());
}

TEST_P(DecimalUtilsSplitAndCombineTest, getWholePartDecimal64)
{
    testGetWhole<Decimal64>(GetParam());
}

TEST_P(DecimalUtilsSplitAndCombineTest, getWholePartDecimal128)
{
    testGetWhole<Decimal128>(GetParam());
}

TEST_P(DecimalUtilsSplitAndCombineTest, getFractionalPartDecimal32)
{
    testGetFractional<Decimal32>(GetParam());
}

TEST_P(DecimalUtilsSplitAndCombineTest, getFractionalPartDecimal64)
{
    testGetFractional<Decimal64>(GetParam());
}

TEST_P(DecimalUtilsSplitAndCombineTest, getFractionalPartDecimal128)
{
    testGetFractional<Decimal128>(GetParam());
}

}

namespace std // NOLINT(cert-dcl58-cpp)
{

std::ostream & operator << (std::ostream & ostr, const DecimalUtilsSplitAndCombineTestParam & param) // NOLINT(cert-dcl58-cpp)
{
    return ostr << param.description;
}

}


// Intentionally small values that fit into 32-bit in order to cover Decimal32, Decimal64 and Decimal128 with single set of data.
INSTANTIATE_TEST_SUITE_P(Basic,
    DecimalUtilsSplitAndCombineTest,
    ::testing::ValuesIn(std::initializer_list<DecimalUtilsSplitAndCombineTestParam>{
        {
            "Positive value with non-zero scale, whole, and fractional parts.",
            1234567'89,
            2,
            {
                1234567,
                89
            }
        },
        {
            "When scale is 0, fractional part is 0.",
            1234567'89,
            0,
            {
                123456789,
                0
            }
        },
        {
            "When scale is not 0 and fractional part is 0.",
            1234567'00,
            2,
            {
                1234567,
                0
            }
        },
        {
            "When scale is not 0 and whole part is 0.",
            123,
            3,
            {
                0,
                123
            }
        },
        {
            "For negative Decimal value whole part is negative, fractional is non-negative.",
            -1234567'89,
            2,
            {
                -1234567,
                89
            }
        }
    })
);
