#include <gtest/gtest.h>

#include <iostream>
#include <map>

#include <DataTypes/NumberTraits.h>

#pragma GCC diagnostic ignored "-Wframe-larger-than="

static const std::map<std::pair<std::string, std::string>, std::string> answer =
{
    {{"uint8", "uint8"}, "uint8"},
    {{"uint8", "uint16"}, "uint16"},
    {{"uint8", "uint32"}, "uint32"},
    {{"uint8", "uint64"}, "uint64"},
    {{"uint8", "uint256"}, "uint256"},
    {{"uint8", "int8"}, "int16"},
    {{"uint8", "int16"}, "int16"},
    {{"uint8", "int32"}, "int32"},
    {{"uint8", "int64"}, "int64"},
    {{"uint8", "int128"}, "int128"},
    {{"uint8", "int256"}, "int256"},
    {{"uint8", "float32"}, "float32"},
    {{"uint8", "float64"}, "float64"},
    {{"uint16", "uint8"}, "uint16"},
    {{"uint16", "uint16"}, "uint16"},
    {{"uint16", "uint32"}, "uint32"},
    {{"uint16", "uint64"}, "uint64"},
    {{"uint16", "uint256"}, "uint256"},
    {{"uint16", "int8"}, "int32"},
    {{"uint16", "int16"}, "int32"},
    {{"uint16", "int32"}, "int32"},
    {{"uint16", "int64"}, "int64"},
    {{"uint16", "int128"}, "int128"},
    {{"uint16", "int256"}, "int256"},
    {{"uint16", "float32"}, "float32"},
    {{"uint16", "float64"}, "float64"},
    {{"uint32", "uint8"}, "uint32"},
    {{"uint32", "uint16"}, "uint32"},
    {{"uint32", "uint32"}, "uint32"},
    {{"uint32", "uint64"}, "uint64"},
    {{"uint32", "uint256"}, "uint256"},
    {{"uint32", "int8"}, "int64"},
    {{"uint32", "int16"}, "int64"},
    {{"uint32", "int32"}, "int64"},
    {{"uint32", "int64"}, "int64"},
    {{"uint32", "int128"}, "int128"},
    {{"uint32", "int256"}, "int256"},
    {{"uint32", "float32"}, "float64"},
    {{"uint32", "float64"}, "float64"},
    {{"uint64", "uint8"}, "uint64"},
    {{"uint64", "uint16"}, "uint64"},
    {{"uint64", "uint32"}, "uint64"},
    {{"uint64", "uint64"}, "uint64"},
    {{"uint64", "uint256"}, "uint256"},
    {{"uint64", "int8"}, "int128"},
    {{"uint64", "int16"}, "int128"},
    {{"uint64", "int32"}, "int128"},
    {{"uint64", "int64"}, "int128"},
    {{"uint64", "int128"}, "int128"},
    {{"uint64", "int256"}, "int256"},
    {{"uint64", "float32"}, "Error"},
    {{"uint64", "float64"}, "Error"},
    {{"uint256", "uint8"}, "uint256"},
    {{"uint256", "uint16"}, "uint256"},
    {{"uint256", "uint32"}, "uint256"},
    {{"uint256", "uint64"}, "uint256"},
    {{"uint256", "uint256"}, "uint256"},
    {{"uint256", "int8"}, "Error"},
    {{"uint256", "int16"}, "Error"},
    {{"uint256", "int32"}, "Error"},
    {{"uint256", "int64"}, "Error"},
    {{"uint256", "int128"}, "Error"},
    {{"uint256", "int256"}, "Error"},
    {{"uint256", "float32"}, "Error"},
    {{"uint256", "float64"}, "Error"},
    {{"int8", "uint8"}, "int16"},
    {{"int8", "uint16"}, "int32"},
    {{"int8", "uint32"}, "int64"},
    {{"int8", "uint64"}, "int128"},
    {{"int8", "uint256"}, "Error"},
    {{"int8", "int8"}, "int8"},
    {{"int8", "int16"}, "int16"},
    {{"int8", "int32"}, "int32"},
    {{"int8", "int64"}, "int64"},
    {{"int8", "int128"}, "int128"},
    {{"int8", "int256"}, "int256"},
    {{"int8", "float32"}, "float32"},
    {{"int8", "float64"}, "float64"},
    {{"int16", "uint8"}, "int16"},
    {{"int16", "uint16"}, "int32"},
    {{"int16", "uint32"}, "int64"},
    {{"int16", "uint64"}, "int128"},
    {{"int16", "uint256"}, "Error"},
    {{"int16", "int8"}, "int16"},
    {{"int16", "int16"}, "int16"},
    {{"int16", "int32"}, "int32"},
    {{"int16", "int64"}, "int64"},
    {{"int16", "int128"}, "int128"},
    {{"int16", "int256"}, "int256"},
    {{"int16", "float32"}, "float32"},
    {{"int16", "float64"}, "float64"},
    {{"int32", "uint8"}, "int32"},
    {{"int32", "uint16"}, "int32"},
    {{"int32", "uint32"}, "int64"},
    {{"int32", "uint64"}, "int128"},
    {{"int32", "uint256"}, "Error"},
    {{"int32", "int8"}, "int32"},
    {{"int32", "int16"}, "int32"},
    {{"int32", "int32"}, "int32"},
    {{"int32", "int64"}, "int64"},
    {{"int32", "int128"}, "int128"},
    {{"int32", "int256"}, "int256"},
    {{"int32", "float32"}, "float64"},
    {{"int32", "float64"}, "float64"},
    {{"int64", "uint8"}, "int64"},
    {{"int64", "uint16"}, "int64"},
    {{"int64", "uint32"}, "int64"},
    {{"int64", "uint64"}, "int128"},
    {{"int64", "uint256"}, "Error"},
    {{"int64", "int8"}, "int64"},
    {{"int64", "int16"}, "int64"},
    {{"int64", "int32"}, "int64"},
    {{"int64", "int64"}, "int64"},
    {{"int64", "int128"}, "int128"},
    {{"int64", "int256"}, "int256"},
    {{"int64", "float32"}, "Error"},
    {{"int64", "float64"}, "Error"},
    {{"int128", "uint8"}, "int128"},
    {{"int128", "uint16"}, "int128"},
    {{"int128", "uint32"}, "int128"},
    {{"int128", "uint64"}, "int128"},
    {{"int128", "uint256"}, "Error"},
    {{"int128", "int8"}, "int128"},
    {{"int128", "int16"}, "int128"},
    {{"int128", "int32"}, "int128"},
    {{"int128", "int64"}, "int128"},
    {{"int128", "int128"}, "int128"},
    {{"int128", "int256"}, "int256"},
    {{"int128", "float32"}, "Error"},
    {{"int128", "float64"}, "Error"},
    {{"int256", "uint8"}, "int256"},
    {{"int256", "uint16"}, "int256"},
    {{"int256", "uint32"}, "int256"},
    {{"int256", "uint64"}, "int256"},
    {{"int256", "uint256"}, "Error"},
    {{"int256", "int8"}, "int256"},
    {{"int256", "int16"}, "int256"},
    {{"int256", "int32"}, "int256"},
    {{"int256", "int64"}, "int256"},
    {{"int256", "int128"}, "int256"},
    {{"int256", "int256"}, "int256"},
    {{"int256", "float32"}, "Error"},
    {{"int256", "float64"}, "Error"},
    {{"float32", "uint8"}, "float32"},
    {{"float32", "uint16"}, "float32"},
    {{"float32", "uint32"}, "float64"},
    {{"float32", "uint64"}, "Error"},
    {{"float32", "uint256"}, "Error"},
    {{"float32", "int8"}, "float32"},
    {{"float32", "int16"}, "float32"},
    {{"float32", "int32"}, "float64"},
    {{"float32", "int64"}, "Error"},
    {{"float32", "int128"}, "Error"},
    {{"float32", "int256"}, "Error"},
    {{"float32", "float32"}, "float32"},
    {{"float32", "float64"}, "float64"},
    {{"float64", "uint8"}, "float64"},
    {{"float64", "uint16"}, "float64"},
    {{"float64", "uint32"}, "float64"},
    {{"float64", "uint64"}, "Error"},
    {{"float64", "uint256"}, "Error"},
    {{"float64", "int8"}, "float64"},
    {{"float64", "int16"}, "float64"},
    {{"float64", "int32"}, "float64"},
    {{"float64", "int64"}, "Error"},
    {{"float64", "int128"}, "Error"},
    {{"float64", "int256"}, "Error"},
    {{"float64", "float32"}, "float64"},
    {{"float64", "float64"}, "float64"}
};

static std::string getTypeString(DB::UInt8) { return "uint8"; }
static std::string getTypeString(DB::UInt16) { return "uint16"; }
static std::string getTypeString(DB::UInt32) { return "uint32"; }
static std::string getTypeString(DB::UInt64) { return "uint64"; }
static std::string getTypeString(DB::UInt256) { return "uint256"; }
static std::string getTypeString(DB::Int8) { return "int8"; }
static std::string getTypeString(DB::Int16) { return "int16"; }
static std::string getTypeString(DB::Int32) { return "int32"; }
static std::string getTypeString(DB::Int64) { return "int64"; }
static std::string getTypeString(DB::Int128) { return "int128"; }
static std::string getTypeString(DB::Int256) { return "int256"; }
static std::string getTypeString(DB::Float32) { return "float32"; }
static std::string getTypeString(DB::Float64) { return "float64"; }
static std::string getTypeString(DB::NumberTraits::Error) { return "Error"; }

template <typename T0, typename T1>
[[maybe_unused]] void printTypes()
{
    std::cout << "{{\"";
    std::cout << getTypeString(T0());
    std::cout << "\", \"";
    std::cout << getTypeString(T1());
    std::cout << "\"}, \"";
    std::cout << getTypeString(typename DB::NumberTraits::ResultOfIf<T0, T1>::Type());
    std::cout << "\"},"<< std::endl;
}

template <typename T0, typename T1>
void ifRightType()
{
    auto desired = getTypeString(typename DB::NumberTraits::ResultOfIf<T0, T1>::Type());
    auto left = getTypeString(T0());
    auto right = getTypeString(T1());
    auto expected = answer.find({left, right});
    ASSERT_TRUE(expected != answer.end());
    ASSERT_EQ(expected->second, desired);
}

template <typename T0>
void ifLeftType()
{
    ifRightType<T0, DB::UInt8>();
    ifRightType<T0, DB::UInt16>();
    ifRightType<T0, DB::UInt32>();
    ifRightType<T0, DB::UInt64>();
    ifRightType<T0, DB::UInt256>();
    ifRightType<T0, DB::Int8>();
    ifRightType<T0, DB::Int16>();
    ifRightType<T0, DB::Int32>();
    ifRightType<T0, DB::Int64>();
    ifRightType<T0, DB::Int128>();
    ifRightType<T0, DB::Int256>();
    ifRightType<T0, DB::Float32>();
    ifRightType<T0, DB::Float64>();
}


TEST(NumberTraits, ResultOfAdditionMultiplication)
{
    ASSERT_EQ(getTypeString(DB::NumberTraits::ResultOfAdditionMultiplication<DB::UInt8, DB::UInt8>::Type()), "uint16");
    ASSERT_EQ(getTypeString(DB::NumberTraits::ResultOfAdditionMultiplication<DB::UInt8, DB::Int32>::Type()), "int64");
    ASSERT_EQ(getTypeString(DB::NumberTraits::ResultOfAdditionMultiplication<DB::UInt8, DB::Float32>::Type()), "float64");
}


TEST(NumberTraits, ResultOfSubtraction)
{
    ASSERT_EQ(getTypeString(DB::NumberTraits::ResultOfSubtraction<DB::UInt8, DB::UInt8>::Type()), "int16");
    ASSERT_EQ(getTypeString(DB::NumberTraits::ResultOfSubtraction<DB::UInt16, DB::UInt8>::Type()), "int32");
    ASSERT_EQ(getTypeString(DB::NumberTraits::ResultOfSubtraction<DB::UInt16, DB::Int8>::Type()), "int32");
}


TEST(NumberTraits, Others)
{
    ASSERT_EQ(getTypeString(DB::NumberTraits::ResultOfFloatingPointDivision<DB::UInt16, DB::Int16>::Type()), "float64");
    ASSERT_EQ(getTypeString(DB::NumberTraits::ResultOfFloatingPointDivision<DB::UInt32, DB::Int16>::Type()), "float64");
    ASSERT_EQ(getTypeString(DB::NumberTraits::ResultOfIntegerDivision<DB::UInt8, DB::Int16>::Type()), "int8");
    ASSERT_EQ(getTypeString(DB::NumberTraits::ResultOfModulo<DB::UInt32, DB::Int8>::Type()), "uint8");
}


TEST(NumberTraits, FunctionIf)
{
    ifLeftType<DB::UInt8>();
    ifLeftType<DB::UInt16>();
    ifLeftType<DB::UInt32>();
    ifLeftType<DB::UInt64>();
    ifLeftType<DB::UInt256>();
    ifLeftType<DB::Int8>();
    ifLeftType<DB::Int16>();
    ifLeftType<DB::Int32>();
    ifLeftType<DB::Int64>();
    ifLeftType<DB::Int128>();
    ifLeftType<DB::Int256>();
    ifLeftType<DB::Float32>();
    ifLeftType<DB::Float64>();
}

