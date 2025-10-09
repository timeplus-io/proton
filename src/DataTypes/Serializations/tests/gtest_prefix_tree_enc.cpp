#include <IO/PrefixTreeEncode.h>

#include <gtest/gtest.h>

namespace
{
struct StringTestCase
{
    StringTestCase(std::string_view lhs_, std::string_view rhs_, bool expected_deepcopy_)
        : lhs(lhs_), rhs(rhs_), expected_deepcopy(expected_deepcopy_)
    {
    }

    std::string lhs;
    std::string rhs;
    bool expected_deepcopy;
};

template <typename T>
struct NumericTestCase
{
    NumericTestCase(T lhs_, T rhs_) : lhs(lhs_), rhs(rhs_) { }

    T lhs;
    T rhs;
};
}

TEST(PrefixTreeEncode, EncodeStringAscending)
{
    std::vector<StringTestCase> test_cases = {
        {"", "1", false},
        {"a", "b", false},
        {"ab", "ac", false},
        {"ab", "bb", false},
        {"a b", "a b1", false},
        {"a b\xff", "a1b\xff", false},
        {std::string_view{"a b\x00", 4}, std::string_view{"a1b\x00", 4}, true}, /// Have null char which is escape char
        {std::string_view{"a\x00 b\x00", 5}, std::string_view{"a\x00 b\x00 ", 6}, true}, /// Have null char which is escape char
    };

    for (const auto & c : test_cases)
    {
        ASSERT_TRUE(c.lhs < c.rhs);

        /// Encode
        std::string lhs_encoded;
        auto lhs_encoded_v = DB::PrefixTreeEncode::encodeStringAscending(c.lhs, lhs_encoded);

        std::string rhs_encoded;
        auto rhs_encoded_v = DB::PrefixTreeEncode::encodeStringAscending(c.rhs, rhs_encoded);

        /// After encoding, the less than invariant still holds
        EXPECT_TRUE(lhs_encoded < rhs_encoded);
        EXPECT_EQ(lhs_encoded_v, lhs_encoded);
        EXPECT_TRUE(lhs_encoded_v < rhs_encoded_v);
        EXPECT_EQ(rhs_encoded_v, rhs_encoded);

        /// Decode
        std::string lhs_decoded;
        auto lhs_decoded_v = DB::PrefixTreeEncode::decodeStringAscending(lhs_encoded_v, lhs_decoded);
        EXPECT_EQ(c.lhs, lhs_decoded_v);
        /// After finishing the decoding, the encode buffer shall be empty
        EXPECT_TRUE(lhs_encoded_v.empty());

        if (c.expected_deepcopy)
            EXPECT_EQ(lhs_decoded_v, lhs_decoded);
        else
            EXPECT_TRUE(lhs_decoded.empty());

        std::string rhs_decoded;
        auto rhs_decoded_v = DB::PrefixTreeEncode::decodeStringAscending(rhs_encoded_v, rhs_decoded);
        EXPECT_EQ(c.rhs, rhs_decoded_v);
        /// After finishing the decoding, the encode buffer shall be empty
        EXPECT_TRUE(rhs_encoded_v.empty());

        if (c.expected_deepcopy)
            EXPECT_EQ(rhs_decoded_v, rhs_decoded);
        else
            EXPECT_TRUE(rhs_decoded.empty());
    }
}

TEST(PrefixTreeEncode, EncodeStringDescending)
{
    std::vector<StringTestCase> test_cases = {
        {"", "1", true},
        {"a", "b", true},
        {"ab", "ac", true},
        {"ab", "bb", true},
        {"a b", "a b1", true},
        {"a b\xff", "a1b\xff", true},
        {std::string_view{"a b\x00", 4}, std::string_view{"a1b\x00", 4}, true}, /// Have null char which is escape char
        {std::string_view{"a\x00 b\x00", 5}, std::string_view{"a\x00 b\x00 ", 6}, true}, /// Have null char which is escape char
    };

    for (const auto & c : test_cases)
    {
        ASSERT_TRUE(c.lhs < c.rhs);

        /// Encode
        std::string lhs_encoded;
        auto lhs_encoded_v = DB::PrefixTreeEncode::encodeStringDescending(c.lhs, lhs_encoded);

        std::string rhs_encoded;
        auto rhs_encoded_v = DB::PrefixTreeEncode::encodeStringDescending(c.rhs, rhs_encoded);

        /// After encoding, the less than invariant shall flip
        EXPECT_TRUE(lhs_encoded > rhs_encoded);
        EXPECT_EQ(lhs_encoded_v, lhs_encoded);
        EXPECT_TRUE(lhs_encoded_v > rhs_encoded_v);
        EXPECT_EQ(rhs_encoded_v, rhs_encoded);

        /// Decode
        std::string lhs_decoded;
        auto lhs_decoded_v = DB::PrefixTreeEncode::decodeStringDescending(lhs_encoded_v, lhs_decoded);
        EXPECT_EQ(c.lhs, lhs_decoded_v);
        EXPECT_EQ(lhs_decoded_v, lhs_decoded);
        /// After finishing the decoding, the encode buffer shall be empty
        EXPECT_TRUE(lhs_encoded_v.empty());

        std::string rhs_decoded;
        auto rhs_decoded_v = DB::PrefixTreeEncode::decodeStringDescending(rhs_encoded_v, rhs_decoded);
        EXPECT_EQ(c.rhs, rhs_decoded_v);
        EXPECT_EQ(rhs_decoded_v, rhs_decoded);
        /// After finishing the decoding, the encode buffer shall be empty
        EXPECT_TRUE(rhs_encoded_v.empty());
    }
}

TEST(PrefixTreeEncode, EncodeStringAscendingMultiple)
{
    std::vector<StringTestCase> test_cases = {
        {"", "1", false},
        {"a", "b", false},
        {"ab", "ac", false},
        {"ab", "bb", false},
        {"a b", "a b1", false},
        {"a b\xff", "a1b\xff", false},
        {std::string_view{"a b\x00", 4}, std::string_view{"a1b\x00", 4}, true}, /// Have null char which is escape char
        {std::string_view{"a\x00 b\x00", 5}, std::string_view{"a\x00 b\x00 ", 6}, true}, /// Have null char which is escape char
    };

    for (const auto & c : test_cases)
    {
        ASSERT_TRUE(c.lhs < c.rhs);

        /// Encode
        std::string encoded;
        auto lhs_encoded_v = DB::PrefixTreeEncode::encodeStringAscending(c.lhs, encoded);
        auto rhs_encoded_v = DB::PrefixTreeEncode::encodeStringAscending(c.rhs, encoded);

        /// After encoding, the less than invariant shall still hold
        EXPECT_TRUE(lhs_encoded_v < rhs_encoded_v);

        /// Decode
        std::string_view encoded_v{encoded};

        std::string lhs_decoded;
        auto lhs_decoded_v = DB::PrefixTreeEncode::decodeStringAscending(encoded_v, lhs_decoded);
        EXPECT_EQ(c.lhs, lhs_decoded_v);
        if (c.expected_deepcopy)
            EXPECT_EQ(lhs_decoded_v, lhs_decoded);
        else
            EXPECT_TRUE(lhs_decoded.empty());

        std::string rhs_decoded;
        auto rhs_decoded_v = DB::PrefixTreeEncode::decodeStringAscending(encoded_v, rhs_decoded);
        EXPECT_EQ(c.rhs, rhs_decoded_v);

        /// After finishing the decoding, the encode buffer shall be empty
        EXPECT_TRUE(encoded_v.empty());

        if (c.expected_deepcopy)
            EXPECT_EQ(rhs_decoded_v, rhs_decoded);
        else
            EXPECT_TRUE(rhs_decoded.empty());
    }
}

TEST(PrefixTreeEncode, EncodeStringDescendingMultiple)
{
    std::vector<StringTestCase> test_cases = {
        {"", "1", true},
        {"a", "b", true},
        {"ab", "ac", true},
        {"ab", "bb", true},
        {"a b", "a b1", true},
        {"a b\xff", "a1b\xff", true},
        {std::string_view{"a b\x00", 4}, std::string_view{"a1b\x00", 4}, true}, /// Have null char which is escape char
        {std::string_view{"a\x00 b\x00", 5}, std::string_view{"a\x00 b\x00 ", 6}, true}, /// Have null char which is escape char
    };

    for (const auto & c : test_cases)
    {
        ASSERT_TRUE(c.lhs < c.rhs);

        /// Encode
        std::string encoded;
        auto lhs_encoded_v = DB::PrefixTreeEncode::encodeStringDescending(c.lhs, encoded);
        auto rhs_encoded_v = DB::PrefixTreeEncode::encodeStringDescending(c.rhs, encoded);

        /// After encoding, the less than invariant shall flip
        EXPECT_TRUE(lhs_encoded_v > rhs_encoded_v);

        /// Decode
        std::string_view encoded_v{encoded};

        std::string lhs_decoded;
        auto lhs_decoded_v = DB::PrefixTreeEncode::decodeStringDescending(encoded_v, lhs_decoded);
        EXPECT_EQ(c.lhs, lhs_decoded_v);
        EXPECT_EQ(lhs_decoded_v, lhs_decoded);

        std::string rhs_decoded;
        auto rhs_decoded_v = DB::PrefixTreeEncode::decodeStringDescending(encoded_v, rhs_decoded);
        EXPECT_EQ(c.rhs, rhs_decoded_v);
        EXPECT_EQ(rhs_decoded_v, rhs_decoded);

        /// After finishing the decoding, the encode buffer shall be empty
        EXPECT_TRUE(encoded_v.empty());
    }
}

namespace
{
auto signedIntegerTestCases()
{
    return std::vector<NumericTestCase<int64_t>>{
        {std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::min() + 1},
        {std::numeric_limits<int64_t>::max() - 1, std::numeric_limits<int64_t>::max()},
        {-1, 0},
        {0, 1},
        {1, 2},
        {-0xffll, -0xffll + 1},
        {-0xffffll, -0xffffll + 1},
        {-0xff'ffffll, -0xff'ffffll + 1},
        {-0xffff'ffffll, -0xffff'ffffll + 1},
        {-0xff'ffff'ffffll, -0xff'ffff'ffffll + 1},
        {-0xffff'ffff'ffffll, -0xffff'ffff'ffffll + 1},
        {-0xff'ffff'ffff'ffffll, -0xff'ffff'ffff'ffffll + 1},
        {-0xfff'ffff'ffff'ffffll, -0xfff'ffff'ffff'ffffll + 1},
        {0xffll, 0xffll + 1},
        {0xffffll, 0xffffll + 1},
        {0xff'ffffll, 0xff'ffffll + 1},
        {0xffff'ffffll, 0xffff'ffffll + 1},
        {0xff'ffff'ffffll, 0xff'ffff'ffffll + 1},
        {0xffff'ffff'ffffll, 0xffff'ffff'ffffll + 1},
        {0xff'ffff'ffff'ffffll, 0xff'ffff'ffff'ffffll + 1},
        {0xfff'ffff'ffff'fffell, 0xfff'ffff'ffff'fffell + 1},
    };
}

}

TEST(PrefixTreeEncode, EncodeIntAscending)
{
    for (const auto & c : signedIntegerTestCases())
    {
        ASSERT_TRUE(c.lhs < c.rhs);

        /// Encode
        std::string lhs_encoded;
        auto lhs_encoded_v = DB::PrefixTreeEncode::encodeVarIntAscending(c.lhs, lhs_encoded);

        std::string rhs_encoded;
        auto rhs_encoded_v = DB::PrefixTreeEncode::encodeVarIntAscending(c.rhs, rhs_encoded);

        /// After encoding, the less than invariant still holds
        EXPECT_TRUE(lhs_encoded < rhs_encoded);
        EXPECT_EQ(lhs_encoded_v, lhs_encoded);
        EXPECT_TRUE(lhs_encoded_v < rhs_encoded_v);
        EXPECT_EQ(rhs_encoded_v, rhs_encoded);

        /// Decode
        auto lhs_decoded_v = DB::PrefixTreeEncode::decodeVarIntAscending(lhs_encoded_v);
        EXPECT_EQ(c.lhs, lhs_decoded_v);

        /// After finishing the decoding, the encode buffer shall be empty
        EXPECT_TRUE(lhs_encoded_v.empty());

        auto rhs_decoded_v = DB::PrefixTreeEncode::decodeVarIntAscending(rhs_encoded_v);
        EXPECT_EQ(c.rhs, rhs_decoded_v);

        /// After finishing the decoding, the encode buffer shall be empty
        EXPECT_TRUE(rhs_encoded_v.empty());
    }
}

TEST(PrefixTreeEncode, EncodeIntAscendingMultiple)
{
    for (const auto & c : signedIntegerTestCases())
    {
        ASSERT_TRUE(c.lhs < c.rhs);

        /// Encode
        std::string encoded;
        auto lhs_encoded_v = DB::PrefixTreeEncode::encodeVarIntAscending(c.lhs, encoded);
        auto rhs_encoded_v = DB::PrefixTreeEncode::encodeVarIntAscending(c.rhs, encoded);

        /// After encoding, the less than invariant still holds
        EXPECT_TRUE(lhs_encoded_v < rhs_encoded_v);

        /// Decode
        std::string_view encoded_v{encoded};
        auto lhs_decoded_v = DB::PrefixTreeEncode::decodeVarIntAscending(encoded_v);
        EXPECT_EQ(c.lhs, lhs_decoded_v);

        auto rhs_decoded_v = DB::PrefixTreeEncode::decodeVarIntAscending(encoded_v);
        EXPECT_EQ(c.rhs, rhs_decoded_v);

        /// After finishing the decoding, the encode buffer shall be empty
        EXPECT_TRUE(encoded_v.empty());
    }
}

TEST(PrefixTreeEncode, EncodeIntDescending)
{
    for (const auto & c : signedIntegerTestCases())
    {
        ASSERT_TRUE(c.lhs < c.rhs);

        /// Encode
        std::string lhs_encoded;
        auto lhs_encoded_v = DB::PrefixTreeEncode::encodeVarIntDescending(c.lhs, lhs_encoded);

        std::string rhs_encoded;
        auto rhs_encoded_v = DB::PrefixTreeEncode::encodeVarIntDescending(c.rhs, rhs_encoded);

        /// After encoding, the less than invariant still holds
        EXPECT_TRUE(lhs_encoded > rhs_encoded);
        EXPECT_EQ(lhs_encoded_v, lhs_encoded);
        EXPECT_TRUE(lhs_encoded_v > rhs_encoded_v);
        EXPECT_EQ(rhs_encoded_v, rhs_encoded);

        /// Decode
        auto lhs_decoded_v = DB::PrefixTreeEncode::decodeVarIntDescending(lhs_encoded_v);
        EXPECT_EQ(c.lhs, lhs_decoded_v);

        /// After finishing the decoding, the encode buffer shall be empty
        EXPECT_TRUE(lhs_encoded_v.empty());

        auto rhs_decoded_v = DB::PrefixTreeEncode::decodeVarIntDescending(rhs_encoded_v);
        EXPECT_EQ(c.rhs, rhs_decoded_v);

        /// After finishing the decoding, the encode buffer shall be empty
        EXPECT_TRUE(rhs_encoded_v.empty());
    }
}

TEST(PrefixTreeEncode, EncodeIntDescendingMultiple)
{
    for (const auto & c : signedIntegerTestCases())
    {
        ASSERT_TRUE(c.lhs < c.rhs);

        /// Encode
        std::string encoded;
        encoded.reserve(1024); /// reserve to avoid returned view invalidation
        auto lhs_encoded_v = DB::PrefixTreeEncode::encodeVarIntDescending(c.lhs, encoded);
        auto rhs_encoded_v = DB::PrefixTreeEncode::encodeVarIntDescending(c.rhs, encoded);

        /// After encoding, the less than invariant still holds
        EXPECT_TRUE(lhs_encoded_v > rhs_encoded_v);

        /// Decode
        std::string_view encoded_v{encoded};
        auto lhs_decoded_v = DB::PrefixTreeEncode::decodeVarIntDescending(encoded_v);
        EXPECT_EQ(c.lhs, lhs_decoded_v);

        auto rhs_decoded_v = DB::PrefixTreeEncode::decodeVarIntDescending(encoded_v);
        EXPECT_EQ(c.rhs, rhs_decoded_v);

        /// After finishing the decoding, the encode buffer shall be empty
        EXPECT_TRUE(encoded_v.empty());
    }
}

namespace
{
auto unsignedIntegerTestCases()
{
    return std::vector<NumericTestCase<uint64_t>>{
        {std::numeric_limits<uint64_t>::min(), std::numeric_limits<uint64_t>::min() + 1},
        {std::numeric_limits<uint64_t>::max() - 1, std::numeric_limits<uint64_t>::max()},
        {0, 1},
        {1, 2},
        {0xffull, 0xffull + 1},
        {0xffffull, 0xffffull + 1},
        {0xff'ffffull, 0xff'ffffull + 1},
        {0xffff'ffffull, 0xffff'ffffull + 1},
        {0xff'ffff'ffffull, 0xff'ffff'ffffull + 1},
        {0xffff'ffff'ffffull, 0xffff'ffff'ffffull + 1},
        {0xff'ffff'ffff'ffffull, 0xff'ffff'ffff'ffffull + 1},
        {0xfff'ffff'ffff'fffeull, 0xfff'ffff'ffff'fffeull + 1},
    };
}
}

TEST(PrefixTreeEncode, EncodeUIntAscending)
{
    for (const auto & c : unsignedIntegerTestCases())
    {
        ASSERT_TRUE(c.lhs < c.rhs);

        /// Encode
        std::string lhs_encoded;
        auto lhs_encoded_v = DB::PrefixTreeEncode::encodeVarUIntAscending(c.lhs, lhs_encoded);

        std::string rhs_encoded;
        auto rhs_encoded_v = DB::PrefixTreeEncode::encodeVarUIntAscending(c.rhs, rhs_encoded);

        /// After encoding, the less than invariant still holds
        EXPECT_TRUE(lhs_encoded < rhs_encoded);
        EXPECT_EQ(lhs_encoded_v, lhs_encoded);
        EXPECT_TRUE(lhs_encoded_v < rhs_encoded_v);
        EXPECT_EQ(rhs_encoded_v, rhs_encoded);

        /// Decode
        auto lhs_decoded_v = DB::PrefixTreeEncode::decodeVarUIntAscending(lhs_encoded_v);
        EXPECT_EQ(c.lhs, lhs_decoded_v);

        /// After finishing the decoding, the encode buffer shall be empty
        EXPECT_TRUE(lhs_encoded_v.empty());

        auto rhs_decoded_v = DB::PrefixTreeEncode::decodeVarUIntAscending(rhs_encoded_v);
        EXPECT_EQ(c.rhs, rhs_decoded_v);

        /// After finishing the decoding, the encode buffer shall be empty
        EXPECT_TRUE(rhs_encoded_v.empty());
    }
}

TEST(PrefixTreeEncode, EncodeUIntDescending)
{
    for (const auto & c : unsignedIntegerTestCases())
    {
        ASSERT_TRUE(c.lhs < c.rhs);

        /// Encode
        std::string lhs_encoded;
        auto lhs_encoded_v = DB::PrefixTreeEncode::encodeVarUIntDescending(c.lhs, lhs_encoded);

        std::string rhs_encoded;
        auto rhs_encoded_v = DB::PrefixTreeEncode::encodeVarUIntDescending(c.rhs, rhs_encoded);

        /// After encoding, the less than invariant shall flip
        EXPECT_TRUE(lhs_encoded > rhs_encoded);
        EXPECT_EQ(lhs_encoded_v, lhs_encoded);
        EXPECT_TRUE(lhs_encoded_v > rhs_encoded_v);
        EXPECT_EQ(rhs_encoded_v, rhs_encoded);

        /// Decode
        auto lhs_decoded_v = DB::PrefixTreeEncode::decodeVarUIntDescending(lhs_encoded_v);
        EXPECT_EQ(c.lhs, lhs_decoded_v);

        /// After finishing the decoding, the encode buffer shall be empty
        EXPECT_TRUE(lhs_encoded_v.empty());

        auto rhs_decoded_v = DB::PrefixTreeEncode::decodeVarUIntDescending(rhs_encoded_v);
        EXPECT_EQ(c.rhs, rhs_decoded_v);

        /// After finishing the decoding, the encode buffer shall be empty
        EXPECT_TRUE(rhs_encoded_v.empty());
    }
}

TEST(PrefixTreeEncode, EncodeUIntDescendingMultiple)
{
    for (const auto & c : unsignedIntegerTestCases())
    {
        ASSERT_TRUE(c.lhs < c.rhs);

        /// Encode
        std::string encoded;
        encoded.reserve(1024); /// reserve to avoid returned view invalidation
        auto lhs_encoded_v = DB::PrefixTreeEncode::encodeVarUIntDescending(c.lhs, encoded);
        auto rhs_encoded_v = DB::PrefixTreeEncode::encodeVarUIntDescending(c.rhs, encoded);

        /// After encoding, the less than invariant shall flip
        EXPECT_TRUE(lhs_encoded_v > rhs_encoded_v);

        /// Decode
        std::string_view encoded_v{encoded};
        auto lhs_decoded_v = DB::PrefixTreeEncode::decodeVarUIntDescending(encoded_v);
        EXPECT_EQ(c.lhs, lhs_decoded_v);

        auto rhs_decoded_v = DB::PrefixTreeEncode::decodeVarUIntDescending(encoded_v);
        EXPECT_EQ(c.rhs, rhs_decoded_v);

        /// After finishing the decoding, the encode buffer shall be empty
        EXPECT_TRUE(encoded_v.empty());
    }
}

TEST(PrefixTreeEncode, PrefixEnd)
{
    {
        std::string encoded;
        DB::PrefixTreeEncode::encodeVarUIntAscending(0, encoded);
        std::string prefix = encoded;
        DB::PrefixTreeEncode::keyPrefixEnd(prefix);
        EXPECT_LT(encoded, prefix);

        std::string encoded_exact;
        DB::PrefixTreeEncode::encodeVarUIntAscending(1, encoded_exact);
        EXPECT_EQ(prefix, encoded_exact);

        std::string encoded_beyond;
        DB::PrefixTreeEncode::encodeVarUIntAscending(2, encoded_beyond);
        EXPECT_LT(prefix, encoded_beyond);
    }

    {
        std::string encoded;
        DB::PrefixTreeEncode::encodeVarUIntAscending(13, encoded);
        std::string prefix = encoded;
        DB::PrefixTreeEncode::keyPrefixEnd(prefix);
        EXPECT_LT(encoded, prefix);

        std::string encoded_exact;
        DB::PrefixTreeEncode::encodeVarUIntAscending(14, encoded_exact);
        EXPECT_EQ(prefix, encoded_exact);

        std::string encoded_beyond;
        DB::PrefixTreeEncode::encodeVarUIntAscending(15, encoded_beyond);
        EXPECT_LT(prefix, encoded_beyond);
    }
}

namespace
{
auto uint64TestCases()
{
    return std::vector<NumericTestCase<uint64_t>>{
        {std::numeric_limits<uint64_t>::min(), std::numeric_limits<uint64_t>::min() + 1},
        {std::numeric_limits<uint64_t>::max() - 1, std::numeric_limits<uint64_t>::max()},
        {0, 1},
        {1, 2},
        {1001, 10432},
    };
}

}

TEST(PrefixTreeEncode, EncodeUInt64Asecending)
{
    for (const auto & c : uint64TestCases())
    {
        ASSERT_TRUE(c.lhs < c.rhs);

        /// Encode
        std::string lhs_encoded;
        auto lhs_encoded_v = DB::PrefixTreeEncode::encodeUInt64Ascending(c.lhs, lhs_encoded);

        std::string rhs_encoded;
        auto rhs_encoded_v = DB::PrefixTreeEncode::encodeUInt64Ascending(c.rhs, rhs_encoded);

        /// After encoding, the less than invariant still holds
        EXPECT_TRUE(lhs_encoded < rhs_encoded);
        EXPECT_EQ(lhs_encoded_v, lhs_encoded);
        EXPECT_TRUE(lhs_encoded_v < rhs_encoded_v);
        EXPECT_EQ(rhs_encoded_v, rhs_encoded);

        /// Decode
        auto lhs_decoded_v = DB::PrefixTreeEncode::decodeUInt64Ascending(lhs_encoded_v);
        EXPECT_EQ(c.lhs, lhs_decoded_v);

        /// After finishing the decoding, the encode buffer shall be empty
        EXPECT_TRUE(lhs_encoded_v.empty());

        auto rhs_decoded_v = DB::PrefixTreeEncode::decodeUInt64Ascending(rhs_encoded_v);
        EXPECT_EQ(c.rhs, rhs_decoded_v);

        /// After finishing the decoding, the encode buffer shall be empty
        EXPECT_TRUE(rhs_encoded_v.empty());
    }
}

TEST(PrefixTreeEncode, EncodeUInt64Descending)
{
    for (const auto & c : uint64TestCases())
    {
        ASSERT_TRUE(c.lhs < c.rhs);

        /// Encode
        std::string lhs_encoded;
        auto lhs_encoded_v = DB::PrefixTreeEncode::encodeUInt64Descending(c.lhs, lhs_encoded);

        std::string rhs_encoded;
        auto rhs_encoded_v = DB::PrefixTreeEncode::encodeUInt64Descending(c.rhs, rhs_encoded);

        /// After encoding, the less than invariant shall flip
        EXPECT_TRUE(lhs_encoded > rhs_encoded);
        EXPECT_EQ(lhs_encoded_v, lhs_encoded);
        EXPECT_TRUE(lhs_encoded_v > rhs_encoded_v);
        EXPECT_EQ(rhs_encoded_v, rhs_encoded);

        /// Decode
        auto lhs_decoded_v = DB::PrefixTreeEncode::decodeUInt64Descending(lhs_encoded_v);
        EXPECT_EQ(c.lhs, lhs_decoded_v);

        /// After finishing the decoding, the encode buffer shall be empty
        EXPECT_TRUE(lhs_encoded_v.empty());

        auto rhs_decoded_v = DB::PrefixTreeEncode::decodeUInt64Descending(rhs_encoded_v);
        EXPECT_EQ(c.rhs, rhs_decoded_v);

        /// After finishing the decoding, the encode buffer shall be empty
        EXPECT_TRUE(rhs_encoded_v.empty());
    }
}

namespace
{
auto doubleTestCases()
{
    return std::vector<NumericTestCase<double>>{
        /// FIXME, NAN cases
        {std::numeric_limits<double>::min(), std::numeric_limits<double>::min() + static_cast<double>(1)},
        /// {std::numeric_limits<double>::max() - static_cast<double>(1), std::numeric_limits<double>::max()}, /// don't know why, ASSERT_LT failed
        {-1.0, 0.0},
        {0.0, 1.0},
        {1.0, 2.0},
        {-10432.432425, -1001.1235},
        {-1001.1235, 10432.432425},
        {1001.1235, 10432.432425}};
}

TEST(PrefixTreeEncode, EncodeDoubleAscending)
{
    for (const auto & c : doubleTestCases())
    {
        ASSERT_LT(c.lhs, c.rhs);

        /// Encode
        std::string lhs_encoded;
        auto lhs_encoded_v = DB::PrefixTreeEncode::encodeDoubleAscending(c.lhs, lhs_encoded);

        std::string rhs_encoded;
        auto rhs_encoded_v = DB::PrefixTreeEncode::encodeDoubleAscending(c.rhs, rhs_encoded);

        /// After encoding, the less than invariant still holds
        EXPECT_LT(lhs_encoded, rhs_encoded);
        EXPECT_EQ(lhs_encoded_v, lhs_encoded);
        EXPECT_LT(lhs_encoded_v, rhs_encoded_v);
        EXPECT_EQ(rhs_encoded_v, rhs_encoded);

        /// Decode
        auto lhs_decoded_v = DB::PrefixTreeEncode::decodeDoubleAscending(lhs_encoded_v);
        EXPECT_EQ(c.lhs, lhs_decoded_v);

        /// After finishing the decoding, the encode buffer shall be empty
        EXPECT_TRUE(lhs_encoded_v.empty());

        auto rhs_decoded_v = DB::PrefixTreeEncode::decodeDoubleAscending(rhs_encoded_v);
        EXPECT_EQ(c.rhs, rhs_decoded_v);

        /// After finishing the decoding, the encode buffer shall be empty
        EXPECT_TRUE(rhs_encoded_v.empty());
    }
}

TEST(PrefixTreeEncode, EncodeDoubleDescending)
{
    for (const auto & c : doubleTestCases())
    {
        ASSERT_TRUE(c.lhs < c.rhs);

        /// Encode
        std::string lhs_encoded;
        auto lhs_encoded_v = DB::PrefixTreeEncode::encodeDoubleDescending(c.lhs, lhs_encoded);

        std::string rhs_encoded;
        auto rhs_encoded_v = DB::PrefixTreeEncode::encodeDoubleDescending(c.rhs, rhs_encoded);

        /// After encoding, the less than invariant shall flip
        EXPECT_GT(lhs_encoded, rhs_encoded);
        EXPECT_EQ(lhs_encoded_v, lhs_encoded);
        EXPECT_GT(lhs_encoded_v, rhs_encoded_v);
        EXPECT_EQ(rhs_encoded_v, rhs_encoded);

        /// Decode
        auto lhs_decoded_v = DB::PrefixTreeEncode::decodeDoubleDescending(lhs_encoded_v);
        EXPECT_EQ(c.lhs, lhs_decoded_v);

        /// After finishing the decoding, the encode buffer shall be empty
        EXPECT_TRUE(lhs_encoded_v.empty());

        auto rhs_decoded_v = DB::PrefixTreeEncode::decodeDoubleDescending(rhs_encoded_v);
        EXPECT_EQ(c.rhs, rhs_decoded_v);

        /// After finishing the decoding, the encode buffer shall be empty
        EXPECT_TRUE(rhs_encoded_v.empty());
    }
}


namespace
{
auto uint32TestCases()
{
    return std::vector<NumericTestCase<uint32_t>>{
        {std::numeric_limits<uint32_t>::min(), std::numeric_limits<uint32_t>::min() + 1},
        {std::numeric_limits<uint32_t>::max() - 1, std::numeric_limits<uint32_t>::max()},
        {0, 1},
        {1, 2},
        {1001, 10432},
    };
}

}

TEST(PrefixTreeEncode, EncodeUInt32Asecending)
{
    for (const auto & c : uint32TestCases())
    {
        ASSERT_TRUE(c.lhs < c.rhs);

        /// Encode
        std::string lhs_encoded;
        auto lhs_encoded_v = DB::PrefixTreeEncode::encodeUInt32Ascending(c.lhs, lhs_encoded);

        std::string rhs_encoded;
        auto rhs_encoded_v = DB::PrefixTreeEncode::encodeUInt32Ascending(c.rhs, rhs_encoded);

        /// After encoding, the less than invariant still holds
        EXPECT_TRUE(lhs_encoded < rhs_encoded);
        EXPECT_EQ(lhs_encoded_v, lhs_encoded);
        EXPECT_TRUE(lhs_encoded_v < rhs_encoded_v);
        EXPECT_EQ(rhs_encoded_v, rhs_encoded);

        /// Decode
        auto lhs_decoded_v = DB::PrefixTreeEncode::decodeUInt32Ascending(lhs_encoded_v);
        EXPECT_EQ(c.lhs, lhs_decoded_v);

        /// After finishing the decoding, the encode buffer shall be empty
        EXPECT_TRUE(lhs_encoded_v.empty());

        auto rhs_decoded_v = DB::PrefixTreeEncode::decodeUInt32Ascending(rhs_encoded_v);
        EXPECT_EQ(c.rhs, rhs_decoded_v);

        /// After finishing the decoding, the encode buffer shall be empty
        EXPECT_TRUE(rhs_encoded_v.empty());
    }
}

TEST(PrefixTreeEncode, EncodeUInt32Descending)
{
    for (const auto & c : uint32TestCases())
    {
        ASSERT_TRUE(c.lhs < c.rhs);

        /// Encode
        std::string lhs_encoded;
        auto lhs_encoded_v = DB::PrefixTreeEncode::encodeUInt32Descending(c.lhs, lhs_encoded);

        std::string rhs_encoded;
        auto rhs_encoded_v = DB::PrefixTreeEncode::encodeUInt32Descending(c.rhs, rhs_encoded);

        /// After encoding, the less than invariant shall flip
        EXPECT_TRUE(lhs_encoded > rhs_encoded);
        EXPECT_EQ(lhs_encoded_v, lhs_encoded);
        EXPECT_TRUE(lhs_encoded_v > rhs_encoded_v);
        EXPECT_EQ(rhs_encoded_v, rhs_encoded);

        /// Decode
        auto lhs_decoded_v = DB::PrefixTreeEncode::decodeUInt32Descending(lhs_encoded_v);
        EXPECT_EQ(c.lhs, lhs_decoded_v);

        /// After finishing the decoding, the encode buffer shall be empty
        EXPECT_TRUE(lhs_encoded_v.empty());

        auto rhs_decoded_v = DB::PrefixTreeEncode::decodeUInt32Descending(rhs_encoded_v);
        EXPECT_EQ(c.rhs, rhs_decoded_v);

        /// After finishing the decoding, the encode buffer shall be empty
        EXPECT_TRUE(rhs_encoded_v.empty());
    }
}

namespace
{
auto floatTestCases()
{
    return std::vector<NumericTestCase<float>>{
        /// FIXME, NAN cases
        {std::numeric_limits<float>::min(), std::numeric_limits<float>::min() + static_cast<double>(1)},
        /// {std::numeric_limits<float>::max() - 1, std::numeric_limits<float>::max()}, /// don't know why, ASSERT_LT failed
        {-1.0, 0.0},
        {0.0, 1.0},
        {1.0, 2.0},
        {-10432.432425f, -1001.1235f},
        {-1001.1235f, 10432.432425f},
        {1001.1235f, 10432.432425f}};
}
}

TEST(PrefixTreeEncode, EncodeFloatAscending)
{
    for (const auto & c : floatTestCases())
    {
        ASSERT_LT(c.lhs, c.rhs);

        /// Encode
        std::string lhs_encoded;
        auto lhs_encoded_v = DB::PrefixTreeEncode::encodeFloatAscending(c.lhs, lhs_encoded);

        std::string rhs_encoded;
        auto rhs_encoded_v = DB::PrefixTreeEncode::encodeFloatAscending(c.rhs, rhs_encoded);

        /// After encoding, the less than invariant still holds
        EXPECT_LT(lhs_encoded, rhs_encoded);
        EXPECT_EQ(lhs_encoded_v, lhs_encoded);
        EXPECT_LT(lhs_encoded_v, rhs_encoded_v);
        EXPECT_EQ(rhs_encoded_v, rhs_encoded);

        /// Decode
        auto lhs_decoded_v = DB::PrefixTreeEncode::decodeFloatAscending(lhs_encoded_v);
        EXPECT_EQ(c.lhs, lhs_decoded_v);

        /// After finishing the decoding, the encode buffer shall be empty
        EXPECT_TRUE(lhs_encoded_v.empty());

        auto rhs_decoded_v = DB::PrefixTreeEncode::decodeFloatAscending(rhs_encoded_v);
        EXPECT_EQ(c.rhs, rhs_decoded_v);

        /// After finishing the decoding, the encode buffer shall be empty
        EXPECT_TRUE(rhs_encoded_v.empty());
    }
}

TEST(PrefixTreeEncode, EncodeFloatDescending)
{
    for (const auto & c : floatTestCases())
    {
        ASSERT_TRUE(c.lhs < c.rhs);

        /// Encode
        std::string lhs_encoded;
        auto lhs_encoded_v = DB::PrefixTreeEncode::encodeFloatDescending(c.lhs, lhs_encoded);

        std::string rhs_encoded;
        auto rhs_encoded_v = DB::PrefixTreeEncode::encodeFloatDescending(c.rhs, rhs_encoded);

        /// After encoding, the less than invariant shall flip
        EXPECT_GT(lhs_encoded, rhs_encoded);
        EXPECT_EQ(lhs_encoded_v, lhs_encoded);
        EXPECT_GT(lhs_encoded_v, rhs_encoded_v);
        EXPECT_EQ(rhs_encoded_v, rhs_encoded);

        /// Decode
        auto lhs_decoded_v = DB::PrefixTreeEncode::decodeFloatDescending(lhs_encoded_v);
        EXPECT_EQ(c.lhs, lhs_decoded_v);

        /// After finishing the decoding, the encode buffer shall be empty
        EXPECT_TRUE(lhs_encoded_v.empty());

        auto rhs_decoded_v = DB::PrefixTreeEncode::decodeFloatDescending(rhs_encoded_v);
        EXPECT_EQ(c.rhs, rhs_decoded_v);

        /// After finishing the decoding, the encode buffer shall be empty
        EXPECT_TRUE(rhs_encoded_v.empty());
    }
}

}
