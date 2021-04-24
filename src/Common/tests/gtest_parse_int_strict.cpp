#include <Common/parseIntStrict.h>

#include <gtest/gtest.h>


TEST(ParseIntStrict, ParseInt)
{
    String s1{"1"};
    auto n = DB::parseIntStrict<Int32>(s1, 0, 1);
    EXPECT_EQ(n, 1);

    String s2{"1 2 3"};
    n = DB::parseIntStrict<Int32>(s2, 2, 3);
    EXPECT_EQ(n, 2);

    try
    {
        n = DB::parseIntStrict<Int32>(s2, 1, 2);
        EXPECT_TRUE(false);
    }
    catch (DB::Exception &)
    {
    }

    try
    {
        n = DB::parseIntStrict<Int32>(s2, 0, 2);
       EXPECT_TRUE(false);
    }
    catch (DB::Exception &)
    {
    }

    try
    {
        n = DB::parseIntStrict<Int32>(s2, 2, 2);
        EXPECT_TRUE(false);
    }
    catch (DB::Exception &)
    {
    }

    try
    {
        n = DB::parseIntStrict<Int32>(s2, 2, 1);
        EXPECT_TRUE(false);
    }
    catch (DB::Exception &)
    {
    }

    try
    {
        n = DB::parseIntStrict<Int32>(s2, 0, 100);
        EXPECT_TRUE(false);
    }
    catch (DB::Exception &)
    {
    }
}
