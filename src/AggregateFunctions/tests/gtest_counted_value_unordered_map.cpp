#include <AggregateFunctions/Streaming/CountedValueHashMap.h>
#include <gtest/gtest.h>

using CountedValueHashMap = DB::Streaming::CountedValueHashMap<int>;

TEST(CountedValueHashMap, Merge)
{
    CountedValueHashMap map1;
    map1.emplace(1);
    map1.emplace(2);
    map1.emplace(1);
    EXPECT_EQ(map1.find(1)->second, 2);
    EXPECT_EQ(map1.find(2)->second, 1);
    EXPECT_EQ(map1.size(), 2);

    CountedValueHashMap map2;
    map2.emplace(1);
    map2.emplace(3);
    map2.emplace(3);
    EXPECT_EQ(map2.find(1)->second, 1);
    EXPECT_EQ(map2.find(3)->second, 2);

    /// test merge
    CountedValueHashMap::merge(map1, map2);
    EXPECT_EQ(map1.find(1)->second, 3);
    EXPECT_EQ(map1.find(2)->second, 1);
    EXPECT_EQ(map1.find(3)->second, 2);

    EXPECT_EQ(map1.size(), 3);
    EXPECT_EQ(map2.size(), 0);

    /// swap test
    map1.swap(map2);
    EXPECT_EQ(map1.size(), 0);
    EXPECT_EQ(map2.size(), 3);


    EXPECT_EQ(map2.contains(3), true);
}
