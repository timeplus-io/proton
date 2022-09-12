
#include <AggregateFunctions/Streaming/CountedValueMap.h>

#include <gtest/gtest.h>

TEST(CountedValueMap, MaxOperationsOne)
{
    DB::Streaming::CountedValueMap<uint64_t, true> m(1);

    uint64_t v = 0;
    auto res = m.firstValue(v);
    EXPECT_FALSE(res);
    EXPECT_TRUE(m.empty());

    res = m.insert(1);
    EXPECT_TRUE(res);
    res = m.firstValue(v);
    EXPECT_TRUE(res);
    EXPECT_EQ(v, 1);
    res = m.lastValue(v);
    EXPECT_TRUE(res);
    EXPECT_EQ(v, 1);
    EXPECT_FALSE(m.empty());
    EXPECT_EQ(m.size(), 1);

    res = m.insert(2);
    EXPECT_TRUE(res);
    res = m.firstValue(v);
    EXPECT_TRUE(res);
    EXPECT_EQ(v, 2);
    res = m.lastValue(v);
    EXPECT_TRUE(res);
    EXPECT_EQ(v, 2);
    EXPECT_FALSE(m.empty());
    EXPECT_EQ(m.size(), 1);

    /// Insert 2 again
    res = m.insert(2);
    EXPECT_TRUE(res);
    res = m.firstValue(v);
    EXPECT_TRUE(res);
    EXPECT_EQ(v, 2);
    res = m.lastValue(v);
    EXPECT_TRUE(res);
    EXPECT_EQ(v, 2);
    EXPECT_FALSE(m.empty());
    EXPECT_EQ(m.size(), 1);

    /// Insert 1 again, will ignore it
    res = m.insert(1);
    EXPECT_FALSE(res);
    res = m.firstValue(v);
    EXPECT_TRUE(res);
    EXPECT_EQ(v, 2);
    res = m.lastValue(v);
    EXPECT_TRUE(res);
    EXPECT_EQ(v, 2);
    EXPECT_FALSE(m.empty());
    EXPECT_EQ(m.size(), 1);

    res = m.erase(1);
    EXPECT_FALSE(res);

    res = m.erase(2);
    EXPECT_TRUE(res);
    res = m.firstValue(v);
    EXPECT_TRUE(res);
    EXPECT_EQ(v, 2);
    res = m.lastValue(v);
    EXPECT_TRUE(res);
    EXPECT_EQ(v, 2);
    EXPECT_EQ(m.size(), 1);

    res = m.erase(2);
    EXPECT_TRUE(res);
    res = m.firstValue(v);
    EXPECT_FALSE(res);
    res = m.lastValue(v);
    EXPECT_FALSE(res);
    EXPECT_EQ(m.size(), 0);
    EXPECT_TRUE(m.empty());
}

TEST(CountedValueMap, MaxOperationsMultiple)
{
    DB::Streaming::CountedValueMap<uint64_t, true> m(3);

    uint64_t v = 0;
    auto res = m.insert(1);
    EXPECT_TRUE(res);
    res = m.firstValue(v);
    EXPECT_TRUE(res);
    EXPECT_EQ(v, 1);
    EXPECT_EQ(m.firstValue(), 1);
    EXPECT_EQ(m.lastValue(), 1);
    EXPECT_FALSE(m.empty());
    EXPECT_EQ(m.size(), 1);

    res = m.insert(2);
    EXPECT_TRUE(res);
    res = m.firstValue(v);
    EXPECT_TRUE(res);
    EXPECT_EQ(v, 2);
    EXPECT_EQ(m.lastValue(), 1);
    EXPECT_FALSE(m.empty());
    EXPECT_EQ(m.size(), 2);

    res = m.insert(3);
    EXPECT_TRUE(res);
    res = m.firstValue(v);
    EXPECT_TRUE(res);
    EXPECT_EQ(v, 3);
    EXPECT_EQ(m.lastValue(), 1);
    EXPECT_FALSE(m.empty());
    EXPECT_EQ(m.size(), 3);

    res = m.insert(4);
    EXPECT_TRUE(res);
    res = m.firstValue(v);
    EXPECT_TRUE(res);
    EXPECT_EQ(v, 4);
    EXPECT_EQ(m.lastValue(), 2);
    EXPECT_FALSE(m.empty());
    EXPECT_EQ(m.size(), 3);

    res = m.erase(4);
    EXPECT_TRUE(res);
    res = m.firstValue(v);
    EXPECT_TRUE(res);
    EXPECT_EQ(v, 3);
    EXPECT_EQ(m.lastValue(), 2);
    EXPECT_FALSE(m.empty());
    EXPECT_EQ(m.size(), 2);
}

TEST(CountedValueMap, MinOperationsOne)
{
    DB::Streaming::CountedValueMap<uint64_t, false> m(1);

    uint64_t v = 0;
    auto res = m.firstValue(v);
    EXPECT_FALSE(res);
    res = m.lastValue(v);
    EXPECT_FALSE(res);
    EXPECT_TRUE(m.empty());

    res = m.insert(2);
    EXPECT_TRUE(res);
    res = m.firstValue(v);
    EXPECT_TRUE(res);
    EXPECT_EQ(v, 2);
    EXPECT_EQ(m.lastValue(), 2);
    EXPECT_FALSE(m.empty());
    EXPECT_EQ(m.size(), 1);

    res = m.insert(1);
    EXPECT_TRUE(res);
    res = m.firstValue(v);
    EXPECT_TRUE(res);
    EXPECT_EQ(v, 1);
    EXPECT_EQ(m.lastValue(), 1);
    EXPECT_FALSE(m.empty());
    EXPECT_EQ(m.size(), 1);

    /// Insert 1 again
    res = m.insert(1);
    EXPECT_TRUE(res);
    res = m.firstValue(v);
    EXPECT_TRUE(res);
    EXPECT_EQ(v, 1);
    EXPECT_EQ(m.lastValue(), 1);
    EXPECT_FALSE(m.empty());
    EXPECT_EQ(m.size(), 1);

    /// Insert 2 again, will ignore it
    res = m.insert(2);
    EXPECT_FALSE(res);
    res = m.firstValue(v);
    EXPECT_TRUE(res);
    EXPECT_EQ(v, 1);
    EXPECT_EQ(m.lastValue(), 1);
    EXPECT_FALSE(m.empty());
    EXPECT_EQ(m.size(), 1);

    res = m.erase(2);
    EXPECT_FALSE(res);

    res = m.erase(1);
    EXPECT_TRUE(res);
    EXPECT_EQ(m.firstValue(), 1);
    EXPECT_EQ(m.lastValue(), 1);
    EXPECT_EQ(m.size(), 1);

    res = m.erase(1);
    EXPECT_TRUE(res);
    EXPECT_EQ(m.size(), 0);
    EXPECT_TRUE(m.empty());
}

TEST(CountedValueMap, MinOperationsMultiple)
{
    DB::Streaming::CountedValueMap<uint64_t, false> m(3);

    uint64_t v = 0;
    auto res = m.insert(1);
    EXPECT_TRUE(res);
    res = m.firstValue(v);
    EXPECT_TRUE(res);
    EXPECT_EQ(v, 1);
    EXPECT_EQ(m.lastValue(), 1);
    EXPECT_FALSE(m.empty());
    EXPECT_EQ(m.size(), 1);

    res = m.insert(2);
    EXPECT_TRUE(res);
    res = m.firstValue(v);
    EXPECT_TRUE(res);
    EXPECT_EQ(v, 1);
    EXPECT_EQ(m.lastValue(), 2);
    EXPECT_FALSE(m.empty());
    EXPECT_EQ(m.size(), 2);

    res = m.insert(3);
    EXPECT_TRUE(res);
    res = m.firstValue(v);
    EXPECT_TRUE(res);
    EXPECT_EQ(v, 1);
    EXPECT_EQ(m.lastValue(), 3);
    EXPECT_FALSE(m.empty());
    EXPECT_EQ(m.size(), 3);

    /// Will ignore 4
    res = m.insert(4);
    EXPECT_FALSE(res);
    res = m.firstValue(v);
    EXPECT_TRUE(res);
    EXPECT_EQ(v, 1);
    EXPECT_EQ(m.lastValue(), 3);
    EXPECT_FALSE(m.empty());
    EXPECT_EQ(m.size(), 3);

    /// Will pop 3
    res = m.insert(0);
    EXPECT_TRUE(res);
    res = m.firstValue(v);
    EXPECT_TRUE(res);
    EXPECT_EQ(v, 0);
    EXPECT_EQ(m.lastValue(), 2);
    EXPECT_FALSE(m.empty());
    EXPECT_EQ(m.size(), 3);

    res = m.erase(2);
    EXPECT_TRUE(res);
    res = m.firstValue(v);
    EXPECT_TRUE(res);
    EXPECT_EQ(v, 0);
    EXPECT_EQ(m.lastValue(), 1);
    EXPECT_FALSE(m.empty());
    EXPECT_EQ(m.size(), 2);
}

TEST(CountedValueMap, MaxMerge)
{
    using MaxCountedValueMap = DB::Streaming::CountedValueMap<uint64_t, true>;

    /// case 1, after merge, lhs.size() + rhs.size() <= max_size
    {
        auto test_case = [](std::function<void(MaxCountedValueMap &, MaxCountedValueMap &)> merge) {
            DB::Streaming::CountedValueMap<uint64_t, true> lhs(3);
            lhs.insert(1);
            lhs.insert(2);
            lhs.insert(3);

            DB::Streaming::CountedValueMap<uint64_t, true> rhs(3);
            rhs.insert(1);
            rhs.insert(2);
            rhs.insert(3);

            merge(lhs, rhs);
            EXPECT_EQ(lhs.size(), 3);
            EXPECT_EQ(lhs.firstValue(), 3);
            EXPECT_EQ(lhs.lastValue(), 1);

            for (uint64_t i = 1; const auto & value_count : lhs)
            {
                EXPECT_EQ(value_count.first, i);
                EXPECT_EQ(value_count.second, 2);
                ++i;
            }
        };

        test_case([](MaxCountedValueMap & lhs, MaxCountedValueMap & rhs) {
            MaxCountedValueMap::merge(lhs, rhs);
            EXPECT_TRUE(rhs.empty());
        });
        test_case([](MaxCountedValueMap & lhs, MaxCountedValueMap & rhs) {
            const auto & rhs_const_ref = rhs;
            lhs.merge(rhs_const_ref);
        });
    }

    /// case 2, after merge, lhs.size() + rhs.size() > max_size
    {
        auto test_case = [](std::function<void(MaxCountedValueMap &, MaxCountedValueMap &)> merge) {
            DB::Streaming::CountedValueMap<uint64_t, true> lhs(3);
            lhs.insert(1);
            lhs.insert(2);
            lhs.insert(3);

            DB::Streaming::CountedValueMap<uint64_t, true> rhs(3);
            rhs.insert(2);
            rhs.insert(3);
            rhs.insert(4);

            merge(lhs, rhs);
            EXPECT_EQ(lhs.size(), 3);
            EXPECT_EQ(lhs.firstValue(), 4);
            EXPECT_EQ(lhs.lastValue(), 2);

            for (uint64_t i = 2; const auto & value_count : lhs)
            {
                EXPECT_EQ(value_count.first, i);
                if (i == 4)
                    EXPECT_EQ(value_count.second, 1);
                else
                    EXPECT_EQ(value_count.second, 2);
                ++i;
            }
        };

        test_case([](MaxCountedValueMap & lhs, MaxCountedValueMap & rhs) {
            MaxCountedValueMap::merge(lhs, rhs);
            EXPECT_TRUE(rhs.empty());
        });
        test_case([](MaxCountedValueMap & lhs, MaxCountedValueMap & rhs) {
            const auto & rhs_const_ref = rhs;
            lhs.merge(rhs_const_ref);
        });
    }

    /// case 3, rhs is at capacity and all values in lhs are less than those in rhs
    {
        auto test_case = [](std::function<void(MaxCountedValueMap &, MaxCountedValueMap &)> merge) {
            DB::Streaming::CountedValueMap<uint64_t, true> lhs(3);
            lhs.insert(1);
            lhs.insert(2);

            DB::Streaming::CountedValueMap<uint64_t, true> rhs(3);
            rhs.insert(6);
            rhs.insert(5);
            rhs.insert(4);

            merge(lhs, rhs);

            EXPECT_EQ(lhs.size(), 3);
            EXPECT_EQ(lhs.firstValue(), 6);
            EXPECT_EQ(lhs.lastValue(), 4);

            for (uint64_t i = 4; const auto & value_count : lhs)
            {
                EXPECT_EQ(value_count.first, i);
                EXPECT_EQ(value_count.second, 1);
                ++i;
            }
        };
        test_case([](MaxCountedValueMap & lhs, MaxCountedValueMap & rhs) {
            MaxCountedValueMap::merge(lhs, rhs);
            EXPECT_TRUE(rhs.empty());
        });
        test_case([](MaxCountedValueMap & lhs, MaxCountedValueMap & rhs) {
            const auto & rhs_const_ref = rhs;
            lhs.merge(rhs_const_ref);
        });
    }

    /// case 4, lhs is at capacity and all values in lhs are greater those in rhs
    {
        auto test_case = [](std::function<void(MaxCountedValueMap &, MaxCountedValueMap &)> merge) {
            DB::Streaming::CountedValueMap<uint64_t, true> lhs(3);
            lhs.insert(6);
            lhs.insert(5);
            lhs.insert(4);

            DB::Streaming::CountedValueMap<uint64_t, true> rhs(3);
            rhs.insert(1);
            rhs.insert(2);

            merge(lhs, rhs);

            EXPECT_EQ(lhs.size(), 3);
            EXPECT_EQ(lhs.firstValue(), 6);
            EXPECT_EQ(lhs.lastValue(), 4);

            for (uint64_t i = 4; const auto & value_count : lhs)
            {
                EXPECT_EQ(value_count.first, i);
                EXPECT_EQ(value_count.second, 1);
                ++i;
            }
        };
        test_case([](MaxCountedValueMap & lhs, MaxCountedValueMap & rhs) {
            MaxCountedValueMap::merge(lhs, rhs);
            EXPECT_TRUE(rhs.empty());
        });
        test_case([](MaxCountedValueMap & lhs, MaxCountedValueMap & rhs) {
            const auto & rhs_const_ref = rhs;
            lhs.merge(rhs_const_ref);
        });
    }

    /// case 5, neither lhs nor rhs is at capacity
    {
        auto test_case = [](std::function<void(MaxCountedValueMap &, MaxCountedValueMap &)> merge) {
            DB::Streaming::CountedValueMap<uint64_t, true> lhs(3);
            lhs.insert(6);
            lhs.insert(2);
            lhs.insert(1);

            DB::Streaming::CountedValueMap<uint64_t, true> rhs(3);
            rhs.insert(4);
            rhs.insert(5);

            merge(lhs, rhs);

            EXPECT_EQ(lhs.size(), 3);
            EXPECT_EQ(lhs.firstValue(), 6);
            EXPECT_EQ(lhs.lastValue(), 4);

            for (uint64_t i = 4; const auto & value_count : lhs)
            {
                EXPECT_EQ(value_count.first, i);
                EXPECT_EQ(value_count.second, 1);
                ++i;
            }
        };
        test_case([](MaxCountedValueMap & lhs, MaxCountedValueMap & rhs) {
            MaxCountedValueMap::merge(lhs, rhs);
            EXPECT_TRUE(rhs.empty());
        });
        test_case([](MaxCountedValueMap & lhs, MaxCountedValueMap & rhs) {
            const auto & rhs_const_ref = rhs;
            lhs.merge(rhs_const_ref);
        });
    }

    /// case 6, neither lhs nor rhs is at capacity. swap case 5
    {
        auto test_case = [](std::function<void(MaxCountedValueMap &, MaxCountedValueMap &)> merge) {
            DB::Streaming::CountedValueMap<uint64_t, true> lhs(3);
            lhs.insert(4);
            lhs.insert(5);

            DB::Streaming::CountedValueMap<uint64_t, true> rhs(3);
            rhs.insert(6);
            rhs.insert(2);
            rhs.insert(1);

            merge(lhs, rhs);

            EXPECT_EQ(lhs.size(), 3);
            EXPECT_EQ(lhs.firstValue(), 6);
            EXPECT_EQ(lhs.lastValue(), 4);

            for (uint64_t i = 4; const auto & value_count : lhs)
            {
                EXPECT_EQ(value_count.first, i);
                EXPECT_EQ(value_count.second, 1);
                ++i;
            }
        };
        test_case([](MaxCountedValueMap & lhs, MaxCountedValueMap & rhs) {
            MaxCountedValueMap::merge(lhs, rhs);
            EXPECT_TRUE(rhs.empty());
        });
        test_case([](MaxCountedValueMap & lhs, MaxCountedValueMap & rhs) {
            const auto & rhs_const_ref = rhs;
            lhs.merge(rhs_const_ref);
        });
    }

    /// case 7, lhs, empty rhs
    {
        auto test_case = [](std::function<void(MaxCountedValueMap &, MaxCountedValueMap &)> merge) {
            DB::Streaming::CountedValueMap<uint64_t, true> lhs(3);
            lhs.insert(4);
            lhs.insert(5);

            DB::Streaming::CountedValueMap<uint64_t, true> rhs(3);

            merge(lhs, rhs);

            EXPECT_EQ(lhs.size(), 2);
            EXPECT_EQ(lhs.firstValue(), 5);
            EXPECT_EQ(lhs.lastValue(), 4);

            for (uint64_t i = 4; const auto & value_count : lhs)
            {
                EXPECT_EQ(value_count.first, i);
                EXPECT_EQ(value_count.second, 1);
                ++i;
            }
        };
        test_case([](MaxCountedValueMap & lhs, MaxCountedValueMap & rhs) {
            MaxCountedValueMap::merge(lhs, rhs);
            EXPECT_TRUE(rhs.empty());
        });
        test_case([](MaxCountedValueMap & lhs, MaxCountedValueMap & rhs) {
            const auto & rhs_const_ref = rhs;
            lhs.merge(rhs_const_ref);
        });
    }

    /// case 8, lhs at capacity, empty rhs
    {
        auto test_case = [](std::function<void(MaxCountedValueMap &, MaxCountedValueMap &)> merge) {
            DB::Streaming::CountedValueMap<uint64_t, true> lhs(3);
            lhs.insert(4);
            lhs.insert(5);
            lhs.insert(6);

            DB::Streaming::CountedValueMap<uint64_t, true> rhs(3);

            merge(lhs, rhs);

            EXPECT_EQ(lhs.size(), 3);
            EXPECT_EQ(lhs.firstValue(), 6);
            EXPECT_EQ(lhs.lastValue(), 4);

            for (uint64_t i = 4; const auto & value_count : lhs)
            {
                EXPECT_EQ(value_count.first, i);
                EXPECT_EQ(value_count.second, 1);
                ++i;
            }
        };
        test_case([](MaxCountedValueMap & lhs, MaxCountedValueMap & rhs) {
            MaxCountedValueMap::merge(lhs, rhs);
            EXPECT_TRUE(rhs.empty());
        });
        test_case([](MaxCountedValueMap & lhs, MaxCountedValueMap & rhs) {
            const auto & rhs_const_ref = rhs;
            lhs.merge(rhs_const_ref);
        });
    }

    /// case 9, empty lhs, rhs, swap 7
    {
        auto test_case = [](std::function<void(MaxCountedValueMap &, MaxCountedValueMap &)> merge) {
            DB::Streaming::CountedValueMap<uint64_t, true> lhs(3);

            DB::Streaming::CountedValueMap<uint64_t, true> rhs(3);
            rhs.insert(4);
            rhs.insert(5);

            merge(lhs, rhs);

            EXPECT_EQ(lhs.size(), 2);
            EXPECT_EQ(lhs.firstValue(), 5);
            EXPECT_EQ(lhs.lastValue(), 4);

            for (uint64_t i = 4; const auto & value_count : lhs)
            {
                EXPECT_EQ(value_count.first, i);
                EXPECT_EQ(value_count.second, 1);
                ++i;
            }
        };
        test_case([](MaxCountedValueMap & lhs, MaxCountedValueMap & rhs) {
            MaxCountedValueMap::merge(lhs, rhs);
            EXPECT_TRUE(rhs.empty());
        });
        test_case([](MaxCountedValueMap & lhs, MaxCountedValueMap & rhs) {
            const auto & rhs_const_ref = rhs;
            lhs.merge(rhs_const_ref);
        });
    }

    /// case 10, rhs at capacity, empty rhs, swap 8
    {
        auto test_case = [](std::function<void(MaxCountedValueMap &, MaxCountedValueMap &)> merge) {
            DB::Streaming::CountedValueMap<uint64_t, true> lhs(3);

            DB::Streaming::CountedValueMap<uint64_t, true> rhs(3);
            rhs.insert(4);
            rhs.insert(5);
            rhs.insert(6);

            merge(lhs, rhs);

            EXPECT_EQ(lhs.size(), 3);
            EXPECT_EQ(lhs.firstValue(), 6);
            EXPECT_EQ(lhs.lastValue(), 4);

            for (uint64_t i = 4; const auto & value_count : lhs)
            {
                EXPECT_EQ(value_count.first, i);
                EXPECT_EQ(value_count.second, 1);
                ++i;
            }
        };
        test_case([](MaxCountedValueMap & lhs, MaxCountedValueMap & rhs) {
            MaxCountedValueMap::merge(lhs, rhs);
            EXPECT_TRUE(rhs.empty());
        });
        test_case([](MaxCountedValueMap & lhs, MaxCountedValueMap & rhs) {
            const auto & rhs_const_ref = rhs;
            lhs.merge(rhs_const_ref);
        });
    }
}

TEST(CountedValueMap, MinMerge)
{
    using MinCountedValueMap = DB::Streaming::CountedValueMap<uint64_t, false>;

    /// case 1, after merge, lhs.size() + rhs.size() <= max_size
    {
        auto test_case = [](std::function<void(MinCountedValueMap &, MinCountedValueMap &)> merge) {
            DB::Streaming::CountedValueMap<uint64_t, false> lhs(3);
            lhs.insert(1);
            lhs.insert(2);
            lhs.insert(3);

            DB::Streaming::CountedValueMap<uint64_t, false> rhs(3);
            rhs.insert(1);
            rhs.insert(2);
            rhs.insert(3);

            merge(lhs, rhs);

            EXPECT_EQ(lhs.size(), 3);
            EXPECT_EQ(lhs.firstValue(), 1);
            EXPECT_EQ(lhs.lastValue(), 3);

            for (uint64_t i = 1; const auto & value_count : lhs)
            {
                EXPECT_EQ(value_count.first, i);
                EXPECT_EQ(value_count.second, 2);
                ++i;
            }
        };

        test_case([](MinCountedValueMap & lhs, MinCountedValueMap & rhs) {
            MinCountedValueMap::merge(lhs, rhs);
            EXPECT_TRUE(rhs.empty());
        });
        test_case([](MinCountedValueMap & lhs, MinCountedValueMap & rhs) {
            const auto & rhs_const_ref = rhs;
            lhs.merge(rhs_const_ref);
        });
    }

    /// case 2, after merge, lhs.size() + rhs.size() > max_size
    {
        auto test_case = [](std::function<void(MinCountedValueMap &, MinCountedValueMap &)> merge) {
            DB::Streaming::CountedValueMap<uint64_t, false> lhs(3);
            lhs.insert(1);
            lhs.insert(2);
            lhs.insert(3);

            DB::Streaming::CountedValueMap<uint64_t, false> rhs(3);
            rhs.insert(2);
            rhs.insert(3);
            rhs.insert(4);

            merge(lhs, rhs);

            EXPECT_EQ(lhs.size(), 3);
            EXPECT_EQ(lhs.firstValue(), 1);
            EXPECT_EQ(lhs.lastValue(), 3);

            for (uint64_t i = 1; const auto & value_count : lhs)
            {
                EXPECT_EQ(value_count.first, i);
                if (i == 1)
                    EXPECT_EQ(value_count.second, 1);
                else
                    EXPECT_EQ(value_count.second, 2);
                ++i;
            }
        };

        test_case([](MinCountedValueMap & lhs, MinCountedValueMap & rhs) {
            MinCountedValueMap::merge(lhs, rhs);
            EXPECT_TRUE(rhs.empty());
        });
        test_case([](MinCountedValueMap & lhs, MinCountedValueMap & rhs) {
            const auto & rhs_const_ref = rhs;
            lhs.merge(rhs_const_ref);
        });
    }

    /// case 3, rhs is at capacity and all values in lhs are greater than those in rhs
    {
        auto test_case = [](std::function<void(MinCountedValueMap &, MinCountedValueMap &)> merge) {
            DB::Streaming::CountedValueMap<uint64_t, false> lhs(3);
            lhs.insert(5);
            lhs.insert(4);

            DB::Streaming::CountedValueMap<uint64_t, false> rhs(3);
            rhs.insert(1);
            rhs.insert(2);
            rhs.insert(3);

            merge(lhs, rhs);

            EXPECT_EQ(lhs.size(), 3);
            EXPECT_EQ(lhs.firstValue(), 1);
            EXPECT_EQ(lhs.lastValue(), 3);

            for (uint64_t i = 1; const auto & value_count : lhs)
            {
                EXPECT_EQ(value_count.first, i);
                EXPECT_EQ(value_count.second, 1);
                ++i;
            }
        };

        test_case([](MinCountedValueMap & lhs, MinCountedValueMap & rhs) {
            MinCountedValueMap::merge(lhs, rhs);
            EXPECT_TRUE(rhs.empty());
        });
        test_case([](MinCountedValueMap & lhs, MinCountedValueMap & rhs) {
            const auto & rhs_const_ref = rhs;
            lhs.merge(rhs_const_ref);
        });
    }

    /// case 4, lhs is at capacity and all values in lhs are greater those in rhs
    {
        auto test_case = [](std::function<void(MinCountedValueMap &, MinCountedValueMap &)> merge) {
            DB::Streaming::CountedValueMap<uint64_t, false> lhs(3);
            lhs.insert(6);
            lhs.insert(5);
            lhs.insert(3);

            DB::Streaming::CountedValueMap<uint64_t, false> rhs(3);
            rhs.insert(1);
            rhs.insert(2);

            merge(lhs, rhs);

            EXPECT_EQ(lhs.size(), 3);
            EXPECT_EQ(lhs.firstValue(), 1);
            EXPECT_EQ(lhs.lastValue(), 3);

            std::vector<std::string> args{"a"};
            for (uint64_t i = 1; const auto & value_count : lhs)
            {
                EXPECT_EQ(value_count.first, i);
                EXPECT_EQ(value_count.second, 1);
                ++i;
            }
        };

        test_case([](MinCountedValueMap & lhs, MinCountedValueMap & rhs) {
            MinCountedValueMap::merge(lhs, rhs);
            EXPECT_TRUE(rhs.empty());
        });
        test_case([](MinCountedValueMap & lhs, MinCountedValueMap & rhs) {
            const auto & rhs_const_ref = rhs;
            lhs.merge(rhs_const_ref);
        });
    }

    /// case 5, neither lhs nor rhs is at capacity
    {
        auto test_case = [](std::function<void(MinCountedValueMap &, MinCountedValueMap &)> merge) {
            DB::Streaming::CountedValueMap<uint64_t, false> lhs(3);
            lhs.insert(6);
            lhs.insert(2);
            lhs.insert(1);

            DB::Streaming::CountedValueMap<uint64_t, false> rhs(3);
            rhs.insert(3);
            rhs.insert(5);

            merge(lhs, rhs);

            EXPECT_EQ(lhs.size(), 3);
            EXPECT_EQ(lhs.firstValue(), 1);
            EXPECT_EQ(lhs.lastValue(), 3);

            std::vector<std::string> args{"g"};
            for (uint64_t i = 1; const auto & value_count : lhs)
            {
                EXPECT_EQ(value_count.first, i);
                EXPECT_EQ(value_count.second, 1);
                ++i;
            }
        };

        test_case([](MinCountedValueMap & lhs, MinCountedValueMap & rhs) {
            MinCountedValueMap::merge(lhs, rhs);
            EXPECT_TRUE(rhs.empty());
        });
        test_case([](MinCountedValueMap & lhs, MinCountedValueMap & rhs) {
            const auto & rhs_const_ref = rhs;
            lhs.merge(rhs_const_ref);
        });
    }

    /// case 6, neither lhs nor rhs is at capacity. swap case 5
    {
        auto test_case = [](std::function<void(MinCountedValueMap &, MinCountedValueMap &)> merge) {
            DB::Streaming::CountedValueMap<uint64_t, false> lhs(3);
            lhs.insert(3);
            lhs.insert(5);

            DB::Streaming::CountedValueMap<uint64_t, false> rhs(3);
            rhs.insert(6);
            rhs.insert(2);
            rhs.insert(1);

            merge(lhs, rhs);

            EXPECT_EQ(lhs.size(), 3);
            EXPECT_EQ(lhs.firstValue(), 1);
            EXPECT_EQ(lhs.lastValue(), 3);

            std::vector<std::string> args{"g"};
            for (uint64_t i = 1; const auto & value_count : lhs)
            {
                EXPECT_EQ(value_count.first, i);
                EXPECT_EQ(value_count.second, 1);
                ++i;
            }
        };

        test_case([](MinCountedValueMap & lhs, MinCountedValueMap & rhs) {
            MinCountedValueMap::merge(lhs, rhs);
            EXPECT_TRUE(rhs.empty());
        });
        test_case([](MinCountedValueMap & lhs, MinCountedValueMap & rhs) {
            const auto & rhs_const_ref = rhs;
            lhs.merge(rhs_const_ref);
        });
    }

    /// case 7, lhs, empty rhs
    {
        auto test_case = [](std::function<void(MinCountedValueMap &, MinCountedValueMap &)> merge) {
            DB::Streaming::CountedValueMap<uint64_t, false> lhs(3);
            lhs.insert(4);
            lhs.insert(5);

            DB::Streaming::CountedValueMap<uint64_t, false> rhs(3);

            merge(lhs, rhs);

            EXPECT_EQ(lhs.size(), 2);
            EXPECT_EQ(lhs.firstValue(), 4);
            EXPECT_EQ(lhs.lastValue(), 5);

            std::vector<std::string> args{"a"};
            for (uint64_t i = 4; const auto & value_count : lhs)
            {
                EXPECT_EQ(value_count.first, i);
                EXPECT_EQ(value_count.second, 1);
                ++i;
            }
        };

        test_case([](MinCountedValueMap & lhs, MinCountedValueMap & rhs) {
            MinCountedValueMap::merge(lhs, rhs);
            EXPECT_TRUE(rhs.empty());
        });
        test_case([](MinCountedValueMap & lhs, MinCountedValueMap & rhs) {
            const auto & rhs_const_ref = rhs;
            lhs.merge(rhs_const_ref);
        });
    }

    /// case 8, lhs at capacity, empty rhs
    {
        auto test_case = [](std::function<void(MinCountedValueMap &, MinCountedValueMap &)> merge) {
            DB::Streaming::CountedValueMap<uint64_t, false> lhs(3);
            lhs.insert(4);
            lhs.insert(5);
            lhs.insert(6);

            DB::Streaming::CountedValueMap<uint64_t, false> rhs(3);

            merge(lhs, rhs);

            EXPECT_EQ(lhs.size(), 3);
            EXPECT_EQ(lhs.firstValue(), 4);
            EXPECT_EQ(lhs.lastValue(), 6);

            std::vector<std::string> args{"a"};
            for (uint64_t i = 4; const auto & value_count : lhs)
            {
                EXPECT_EQ(value_count.first, i);
                EXPECT_EQ(value_count.second, 1);
                ++i;
            }
        };

        test_case([](MinCountedValueMap & lhs, MinCountedValueMap & rhs) {
            MinCountedValueMap::merge(lhs, rhs);
            EXPECT_TRUE(rhs.empty());
        });
        test_case([](MinCountedValueMap & lhs, MinCountedValueMap & rhs) {
            const auto & rhs_const_ref = rhs;
            lhs.merge(rhs_const_ref);
        });
    }

    /// case 9, empty lhs, rhs, swap 7
    {
        auto test_case = [](std::function<void(MinCountedValueMap &, MinCountedValueMap &)> merge) {
            DB::Streaming::CountedValueMap<uint64_t, false> lhs(3);

            DB::Streaming::CountedValueMap<uint64_t, false> rhs(3);
            rhs.insert(4);
            rhs.insert(5);

            merge(lhs, rhs);

            EXPECT_EQ(lhs.firstValue(), 4);
            EXPECT_EQ(lhs.lastValue(), 5);
            EXPECT_EQ(lhs.size(), 2);

            std::vector<std::string> args{"a"};
            for (uint64_t i = 4; const auto & value_count : lhs)
            {
                EXPECT_EQ(value_count.first, i);
                EXPECT_EQ(value_count.second, 1);
                ++i;
            }
        };

        test_case([](MinCountedValueMap & lhs, MinCountedValueMap & rhs) {
            MinCountedValueMap::merge(lhs, rhs);
            EXPECT_TRUE(rhs.empty());
        });
        test_case([](MinCountedValueMap & lhs, MinCountedValueMap & rhs) {
            const auto & rhs_const_ref = rhs;
            lhs.merge(rhs_const_ref);
        });
    }

    /// case 10, rhs at capacity, empty rhs, swap 8
    {
        auto test_case = [](std::function<void(MinCountedValueMap &, MinCountedValueMap &)> merge) {
            DB::Streaming::CountedValueMap<uint64_t, false> lhs(3);

            DB::Streaming::CountedValueMap<uint64_t, false> rhs(3);

            rhs.insert(4);
            rhs.insert(5);
            rhs.insert(6);

            merge(lhs, rhs);

            EXPECT_EQ(lhs.size(), 3);
            EXPECT_EQ(lhs.firstValue(), 4);
            EXPECT_EQ(lhs.lastValue(), 6);

            for (uint64_t i = 4; const auto & value_count : lhs)
            {
                EXPECT_EQ(value_count.first, i);
                EXPECT_EQ(value_count.second, 1);
                ++i;
            }
        };

        test_case([](MinCountedValueMap & lhs, MinCountedValueMap & rhs) {
            MinCountedValueMap::merge(lhs, rhs);
            EXPECT_TRUE(rhs.empty());
        });
        test_case([](MinCountedValueMap & lhs, MinCountedValueMap & rhs) {
            const auto & rhs_const_ref = rhs;
            lhs.merge(rhs_const_ref);
        });
    }
}
