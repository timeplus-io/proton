
#include <AggregateFunctions/Streaming/CountedArgValueMap.h>

#include <gtest/gtest.h>

TEST(CountedArgValueMap, MaxOperationsOne)
{
    /// Keep around max N elements
    DB::Streaming::CountedArgValueMap<uint64_t, std::string, true> m(1);

    std::string v;
    auto res = m.firstArg(v);
    EXPECT_FALSE(res);
    EXPECT_TRUE(m.empty());

    res = m.insert(1, "a");
    EXPECT_TRUE(res);
    res = m.firstArg(v);
    EXPECT_TRUE(res);
    EXPECT_EQ(v, "a");
    EXPECT_EQ(m.firstValue(), 1);
    EXPECT_EQ(m.lastValue(), 1);
    EXPECT_FALSE(m.empty());
    EXPECT_EQ(m.size(), 1);

    /// Same value, same arg
    res = m.insert(1, "a");
    EXPECT_TRUE(res);
    res = m.firstArg(v);
    EXPECT_TRUE(res);
    EXPECT_EQ(v, "a");
    EXPECT_EQ(m.firstValue(), 1);
    EXPECT_EQ(m.lastValue(), 1);
    EXPECT_FALSE(m.empty());
    EXPECT_EQ(m.size(), 1);

    /// Same value, but diff arg
    res = m.insert(1, "b");
    EXPECT_TRUE(res);
    res = m.firstArg(v);
    EXPECT_TRUE(res);
    EXPECT_EQ(v, "a");
    EXPECT_EQ(m.firstValue(), 1);
    EXPECT_EQ(m.lastValue(), 1);
    EXPECT_FALSE(m.empty());
    EXPECT_EQ(m.size(), 1);

    res = m.insert(2, "aa");
    EXPECT_TRUE(res);
    res = m.firstArg(v);
    EXPECT_TRUE(res);
    EXPECT_EQ(v, "aa");
    EXPECT_EQ(m.firstValue(), 2);
    EXPECT_EQ(m.lastValue(), 2);
    EXPECT_FALSE(m.empty());
    EXPECT_EQ(m.size(), 1);

    /// Insert 2, 'aa' again
    res = m.insert(2, "aa");
    EXPECT_TRUE(res);
    res = m.firstArg(v);
    EXPECT_TRUE(res);
    EXPECT_EQ(v, "aa");
    EXPECT_EQ(m.firstValue(), 2);
    EXPECT_EQ(m.lastValue(), 2);
    EXPECT_FALSE(m.empty());
    EXPECT_EQ(m.size(), 1);

    /// Insert 1 again, will ignore it
    res = m.insert(1, "a");
    EXPECT_FALSE(res);
    res = m.firstArg(v);
    EXPECT_TRUE(res);
    EXPECT_EQ(v, "aa");
    EXPECT_EQ(m.firstValue(), 2);
    EXPECT_EQ(m.lastValue(), 2);
    EXPECT_FALSE(m.empty());
    EXPECT_EQ(m.size(), 1);

    res = m.erase(1, "a");
    EXPECT_FALSE(res);
    EXPECT_EQ(m.firstValue(), 2);
    EXPECT_EQ(m.lastValue(), 2);

    res = m.erase(2, "aa");
    EXPECT_TRUE(res);
    EXPECT_EQ(m.size(), 1);
    EXPECT_EQ(m.firstValue(), 2);
    EXPECT_EQ(m.lastValue(), 2);

    res = m.erase(2, "aa");
    EXPECT_TRUE(res);
    EXPECT_EQ(m.size(), 0);
    EXPECT_TRUE(m.empty());
}

TEST(CountedArgValueMap, MaxOperationsMultiple)
{
    DB::Streaming::CountedArgValueMap<uint64_t, std::string, true> m(3);

    std::string v;
    auto res = m.insert(1, "a");
    EXPECT_TRUE(res);
    res = m.firstArg(v);
    EXPECT_TRUE(res);
    EXPECT_EQ(v, "a");
    EXPECT_EQ(m.firstValue(), 1);
    EXPECT_EQ(m.lastValue(), 1);
    EXPECT_FALSE(m.empty());
    EXPECT_EQ(m.size(), 1);

    res = m.insert(2, "b");
    EXPECT_TRUE(res);
    res = m.firstArg(v);
    EXPECT_TRUE(res);
    EXPECT_EQ(v, "b");
    EXPECT_EQ(m.firstValue(), 2);
    EXPECT_EQ(m.lastValue(), 1);
    EXPECT_FALSE(m.empty());
    EXPECT_EQ(m.size(), 2);

    res = m.insert(3, "c");
    EXPECT_TRUE(res);
    res = m.firstArg(v);
    EXPECT_TRUE(res);
    EXPECT_EQ(v, "c");
    EXPECT_EQ(m.firstValue(), 3);
    EXPECT_EQ(m.lastValue(), 1);
    EXPECT_FALSE(m.empty());
    EXPECT_EQ(m.size(), 3);

    res = m.insert(4, "d");
    EXPECT_TRUE(res);
    res = m.firstArg(v);
    EXPECT_TRUE(res);
    EXPECT_EQ(v, "d");
    EXPECT_EQ(m.firstValue(), 4);
    EXPECT_EQ(m.lastValue(), 2);
    EXPECT_FALSE(m.empty());
    EXPECT_EQ(m.size(), 3);

    res = m.erase(4, "d");
    EXPECT_TRUE(res);
    res = m.firstArg(v);
    EXPECT_TRUE(res);
    EXPECT_EQ(v, "c");
    EXPECT_EQ(m.firstValue(), 3);
    EXPECT_EQ(m.lastValue(), 2);
    EXPECT_FALSE(m.empty());
    EXPECT_EQ(m.size(), 2);
}

TEST(CountedArgValueMap, MinOperationsOne)
{
    DB::Streaming::CountedArgValueMap<uint64_t, std::string, false> m(1);

    std::string v;
    auto res = m.firstArg(v);
    EXPECT_FALSE(res);
    EXPECT_TRUE(m.empty());

    res = m.insert(2, "c");
    EXPECT_TRUE(res);
    res = m.firstArg(v);
    EXPECT_TRUE(res);
    EXPECT_EQ(v, "c");
    EXPECT_EQ(m.firstValue(), 2);
    EXPECT_EQ(m.lastValue(), 2);
    EXPECT_FALSE(m.empty());
    EXPECT_EQ(m.size(), 1);

    /// Same value, same arg
    res = m.insert(2, "c");
    EXPECT_TRUE(res);
    res = m.firstArg(v);
    EXPECT_TRUE(res);
    EXPECT_EQ(v, "c");
    EXPECT_EQ(m.firstValue(), 2);
    EXPECT_EQ(m.lastValue(), 2);
    EXPECT_FALSE(m.empty());
    EXPECT_EQ(m.size(), 1);

    /// Same value, but diff arg
    res = m.insert(2, "b");
    EXPECT_TRUE(res);
    res = m.firstArg(v);
    EXPECT_TRUE(res);
    EXPECT_EQ(v, "c");
    EXPECT_EQ(m.firstValue(), 2);
    EXPECT_EQ(m.lastValue(), 2);
    EXPECT_FALSE(m.empty());
    EXPECT_EQ(m.size(), 1);

    res = m.insert(1, "a");
    EXPECT_TRUE(res);
    res = m.firstArg(v);
    EXPECT_TRUE(res);
    EXPECT_EQ(v, "a");
    EXPECT_EQ(m.firstValue(), 1);
    EXPECT_EQ(m.lastValue(), 1);
    EXPECT_FALSE(m.empty());
    EXPECT_EQ(m.size(), 1);

    /// Insert 1, 'a' again
    res = m.insert(1, "a");
    EXPECT_TRUE(res);
    res = m.firstArg(v);
    EXPECT_TRUE(res);
    EXPECT_EQ(v, "a");
    EXPECT_EQ(m.firstValue(), 1);
    EXPECT_EQ(m.lastValue(), 1);
    EXPECT_FALSE(m.empty());
    EXPECT_EQ(m.size(), 1);

    res = m.insert(2, "b");
    EXPECT_FALSE(res);
    res = m.firstArg(v);
    EXPECT_TRUE(res);
    EXPECT_EQ(v, "a");
    EXPECT_EQ(m.firstValue(), 1);
    EXPECT_EQ(m.lastValue(), 1);
    EXPECT_FALSE(m.empty());
    EXPECT_EQ(m.size(), 1);

    res = m.erase(2, "b");
    EXPECT_FALSE(res);
    EXPECT_EQ(m.firstValue(), 1);
    EXPECT_EQ(m.lastValue(), 1);

    res = m.erase(1, "a");
    EXPECT_TRUE(res);
    EXPECT_EQ(m.firstValue(), 1);
    EXPECT_EQ(m.lastValue(), 1);
    EXPECT_EQ(m.size(), 1);

    res = m.erase(1, "a");
    EXPECT_TRUE(res);
    EXPECT_EQ(m.size(), 0);
    EXPECT_TRUE(m.empty());
}

TEST(CountedArgValueMap, MinOperationsMultiple)
{
    DB::Streaming::CountedArgValueMap<uint64_t, std::string, false> m(3);

    std::string v;
    auto res = m.insert(3, "a");
    EXPECT_TRUE(res);
    res = m.firstArg(v);
    EXPECT_TRUE(res);
    EXPECT_EQ(v, "a");
    EXPECT_EQ(m.firstValue(), 3);
    EXPECT_EQ(m.lastValue(), 3);
    EXPECT_FALSE(m.empty());
    EXPECT_EQ(m.size(), 1);

    res = m.insert(2, "b");
    EXPECT_TRUE(res);
    res = m.firstArg(v);
    EXPECT_TRUE(res);
    EXPECT_EQ(v, "b");
    EXPECT_EQ(m.firstValue(), 2);
    EXPECT_EQ(m.lastValue(), 3);
    EXPECT_FALSE(m.empty());
    EXPECT_EQ(m.size(), 2);

    res = m.insert(1, "c");
    EXPECT_TRUE(res);
    res = m.firstArg(v);
    EXPECT_TRUE(res);
    EXPECT_EQ(v, "c");
    EXPECT_EQ(m.firstValue(), 1);
    EXPECT_EQ(m.lastValue(), 3);
    EXPECT_FALSE(m.empty());
    EXPECT_EQ(m.size(), 3);

    res = m.insert(0, "d");
    EXPECT_TRUE(res);
    res = m.firstArg(v);
    EXPECT_TRUE(res);
    EXPECT_EQ(v, "d");
    EXPECT_EQ(m.firstValue(), 0);
    EXPECT_EQ(m.lastValue(), 2);
    EXPECT_FALSE(m.empty());
    EXPECT_EQ(m.size(), 3);

    res = m.erase(0, "d");
    EXPECT_TRUE(res);
    res = m.firstArg(v);
    EXPECT_TRUE(res);
    EXPECT_EQ(v, "c");
    EXPECT_EQ(m.firstValue(), 1);
    EXPECT_EQ(m.lastValue(), 2);
    EXPECT_FALSE(m.empty());
    EXPECT_EQ(m.size(), 2);
}

TEST(CountedArgValueMap, MaxMerge)
{
    using MaxCountedArgValueMap = DB::Streaming::CountedArgValueMap<uint64_t, std::string, true>;

    /// case 1, after merge, lhs.size() + rhs.size() <= max_size
    {
        auto test_case = [](std::function<void(MaxCountedArgValueMap &, MaxCountedArgValueMap &)> merge) {
            DB::Streaming::CountedArgValueMap<uint64_t, std::string, true> lhs(3);
            lhs.insert(1, "a");
            lhs.insert(2, "b");
            lhs.insert(3, "c");

            DB::Streaming::CountedArgValueMap<uint64_t, std::string, true> rhs(3);
            rhs.insert(1, "a");
            rhs.insert(2, "b");
            rhs.insert(3, "c");

            merge(lhs, rhs);

            EXPECT_EQ(lhs.size(), 3);
            EXPECT_EQ(lhs.firstArg(), "c");
            EXPECT_EQ(lhs.firstValue(), 3);
            EXPECT_EQ(lhs.lastValue(), 1);

            std::vector<std::string> args{"a", "b", "c"};

            for (uint64_t i = 1; const auto & value_count : lhs)
            {
                EXPECT_EQ(value_count.first, i);
                for (const auto & arg_count : *value_count.second)
                {
                    EXPECT_EQ(arg_count.count, 2);
                    EXPECT_EQ(arg_count.arg, args[i - 1]);
                }
                ++i;
            }
        };

        test_case([](MaxCountedArgValueMap & lhs, MaxCountedArgValueMap & rhs) {
            MaxCountedArgValueMap::merge(lhs, rhs);
            EXPECT_TRUE(rhs.empty());
        });
        test_case([](MaxCountedArgValueMap & lhs, MaxCountedArgValueMap & rhs) {
            const auto & rhs_const_ref = rhs;
            lhs.merge(rhs_const_ref);
        });
    }

    /// case 2, after merge, lhs.size() + rhs.size() > max_size
    {
        auto test_case = [](std::function<void(MaxCountedArgValueMap &, MaxCountedArgValueMap &)> merge) {
            DB::Streaming::CountedArgValueMap<uint64_t, std::string, true> lhs(4);
            lhs.insert(1, "a");
            lhs.insert(2, "b");
            lhs.insert(3, "c");

            DB::Streaming::CountedArgValueMap<uint64_t, std::string, true> rhs(4);
            rhs.insert(2, "b");
            rhs.insert(3, "cc");
            rhs.insert(4, "dd");

            merge(lhs, rhs);
            EXPECT_EQ(lhs.size(), 4);
            EXPECT_EQ(lhs.firstArg(), "dd");
            EXPECT_EQ(lhs.firstValue(), 4);
            EXPECT_EQ(lhs.lastValue(), 2);

            auto iter = lhs.begin();

            /// value 2
            EXPECT_EQ(iter->first, 2);
            EXPECT_EQ(iter->second->size(), 1);
            EXPECT_EQ(iter->second->at(0).arg, "b");
            EXPECT_EQ(iter->second->at(0).count, 2);

            /// value 3
            ++iter;
            EXPECT_EQ(iter->first, 3);
            EXPECT_EQ(iter->second->size(), 2);
            EXPECT_EQ(iter->second->at(0).arg, "c");
            EXPECT_EQ(iter->second->at(0).count, 1);
            EXPECT_EQ(iter->second->at(1).arg, "cc");
            EXPECT_EQ(iter->second->at(1).count, 1);

            /// value 4
            ++iter;
            EXPECT_EQ(iter->first, 4);
            EXPECT_EQ(iter->second->size(), 1);
            EXPECT_EQ(iter->second->at(0).arg, "dd");
            EXPECT_EQ(iter->second->at(0).count, 1);
        };

        test_case([](MaxCountedArgValueMap & lhs, MaxCountedArgValueMap & rhs) {
            MaxCountedArgValueMap::merge(lhs, rhs);
            EXPECT_TRUE(rhs.empty());
        });
        test_case([](MaxCountedArgValueMap & lhs, MaxCountedArgValueMap & rhs) {
            const auto & rhs_const_ref = rhs;
            lhs.merge(rhs_const_ref);
        });
    }

    /// case 3, rhs is at capacity and all values in lhs are less than those in rhs
    {
        auto test_case = [](std::function<void(MaxCountedArgValueMap &, MaxCountedArgValueMap &)> merge) {
            DB::Streaming::CountedArgValueMap<uint64_t, std::string, true> lhs(3);
            lhs.insert(1, "a");
            lhs.insert(2, "b");

            DB::Streaming::CountedArgValueMap<uint64_t, std::string, true> rhs(3);
            rhs.insert(6, "e");
            rhs.insert(5, "f");
            rhs.insert(4, "g");

            merge(lhs, rhs);

            EXPECT_EQ(lhs.size(), 3);
            EXPECT_EQ(lhs.firstArg(), "e");
            EXPECT_EQ(lhs.firstValue(), 6);
            EXPECT_EQ(lhs.lastValue(), 4);

            std::vector<std::string> args{"g", "f", "e"};
            for (uint64_t i = 4; const auto & value_count : lhs)
            {
                EXPECT_EQ(value_count.first, i);
                for (const auto & arg_count : *value_count.second)
                {
                    EXPECT_EQ(arg_count.count, 1);
                    EXPECT_EQ(arg_count.arg, args[i - 4]);
                }
                ++i;
            }
        };

        test_case([](MaxCountedArgValueMap & lhs, MaxCountedArgValueMap & rhs) {
            MaxCountedArgValueMap::merge(lhs, rhs);
            EXPECT_TRUE(rhs.empty());
        });
        test_case([](MaxCountedArgValueMap & lhs, MaxCountedArgValueMap & rhs) {
            const auto & rhs_const_ref = rhs;
            lhs.merge(rhs_const_ref);
        });
    }

    /// case 4, lhs is at capacity and all values in lhs are greater those in rhs
    {
        auto test_case = [](std::function<void(MaxCountedArgValueMap &, MaxCountedArgValueMap &)> merge) {
            DB::Streaming::CountedArgValueMap<uint64_t, std::string, true> lhs(3);
            lhs.insert(6, "e");
            lhs.insert(5, "f");
            lhs.insert(4, "g");

            DB::Streaming::CountedArgValueMap<uint64_t, std::string, true> rhs(3);
            rhs.insert(1, "a");
            rhs.insert(2, "b");

            merge(lhs, rhs);

            EXPECT_EQ(lhs.size(), 3);
            EXPECT_EQ(lhs.firstArg(), "e");
            EXPECT_EQ(lhs.firstValue(), 6);
            EXPECT_EQ(lhs.lastValue(), 4);

            std::vector<std::string> args{"g", "f", "e"};
            for (uint64_t i = 4; const auto & value_count : lhs)
            {
                EXPECT_EQ(value_count.first, i);
                for (const auto & arg_count : *value_count.second)
                {
                    EXPECT_EQ(arg_count.count, 1);
                    EXPECT_EQ(arg_count.arg, args[i - 4]);
                }
                ++i;
            }
        };

        test_case([](MaxCountedArgValueMap & lhs, MaxCountedArgValueMap & rhs) {
            MaxCountedArgValueMap::merge(lhs, rhs);
            EXPECT_TRUE(rhs.empty());
        });
        test_case([](MaxCountedArgValueMap & lhs, MaxCountedArgValueMap & rhs) {
            const auto & rhs_const_ref = rhs;
            lhs.merge(rhs_const_ref);
        });
    }

    /// case 5, neither lhs nor rhs is at capacity
    {
        auto test_case = [](std::function<void(MaxCountedArgValueMap &, MaxCountedArgValueMap &)> merge) {
            DB::Streaming::CountedArgValueMap<uint64_t, std::string, true> lhs(3);
            lhs.insert(6, "e");
            lhs.insert(2, "f");
            lhs.insert(1, "g");

            DB::Streaming::CountedArgValueMap<uint64_t, std::string, true> rhs(3);
            rhs.insert(4, "a");
            rhs.insert(5, "b");

            merge(lhs, rhs);

            EXPECT_EQ(lhs.size(), 3);
            EXPECT_EQ(lhs.firstArg(), "e");
            EXPECT_EQ(lhs.firstValue(), 6);
            EXPECT_EQ(lhs.lastValue(), 4);

            std::vector<std::string> args{"a", "b", "e"};
            for (uint64_t i = 4; const auto & value_count : lhs)
            {
                EXPECT_EQ(value_count.first, i);
                for (const auto & arg_count : *value_count.second)
                {
                    EXPECT_EQ(arg_count.count, 1);
                    EXPECT_EQ(arg_count.arg, args[i - 4]);
                }
                ++i;
            }
        };

        test_case([](MaxCountedArgValueMap & lhs, MaxCountedArgValueMap & rhs) {
            MaxCountedArgValueMap::merge(lhs, rhs);
            EXPECT_TRUE(rhs.empty());
        });
        test_case([](MaxCountedArgValueMap & lhs, MaxCountedArgValueMap & rhs) {
            const auto & rhs_const_ref = rhs;
            lhs.merge(rhs_const_ref);
        });
    }

    /// case 6, neither lhs nor rhs is at capacity. swap case 5
    {
        auto test_case = [](std::function<void(MaxCountedArgValueMap &, MaxCountedArgValueMap &)> merge) {
            DB::Streaming::CountedArgValueMap<uint64_t, std::string, true> lhs(3);
            lhs.insert(4, "a");
            lhs.insert(5, "b");

            DB::Streaming::CountedArgValueMap<uint64_t, std::string, true> rhs(3);
            rhs.insert(6, "e");
            rhs.insert(2, "f");
            rhs.insert(1, "g");

            merge(lhs, rhs);

            EXPECT_EQ(lhs.size(), 3);
            EXPECT_EQ(lhs.firstArg(), "e");
            EXPECT_EQ(lhs.firstValue(), 6);
            EXPECT_EQ(lhs.lastValue(), 4);

            std::vector<std::string> args{"a", "b", "e"};
            for (uint64_t i = 4; const auto & value_count : lhs)
            {
                EXPECT_EQ(value_count.first, i);
                for (const auto & arg_count : *value_count.second)
                {
                    EXPECT_EQ(arg_count.count, 1);
                    EXPECT_EQ(arg_count.arg, args[i - 4]);
                }
                ++i;
            }
        };

        test_case([](MaxCountedArgValueMap & lhs, MaxCountedArgValueMap & rhs) {
            MaxCountedArgValueMap::merge(lhs, rhs);
            EXPECT_TRUE(rhs.empty());
        });
        test_case([](MaxCountedArgValueMap & lhs, MaxCountedArgValueMap & rhs) {
            const auto & rhs_const_ref = rhs;
            lhs.merge(rhs_const_ref);
        });
    }

    /// case 7, lhs, empty rhs
    {
        auto test_case = [](std::function<void(MaxCountedArgValueMap &, MaxCountedArgValueMap &)> merge) {
            DB::Streaming::CountedArgValueMap<uint64_t, std::string, true> lhs(3);
            lhs.insert(4, "a");
            lhs.insert(5, "b");

            DB::Streaming::CountedArgValueMap<uint64_t, std::string, true> rhs(3);

            merge(lhs, rhs);

            EXPECT_EQ(lhs.size(), 2);
            EXPECT_EQ(lhs.firstArg(), "b");
            EXPECT_EQ(lhs.firstValue(), 5);
            EXPECT_EQ(lhs.lastValue(), 4);

            std::vector<std::string> args{"a", "b"};
            for (uint64_t i = 4; const auto & value_count : lhs)
            {
                EXPECT_EQ(value_count.first, i);
                for (const auto & arg_count : *value_count.second)
                {
                    EXPECT_EQ(arg_count.count, 1);
                    EXPECT_EQ(arg_count.arg, args[i - 4]);
                }
                ++i;
            }
        };

        test_case([](MaxCountedArgValueMap & lhs, MaxCountedArgValueMap & rhs) {
            MaxCountedArgValueMap::merge(lhs, rhs);
            EXPECT_TRUE(rhs.empty());
        });
        test_case([](MaxCountedArgValueMap & lhs, MaxCountedArgValueMap & rhs) {
            const auto & rhs_const_ref = rhs;
            lhs.merge(rhs_const_ref);
        });
    }

    /// case 8, lhs at capacity, empty rhs
    {
        auto test_case = [](std::function<void(MaxCountedArgValueMap &, MaxCountedArgValueMap &)> merge) {
            DB::Streaming::CountedArgValueMap<uint64_t, std::string, true> lhs(3);
            lhs.insert(4, "a");
            lhs.insert(5, "b");
            lhs.insert(6, "c");

            DB::Streaming::CountedArgValueMap<uint64_t, std::string, true> rhs(3);

            merge(lhs, rhs);

            EXPECT_EQ(lhs.size(), 3);
            EXPECT_EQ(lhs.firstArg(), "c");
            EXPECT_EQ(lhs.firstValue(), 6);
            EXPECT_EQ(lhs.lastValue(), 4);

            std::vector<std::string> args{"a", "b", "c"};
            for (uint64_t i = 4; const auto & value_count : lhs)
            {
                EXPECT_EQ(value_count.first, i);
                for (const auto & arg_count : *value_count.second)
                {
                    EXPECT_EQ(arg_count.count, 1);
                    EXPECT_EQ(arg_count.arg, args[i - 4]);
                }
                ++i;
            }
        };

        test_case([](MaxCountedArgValueMap & lhs, MaxCountedArgValueMap & rhs) {
            MaxCountedArgValueMap::merge(lhs, rhs);
            EXPECT_TRUE(rhs.empty());
        });
        test_case([](MaxCountedArgValueMap & lhs, MaxCountedArgValueMap & rhs) {
            const auto & rhs_const_ref = rhs;
            lhs.merge(rhs_const_ref);
        });
    }

    /// case 9, empty lhs, rhs, swap 7
    {
        auto test_case = [](std::function<void(MaxCountedArgValueMap &, MaxCountedArgValueMap &)> merge) {
            DB::Streaming::CountedArgValueMap<uint64_t, std::string, true> lhs(3);

            DB::Streaming::CountedArgValueMap<uint64_t, std::string, true> rhs(3);
            rhs.insert(4, "a");
            rhs.insert(5, "b");

            merge(lhs, rhs);
            EXPECT_EQ(lhs.firstArg(), "b");
            EXPECT_EQ(lhs.firstValue(), 5);
            EXPECT_EQ(lhs.lastValue(), 4);

            EXPECT_EQ(lhs.size(), 2);

            std::vector<std::string> args{"a", "b"};
            for (uint64_t i = 4; const auto & value_count : lhs)
            {
                EXPECT_EQ(value_count.first, i);
                for (const auto & arg_count : *value_count.second)
                {
                    EXPECT_EQ(arg_count.count, 1);
                    EXPECT_EQ(arg_count.arg, args[i - 4]);
                }
                ++i;
            }
        };

        test_case([](MaxCountedArgValueMap & lhs, MaxCountedArgValueMap & rhs) {
            MaxCountedArgValueMap::merge(lhs, rhs);
            EXPECT_TRUE(rhs.empty());
        });
        test_case([](MaxCountedArgValueMap & lhs, MaxCountedArgValueMap & rhs) {
            const auto & rhs_const_ref = rhs;
            lhs.merge(rhs_const_ref);
        });
    }

    /// case 10, rhs at capacity, empty rhs, swap 8
    {
        auto test_case = [](std::function<void(MaxCountedArgValueMap &, MaxCountedArgValueMap &)> merge) {
            DB::Streaming::CountedArgValueMap<uint64_t, std::string, true> lhs(3);

            DB::Streaming::CountedArgValueMap<uint64_t, std::string, true> rhs(3);
            rhs.insert(4, "a");
            rhs.insert(5, "b");
            rhs.insert(6, "c");

            merge(lhs, rhs);

            EXPECT_EQ(lhs.size(), 3);
            EXPECT_EQ(lhs.firstArg(), "c");
            EXPECT_EQ(lhs.firstValue(), 6);
            EXPECT_EQ(lhs.lastValue(), 4);

            std::vector<std::string> args{"a", "b", "c"};
            for (uint64_t i = 4; const auto & value_count : lhs)
            {
                EXPECT_EQ(value_count.first, i);
                for (const auto & arg_count : *value_count.second)
                {
                    EXPECT_EQ(arg_count.count, 1);
                    EXPECT_EQ(arg_count.arg, args[i - 4]);
                }
                ++i;
            }
        };

        test_case([](MaxCountedArgValueMap & lhs, MaxCountedArgValueMap & rhs) {
            MaxCountedArgValueMap::merge(lhs, rhs);
            EXPECT_TRUE(rhs.empty());
        });
        test_case([](MaxCountedArgValueMap & lhs, MaxCountedArgValueMap & rhs) {
            const auto & rhs_const_ref = rhs;
            lhs.merge(rhs_const_ref);
        });
    }
}

TEST(CountedArgValueMap, MinMerge)
{
    using MinCountedArgValueMap = DB::Streaming::CountedArgValueMap<uint64_t, std::string, false>;
    /// case 1, after merge, lhs.size() + rhs.size() <= max_size
    {
        auto test_case = [](std::function<void(MinCountedArgValueMap &, MinCountedArgValueMap &)> merge) {
            DB::Streaming::CountedArgValueMap<uint64_t, std::string, false> lhs(3);
            lhs.insert(1, "a");
            lhs.insert(2, "b");
            lhs.insert(3, "c");

            DB::Streaming::CountedArgValueMap<uint64_t, std::string, false> rhs(3);
            rhs.insert(1, "a");
            rhs.insert(2, "b");
            rhs.insert(3, "c");

            merge(lhs, rhs);
            EXPECT_EQ(lhs.size(), 3);
            EXPECT_EQ(lhs.firstArg(), "a");
            EXPECT_EQ(lhs.firstValue(), 1);
            EXPECT_EQ(lhs.lastValue(), 3);

            std::vector<std::string> args{"a", "b", "c"};

            for (uint64_t i = 1; const auto & value_count : lhs)
            {
                EXPECT_EQ(value_count.first, i);
                for (const auto & arg_count : *value_count.second)
                {
                    EXPECT_EQ(arg_count.count, 2);
                    EXPECT_EQ(arg_count.arg, args[i - 1]);
                }
                ++i;
            }
        };
        test_case([](MinCountedArgValueMap & lhs, MinCountedArgValueMap & rhs) {
            MinCountedArgValueMap::merge(lhs, rhs);
            EXPECT_TRUE(rhs.empty());
        });
        test_case([](MinCountedArgValueMap & lhs, MinCountedArgValueMap & rhs) {
            const auto & rhs_const_ref = rhs;
            lhs.merge(rhs_const_ref);
        });
    }

    /// case 2, after merge, lhs.size() + rhs.size() > max_size
    {
        auto test_case = [](std::function<void(MinCountedArgValueMap &, MinCountedArgValueMap &)> merge) {
            DB::Streaming::CountedArgValueMap<uint64_t, std::string, false> lhs(4);
            lhs.insert(1, "a");
            lhs.insert(2, "b");
            lhs.insert(3, "c");

            DB::Streaming::CountedArgValueMap<uint64_t, std::string, false> rhs(4);
            rhs.insert(2, "b");
            rhs.insert(3, "cc");
            rhs.insert(4, "dd");

            merge(lhs, rhs);
            EXPECT_EQ(lhs.size(), 4);
            EXPECT_EQ(lhs.firstArg(), "a");
            EXPECT_EQ(lhs.firstValue(), 1);
            EXPECT_EQ(lhs.lastValue(), 3);

            auto iter = lhs.begin();

            /// value 1
            EXPECT_EQ(iter->first, 1);
            EXPECT_EQ(iter->second->size(), 1);
            EXPECT_EQ(iter->second->at(0).arg, "a");
            EXPECT_EQ(iter->second->at(0).count, 1);

            /// value 2
            ++iter;
            EXPECT_EQ(iter->first, 2);
            EXPECT_EQ(iter->second->size(), 1);
            EXPECT_EQ(iter->second->at(0).arg, "b");
            EXPECT_EQ(iter->second->at(0).count, 2);

            /// value 3
            ++iter;
            EXPECT_EQ(iter->first, 3);
            EXPECT_EQ(iter->second->size(), 2);
            EXPECT_EQ(iter->second->at(0).arg, "c");
            EXPECT_EQ(iter->second->at(0).count, 1);
            EXPECT_EQ(iter->second->at(1).arg, "cc");
            EXPECT_EQ(iter->second->at(1).count, 1);
        };
        test_case([](MinCountedArgValueMap & lhs, MinCountedArgValueMap & rhs) {
            MinCountedArgValueMap::merge(lhs, rhs);
            EXPECT_TRUE(rhs.empty());
        });
        test_case([](MinCountedArgValueMap & lhs, MinCountedArgValueMap & rhs) {
            const auto & rhs_const_ref = rhs;
            lhs.merge(rhs_const_ref);
        });
    }

    /// case 3, after merge, lhs.size() + rhs.size() > max_size and no key overlap
    {
        auto test_case = [](std::function<void(MinCountedArgValueMap &, MinCountedArgValueMap &)> merge) {
            DB::Streaming::CountedArgValueMap<uint64_t, std::string, false> lhs(3);
            lhs.insert(1, "a");
            lhs.insert(2, "b");
            lhs.insert(3, "c");

            DB::Streaming::CountedArgValueMap<uint64_t, std::string, false> rhs(3);
            rhs.insert(6, "e");
            rhs.insert(5, "f");
            rhs.insert(4, "g");

            merge(lhs, rhs);
            EXPECT_EQ(lhs.size(), 3);
            EXPECT_EQ(lhs.firstArg(), "a");
            EXPECT_EQ(lhs.firstValue(), 1);
            EXPECT_EQ(lhs.lastValue(), 3);

            std::vector<std::string> args{"a", "b", "c"};
            for (uint64_t i = 1; const auto & value_count : lhs)
            {
                EXPECT_EQ(value_count.first, i);
                for (const auto & arg_count : *value_count.second)
                {
                    EXPECT_EQ(arg_count.count, 1);
                    EXPECT_EQ(arg_count.arg, args[i - 1]);
                }
                ++i;
            }
        };

        test_case([](MinCountedArgValueMap & lhs, MinCountedArgValueMap & rhs) {
            MinCountedArgValueMap::merge(lhs, rhs);
            EXPECT_TRUE(rhs.empty());
        });
        test_case([](MinCountedArgValueMap & lhs, MinCountedArgValueMap & rhs) {
            const auto & rhs_const_ref = rhs;
            lhs.merge(rhs_const_ref);
        });
    }

    /// case 4, lhs is at capacity and all values in lhs are greater those in rhs
    {
        auto test_case = [](std::function<void(MinCountedArgValueMap &, MinCountedArgValueMap &)> merge) {
            DB::Streaming::CountedArgValueMap<uint64_t, std::string, false> lhs(3);
            lhs.insert(6, "e");
            lhs.insert(5, "f");
            lhs.insert(3, "g");

            DB::Streaming::CountedArgValueMap<uint64_t, std::string, false> rhs(3);
            rhs.insert(1, "a");
            rhs.insert(2, "b");

            merge(lhs, rhs);

            EXPECT_EQ(lhs.size(), 3);
            EXPECT_EQ(lhs.firstArg(), "a");
            EXPECT_EQ(lhs.firstValue(), 1);
            EXPECT_EQ(lhs.lastValue(), 3);

            std::vector<std::string> args{"a", "b", "g"};
            for (uint64_t i = 1; const auto & value_count : lhs)
            {
                EXPECT_EQ(value_count.first, i);
                for (const auto & arg_count : *value_count.second)
                {
                    EXPECT_EQ(arg_count.count, 1);
                    EXPECT_EQ(arg_count.arg, args[i - 1]);
                }
                ++i;
            }
        };

        test_case([](MinCountedArgValueMap & lhs, MinCountedArgValueMap & rhs) {
            MinCountedArgValueMap::merge(lhs, rhs);
            EXPECT_TRUE(rhs.empty());
        });
        test_case([](MinCountedArgValueMap & lhs, MinCountedArgValueMap & rhs) {
            const auto & rhs_const_ref = rhs;
            lhs.merge(rhs_const_ref);
        });
    }

    /// case 5, neither lhs nor rhs is at capacity
    {
        auto test_case = [](std::function<void(MinCountedArgValueMap &, MinCountedArgValueMap &)> merge) {
            DB::Streaming::CountedArgValueMap<uint64_t, std::string, false> lhs(3);
            lhs.insert(6, "e");
            lhs.insert(2, "f");
            lhs.insert(1, "g");

            DB::Streaming::CountedArgValueMap<uint64_t, std::string, false> rhs(3);
            rhs.insert(3, "a");
            rhs.insert(5, "b");

            merge(lhs, rhs);

            EXPECT_EQ(lhs.size(), 3);
            EXPECT_EQ(lhs.firstArg(), "g");
            EXPECT_EQ(lhs.firstValue(), 1);
            EXPECT_EQ(lhs.lastValue(), 3);

            std::vector<std::string> args{"g", "f", "a"};
            for (uint64_t i = 1; const auto & value_count : lhs)
            {
                EXPECT_EQ(value_count.first, i);
                for (const auto & arg_count : *value_count.second)
                {
                    EXPECT_EQ(arg_count.count, 1);
                    EXPECT_EQ(arg_count.arg, args[i - 1]);
                }
                ++i;
            }
        };

        test_case([](MinCountedArgValueMap & lhs, MinCountedArgValueMap & rhs) {
            MinCountedArgValueMap::merge(lhs, rhs);
            EXPECT_TRUE(rhs.empty());
        });
        test_case([](MinCountedArgValueMap & lhs, MinCountedArgValueMap & rhs) {
            const auto & rhs_const_ref = rhs;
            lhs.merge(rhs_const_ref);
        });
    }

    /// case 6, neither lhs nor rhs is at capacity. swap case 5
    {
        auto test_case = [](std::function<void(MinCountedArgValueMap &, MinCountedArgValueMap &)> merge) {
            DB::Streaming::CountedArgValueMap<uint64_t, std::string, false> lhs(3);
            lhs.insert(3, "a");
            lhs.insert(5, "b");

            DB::Streaming::CountedArgValueMap<uint64_t, std::string, false> rhs(3);
            rhs.insert(6, "e");
            rhs.insert(2, "f");
            rhs.insert(1, "g");

            merge(lhs, rhs);

            EXPECT_EQ(lhs.size(), 3);
            EXPECT_EQ(lhs.firstArg(), "g");
            EXPECT_EQ(lhs.firstValue(), 1);
            EXPECT_EQ(lhs.lastValue(), 3);

            std::vector<std::string> args{"g", "f", "a"};
            for (uint64_t i = 1; const auto & value_count : lhs)
            {
                EXPECT_EQ(value_count.first, i);
                for (const auto & arg_count : *value_count.second)
                {
                    EXPECT_EQ(arg_count.count, 1);
                    EXPECT_EQ(arg_count.arg, args[i - 1]);
                }
                ++i;
            }
        };

        test_case([](MinCountedArgValueMap & lhs, MinCountedArgValueMap & rhs) {
            MinCountedArgValueMap::merge(lhs, rhs);
            EXPECT_TRUE(rhs.empty());
        });
        test_case([](MinCountedArgValueMap & lhs, MinCountedArgValueMap & rhs) {
            const auto & rhs_const_ref = rhs;
            lhs.merge(rhs_const_ref);
        });
    }

    /// case 7, lhs, empty rhs
    {
        auto test_case = [](std::function<void(MinCountedArgValueMap &, MinCountedArgValueMap &)> merge) {
            DB::Streaming::CountedArgValueMap<uint64_t, std::string, false> lhs(3);
            lhs.insert(4, "a");
            lhs.insert(5, "b");

            DB::Streaming::CountedArgValueMap<uint64_t, std::string, false> rhs(3);

            merge(lhs, rhs);

            EXPECT_EQ(lhs.size(), 2);
            EXPECT_EQ(lhs.firstArg(), "a");
            EXPECT_EQ(lhs.firstValue(), 4);
            EXPECT_EQ(lhs.lastValue(), 5);

            std::vector<std::string> args{"a", "b"};
            for (uint64_t i = 4; const auto & value_count : lhs)
            {
                EXPECT_EQ(value_count.first, i);
                for (const auto & arg_count : *value_count.second)
                {
                    EXPECT_EQ(arg_count.count, 1);
                    EXPECT_EQ(arg_count.arg, args[i - 4]);
                }
                ++i;
            }
        };

        test_case([](MinCountedArgValueMap & lhs, MinCountedArgValueMap & rhs) {
            MinCountedArgValueMap::merge(lhs, rhs);
            EXPECT_TRUE(rhs.empty());
        });
        test_case([](MinCountedArgValueMap & lhs, MinCountedArgValueMap & rhs) {
            const auto & rhs_const_ref = rhs;
            lhs.merge(rhs_const_ref);
        });
    }

    /// case 8, lhs at capacity, empty rhs
    {
        auto test_case = [](std::function<void(MinCountedArgValueMap &, MinCountedArgValueMap &)> merge) {
            DB::Streaming::CountedArgValueMap<uint64_t, std::string, false> lhs(3);
            lhs.insert(4, "a");
            lhs.insert(5, "b");
            lhs.insert(6, "c");

            DB::Streaming::CountedArgValueMap<uint64_t, std::string, false> rhs(3);

            merge(lhs, rhs);

            EXPECT_EQ(lhs.size(), 3);
            EXPECT_EQ(lhs.firstArg(), "a");
            EXPECT_EQ(lhs.firstValue(), 4);
            EXPECT_EQ(lhs.lastValue(), 6);

            std::vector<std::string> args{"a", "b", "c"};
            for (uint64_t i = 4; const auto & value_count : lhs)
            {
                EXPECT_EQ(value_count.first, i);
                for (const auto & arg_count : *value_count.second)
                {
                    EXPECT_EQ(arg_count.count, 1);
                    EXPECT_EQ(arg_count.arg, args[i - 4]);
                }
                ++i;
            }
        };

        test_case([](MinCountedArgValueMap & lhs, MinCountedArgValueMap & rhs) {
            MinCountedArgValueMap::merge(lhs, rhs);
            EXPECT_TRUE(rhs.empty());
        });
        test_case([](MinCountedArgValueMap & lhs, MinCountedArgValueMap & rhs) {
            const auto & rhs_const_ref = rhs;
            lhs.merge(rhs_const_ref);
        });
    }

    /// case 9, empty lhs, rhs, swap 7
    {
        auto test_case = [](std::function<void(MinCountedArgValueMap &, MinCountedArgValueMap &)> merge) {
            DB::Streaming::CountedArgValueMap<uint64_t, std::string, false> lhs(3);

            DB::Streaming::CountedArgValueMap<uint64_t, std::string, false> rhs(3);
            rhs.insert(4, "a");
            rhs.insert(5, "b");

            merge(lhs, rhs);
            EXPECT_EQ(lhs.firstArg(), "a");
            EXPECT_EQ(lhs.firstValue(), 4);
            EXPECT_EQ(lhs.lastValue(), 5);

            EXPECT_EQ(lhs.size(), 2);

            std::vector<std::string> args{"a", "b"};
            for (uint64_t i = 4; const auto & value_count : lhs)
            {
                EXPECT_EQ(value_count.first, i);
                for (const auto & arg_count : *value_count.second)
                {
                    EXPECT_EQ(arg_count.count, 1);
                    EXPECT_EQ(arg_count.arg, args[i - 4]);
                }
                ++i;
            }
        };

        test_case([](MinCountedArgValueMap & lhs, MinCountedArgValueMap & rhs) {
            MinCountedArgValueMap::merge(lhs, rhs);
            EXPECT_TRUE(rhs.empty());
        });
        test_case([](MinCountedArgValueMap & lhs, MinCountedArgValueMap & rhs) {
            const auto & rhs_const_ref = rhs;
            lhs.merge(rhs_const_ref);
        });
    }

    /// case 10, rhs at capacity, empty rhs, swap 8
    {
        auto test_case = [](std::function<void(MinCountedArgValueMap &, MinCountedArgValueMap &)> merge) {
            DB::Streaming::CountedArgValueMap<uint64_t, std::string, false> lhs(3);

            DB::Streaming::CountedArgValueMap<uint64_t, std::string, false> rhs(3);
            rhs.insert(4, "a");
            rhs.insert(5, "b");
            rhs.insert(6, "c");

            merge(lhs, rhs);

            EXPECT_EQ(lhs.size(), 3);
            EXPECT_EQ(lhs.firstArg(), "a");
            EXPECT_EQ(lhs.firstValue(), 4);
            EXPECT_EQ(lhs.lastValue(), 6);

            std::vector<std::string> args{"a", "b", "c"};
            for (uint64_t i = 4; const auto & value_count : lhs)
            {
                EXPECT_EQ(value_count.first, i);
                for (const auto & arg_count : *value_count.second)
                {
                    EXPECT_EQ(arg_count.count, 1);
                    EXPECT_EQ(arg_count.arg, args[i - 4]);
                }
                ++i;
            }
        };

        test_case([](MinCountedArgValueMap & lhs, MinCountedArgValueMap & rhs) {
            MinCountedArgValueMap::merge(lhs, rhs);
            EXPECT_TRUE(rhs.empty());
        });
        test_case([](MinCountedArgValueMap & lhs, MinCountedArgValueMap & rhs) {
            const auto & rhs_const_ref = rhs;
            lhs.merge(rhs_const_ref);
        });
    }
}
