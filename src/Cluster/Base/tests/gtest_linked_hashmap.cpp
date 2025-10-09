#include <Cluster/Base/LinkedHashMap.h>

#include <gtest/gtest.h>

TEST(LinkedHashMap, LinkedHashMap)
{
    cluster::LinkedHashMap<int32_t, int32_t> linked;

    linked.insert(1, 1);
    ASSERT_EQ(linked.size(), 1);
    linked.insert(1, 2);
    ASSERT_EQ(linked.size(), 1);
    {
        const auto & latest = linked.latest();
        ASSERT_EQ(latest.k, 1);
        ASSERT_EQ(latest.v, 2);
    }

    linked.insert(11, 11);
    ASSERT_EQ(linked.size(), 2);
    {
        const auto & earliest = linked.earliest();
        ASSERT_EQ(earliest.k, 1);
        ASSERT_EQ(earliest.v, 2);

        const auto & latest = linked.latest();
        ASSERT_EQ(latest.k, 11);
        ASSERT_EQ(latest.v, 11);
    }

    linked.insert(22, 22); /// New
    linked.insert(11, 33); /// Override
    linked.insert(1, 3); /// Override
    linked.insert(44, 55); /// New
    ASSERT_EQ(linked.size(), 4);
    {
        const auto & earliest = linked.earliest();
        ASSERT_EQ(earliest.k, 22);
        ASSERT_EQ(earliest.v, 22);

        const auto & latest = linked.latest();
        ASSERT_EQ(latest.k, 44);
        ASSERT_EQ(latest.v, 55);
    }

    ASSERT_EQ(linked.erase(1), 1); /// Erased
    ASSERT_EQ(linked.size(), 3);

    ASSERT_EQ(linked.erase(77), 0);
    ASSERT_EQ(linked.size(), 3);

    {
        auto iter = linked.find(11);
        ASSERT_TRUE(iter != linked.end());
        ASSERT_EQ(iter->k, 11);
        ASSERT_EQ(iter->v, 33);
    }

    ASSERT_TRUE(linked.find(77) == linked.end());

    {
        auto iter = linked.rbegin();
        /// (22, 22) <- (11, 33) <- (44, 55)
        ASSERT_EQ(iter->k, 22);
        ASSERT_EQ(iter->v, 22);
        ++iter;
        ASSERT_EQ(iter->k, 11);
        ASSERT_EQ(iter->v, 33);
        ++iter;
        ASSERT_EQ(iter->k, 44);
        ASSERT_EQ(iter->v, 55);
        ASSERT_EQ(++iter, linked.rend());
    }

    {
        auto iter = linked.begin();
        /// (22, 22) <- (11, 33) <- (44, 55)
        ASSERT_EQ(iter->k, 44);
        ASSERT_EQ(iter->v, 55);
        ++iter;
        ASSERT_EQ(iter->k, 11);
        ASSERT_EQ(iter->v, 33);
        ++iter;
        ASSERT_EQ(iter->k, 22);
        ASSERT_EQ(iter->v, 22);

        ASSERT_EQ(++iter, linked.end());
    }

    {
        auto iter = linked.find(11);
        ASSERT_TRUE(iter != linked.end());
        iter = linked.erase(iter);
        ASSERT_EQ(linked.size(), 2);
        ASSERT_TRUE(iter != linked.end());
        ASSERT_EQ(iter->k, 22);
        ASSERT_EQ(iter->v, 22);
    }
}
