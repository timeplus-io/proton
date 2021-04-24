#include <DistributedMetadata/PlacementStrategy.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <random>


using namespace DB;

TEST(PlacementService, PlaceNodesByDiskSpace)
{
    std::vector<Int32> default_disks;
    std::vector<Int32> cold_disks;
    for (int i = 0; i < 100; i++)
    {
        default_disks.push_back(i + 1);
        cold_disks.push_back(i + 1);
    }

    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(default_disks.begin(), default_disks.end(), g);
    std::shuffle(cold_disks.begin(), cold_disks.end(), g);

    NodeMetricsContainer container;
    String default_policy = "default";
    String cold_policy = "cold_policy";
    for (int i = 0; i < 100; i++)
    {
        String node = std::to_string(i);
        NodeMetricsPtr node_metrics = std::make_shared<NodeMetrics>(node, node);
        node_metrics->disk_space.emplace(default_policy, default_disks[i]);
        node_metrics->disk_space.emplace(cold_policy, cold_disks[i]);
        container.emplace(node, node_metrics);
    }

    DiskStrategy strategy;

    /// Case1: no node can fulfill the request
    {
        PlacementStrategy::PlacementRequest non_request{20, "non-exists"};
        auto nodes = strategy.qualifiedNodes(container, non_request);
        EXPECT_EQ(nodes.size(), 0);
    }

    /// Case2: nodes are not enough for the request
    {
        PlacementStrategy::PlacementRequest big_request{1000, default_policy};
        EXPECT_EQ(strategy.qualifiedNodes(container, big_request).size(), 0);
    }

    std::uniform_int_distribution<size_t> required_number(0, 100);
    /// Case3: get nodes for default policy
    {
        size_t required = required_number(g);
        PlacementStrategy::PlacementRequest default_request{required, default_policy};
        auto nodes = strategy.qualifiedNodes(container, default_request);
        EXPECT_EQ(nodes.size(), required);
        for (int i = 0; i < required; i++)
        {
            EXPECT_EQ(nodes[i]->disk_space.find(default_policy)->second, 100 - i);
        }
    }

    /// Case4: get nodes for cold policy
    {
        size_t cold_required = required_number(g);
        PlacementStrategy::PlacementRequest cold_request{cold_required, cold_policy};
        auto cold_nodes = strategy.qualifiedNodes(container, cold_request);
        EXPECT_EQ(cold_nodes.size(), cold_required);
        for (int i = 0; i < cold_required; i++)
        {
            EXPECT_EQ(cold_nodes[i]->disk_space.find(cold_policy)->second, 100 - i);
        }
    }
}

TEST(PlacementService, PlaceNodesByTableCounts)
{
    std::vector<Int32> num_of_tables;
    num_of_tables.reserve(100);
    for (int i = 0; i < 100; i++)
    {
        num_of_tables.push_back(i + 1);
    }

    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(num_of_tables.begin(), num_of_tables.end(), g);

    NodeMetricsContainer container;
    String default_policy = "default";
    for (int i = 0; i < 100; i++)
    {
        String node = std::to_string(i);
        NodeMetricsPtr node_metrics = std::make_shared<NodeMetrics>(node, node);
        node_metrics->disk_space.emplace(default_policy, 100);
        node_metrics->num_of_tables = num_of_tables[i];
        container.emplace(node, node_metrics);
    }

    DiskStrategy strategy;

    /// Case1: no node can fulfill the request
    {
        PlacementStrategy::PlacementRequest non_request{20, "non-exists"};
        auto nodes = strategy.qualifiedNodes(container, non_request);
        EXPECT_EQ(nodes.size(), 0);
    }

    /// Case2: nodes are not enough for the request
    {
        PlacementStrategy::PlacementRequest big_request{1000, default_policy};
        EXPECT_EQ(strategy.qualifiedNodes(container, big_request).size(), 0);
    }

    std::uniform_int_distribution<size_t> required_number(0, 100);
    /// Case3: get nodes for default policy
    {
        size_t required = required_number(g);
        PlacementStrategy::PlacementRequest default_request{required, default_policy};
        auto nodes = strategy.qualifiedNodes(container, default_request);
        EXPECT_EQ(nodes.size(), required);
        for (int i = 0; i < required; i++)
        {
            EXPECT_EQ(nodes[i]->num_of_tables, i + 1);
        }
    }
}

