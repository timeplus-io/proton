#include <Cluster/MetaStore/ProposalRegistry.h>
#include <Common/Logger.h>
#include <gtest/gtest.h>
#include <thread>

namespace DB::ErrorCodes
{
extern const int TIMEOUT_EXCEEDED;
extern const int LOGICAL_ERROR;
}

TEST(ProposalRegistry, BasicOperations)
{
    auto registry = std::make_shared<cluster::meta::ProposalRegistry>(getLogger("ProposalRegistry"));
    
    // Test register and complete
    {
        auto future = registry->registerProposal(1);
        EXPECT_EQ(registry->pendingCount(), 1);
        
        registry->completeProposal(1);
        EXPECT_EQ(registry->pendingCount(), 0);
        
        auto result = future.get();
        EXPECT_FALSE(result.hasError());
    }
    
    // Test register and cancel
    {
        auto future = registry->registerProposal(2);
        EXPECT_EQ(registry->pendingCount(), 1);
        
        cluster::Error cancel_error(DB::ErrorCodes::LOGICAL_ERROR, "Test cancellation");
        registry->cancelProposal(2, cancel_error);
        EXPECT_EQ(registry->pendingCount(), 0);
        
        auto result = future.get();
        EXPECT_TRUE(result.hasError());
        EXPECT_EQ(result.error_code, DB::ErrorCodes::LOGICAL_ERROR);
        EXPECT_EQ(result.error_message, "Test cancellation");
    }
}

TEST(ProposalRegistry, DuplicateCorrelationID)
{
    auto registry = std::make_shared<cluster::meta::ProposalRegistry>(getLogger("ProposalRegistry"));
    
    auto future1 = registry->registerProposal(100);
    EXPECT_EQ(registry->pendingCount(), 1);
    
    // Try to register with same correlation ID
    auto future2 = registry->registerProposal(100);
    EXPECT_EQ(registry->pendingCount(), 1); // Should still be 1
    
    // Second registration should fail immediately
    auto result2 = future2.get();
    EXPECT_TRUE(result2.hasError());
    EXPECT_EQ(result2.error_code, DB::ErrorCodes::LOGICAL_ERROR);
    
    // First one should still work
    registry->completeProposal(100);
    auto result1 = future1.get();
    EXPECT_FALSE(result1.hasError());
}

TEST(ProposalRegistry, NonExistentComplete)
{
    auto registry = std::make_shared<cluster::meta::ProposalRegistry>(getLogger("ProposalRegistry"));
    
    // Complete non-existent proposal should not crash
    registry->completeProposal(999);
    EXPECT_EQ(registry->pendingCount(), 0);
    
    // Cancel non-existent proposal should not crash
    registry->cancelProposal(999, cluster::Error(DB::ErrorCodes::LOGICAL_ERROR, "Test"));
    EXPECT_EQ(registry->pendingCount(), 0);
}

TEST(ProposalRegistry, ConcurrentOperations)
{
    auto registry = std::make_shared<cluster::meta::ProposalRegistry>(getLogger("ProposalRegistry"));
    
    const int num_proposals = 100;
    std::vector<std::future<cluster::Error>> futures(num_proposals);
    std::vector<std::thread> threads;
    
    // Register proposals from multiple threads
    for (int i = 0; i < num_proposals; ++i)
    {
        threads.emplace_back([&registry, &futures, i]() {
            futures[i] = registry->registerProposal(i);
        });
    }
    
    for (auto & t : threads)
        t.join();
    
    EXPECT_EQ(registry->pendingCount(), num_proposals);
    
    threads.clear();
    
    // Complete half and cancel half from multiple threads
    for (int i = 0; i < num_proposals; ++i)
    {
        threads.emplace_back([&registry, i]() {
            if (i % 2 == 0)
                registry->completeProposal(i);
            else
                registry->cancelProposal(i, cluster::Error(DB::ErrorCodes::LOGICAL_ERROR, "Cancelled"));
        });
    }
    
    for (auto & t : threads)
        t.join();
    
    EXPECT_EQ(registry->pendingCount(), 0);
    
    // Check results
    for (int i = 0; i < num_proposals; ++i)
    {
        auto result = futures[i].get();
        if (i % 2 == 0)
        {
            EXPECT_FALSE(result.hasError());
        }
        else
        {
            EXPECT_TRUE(result.hasError());
            EXPECT_EQ(result.error_code, DB::ErrorCodes::LOGICAL_ERROR);
        }
    }
}

TEST(ProposalRegistry, DestructorCancelsAll)
{
    std::vector<std::future<cluster::Error>> futures;
    
    {
        auto registry = std::make_shared<cluster::meta::ProposalRegistry>(getLogger("ProposalRegistry"));
        
        for (int i = 0; i < 5; ++i)
        {
            futures.push_back(registry->registerProposal(i));
        }
        
        EXPECT_EQ(registry->pendingCount(), 5);
        // Registry destroyed here
    }
    
    // All futures should be cancelled
    for (auto & future : futures)
    {
        auto result = future.get();
        EXPECT_TRUE(result.hasError());
        EXPECT_EQ(result.error_code, DB::ErrorCodes::LOGICAL_ERROR);
    }
}