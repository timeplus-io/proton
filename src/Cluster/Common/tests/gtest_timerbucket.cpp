#include <Cluster/Common/TimeWheel/TimerTaskList.h>

#include <gtest/gtest.h>

#include <thread>

class TimerTaskListTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        task_executed = 0;
        list = std::make_shared<cluster::TimerTaskList>();
    }

    static void incrementCounter() { task_executed++; }

    static cluster::TimerTaskEntryPtr createTask(bool is_cancelled = false)
    {
        auto task = std::make_shared<cluster::TimerTaskEntry>(
            1000, /// 1 second interval
            []() { incrementCounter(); },
            false /// non-repeating
        );
        if (is_cancelled)
            task->cancel();

        return task;
    }

    /// TimerTaskEntry::add() use `enable_shared_from_this()` require heap alloc
    std::shared_ptr<cluster::TimerTaskList> list;
    static inline int task_executed;
};

/// Test basic add and flush functionality
TEST_F(TimerTaskListTest, BasicAddAndFlush)
{
    auto task = createTask();

    list->add(std::move(task));

    list->flush([](cluster::TimerTaskEntryPtr task_) { task_->execute(); });

    EXPECT_EQ(task_executed, 1);
}

/// Test handling of cancelled tasks
TEST_F(TimerTaskListTest, CancelledTasksAreFiltered)
{
    list->add(createTask(true)); /// Cancelled task
    list->add(createTask()); /// Normal task
    list->add(createTask(true)); /// Another cancelled task

    list->flush([](cluster::TimerTaskEntryPtr task) { task->execute(); });

    EXPECT_EQ(task_executed, 3);
}

/// Test capacity management
TEST_F(TimerTaskListTest, CapacityManagement)
{
    for (int i = 0; i < 166; ++i)
        list->add(createTask());


    /// Flush to trigger capacity management
    list->flush([](cluster::TimerTaskEntryPtr task) { task->execute(); });

    EXPECT_EQ(task_executed, 166);

    /// Add one more task to verify the list is still functional
    list->add(createTask());
    task_executed = 0; /// Reset counter

    list->flush([](cluster::TimerTaskEntryPtr task) { task->execute(); });

    EXPECT_EQ(task_executed, 1);
}

/// Test concurrent access
TEST_F(TimerTaskListTest, ConcurrentAccess)
{
    const int num_threads = 4;
    const int tasks_per_thread = 1000;

    std::vector<std::thread> threads;

    /// Launch multiple threads to add tasks
    for (int i = 0; i < num_threads; ++i)
    {
        threads.emplace_back([this]() {
            for (int j = 0; j < tasks_per_thread; ++j)
                list->add(createTask());
        });
    }

    /// Wait for all threads to complete
    for (auto & thread : threads)
        thread.join();


    /// Flush and verify
    task_executed = 0;
    list->flush([](cluster::TimerTaskEntryPtr task) { task->execute(); });

    EXPECT_EQ(task_executed, num_threads * tasks_per_thread);
}

/// Test empty list behavior
TEST_F(TimerTaskListTest, EmptyListBehavior)
{
    /// Flushing an empty list should not cause any issues
    list->flush([](cluster::TimerTaskEntryPtr) { FAIL() << "Flush callback should not be called for empty list"; });

    EXPECT_EQ(task_executed, 0);
}

/// Test multiple flush operations
TEST_F(TimerTaskListTest, MultipleFlushOperations)
{
    /// First round
    list->add(createTask());
    list->flush([](cluster::TimerTaskEntryPtr task) { task->execute(); });
    EXPECT_EQ(task_executed, 1);

    /// Second round
    list->add(createTask());
    list->add(createTask());
    list->flush([](cluster::TimerTaskEntryPtr task) { task->execute(); });
    EXPECT_EQ(task_executed, 3);
}

TEST_F(TimerTaskListTest, BucketExpiration)
{
    cluster::TimerTaskList bucket;
    ASSERT_TRUE(bucket.setExpiration(512));

    ASSERT_EQ(bucket.getExpiration(), 512);

    ASSERT_TRUE(bucket.hasExpiration());
}

/// Note: We intentionally don't test concurrent flush() and cancel() operations
/// because TimerTaskList is an internal component that relies on TimeWheel
/// for proper task lifecycle management. TimeWheel::add() handles cancellation
/// checks, so adding such checks in flush() would introduce unnecessary overhead.
TEST_F(TimerTaskListTest, FlushBehavior)
{
    auto list = std::make_shared<cluster::TimerTaskList>();
    std::vector<cluster::TimerTaskEntryPtr> tasks;

    /// Add non-cancelled tasks
    for (int i = 0; i < 100; i++)
    {
        auto task = std::make_shared<cluster::TimerTaskEntry>(1000, []() {});
        tasks.push_back(task);
        list->add(task);
    }

    /// Flush should work normally with valid tasks
    list->flush([](cluster::TimerTaskEntryPtr t) { EXPECT_FALSE(t->isCancelled()); });
}
