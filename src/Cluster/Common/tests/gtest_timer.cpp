#include <Cluster/Common/TimeWheel/SystemTimer.h>
#include <Cluster/Common/TimeWheel/TimerService.h>
#include <Common/Stopwatch.h>

#include <gtest/gtest.h>

#include <base/sleep.h>

#include <unistd.h>

#include <atomic>
#include <iostream>

using namespace std::chrono_literals;

TEST(TimeWheel, EdgeCaseTimer)
{
    std::atomic_bool onEdgeCalled{false};
    std::atomic_bool afterEdgeCalled{false};
    {
        cluster::SystemTimer timer(4, 2, 6000);

        auto timer_task = timer.add(10, [&]() { onEdgeCalled.store(true); });
        ASSERT_TRUE(timer_task != nullptr);
        timer_task = timer.add(11, [&]() { afterEdgeCalled.store(true); });
        ASSERT_TRUE(timer_task != nullptr);

        timer.poll(31);
        timer.poll(20);
    }

    ASSERT_TRUE(onEdgeCalled.load());
    ASSERT_TRUE(afterEdgeCalled.load());
}

TEST(TimeWheel, PollFirstTask)
{
    std::atomic_bool onEdgeCalled{false};
    std::atomic_bool afterEdgeCalled{false};
    {
        cluster::SystemTimer timer(4, 2, 6000);

        auto timer_task = timer.add(10, [&]() { onEdgeCalled.store(true); });
        ASSERT_TRUE(timer_task != nullptr);
        timer_task = timer.add(15, [&]() { afterEdgeCalled.store(true); });
        ASSERT_TRUE(timer_task != nullptr);

        timer.poll(11);
    }

    ASSERT_TRUE(onEdgeCalled.load());
    ASSERT_FALSE(afterEdgeCalled.load());
}

/// This test passes because the internal pool size is set to 2.
/// However, even with the try-catch block, the thread pool does not stop exiting as expected.
TEST(TimeWheel, ThrowException)
{
    std::atomic_int32_t counter{0};

    {
        cluster::SystemTimer timer(/*worker_pool_size=*/1, 5, 6000);
        auto timer_task = timer.add(10, [&]() { counter++; });
        ASSERT_TRUE(timer_task != nullptr);
        timer_task = timer.add(15, [&]() { throw std::runtime_error{"issue-4640"}; });
        ASSERT_TRUE(timer_task != nullptr);

        timer_task = timer.add(20, [&]() { counter++; });
        ASSERT_TRUE(timer_task != nullptr);
        timer_task = timer.add(20, [&]() { counter++; });
        ASSERT_TRUE(timer_task != nullptr);

        timer.poll(30);
        timer.poll(30);
        timer.poll(30);

        /// Because stack trace unwinding takes sometime on X86 especially in debug build
        /// Debug ~5000ms, release ~200ms
        std::this_thread::sleep_for(5000ms);
        timer.poll(30);
        timer.poll(30);
    }

    ASSERT_EQ(counter.load(), 3);
}

TEST(TimeWheel, AddAndRemove)
{
    std::atomic_int32_t counter{0};
    {
        cluster::SystemTimer timer(4, 5, 6000);

        auto timer_task = timer.add(20, [&]() { counter++; });
        ASSERT_TRUE(timer_task != nullptr);
        auto t2 = timer.add(20, [&]() { counter++; });

        t2->cancel();

        timer.poll(30);
    }

    ASSERT_EQ(counter.load(), 1);
}

TEST(TimeWheel, Concurrency_Thread)
{
    constexpr uint32_t numThreads = 4;
    constexpr uint32_t timersPerThread = 1000;

    std::atomic_int32_t counter{0};
    {
        cluster::SystemTimer timer(4, 10, 6000);

        /// This is the work each thread will perform.
        auto work = [&]() {
            for (uint32_t i = 0; i < timersPerThread; ++i)
            {
                auto timer_task = timer.add(100, [&counter]() { counter.fetch_add(1, std::memory_order_relaxed); });
                ASSERT_TRUE(timer_task != nullptr);
            }
        };

        std::vector<std::thread> threads;
        threads.reserve(numThreads);
        for (uint32_t i = 0; i < numThreads; ++i)
            threads.emplace_back(work);

        for (auto & t : threads)
            t.join();

        timer.poll(400);
        for (int i = 0; i < 10; i++)
            timer.poll(1);
    }

    std::this_thread::sleep_for(10ms);
    ASSERT_EQ(counter.load(std::memory_order_relaxed), numThreads * timersPerThread);
}

TEST(TimeWheel, RaceConditionTest)
{
    std::atomic_bool callback_executed{false};
    {
        cluster::SystemTimer timer(4, 10, 6000);
        auto task_id = timer.add(50, [&callback_executed]() { callback_executed.store(true); });

        /// Wait a tiny bit to increase the chances of the cancel operation overlapping with timer advancement.
        std::this_thread::sleep_for(1ms);

        /// Try to cancel the task.
        task_id->cancel();

        timer.poll(60);
    }

    ASSERT_FALSE(callback_executed.load()); /// This assertion might fail if there is a race condition.
}

TEST(TimeWheel, CancelInCallback)
{
    std::atomic_int32_t counter{};
    {
        cluster::SystemTimer timer;

        auto t1 = timer.add(10, [&]() { counter++; });
        auto t2 = timer.add(15, [&]() { counter++; });
        auto timer_task = timer.add(5, [t1_ = std::move(t1), t2_ = std::move(t2)]() {
            t1_->cancel();
            t2_->cancel();
        });

        ASSERT_TRUE(timer_task != nullptr);

        timer.poll(50);
    }

    ASSERT_TRUE(counter.load() == 0);
}

TEST(TimeWheel, TimerTestCallback)
{
    std::atomic_int32_t counter{};
    {
        cluster::SystemTimer timer(4, 1, 6000);
        auto timer_task = timer.add(10, [&]() { counter++; });
        ASSERT_TRUE(timer_task != nullptr);
        GTEST_ASSERT_EQ(counter.load(), 0);

        timer.poll(1);
        GTEST_ASSERT_EQ(counter.load(), 0);

        timer.poll(1);
        GTEST_ASSERT_EQ(counter.load(), 0);

        timer.poll(1);
        GTEST_ASSERT_EQ(counter.load(), 0);

        timer.poll(1);
        GTEST_ASSERT_EQ(counter.load(), 0);

        timer.poll(7);
        timer.wait();
        GTEST_ASSERT_EQ(counter.load(), 1);

        timer.poll(5);
        timer.wait();
        GTEST_ASSERT_EQ(counter.load(), 1);
    }
}

TEST(TimeWheel, TimerTestCallbackWithOrder)
{
    std::atomic_int32_t counter{};
    {
        cluster::SystemTimer timer(4, 1, 6000);

        auto timer_task = timer.add(30, [&]() { counter++; });
        ASSERT_TRUE(timer_task != nullptr);
        timer_task = timer.add(30, [&]() { counter++; });
        ASSERT_TRUE(timer_task != nullptr);
        timer_task = timer.add(30, [&]() { counter++; });
        ASSERT_TRUE(timer_task != nullptr);
        GTEST_ASSERT_EQ(counter.load(), 0);

        timer.poll(31);
        timer.poll(31);
    }

    GTEST_ASSERT_EQ(counter.load(), 3);
}

TEST(TimeWheel, TimerTestWithCancel)
{
    std::atomic_int32_t counter{};
    {
        cluster::SystemTimer timer(4, 1, 6000);

        auto timer_task = timer.add(30, [&]() { counter++; });
        ASSERT_TRUE(timer_task != nullptr);
        auto task = timer.add(30, [&]() { counter++; });
        timer_task = timer.add(30, [&]() { counter++; });
        ASSERT_TRUE(timer_task != nullptr);
        GTEST_ASSERT_EQ(counter.load(), 0);

        task->cancel();

        timer.poll(31);
        timer.poll(31);
    }
    GTEST_ASSERT_EQ(counter.load(), 2);
}

TEST(TimeWheel, TimerTestWithOverflowWheel)
{
    std::atomic_int32_t counter{};
    {
        cluster::SystemTimer timer(4, 1, 20);
        auto timer_task = timer.add(40, [&]() { counter++; });
        ASSERT_TRUE(timer_task != nullptr);

        /// The overflow wheel needs to advance the clock twice to work properly.
        /// The same behavior is locally verified in the Kafka source code.

        timer.poll(1024);
        timer.poll(1024);
    }

    GTEST_ASSERT_EQ(counter.load(), 1);
}

TEST(TimeWheel, TimerTestWithOverflowWheelBoundary)
{
    std::atomic_int32_t counter{};
    {
        cluster::SystemTimer timer(4, 1, 20);
        auto timer_task = timer.add(20, [&]() { counter++; });
        ASSERT_TRUE(timer_task != nullptr);

        /// The equality boundary also falls into the overflow wheel.
        timer.poll(1024);
        timer.poll(1024);
    }

    GTEST_ASSERT_EQ(counter.load(), 1);
}

TEST(TimeWheel, TimerTestWithOverflowWheelMore)
{
    std::atomic_int32_t counter{};
    {
        cluster::SystemTimer timer(4, 1, 20);

        auto timer_task = timer.add(560, [&]() { counter++; });
        ASSERT_TRUE(timer_task != nullptr);

        timer.poll(1024);
        GTEST_ASSERT_EQ(counter.load(), 0);

        /// Since the Timer expiration is based on absolute time,
        /// it may not simply be put in a third-level queue
        /// (e.g. interval 20ms, 20*20ms, 400*20ms).
        /// In actual usage, consider running a continuous
        /// advanceClock loop in a background thread.

        timer.poll(1024);
        timer.poll(1024);
        timer.poll(1024);
    }
    GTEST_ASSERT_EQ(counter.load(), 1);
}

TEST(TimeWheel, TimerTestWithExpiredTask)
{
    std::atomic_int32_t counter{};
    {
        cluster::SystemTimer timer(4, 1, 20);

        auto timer_task = timer.add(-10, [&]() { counter++; });
        ASSERT_TRUE(timer_task == nullptr);

        timer.poll(1024);
    }
    GTEST_ASSERT_EQ(counter.load(), 1);
}

TEST(TimeWheel, TimerTestCallbackAsync)
{
    constexpr uint32_t THREAD_NUM = 10;
    std::atomic_int32_t counter{};
    {
        cluster::SystemTimer timer(4, 1, 6000);

        std::vector<std::thread> threads;
        threads.reserve(THREAD_NUM);

        auto work = [&]() {
            auto timer_task = timer.add(20, [&]() { counter++; });
            ASSERT_TRUE(timer_task != nullptr);
        };

        for (uint32_t i = 0; i < THREAD_NUM; ++i)
            threads.emplace_back(work);

        GTEST_ASSERT_EQ(counter.load(), 0);

        for (auto & i : threads)
            i.join();

        timer.poll(30);
        for (int i = 0; i < 10; i++)
            timer.poll(1);

        /// Give ample time for timer to execute the expired timer or we need change the timer default pool size to task count
        /// https://github.com/timeplus-io/proton-enterprise/issues/3710
        std::this_thread::sleep_for(1ms);
    }
    GTEST_ASSERT_EQ(counter.load(), THREAD_NUM);
}

TEST(TimeWheel, RepeatAndCancelTimer)
{
    cluster::SystemTimer timer(4, 1, 6000);
    std::atomic_int32_t counter{0};
    constexpr int32_t repeat_count = 3;
    constexpr int32_t interval_ms = 20;

    auto task = timer.add(interval_ms, [&]() { counter++; }, /*repeat*/ true);

    for (int32_t i = 0; i < repeat_count; ++i)
        timer.poll(interval_ms);

    std::this_thread::sleep_for(3ms);
    task->cancel();

    ASSERT_EQ(counter.load(), repeat_count);

    counter.store(0);
    auto new_task = timer.add(interval_ms, [&]() { counter++; }, /*repeat*/ true);

    timer.poll(interval_ms * 2);
    std::this_thread::sleep_for(1ms);
    new_task->cancel();

    /// verify after cancel the timer, it will not continue add counter
    int finalCount = counter.load();
    timer.poll(interval_ms * 2);
    std::this_thread::sleep_for(3ms);
    ASSERT_EQ(counter.load(), finalCount);
}

TEST(TimeWheel, CancelTimer)
{
    std::atomic_int32_t counter{};

    cluster::SystemTimer timer(4, 1, 6000);
    constexpr int32_t interval_ms = 20;

    auto timer_task = timer.add(interval_ms, [&]() { counter++; }, /*repeat*/ true);
    ASSERT_TRUE(timer_task != nullptr);

    timer_task->cancel();
    ASSERT_TRUE(timer_task->isCancelled());

    timer.poll(interval_ms);
    ASSERT_EQ(counter, 0);
}


TEST(TimeWheel, RepeatButForgetToCancelTimer)
{
    std::atomic_int32_t counter{};

    cluster::SystemTimer timer(4, 1, 6000);
    constexpr int32_t repeat_count = 3;
    constexpr int32_t interval_ms = 20;

    auto timer_task = timer.add(interval_ms, [&]() { counter++; }, /*repeat*/ true);
    ASSERT_TRUE(timer_task != nullptr);

    for (int32_t i = 0; i < repeat_count; ++i)
        timer.poll(interval_ms);

    std::this_thread::sleep_for(1ms);

    timer_task = timer.add(interval_ms, [&]() { counter++; }, /*repeat*/ true);
    ASSERT_TRUE(timer_task != nullptr);

    timer.poll(interval_ms * 2);

    std::this_thread::sleep_for(1ms);

    std::cout << "counter.load = " << counter.load() << '\n';
}

TEST(TimeWheel, PollBucket)
{
    std::vector<int64_t> execution_times;
    execution_times.reserve(3);
    {
        auto scheduled_time = std::chrono::system_clock::now();
        std::mutex mtx;

        cluster::SystemTimer timer(4, 1, 6000);

        auto count_time = [&]() {
            auto now = std::chrono::system_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(now - scheduled_time).count();
            {
                std::lock_guard<std::mutex> lock(mtx);
                execution_times.push_back(duration);
            }
        };

        auto timer_task1 = timer.add(10, count_time);
        auto timer_task2 = timer.add(100, count_time);
        auto timer_task3 = timer.add(1000, count_time);

        ASSERT_TRUE(timer_task1);
        ASSERT_TRUE(timer_task2);
        ASSERT_TRUE(timer_task3);

        timer.poll(2000);
        timer.poll(2000);
        timer.poll(2000);
        timer.poll(2000);
    }

    ASSERT_EQ(execution_times.size(), 3);
    ASSERT_NEAR(execution_times[0], 10, 20);
    ASSERT_NEAR(execution_times[1], 100, 20);
    ASSERT_NEAR(execution_times[2], 1000, 20);
}


TEST(TimeWheel, ConcurrentCancelAndPoll)
{
    std::atomic_int32_t counter{0};
    cluster::SystemTimer timer(4, 1, 6000);

    std::vector<cluster::TimerTaskEntryPtr> tasks;
    for (int i = 0; i < 1000; i++)
        tasks.push_back(timer.add(20, [&]() { counter++; }));

    std::vector<std::thread> cancel_threads;
    for (int i = 0; i < 4; i++)
    {
        cancel_threads.emplace_back([&, i]() {
            for (size_t j = i; j < tasks.size(); j += 4)
                tasks[j]->cancel();
        });
    }

    for (int i = 0; i < 5; i++)
        timer.poll(20);

    for (auto & t : cancel_threads)
        t.join();

    timer.poll(20);
}

TEST(TimeWheel, CancelAndReinsert)
{
    std::atomic_int32_t counter{};
    cluster::SystemTimer timer(4, 1, 6000);

    auto timer_task = timer.add(20, [&]() { counter++; }, true);
    ASSERT_TRUE(timer_task != nullptr);

    timer_task->cancel();
    ASSERT_TRUE(timer_task->isCancelled());

    timer.poll(20);
    ASSERT_EQ(counter, 0);

    // Try to reinsert cancelled task
    auto reinserted = timer.add(20, [&]() { counter++; }, true);
    ASSERT_TRUE(reinserted != nullptr);
    ASSERT_FALSE(reinserted->isCancelled());

    timer.poll(20);
    /// sleep to yield execution to expired_timer
    std::this_thread::sleep_for(5ms);
    ASSERT_EQ(counter, 1);

    timer.poll(20);
    std::this_thread::sleep_for(5ms);
    ASSERT_EQ(counter, 2);
}

TEST(TimeWheel, TimerServiceShutdownWithActiveRepeatOverflowTimer)
{
    GTEST_FLAG_SET(death_test_style, "threadsafe");

    ASSERT_EXIT(
        [] {
            ::alarm(5);

            cluster::TimerService timer_service(/*worker_pool_size=*/1, /*tick_ms=*/1, /*wheel_size=*/20, /*expired_timers_size=*/1);
            std::atomic_int32_t callback_count{0};

            auto timer_task = timer_service.add(
                /*timeout_ms=*/20,
                [&]() {
                    ++callback_count;
                    sleepForMilliseconds(10);
                },
                /*repeat=*/true);

            if (!timer_task)
                _exit(2);

            for (int i = 0; i < 4000 && callback_count.load() < 3; ++i)
                sleepForMilliseconds(1);

            if (callback_count.load() < 3)
                _exit(3);

            timer_service.shutdown();
            _exit(0);
        }(),
        ::testing::ExitedWithCode(0),
        "");
}
