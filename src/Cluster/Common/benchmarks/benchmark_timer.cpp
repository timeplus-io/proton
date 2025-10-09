#include <Cluster/Common/TimeWheel/SystemTimer.h>

#include <random>
#include <uv.h>
#include <benchmark/benchmark.h>

namespace
{

constexpr size_t kDefaultWorkerPoolSize = 4;
constexpr int64_t kDefaultTickMs = 1;
constexpr uint64_t kDefaultWheelSize = 6000;
constexpr double kMeanTimeout = 512;
constexpr size_t kTimeoutPoolSize = 100'0000;

/// Helper struct to store libuv timer context
struct UvTimerContext
{
    uv_timer_t timer;
    std::function<void()> callback;
};

/// Fixture for combined benchmarks
class TimerBenchmarkFixture : public benchmark::Fixture
{
protected:
    void SetUp(const benchmark::State &) override
    {
        timer = std::make_unique<cluster::SystemTimer>(kDefaultWorkerPoolSize, kDefaultTickMs, kDefaultWheelSize);
        uv_loop = static_cast<uv_loop_t *>(malloc(sizeof(uv_loop_t)));
        uv_loop_init(uv_loop);

        // Pre-generate timeout pools
        generateTimeoutPools();
    }

    void TearDown(const benchmark::State &) override
    {
        timer.reset();
        uv_loop_close(uv_loop);
        free(uv_loop);
    }

private:
    void generateTimeoutPools() noexcept
    {
        std::random_device rd;
        std::mt19937_64 rng(rd());

        // Generate Poisson timeouts
        std::poisson_distribution<uint64_t> poisson_dist(kMeanTimeout);
        poisson_timeouts.reserve(kTimeoutPoolSize);
        for (size_t i = 0; i < kTimeoutPoolSize; ++i)
            poisson_timeouts.push_back(std::max(uint64_t(1), poisson_dist(rng)));
    }

protected:
    uint64_t getPoissonTimeout(size_t i) const noexcept { return poisson_timeouts[i % kTimeoutPoolSize]; }

    std::unique_ptr<cluster::SystemTimer> timer;
    uv_loop_t * uv_loop;
    std::vector<uint64_t> poisson_timeouts;
};

void uv_timer_callback(uv_timer_t * handle)
{
    auto * context = static_cast<UvTimerContext *>(handle->data);
    if (context && context->callback)
    {
        context->callback();
    }
}

} // anonymous namespace


#if 0
/// TimeWheel Benchmarks (original)
BENCHMARK_DEFINE_F(TimerBenchmarkFixture, BM_TimeWheel_AddTimer)(benchmark::State & state)
{
    for (auto _ : state)
    {
        state.PauseTiming();
        std::atomic_int32_t counter{0};
        state.ResumeTiming();

        for (int i = 0; i < state.range(0); ++i)
        {
            auto timer_task = timer->add(127, [&counter] { counter++; });
            if (!timer_task)
            {
                state.SkipWithError("Failed to add timer");
                break;
            }
        }

        state.PauseTiming();
        benchmark::DoNotOptimize(counter.load());
        state.ResumeTiming();
    }
}

/// libuv Equivalent Benchmark
BENCHMARK_DEFINE_F(TimerBenchmarkFixture, BM_Libuv_AddTimer)(benchmark::State & state)
{
    for (auto _ : state)
    {
        state.PauseTiming();
        std::atomic_int32_t counter{0};
        std::vector<std::unique_ptr<UvTimerContext>> contexts;
        contexts.reserve(state.range(0));
        state.ResumeTiming();

        for (int i = 0; i < state.range(0); ++i)
        {
            auto context = std::make_unique<UvTimerContext>();
            uv_timer_init(uv_loop, &context->timer);
            context->callback = [&counter] { counter++; };
            context->timer.data = context.get();

            uv_timer_start(&context->timer, uv_timer_callback, 127, 0);
            contexts.push_back(std::move(context));
        }

        state.PauseTiming();
        benchmark::DoNotOptimize(counter.load());

        /// Cleanup timers
        for (auto & context : contexts)
        {
            uv_timer_stop(&context->timer);
            uv_close(reinterpret_cast<uv_handle_t *>(&context->timer), nullptr);
        }
        uv_run(uv_loop, UV_RUN_NOWAIT);
        state.ResumeTiming();
    }
}
#endif

/// TimeWheel Benchmarks with Poisson distribution
BENCHMARK_DEFINE_F(TimerBenchmarkFixture, BM_TimeWheel_AddTimer_Poisson)(benchmark::State & state)
{
    for (auto _ : state)
    {
        state.PauseTiming();
        std::atomic_int32_t counter{0};
        state.ResumeTiming();

        for (int i = 0; i < state.range(0); ++i)
        {
            auto timer_task = timer->add(getPoissonTimeout(i), [&counter] { counter++; });
            if (!timer_task)
            {
                state.SkipWithError("Failed to add timer");
                break;
            }
        }

        state.PauseTiming();
        benchmark::DoNotOptimize(counter.load());
        state.ResumeTiming();
    }
}

/// libuv Equivalent Benchmark
BENCHMARK_DEFINE_F(TimerBenchmarkFixture, BM_Libuv_AddTimer_Poisson)(benchmark::State & state)
{
    for (auto _ : state)
    {
        state.PauseTiming();
        std::atomic_int32_t counter{0};
        std::vector<std::unique_ptr<UvTimerContext>> contexts;
        contexts.reserve(state.range(0));
        state.ResumeTiming();

        for (int i = 0; i < state.range(0); ++i)
        {
            auto context = std::make_unique<UvTimerContext>();
            uv_timer_init(uv_loop, &context->timer);
            context->callback = [&counter] { counter++; };
            context->timer.data = context.get();

            uv_timer_start(&context->timer, uv_timer_callback, getPoissonTimeout(i), 0);
            contexts.push_back(std::move(context));
        }

        state.PauseTiming();
        benchmark::DoNotOptimize(counter.load());

        /// Cleanup timers
        for (auto & context : contexts)
        {
            uv_timer_stop(&context->timer);
            uv_close(reinterpret_cast<uv_handle_t *>(&context->timer), nullptr);
        }
        uv_run(uv_loop, UV_RUN_NOWAIT);
        state.ResumeTiming();
    }
}

/// TimeWheel Cancel Benchmark (original)
BENCHMARK_DEFINE_F(TimerBenchmarkFixture, BM_TimeWheel_CancelTimer)(benchmark::State & state)
{
    for (auto _ : state)
    {
        state.PauseTiming();
        std::atomic_int32_t counter{0};
        std::vector<cluster::TimerTaskEntryPtr> ids;
        ids.reserve(state.range(0));

        for (int i = 0; i < state.range(0); ++i)
        {
            ids.emplace_back(timer->add(getPoissonTimeout(i), [&counter] { counter++; }));
        }
        state.ResumeTiming();

        for (const auto & id : ids)
        {
            id->cancel();
        }

        state.PauseTiming();
        benchmark::DoNotOptimize(counter.load());
        state.ResumeTiming();
    }
}

/// libuv Cancel Benchmark
BENCHMARK_DEFINE_F(TimerBenchmarkFixture, BM_Libuv_CancelTimer)(benchmark::State & state)
{
    for (auto _ : state)
    {
        state.PauseTiming();
        std::atomic_int32_t counter{0};
        std::vector<std::unique_ptr<UvTimerContext>> contexts;
        contexts.reserve(state.range(0));

        for (int i = 0; i < state.range(0); ++i)
        {
            auto context = std::make_unique<UvTimerContext>();
            uv_timer_init(uv_loop, &context->timer);
            context->callback = [&counter] { counter++; };
            context->timer.data = context.get();
            uv_timer_start(&context->timer, uv_timer_callback, getPoissonTimeout(i), 0);
            contexts.push_back(std::move(context));
        }
        state.ResumeTiming();

        for (auto & context : contexts)
        {
            uv_timer_stop(&context->timer);
        }

        state.PauseTiming();
        benchmark::DoNotOptimize(counter.load());

        /// Cleanup
        for (auto & context : contexts)
        {
            uv_close(reinterpret_cast<uv_handle_t *>(&context->timer), nullptr);
        }
        uv_run(uv_loop, UV_RUN_NOWAIT);
        state.ResumeTiming();
    }
}

/// TimeWheel Callback Timing Benchmark (original)
// Structure to hold timing statistics
struct TimingStats
{
    std::atomic<int64_t> sum_delay{0};
    std::atomic<int64_t> max_delay{0};
    std::atomic<int64_t> min_delay{std::numeric_limits<int64_t>::max()};
    std::atomic<int32_t> counter{0};

    // Helper to update min/max atomically
    void update_min_max(int64_t delay)
    {
        int64_t current_max = max_delay.load();
        while (delay > current_max && !max_delay.compare_exchange_weak(current_max, delay))
        {
        }

        int64_t current_min = min_delay.load();
        while (delay < current_min && !min_delay.compare_exchange_weak(current_min, delay))
        {
        }
    }
};

BENCHMARK_DEFINE_F(TimerBenchmarkFixture, BM_TimeWheel_CallbackTiming)(benchmark::State & state)
{
    for (auto _ : state)
    {
        state.PauseTiming();
        auto stats = std::make_shared<TimingStats>();
        std::vector<std::chrono::system_clock::time_point> scheduled_times;
        scheduled_times.reserve(state.range(0));
        state.ResumeTiming();

        for (int i = 0; i < state.range(0); ++i)
        {
            auto scheduled_time = std::chrono::system_clock::now();
            scheduled_times.push_back(scheduled_time);

            auto timer_task = timer->add(getPoissonTimeout(i), [scheduled_time, stats]() {
                auto now = std::chrono::system_clock::now();
                auto delay = std::chrono::duration_cast<std::chrono::milliseconds>(now - scheduled_time).count();

                stats->sum_delay += delay;
                stats->update_min_max(delay);
                stats->counter++;
            });

            if (!timer_task)
            {
                state.SkipWithError("Failed to add timer");
                break;
            }
        }

        state.PauseTiming();
        // Run enough poll iterations to ensure all callbacks are processed
        constexpr int max_poll_attempts = 200; // Increased from 100 for better reliability
        int poll_count = 0;
        while (stats->counter.load() < state.range(0) && poll_count++ < max_poll_attempts)
        {
            timer->poll(2000);
        }

        if (stats->counter.load() != state.range(0))
        {
            state.SkipWithError("Not all callbacks were called as expected");
        }
        else
        {
            // Report timing statistics
            double avg_delay = static_cast<double>(stats->sum_delay.load()) / stats->counter.load();
            state.counters["Avg_Delay_ms"] = avg_delay;
            state.counters["Max_Delay_ms"] = stats->max_delay.load();
            state.counters["Min_Delay_ms"] = stats->min_delay.load();
            state.counters["Total_Callbacks"] = stats->counter.load();
        }

        benchmark::DoNotOptimize(stats->counter.load());
        benchmark::DoNotOptimize(stats->sum_delay.load());
        state.ResumeTiming();
    }
}

BENCHMARK_DEFINE_F(TimerBenchmarkFixture, BM_Libuv_CallbackTiming)(benchmark::State & state)
{
    for (auto _ : state)
    {
        state.PauseTiming();
        auto stats = std::make_shared<TimingStats>();
        std::vector<std::unique_ptr<UvTimerContext>> contexts;
        contexts.reserve(state.range(0));
        state.ResumeTiming();

        for (int i = 0; i < state.range(0); ++i)
        {
            auto context = std::make_unique<UvTimerContext>();
            auto scheduled_time = std::chrono::system_clock::now();

            context->callback = [scheduled_time, stats]() {
                auto now = std::chrono::system_clock::now();
                auto delay = std::chrono::duration_cast<std::chrono::milliseconds>(now - scheduled_time).count();

                stats->sum_delay += delay;
                stats->update_min_max(delay);
                stats->counter++;
            };

            uv_timer_init(uv_loop, &context->timer);
            context->timer.data = context.get();
            uv_timer_start(&context->timer, uv_timer_callback, getPoissonTimeout(i), 0);
            contexts.push_back(std::move(context));
        }

        state.PauseTiming();
        /// Run the event loop with a timeout to prevent infinite waiting
        uv_run(uv_loop, UV_RUN_DEFAULT);

        if (stats->counter.load() != state.range(0))
        {
            state.SkipWithError("Not all callbacks were called as expected");
        }
        else
        {
            // Report timing statistics
            double avg_delay = static_cast<double>(stats->sum_delay.load()) / stats->counter.load();
            state.counters["Avg_Delay_ms"] = avg_delay;
            state.counters["Max_Delay_ms"] = stats->max_delay.load();
            state.counters["Min_Delay_ms"] = stats->min_delay.load();
            state.counters["Total_Callbacks"] = stats->counter.load();
        }

        benchmark::DoNotOptimize(stats->counter.load());
        benchmark::DoNotOptimize(stats->sum_delay.load());

        for (auto & context : contexts)
            uv_close(reinterpret_cast<uv_handle_t *>(&context->timer), nullptr);
        uv_run(uv_loop, UV_RUN_DEFAULT);
        state.ResumeTiming();
    }
}


/// TimeWheel Multi-threaded Benchmark
BENCHMARK_DEFINE_F(TimerBenchmarkFixture, BM_TimeWheel_ConcurrentAdd)(benchmark::State & state)
{
    const int64_t num_threads = state.range(1);
    const int64_t timers_per_thread = state.range(0) / num_threads;

    for (auto _ : state)
    {
        state.PauseTiming();
        std::atomic_int32_t counter{0};
        std::vector<std::thread> threads;
        threads.reserve(num_threads);
        state.ResumeTiming();

        /// Launch threads
        for (int t = 0; t < num_threads; ++t)
        {
            threads.emplace_back([&]() {
                for (int i = 0; i < timers_per_thread; ++i)
                {
                    auto timer_task = timer->add(getPoissonTimeout(i), [&counter] { counter.fetch_add(1, std::memory_order_relaxed); });
                    if (!timer_task)
                    {
                        state.SkipWithError("Failed to add timer");
                        return;
                    }
                }
            });
        }

        /// Wait for all threads to complete
        for (auto & thread : threads)
            thread.join();

        state.PauseTiming();
        benchmark::DoNotOptimize(counter.load());
        state.ResumeTiming();
    }
}

/// libuv Multi-threaded Benchmark
BENCHMARK_DEFINE_F(TimerBenchmarkFixture, BM_Libuv_ConcurrentAdd)(benchmark::State & state)
{
    const int64_t num_threads = state.range(1);
    const int64_t timers_per_thread = state.range(0) / num_threads;

    for (auto _ : state)
    {
        state.PauseTiming();
        std::atomic_int32_t counter{0};
        std::vector<std::unique_ptr<UvTimerContext>> contexts;
        contexts.reserve(state.range(0));
        std::mutex contexts_mutex; /// Protect access to contexts vector

        /// Create a channel for thread-safe timer requests
        struct TimerRequest
        {
            std::function<void()> callback;
            uint64_t timeout;
        };
        std::queue<TimerRequest> timer_requests;
        std::mutex queue_mutex;
        std::condition_variable queue_cv;
        bool shutdown = false;

        /// Start event loop thread
        std::thread event_loop_thread([&]() {
            while (true)
            {
                TimerRequest req;
                {
                    std::unique_lock<std::mutex> lock(queue_mutex);
                    queue_cv.wait(lock, [&]() { return !timer_requests.empty() || shutdown; });

                    if (shutdown && timer_requests.empty())
                    {
                        break;
                    }

                    if (!timer_requests.empty())
                    {
                        req = std::move(timer_requests.front());
                        timer_requests.pop();
                    }
                }

                /// Create and start timer
                auto context = std::make_unique<UvTimerContext>();
                uv_timer_init(uv_loop, &context->timer);
                context->callback = std::move(req.callback);
                context->timer.data = context.get();
                uv_timer_start(&context->timer, uv_timer_callback, req.timeout, 0);

                {
                    std::lock_guard<std::mutex> lock(contexts_mutex);
                    contexts.push_back(std::move(context));
                }
            }

            /// Don't wait for timer execution, just process the creation
            uv_run(uv_loop, UV_RUN_NOWAIT);
        });

        std::vector<std::thread> threads;
        threads.reserve(num_threads);
        state.ResumeTiming();

        /// Launch worker threads
        for (int t = 0; t < num_threads; ++t)
        {
            threads.emplace_back([&]() {
                for (int i = 0; i < timers_per_thread; ++i)
                {
                    TimerRequest req;
                    req.callback = [&counter] { counter.fetch_add(1, std::memory_order_relaxed); };
                    req.timeout = getPoissonTimeout(i);

                    {
                        std::lock_guard<std::mutex> lock(queue_mutex);
                        timer_requests.push(std::move(req));
                    }
                    queue_cv.notify_one();
                }
            });
        }

        /// Wait for worker threads to complete
        for (auto & thread : threads)
            thread.join();

        state.PauseTiming();

        /// Signal event loop shutdown
        {
            std::lock_guard<std::mutex> lock(queue_mutex);
            shutdown = true;
        }
        queue_cv.notify_one();
        event_loop_thread.join();

        /// Cleanup timers
        {
            std::lock_guard<std::mutex> lock(contexts_mutex);
            for (auto & context : contexts)
            {
                uv_timer_stop(&context->timer);
                uv_close(reinterpret_cast<uv_handle_t *>(&context->timer), nullptr);
            }
        }
        uv_run(uv_loop, UV_RUN_NOWAIT);

        benchmark::DoNotOptimize(counter.load());
        state.ResumeTiming();
    }
}


#if 0
/// Register benchmarks with same parameters for fair comparison
BENCHMARK_REGISTER_F(TimerBenchmarkFixture, BM_TimeWheel_AddTimer)
    ->RangeMultiplier(2)
    ->Range(1024, 16384 * 8)
    ->Unit(benchmark::kMicrosecond)
    ->UseRealTime()
    ->MeasureProcessCPUTime()
    ->DisplayAggregatesOnly(true);

BENCHMARK_REGISTER_F(TimerBenchmarkFixture, BM_Libuv_AddTimer)
    ->RangeMultiplier(2)
    ->Range(1024, 16384 * 8)
    ->Unit(benchmark::kMicrosecond)
    ->UseRealTime()
    ->MeasureProcessCPUTime()
    ->DisplayAggregatesOnly(true);
#endif


BENCHMARK_REGISTER_F(TimerBenchmarkFixture, BM_TimeWheel_AddTimer_Poisson)
    ->RangeMultiplier(16)
    ->Range(8, 100'0000)
    ->Unit(benchmark::kMicrosecond)
    ->UseRealTime()
    ->MeasureProcessCPUTime()
    ->DisplayAggregatesOnly(true);

BENCHMARK_REGISTER_F(TimerBenchmarkFixture, BM_Libuv_AddTimer_Poisson)
    ->RangeMultiplier(16)
    ->Range(8, 100'0000)
    ->Unit(benchmark::kMicrosecond)
    ->UseRealTime()
    ->MeasureProcessCPUTime()
    ->DisplayAggregatesOnly(true);

BENCHMARK_REGISTER_F(TimerBenchmarkFixture, BM_TimeWheel_CancelTimer)
    ->RangeMultiplier(16)
    ->Range(8, 100'0000)
    ->Unit(benchmark::kMicrosecond)
    ->UseRealTime()
    ->MeasureProcessCPUTime()
    ->DisplayAggregatesOnly(true);

BENCHMARK_REGISTER_F(TimerBenchmarkFixture, BM_Libuv_CancelTimer)
    ->RangeMultiplier(16)
    ->Range(8, 100'0000)
    ->Unit(benchmark::kMicrosecond)
    ->UseRealTime()
    ->MeasureProcessCPUTime()
    ->DisplayAggregatesOnly(true);

BENCHMARK_REGISTER_F(TimerBenchmarkFixture, BM_TimeWheel_CallbackTiming)
    ->Arg(1'000'000)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime()
    ->MeasureProcessCPUTime()
    ->DisplayAggregatesOnly(true);

BENCHMARK_REGISTER_F(TimerBenchmarkFixture, BM_Libuv_CallbackTiming)
    ->Arg(1'000'000)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime()
    ->MeasureProcessCPUTime()
    ->DisplayAggregatesOnly(true);

BENCHMARK_REGISTER_F(TimerBenchmarkFixture, BM_TimeWheel_ConcurrentAdd)
    ->RangeMultiplier(16)
    ->Ranges({{8, 100'0000}, {2, 8}}) /// First range is total timers, second is thread count
    ->Unit(benchmark::kMicrosecond)
    ->UseRealTime()
    ->MeasureProcessCPUTime()
    ->DisplayAggregatesOnly(true);

BENCHMARK_REGISTER_F(TimerBenchmarkFixture, BM_Libuv_ConcurrentAdd)
    ->RangeMultiplier(16)
    ->Ranges({{8, 100'0000}, {2, 8}}) /// First range is total timers, second is thread count
    ->Unit(benchmark::kMicrosecond)
    ->UseRealTime()
    ->MeasureProcessCPUTime()
    ->DisplayAggregatesOnly(true);

BENCHMARK_MAIN();
