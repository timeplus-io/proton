#include <benchmark/benchmark.h>

#include <Poco/Mutex.h>

#include <atomic>
#include <list>
#include <memory>
#include <mutex>
#include <thread>


class ComplexSharedResource
{
    [[maybe_unused]] std::list<int> lst;
    [[maybe_unused]] std::list<int>::iterator itor;
    [[maybe_unused]] int valid_num;
};

static void BM_ScopedLockOnly(benchmark::State & state)
{
    std::mutex m;

    auto gen = std::make_shared<ComplexSharedResource>();
    std::shared_ptr<ComplexSharedResource> ptr_1;

    bool has_new_ckpt_request = false;
    std::atomic<bool> done = false;

    std::thread schedule_ckpt([&] {
        using namespace std::chrono_literals;
        for (; !done;)
        {
            std::this_thread::sleep_for(2s);

            {
                std::scoped_lock lock(m);
                has_new_ckpt_request = true;
                gen = std::make_shared<ComplexSharedResource>();
            }
        }
    });

    for (auto _ : state)
    {
        std::scoped_lock lock(m);
        if (has_new_ckpt_request)
        {
            ptr_1.swap(gen);
            has_new_ckpt_request = false;
        }

        benchmark::DoNotOptimize(ptr_1);
    }

    done = true;
    schedule_ckpt.join();
}
BENCHMARK(BM_ScopedLockOnly)->MinTime(3);

static void BM_AtomicWithScopedLockOnStdMutex(benchmark::State & state)
{
    std::mutex m;

    std::atomic<bool> has_new_ckpt_request{false};
    std::atomic<bool> done = false;

    auto gen = std::make_shared<ComplexSharedResource>();
    std::shared_ptr<ComplexSharedResource> ptr_1;

    std::thread schedule_ckpt([&] {
        using namespace std::chrono_literals;
        for (; !done;)
        {
            std::this_thread::sleep_for(2s);

            {
                std::scoped_lock lock(m);
                gen = std::make_shared<ComplexSharedResource>();
            }
            has_new_ckpt_request = true;
        }
    });

    for (auto _ : state)
    {
        if (has_new_ckpt_request)
        {
            {
                std::scoped_lock lock(m);
                ptr_1.swap(gen);
            }
            has_new_ckpt_request = false;
        }

        benchmark::DoNotOptimize(ptr_1);
    }

    done = true;
    schedule_ckpt.join();
}
BENCHMARK(BM_AtomicWithScopedLockOnStdMutex)->MinTime(3);


static void BM_AtomicWithScopedLockOnPocoFastMutex(benchmark::State & state)
{
    Poco::FastMutex m;

    std::atomic<bool> has_new_ckpt_request{false};
    std::atomic<bool> done = false;

    auto gen = std::make_shared<ComplexSharedResource>();
    std::shared_ptr<ComplexSharedResource> ptr_1;

    std::thread schedule_ckpt([&] {
        using namespace std::chrono_literals;
        for (; !done;)
        {
            std::this_thread::sleep_for(2s);

            {
                Poco::FastMutex::ScopedLock lock(m);
                gen = std::make_shared<ComplexSharedResource>();
            }
            has_new_ckpt_request = true;
        }
    });

    for (auto _ : state)
    {
        if (has_new_ckpt_request)
        {
            {
                Poco::FastMutex::ScopedLock lock(m);
                ptr_1.swap(gen);
            }
            has_new_ckpt_request = false;
        }

        benchmark::DoNotOptimize(ptr_1);
    }

    done = true;
    schedule_ckpt.join();
}
BENCHMARK(BM_AtomicWithScopedLockOnPocoFastMutex)->MinTime(3);
