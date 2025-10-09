#include <chrono>
#include <iostream>
#include <thread>

#include <stlab/concurrency/default_executor.hpp>
#include <stlab/concurrency/future.hpp>

void async_func(stlab::packaged_task<int> task)
{
    std::thread runner([task = std::move(task)]() { task(10); });
    runner.join();
}

int main()
{
    auto p = stlab::package<int(int)>(stlab::default_executor, [](int x) { return x + 2; });

    auto f = p.second;
    /// auto f = p.second | [](int x) { std::cout << x << "\n"; };

    async_func(std::move(p.first));

    while (!f.get_try())
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    std::cout << "The answer is " << *f.get_try() << "\n";

    stlab::packaged_task<int> task;
    task(10);

    stlab::pre_exit();

    return 0;
}
