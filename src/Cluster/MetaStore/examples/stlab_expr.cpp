#include <exception>
#include <iostream>
#include <thread>

#include <stlab/concurrency/default_executor.hpp>
#include <stlab/concurrency/future.hpp>

int main()
{
    auto f = stlab::async(stlab::default_executor, [] { return 42; });
    while (!f.get_try())
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    std::cout << "The answer is " << *f.get_try() << "\n";

    auto p = stlab::package<int(int)>(stlab::default_executor, [](int x) { return x + x; });
    p.first(20);
    std::cout << "The new answer is " << *p.second.get_try() << "\n";

    stlab::pre_exit();
}
