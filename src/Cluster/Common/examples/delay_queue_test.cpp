#include <Cluster/Common/TimeWheel/DelayQueue.h>

#include <chrono>
#include <iostream>
#include <memory>
#include <string>

class DelayedElement
{
public:
    DelayedElement(std::string id_, int64_t delay_ms)
        : id(id_), expire_time(std::chrono::steady_clock::now() + std::chrono::milliseconds(delay_ms))
    {
    }

    DelayedElement(std::string id_, std::chrono::milliseconds delay_ms) : id(id_), expire_time(std::chrono::steady_clock::now() + delay_ms)
    {
    }

    DelayedElement(std::string id_, std::chrono::time_point<std::chrono::steady_clock> delay) : id(id_), expire_time(delay) { }

    /// Return the remaining delay before the element is ready.
    std::chrono::milliseconds getDelay() const
    {
        auto now = std::chrono::steady_clock::now();
        auto delay = expire_time - now;
        if (delay < std::chrono::milliseconds(0))
            return std::chrono::milliseconds(0);
        return std::chrono::duration_cast<std::chrono::milliseconds>(delay);
    }

    bool operator<(const std::shared_ptr<DelayedElement> & other) const { return getDelay().count() - other->getDelay().count(); }

    std::string id;

private:
    std::chrono::time_point<std::chrono::steady_clock> expire_time;
};

int main()
{
#if 0
    cluster::DelayQueue<std::shared_ptr<DelayedElement>> queue;
    auto base = std::chrono::steady_clock::now();

    using namespace std::literals;

    queue.offer(std::make_shared<DelayedElement>("two", 20));
    queue.offer(std::make_shared<DelayedElement>("three", 30ms));
    queue.offer(std::make_shared<DelayedElement>("one", base + 10ms));

    std::cout << queue.poll(00ms).value_or(std::make_shared<DelayedElement>("timeout", 0ms))->id << '\n'; // timeout
    std::cout << queue.poll(15ms).value_or(std::make_shared<DelayedElement>("timeout", 0ms))->id << '\n'; // one
    std::cout << queue.poll(15ms).value_or(std::make_shared<DelayedElement>("timeout", 0ms))->id << '\n'; // two
    std::cout << queue.poll(15ms).value_or(std::make_shared<DelayedElement>("timeout", 0ms))->id << '\n'; // three
#endif
    return 0;
}
