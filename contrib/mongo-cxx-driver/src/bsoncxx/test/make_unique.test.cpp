#include <algorithm>
#include <cstdint>
#include <new>

#include <bsoncxx/stdx/make_unique.hpp>
#include <bsoncxx/test/catch.hh>

namespace {

struct something {
    something(int val) : value(val) {}
    int value;
};

TEST_CASE("Create a unique_ptr") {
    auto ptr = bsoncxx::stdx::make_unique<int>(12);
    REQUIRE(ptr);
    CHECK(*ptr == 12);

    auto thing = bsoncxx::stdx::make_unique<something>(5);
    REQUIRE(thing);
    CHECK(thing->value == 5);
}

TEST_CASE("Create a unique_ptr<T[]>") {
    const unsigned length = 12;
    auto ptr = bsoncxx::stdx::make_unique<int[]>(length);
    REQUIRE(ptr);
    // All elements are direct-initialized, which produces '0' for `int`
    CHECK(ptr[0] == 0);
    auto res = std::equal_range(ptr.get(), ptr.get() + length, 0);
    CHECK(res.first == ptr.get());
    CHECK(res.second == (ptr.get() + length));

    ptr = bsoncxx::stdx::make_unique_for_overwrite<int[]>(length);
    std::fill_n(ptr.get(), length, 42);
    CHECK(std::all_of(ptr.get(), ptr.get() + length, [](int n) { return n == 42; }));
}

}  // namespace
