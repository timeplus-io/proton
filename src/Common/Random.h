#pragma once

#include <Common/randomSeed.h>

#include <pcg_random.hpp>

#include <mutex>
#include <random>

namespace DB
{
/// Global random integer generator which generates integer in range [low, high]
/// with uniform distribution
template <typename T>
T globalRandomInt(T low, T high)
{
    static std::mutex rng_mu;
    static pcg64 rng(randomSeed());

    std::scoped_lock lock(rng_mu);

    return std::uniform_int_distribution<T>(low, high)(rng);
}

template <typename T>
T globalRandomReal(T low, T high)
{
    static std::mutex rng_mu;
    static pcg64 rng(randomSeed());

    std::scoped_lock lock(rng_mu);

    return std::uniform_real_distribution<T>(low, high)(rng);
}
}
