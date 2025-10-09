#pragma once

#include <Common/Random.h>

#include <cmath>
#include <limits>

namespace cluster
{

/// A utility class for keeping the parameters and providing the value of exponential
/// retry back off, exponential reconnect backoff, exponential timeout etc
/// The formula is
/// backoff(attempts) = random(1 - jitter, 1 + jitter) * initial_interval * multiplier ^ attempts
/// If the max_interval is less than initial_interval, a constant backoff of max_interval will
/// be provided. The jitter will never cause the backoff to exceed max_interval
///
/// It is thread safe
struct ExponentialBackoff
{
    ExponentialBackoff(int64_t initial_interval_, int64_t multiplier_, int64_t max_interval_, double jitter_)
        : multiplier(multiplier_), max_interval(max_interval_), jitter(jitter_)
    {
        initial_interval = max_interval_ < initial_interval_ ? max_interval_ : initial_interval_;

        if (max_interval > initial_interval)
            exp_max = std::log(static_cast<double>(max_interval) / static_cast<double>(std::max<int64_t>(initial_interval, 1)))
                / std::log(multiplier);
        else
            exp_max = 0;
    }

    int64_t backoff(size_t attempts)
    {
        if (exp_max == 0)
            return initial_interval;

        double exp = std::min<double>(attempts, exp_max);
        double term = initial_interval * std::pow(multiplier, exp);
        double random_factor = jitter < std::numeric_limits<double>::denorm_min() ? 1.0 : DB::globalRandomReal(1 - jitter, 1 + jitter);
        int64_t backoff_value = static_cast<int64_t>(random_factor * term);
        return backoff_value > max_interval ? max_interval : backoff_value;
    }

    int64_t initial_interval;
    int64_t multiplier;
    int64_t max_interval;
    double jitter;
    double exp_max;
};
}
