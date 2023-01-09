#pragma once

#include <Common/intExp.h>

#include <ctime>

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_CLOCK_GETTIME;
}

inline Field nowSubsecond(UInt32 scale)
{
    static constexpr Int32 fractional_scale = 9;

    timespec spec{};
    if (clock_gettime(CLOCK_REALTIME, &spec))
        throwFromErrno("Cannot clock_gettime.", ErrorCodes::CANNOT_CLOCK_GETTIME);

    DecimalUtils::DecimalComponents<DateTime64> components{spec.tv_sec, spec.tv_nsec};

    // clock_gettime produces subsecond part in nanoseconds, but decimalFromComponents fractional is scale-dependent.
    // Adjust fractional to scale, e.g. for 123456789 nanoseconds:
    //   if scale is  6 (microseconds) => divide by 9 - 6 = 3 to get 123456 microseconds
    //   if scale is 12 (picoseconds)  => multiply by abs(9 - 12) = 3 to get 123456789000 picoseconds
    const auto adjust_scale = fractional_scale - static_cast<Int32>(scale);
    if (adjust_scale < 0)
        components.fractional *= intExp10(std::abs(adjust_scale));
    else if (adjust_scale > 0)
        components.fractional /= intExp10(adjust_scale);

    return DecimalField(DecimalUtils::decimalFromComponents<DateTime64>(components, scale), scale);
}
}
