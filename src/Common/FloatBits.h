#pragma once

#include <cstdint>

namespace DB
{
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wundefined-reinterpret-cast"

/// \return the IEEE 754 binary representation of f,
/// with the sign bit of f and the result in the same bit position.
/// floatBits(floatFromBits(x)) == x
inline uint32_t floatBits(float f)
{
    return *reinterpret_cast<uint32_t *>(&f);
}

/// floatFromBits returns the floating-point number corresponding
/// to the IEEE 754 binary representation b, with the sign bit of b
/// and the result in the same bit position.
/// floatFromBits(floatBits(x)) == x
inline float floatFromBits(uint32_t b)
{
    return *reinterpret_cast<float *>(&b);
}

/// \return the IEEE 754 binary representation of f,
/// with the sign bit of f and the result in the same bit position,
/// and doubleBits(doubleFromBits(x)) == x.
inline uint64_t doubleBits(double f)
{
    return *reinterpret_cast<uint64_t *>(&f);
}

/// \returns return the floating-point number corresponding
/// to the IEEE 754 binary representation b, with the sign bit of b
/// and the result in the same bit position.
/// doubleFromBits(doubleBits(x)) == x.
inline double doubleFromBits(uint64_t b)
{
    return *reinterpret_cast<double *>(&b);
}

#pragma clang diagnostic pop

}
