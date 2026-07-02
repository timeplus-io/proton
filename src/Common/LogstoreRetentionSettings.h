#pragma once

#include <base/types.h>

#include <limits>

namespace DB
{

/// Logstore retention semantics across settings / metastore / runtime:
/// - User-facing settings (Int64):
///   - `0`: inherit system defaults
///   - `< 0` (e.g. `-1`): no retention limit (keep all data)
///   - `> 0`: explicit threshold
/// - Metastore/on-wire representation (UInt64):
///   - `0`: inherit system defaults
///   - `UINT64_MAX`: no retention limit (keep all data)
///   - `> 0`: explicit threshold
constexpr inline UInt64 kLogstoreRetentionInheritDefaults = 0;
constexpr inline UInt64 kLogstoreRetentionNoLimit = std::numeric_limits<UInt64>::max();

inline UInt64 encodeLogstoreRetentionForMetastore(Int64 value)
{
    if (value == 0)
        return kLogstoreRetentionInheritDefaults;

    if (value < 0)
        return kLogstoreRetentionNoLimit;

    return static_cast<UInt64>(value);
}

/// Decode metastore retention into the runtime-alter/settings representation:
/// - `0` -> `0` (inherit defaults)
/// - `UINT64_MAX` -> `-1` (no retention limit)
/// - `> 0` -> value (clamped to Int64::MAX if necessary)
inline Int64 decodeLogstoreRetentionForRuntime(UInt64 value)
{
    if (value == kLogstoreRetentionInheritDefaults)
        return 0;

    if (value == kLogstoreRetentionNoLimit)
        return -1;

    constexpr auto int64_max_as_u64 = static_cast<UInt64>(std::numeric_limits<Int64>::max());
    if (value > int64_max_as_u64)
        return std::numeric_limits<Int64>::max();

    return static_cast<Int64>(value);
}

}
