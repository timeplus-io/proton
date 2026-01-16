#pragma once

#include "config.h"

#if USE_V8 && defined(OS_LINUX) && (defined(__x86_64__) || defined(__i386__))

#include <atomic>
#include <cstdint>

#include <cpuid.h>

namespace DB::V8::PkuSupport
{
namespace detail
{
inline constexpr uint32_t pkru_default_value_with_all_nonzero_keys_disabled = 0x55555554u;

inline bool cpuSupportsPku()
{
    static const bool supported = [] {
        unsigned int max_leaf = __get_cpuid_max(/*__level=*/0, /*__sig=*/nullptr);
        if (max_leaf < 7)
            return false;

        unsigned int eax = 0;
        unsigned int ebx = 0;
        unsigned int ecx = 0;
        unsigned int edx = 0;
        __cpuid_count(/*__level=*/7, /*__count=*/0, eax, ebx, ecx, edx);

        constexpr unsigned int pku_bit = 1u << 3u;
        constexpr unsigned int ospke_bit = 1u << 4u;
        return (ecx & pku_bit) && (ecx & ospke_bit);
    }();

    return supported;
}

inline uint32_t readPkru()
{
    uint32_t eax = 0;
    uint32_t edx = 0;
    asm volatile("rdpkru" : "=a"(eax), "=d"(edx) : "c"(0)); // NOLINT
    return eax;
}

inline void writePkru(uint32_t pkru)
{
    uint32_t edx = 0;
    asm volatile("wrpkru" : : "a"(pkru), "c"(0), "d"(edx) : "memory"); // NOLINT
}

inline std::atomic<uint32_t> v8_baseline_pkru{0};
inline std::atomic<uint8_t> v8_baseline_pkru_state{0}; /// 0=uninitialized, 1=capturing, 2=initialized
}

inline void captureBaselinePkruForV8()
{
    if (!detail::cpuSupportsPku())
        return;

    uint8_t expected = 0;
    if (!detail::v8_baseline_pkru_state.compare_exchange_strong(expected, 1, std::memory_order_acq_rel))
        return;

    const uint32_t current = detail::readPkru();
    if (current == detail::pkru_default_value_with_all_nonzero_keys_disabled)
    {
        /// Baseline PKRU must allow V8 to access its pkey-tagged JIT/code pages.
        detail::v8_baseline_pkru_state.store(0, std::memory_order_release);
        return;
    }

    detail::v8_baseline_pkru.store(current, std::memory_order_release);
    detail::v8_baseline_pkru_state.store(2, std::memory_order_release);
}

inline bool isBaselinePkruAvailable()
{
    return detail::v8_baseline_pkru_state.load(std::memory_order_acquire) == 2;
}

inline void ensureThreadPkruForV8()
{
    if (!isBaselinePkruAvailable())
        return;

    if (!detail::cpuSupportsPku())
        return;

    const uint32_t desired = detail::v8_baseline_pkru.load(std::memory_order_relaxed);
    const uint32_t current = detail::readPkru();
    if (current != desired)
        detail::writePkru(desired);
}

class ThreadPkruGuard
{
public:
    ThreadPkruGuard()
    {
        if (!isBaselinePkruAvailable())
            captureBaselinePkruForV8();

        if (!isBaselinePkruAvailable())
            return;

        if (!detail::cpuSupportsPku())
            return;

        previous_pkru = detail::readPkru();
        const uint32_t desired = detail::v8_baseline_pkru.load(std::memory_order_relaxed);
        if (previous_pkru != desired)
        {
            detail::writePkru(desired);
            changed = true;
        }
    }

    ~ThreadPkruGuard()
    {
        if (!changed)
            return;

        detail::writePkru(previous_pkru);
    }

    ThreadPkruGuard(const ThreadPkruGuard &) = delete;
    ThreadPkruGuard & operator=(const ThreadPkruGuard &) = delete;

private:
    uint32_t previous_pkru = detail::pkru_default_value_with_all_nonzero_keys_disabled;
    bool changed = false;
};
}

#else

namespace DB::V8::PkuSupport
{
inline void captureBaselinePkruForV8()
{
}
inline bool isBaselinePkruAvailable()
{
    return false;
}
inline void ensureThreadPkruForV8()
{
}

class ThreadPkruGuard
{
public:
    ThreadPkruGuard() = default;
};
}

#endif
