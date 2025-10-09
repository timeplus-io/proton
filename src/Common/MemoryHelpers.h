#pragma once

#include <base/defines.h>

#include "config.h"

#if USE_JEMALLOC
#    include <jemalloc/jemalloc.h>
#endif

#if !USE_JEMALLOC
#    include <cstdlib>
#endif

#include <memory>

namespace DB
{
using DataUniqPtr = std::unique_ptr<char, decltype(&free)>;

[[maybe_unused]] static ALWAYS_INLINE DataUniqPtr alignedAllocateNoExcept(size_t size, size_t alignment) noexcept
{
    chassert(alignment > 0);
    chassert(!((alignment - 1) & alignment));

#ifdef OS_DARWIN
    /// macOS requires alignment to be at least sizeof(void *)
    alignment = std::max(alignment, sizeof(void *));
#endif

    auto round_up_size = (size + alignment - 1) / alignment * alignment;
    void * ptr = aligned_alloc(alignment, round_up_size);
    return {static_cast<char *>(ptr), &free};
}

[[maybe_unused]] static ALWAYS_INLINE DataUniqPtr alignedAllocate(size_t size_of_data, size_t align_of_data)
{
    auto ptr = alignedAllocateNoExcept(size_of_data, align_of_data);
    if (likely(ptr))
        return ptr;

    throw std::bad_alloc{};
}
}
