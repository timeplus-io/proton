#pragma once

#include <Core/Types.h>


namespace DB
{

namespace UUIDHelpers
{
    /// Generate random UUID.
    UUID generateV4();

    constexpr size_t HighBytes = (std::endian::native == std::endian::little) ? 0 : 1;
    constexpr size_t LowBytes = (std::endian::native == std::endian::little) ? 1 : 0;

    inline uint64_t getHighBytes(const UUID & uuid)
    {
        return uuid.toUnderType().items[HighBytes];
    }

    inline uint64_t & getHighBytes(UUID & uuid)
    {
        return uuid.toUnderType().items[HighBytes];
    }

    inline uint64_t getLowBytes(const UUID & uuid)
    {
        return uuid.toUnderType().items[LowBytes];
    }

    inline uint64_t & getLowBytes(UUID & uuid)
    {
        return uuid.toUnderType().items[LowBytes];
    }

    const UUID Nil{};
}

}
