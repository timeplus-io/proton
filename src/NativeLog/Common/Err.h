#pragma once

#include "EnumMap.h"

#include <cstdint>
#include <cstring>
#include <iosfwd>

namespace nlog
{
enum class E : std::uint16_t
{
#define ERROR_CODE(id, val, str) id = val,
#include "errors.inc"

    UNKNOWN = 1024,
    MAX
};

using EStatus = E;

struct StatusHasher
{
    size_t operator()(EStatus status) const { return static_cast<size_t>(status); }
};

struct ErrorCodeInfo
{
    const char * name;
    const char * description;

    bool valid() const { return name != nullptr && strcmp(name, "UNKNOWN") != 0; }

    bool operator==(const ErrorCodeInfo & rhs) const { return name == rhs.name && description == rhs.description; }
};

/// All NativeLog functions report failures through this thread-local.

/// extern __thread E err;

using ErrorCodeStringMap = EnumMap<E, ErrorCodeInfo, E::UNKNOWN>;
const ErrorCodeStringMap & errorStrings();

inline const char * errorName(E error)
{
    return errorStrings()[error].name;
}

inline const char * errorDescription(E error)
{
    return errorStrings()[error].description;
}

std::ostream & operator<<(std::ostream &, const E &);
}
