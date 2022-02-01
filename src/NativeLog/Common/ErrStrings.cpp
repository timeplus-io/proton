#include "Err.h"

#include <ostream>

namespace nlog
{
const ErrorCodeStringMap & errorStrings()
{
    static ErrorCodeStringMap instance;
    return instance;
}

template <>
const ErrorCodeInfo & ErrorCodeStringMap::invalidValue()
{
    static const ErrorCodeInfo invalidErrorCodeInfo{"UNKNOWN", "invalid error code"};
    return invalidErrorCodeInfo;
}

template <>
void ErrorCodeStringMap::setValues()
{
#define ERROR_CODE(e, _, d) \
    set(E::e, ErrorCodeInfo{#e, #e ": " d}); \
    static_assert(static_cast<int32_t>(E::e) >= 0, #e); \
    static_assert(static_cast<int32_t>(E::e) < static_cast<int32_t>(E::UNKNOWN), #e);
#include "errors.inc"
}

std::ostream & operator<<(std::ostream & os, const E & e)
{
    return os << errorDescription(e);
}

}
