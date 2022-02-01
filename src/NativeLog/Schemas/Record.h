#pragma once

#ifdef __clang__
#    pragma clang diagnostic push
#    pragma clang diagnostic ignored "-Wreserved-identifier"
#    pragma clang diagnostic ignored "-Wredundant-parens"
#endif /// __clang__

#include "Record_generated.h"

#ifdef __clang__
#    pragma clang diagnostic pop
#endif /// __clang__

#include <memory>

namespace nlog
{
using RecordBatchPtr = std::shared_ptr<RecordBatch>;

enum class RecordVersion : uint8_t
{
    None = 0,
    V1 = 1,
};

inline RecordVersion toRecordVersion(uint8_t v)
{
    switch (v)
    {
        case 1:
            return RecordVersion::V1;
        default:
            return RecordVersion::None;
    }
}
}
