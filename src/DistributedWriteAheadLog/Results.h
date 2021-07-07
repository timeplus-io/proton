#pragma once

#include "Record.h"

#include <any>

namespace DWAL
{
struct AppendResult
{
    RecordSN sn = -1;

    /// 0 if success, otherwise non-zero
    int32_t err = 0;

    /// Other context
    std::any ctx;
};

struct ConsumeResult
{
    int32_t err = 0;
    RecordPtrs records;
};

using AppendCallback = void (*)(const AppendResult & result, void * data);
using ConsumeCallback = void (*)(RecordPtrs records, void * data);

struct DescribeResult
{
    int32_t err = 0;

    /// Other context
    std::any ctx;
};
}
