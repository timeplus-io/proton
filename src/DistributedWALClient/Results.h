#pragma once

#include "Record.h"

namespace DWAL
{
struct AppendResult
{
    /// 0 if success, otherwise non-zero
    int32_t err = 0;

    RecordSN sn = -1;

    /// Other context
    int32_t partition = -1;
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

    int32_t partitions;
};
}
